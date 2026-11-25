"""The daemon's job registry: the one place a ticket adds a scheduled job.

Add a `cron.register(Job(...))` line to `register_jobs`; the job closes over
what it needs from `Dependencies`. Read `deps.schwab.client` inside the run,
never at registration, so a job picks up the client rebuilt after a re-login.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import time, timedelta

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import EASTERN, Clock, TradingCalendar
from rs_ingest.alert_writer import AlertWriter
from rs_ingest.chains import (
    MORNING_STARTS_AT,
    sweep_evening_chains,
    sweep_morning_open_interest,
)
from rs_ingest.cron import CronTable, Daily, Every, Job
from rs_ingest.daily_bars import STOP_AT, DailyBarsError, stop_time, sync_daily_bars
from rs_ingest.earnings_snapshot import (
    SNAPSHOT_AT,
    EarningsSnapshotError,
    RetryingDays,
    StoredEarningsCalendar,
    snapshot_earnings,
)
from rs_ingest.evening import evening_start, run_evening
from rs_ingest.monitored import (
    MonitoredFilings,
    MonitoredPopularitySource,
    MonitoredQuotes,
    TalliedHistory,
    finish_sweep,
)
from rs_ingest.popularity_poll import POLL_MINUTES, PopularityPoll
from rs_ingest.rate_limit import RateLimiter
from rs_ingest.schwab_session import SchwabSession
from rs_ingest.scoring import PopularityScorer
from rs_ingest.selector import BAR_TARGET_PER_DAY
from rs_ingest.surge_facts import SurgeFactsGatherer
from rs_ingest.track_record import run_outcomes as run_outcomes_subprocess
from rs_ingest.universe import (
    EVENING_STARTS_AT,
    MAX_WAIT,
    UniverseSnapshotter,
    last_evening_session,
)
from rs_observability import (
    HealthTracker,
    QuietFailure,
    Source,
    SourceFailed,
    SweepTally,
)
from rs_sources import ApeWisdomClient
from rs_sources.earnings import EarningsLookup
from rs_sources.edgar import EdgarClient
from rs_sources.nasdaq import NasdaqEarningsCalendar
from rs_sources.schwab import TOKEN_ERRORS, SchwabMarketData, SchwabOptionChains
from rs_sources.symbol_directory import SymbolDirectory

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Dependencies:
    session_factory: async_sessionmaker[AsyncSession]
    # What the subprocess jobs connect with: they build their own pool.
    db_url: str
    clock: Clock
    schwab: SchwabSession
    # One budget for every Schwab job: acquire before each request.
    schwab_limiter: RateLimiter
    calendar: TradingCalendar
    http: httpx.AsyncClient
    edgar_user_agent: str
    # Pauses the Schwab jobs; for a job that is not itself needs_schwab but met a dead token.
    trip_token_gate: Callable[[str], Awaitable[None]]
    # Where each source's failures and recoveries become Ops pings.
    health: HealthTracker
    # Central clock time after the evening starts that the daily bars pass stops at, before the
    # morning pass starts at 07:00 ET.
    daily_bars_stop_at: time = STOP_AT
    # The Outcomes and Track record run, given the database URL; tests replace it.
    run_outcomes: Callable[[str], Awaitable[None]] = run_outcomes_subprocess
    # The pause between a failed Nasdaq day and its retry; tests replace it.
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep


def register_jobs(cron: CronTable, deps: Dependencies) -> None:
    async def evening_sweep() -> None:
        # Each pass is judged against its own underlyings or requests, so a Tombstone-heavy
        # chain pass is not diluted by the bars; both are judged once, at the end, so a pass that
        # failed and the next that went well do not ping a failure and a recovery minutes apart.
        chain_tally = SweepTally(unit="chains")
        bar_tally = SweepTally(unit="bar requests")
        session = last_evening_session(deps.calendar, deps.clock.now())
        stop_at = stop_time(evening_start(session), deps.daily_bars_stop_at)

        async def chains() -> None:
            await sweep_evening_chains(
                session_factory=deps.session_factory,
                source=SchwabOptionChains(deps.schwab.client),
                limiter=deps.schwab_limiter,
                calendar=deps.calendar,
                clock=deps.clock,
                tally=chain_tally,
            )

        async def bars() -> None:
            if deps.clock.now() >= stop_at:
                # A restart after the stop: the pass would only replay the Popularity Trigger to
                # find it has no time left, so leave the bars to the next evening.
                logger.info("daily_bars: the stop time %s has passed, resuming the next evening", stop_at)
                return
            try:
                await sync_daily_bars(
                    session_factory=deps.session_factory,
                    source=TalliedHistory(SchwabMarketData(deps.schwab.client, deps.calendar), bar_tally),
                    limiter=deps.schwab_limiter,
                    calendar=deps.calendar,
                    clock=deps.clock,
                    stop_at=stop_at,
                )
            except DailyBarsError as exc:
                # A few stocks failed: the next run retries them, and a stock never pings.
                raise QuietFailure(str(exc)) from exc

        async def judge(error: Exception | None) -> None:
            await finish_sweep(deps.health, Source.SCHWAB, chain_tally, bar_tally, error=error)

        await run_evening(
            [
                ("chains", chains),
                ("daily bars", bars),
                ("outcomes", lambda: deps.run_outcomes(deps.db_url)),
            ],
            judge,
        )

    cron.register(
        Job(
            "evening_sweep",
            # The chains start when the evening does and are owed until 19:00 ET; the bars start
            # when the chains finish and the Outcomes when the bars do. Weekends and holidays find
            # their last session's passes recorded, or write the Tombstones of a night missed.
            Daily(EVENING_STARTS_AT, EASTERN),
            evening_sweep,
            needs_schwab=True,
            # A restart during the evening resumes each pass from what it recorded.
            run_on_start=True,
            # The chains end by 19:00 ET and the bars stop by 04:00 ET at the default (3 a.m.
            # Central), 10 hours after the slot; the Outcomes need up to 2 more. The timeout is a
            # backstop just past that, and the morning pass is its own job.
            timeout=timedelta(hours=13),
        )
    )

    async def morning_open_interest() -> None:
        tally = SweepTally(unit="chains")
        try:
            await sweep_morning_open_interest(
                session_factory=deps.session_factory,
                source=SchwabOptionChains(deps.schwab.client),
                limiter=deps.schwab_limiter,
                calendar=deps.calendar,
                clock=deps.clock,
                tally=tally,
            )
        except TOKEN_ERRORS:
            raise  # the daemon pauses Schwab jobs on a dead or missing token
        except Exception as exc:
            await finish_sweep(deps.health, Source.SCHWAB, tally, error=exc)
            raise
        await finish_sweep(deps.health, Source.SCHWAB, tally)

    cron.register(
        Job(
            "morning_open_interest",
            # Once open interest has settled; every morning is a slot, and a weekend or holiday
            # finds the last trading morning's pass recorded.
            Daily(MORNING_STARTS_AT, EASTERN),
            morning_open_interest,
            needs_schwab=True,
            # A restart inside the window resumes the pass; one after the open writes the
            # Tombstones of an unrecorded morning, which is how a missed morning is recorded.
            run_on_start=True,
            # About 5,500 calls at the shared 120 a minute fit the 2.5 hours to the open; past
            # the open the pass makes no call.
            timeout=timedelta(hours=3),
        )
    )
    universe = UniverseSnapshotter(
        deps.session_factory, SymbolDirectory(deps.http), deps.clock, deps.calendar, deps.health
    )

    async def universe_snapshot() -> None:
        use = await universe.take()
        if not use.fresh:
            # The source's own Ops ping (or the superseded-file log line) already covers it, so the job
            # failure stays quiet; the run still counts as failed.
            raise QuietFailure(f"the Universe of {use.session} is not available; {use.night_uses}")

    cron.register(
        Job(
            "universe_snapshot",
            # Once the listing files are stamped after the close (about 18:01 ET); weekends and
            # holidays find the last session's snapshot stored and do nothing.
            Daily(EVENING_STARTS_AT, EASTERN),
            universe_snapshot,
            # A restart during the evening still takes the night's snapshot.
            run_on_start=True,
            timeout=MAX_WAIT + timedelta(minutes=30),
        )
    )

    async def earnings_snapshot() -> None:
        try:
            report = await snapshot_earnings(
                session_factory=deps.session_factory,
                source=RetryingDays(NasdaqEarningsCalendar(deps.http, deps.clock), sleep=deps.sleep),
                calendar=deps.calendar,
                clock=deps.clock,
            )
        except EarningsSnapshotError as exc:
            # The snapshot is whole or nothing, so a night that stores nothing is Nasdaq failing:
            # a failure that repeats every night must reach Ops, not only the logs.
            await deps.health.report_failed(Source.NASDAQ, exc.reason)
            raise SourceFailed(Source.NASDAQ) from exc
        if report is not None:  # an evening already snapshotted says nothing about Nasdaq
            await deps.health.report_ok(Source.NASDAQ)

    cron.register(
        Job(
            "earnings_snapshot",
            # After the close and the after-hours reports; weekends and holidays find the last
            # trading evening's snapshot stored and do nothing.
            Daily(SNAPSHOT_AT, EASTERN),
            earnings_snapshot,
            # A restart, even the next morning, takes the latest evening's snapshot if it is missing.
            run_on_start=True,
            # About 70 days at 0.25s spacing plus latency; a day that fails ends the run after its
            # retries, and a hung endpoint must not hold the slot.
            timeout=timedelta(minutes=30),
        )
    )
    poll = _popularity_poll(deps)
    # Runs at startup too: a restart mid-boundary finishes that boundary's alerts.
    cron.register(
        Job("popularity_poll", Every(POLL_MINUTES), poll.run, run_on_start=True, timeout=timedelta(minutes=20))
    )


def _popularity_poll(deps: Dependencies) -> PopularityPoll:
    edgar = EdgarClient(deps.http, deps.edgar_user_agent, deps.clock)
    earnings = EarningsLookup(
        StoredEarningsCalendar(deps.session_factory, deps.calendar), MonitoredFilings(edgar, deps.health)
    )
    return PopularityPoll(
        source=MonitoredPopularitySource(ApeWisdomClient(deps.http), deps.health),
        session_factory=deps.session_factory,
        clock=deps.clock,
        writer=AlertWriter(deps.session_factory),
        gatherer=lambda: SurgeFactsGatherer(
            # Read the client per lookup: a re-login rebuilds it.
            quotes=lambda: MonitoredQuotes(SchwabMarketData(deps.schwab.client, deps.calendar), deps.health),
            limiter=deps.schwab_limiter,
            earnings=earnings,
            on_token_dead=deps.trip_token_gate,
        ),
        scorer=PopularityScorer(deps.session_factory, bar_target=BAR_TARGET_PER_DAY),
    )
