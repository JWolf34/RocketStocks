"""The daemon's job registry: the one place a ticket adds a scheduled job.

Add a `cron.register(Job(...))` line to `register_jobs`; the job closes over
what it needs from `Dependencies`. Read `deps.schwab.client` inside the run,
never at registration, so a job picks up the client rebuilt after a re-login.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import time, timedelta

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import EASTERN, Clock, TradingCalendar
from rs_ingest.alert_writer import AlertWriter
from rs_ingest.cron import CronTable, Daily, Every, Job
from rs_ingest.earnings_snapshot import (
    SNAPSHOT_AT,
    EarningsSnapshotError,
    RetryingDays,
    StoredEarningsCalendar,
    snapshot_earnings,
)
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
from rs_ingest.surge_bars import SurgeBarsError, sync_surge_bars
from rs_ingest.surge_facts import SurgeFactsGatherer
from rs_ingest.track_record import track_record_job
from rs_ingest.universe import EVENING_STARTS_AT, UniverseSnapshotter
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
from rs_sources.schwab import SchwabAuthError, SchwabMarketData
from rs_sources.symbol_directory import SymbolDirectory


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
    # The pause between a failed Nasdaq day and its retry; tests replace it.
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep


def register_jobs(cron: CronTable, deps: Dependencies) -> None:
    async def surge_bars() -> None:
        tally = SweepTally()
        try:
            await sync_surge_bars(
                session_factory=deps.session_factory,
                source=TalliedHistory(SchwabMarketData(deps.schwab.client, deps.calendar), tally),
                limiter=deps.schwab_limiter,
                calendar=deps.calendar,
                clock=deps.clock,
            )
        except SchwabAuthError:
            raise  # the daemon pauses Schwab jobs on this
        except Exception as exc:
            await finish_sweep(deps.health, Source.SCHWAB, tally, exc)
            if isinstance(exc, SurgeBarsError):
                # A few stocks failed: the next run retries them, and a stock never pings.
                raise QuietFailure(str(exc)) from exc
            raise
        await finish_sweep(deps.health, Source.SCHWAB, tally)

    cron.register(
        Job(
            "surge_bars",
            # 90 minutes after the close; weekends and holidays find nothing due.
            Daily(time(17, 30), EASTERN),
            surge_bars,
            needs_schwab=True,
            run_on_start=True,
            # A first run walks every Popularity-hit symbol a year back, one call each at the
            # shared 100 a minute: 3 hours fits 18,000 calls, and a stopped run resumes per symbol.
            timeout=timedelta(hours=3),
        )
    )
    cron.register(track_record_job(deps.db_url))
    universe = UniverseSnapshotter(
        deps.session_factory, SymbolDirectory(deps.http), deps.clock, deps.calendar, deps.health
    )

    async def universe_snapshot() -> None:
        use = await universe.take()
        if not use.fresh:
            # The Ops ping went out as the snapshot was given up on; the run still counts as failed.
            raise QuietFailure(f"the Universe of {use.session} is not available; the night uses {use.snapshot_date}")

    cron.register(
        Job(
            "universe_snapshot",
            # Once the listing files are stamped after the close (about 18:01 ET); weekends and
            # holidays find the last session's snapshot stored and do nothing.
            Daily(EVENING_STARTS_AT, EASTERN),
            universe_snapshot,
            # A restart during the evening still takes the night's snapshot.
            run_on_start=True,
            # The wait for fresh files ends at 20:00 ET, 115 minutes after the slot; the rest is
            # the downloads of a possibly retried 90 MB file and the write.
            timeout=timedelta(hours=2, minutes=30),
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
        if report is not None:  # a night with nothing due says nothing about Nasdaq
            await deps.health.report_ok(Source.NASDAQ)

    cron.register(
        Job(
            "earnings_snapshot",
            # After the close and the after-hours reports; weekends and holidays find nothing to do.
            Daily(SNAPSHOT_AT, EASTERN),
            earnings_snapshot,
            # A restart after the slot still takes the night's snapshot; before it, nothing is due.
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
