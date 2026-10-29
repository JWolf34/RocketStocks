"""The daemon's job registry: the one place a ticket adds a scheduled job.

Add a `cron.register(Job(...))` line to `register_jobs`; the job closes over
what it needs from `Dependencies`. Read `deps.schwab.client` inside the run,
never at registration, so a job picks up the client rebuilt after a re-login.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import time, timedelta

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import EASTERN, Clock, TradingCalendar, eastern_day
from rs_ingest.alert_writer import AlertWriter
from rs_ingest.cron import CronTable, Daily, Every, Job
from rs_ingest.daily_bars import STOP_AT, DailyBarsError, stop_time, sync_daily_bars
from rs_ingest.monitored import (
    MonitoredEarningsCalendar,
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
from rs_ingest.track_record import track_record_job
from rs_ingest.universe import EVENING_STARTS_AT, UniverseSnapshotter
from rs_observability import HealthTracker, QuietFailure, Source, SweepTally
from rs_sources import ApeWisdomClient
from rs_sources.earnings import SCAN_WEEKDAYS, EarningsLookup
from rs_sources.edgar import EdgarClient
from rs_sources.nasdaq import EarningsWarmError, NasdaqEarningsCalendar
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
    # Central clock time the daily bars pass stops at, before the morning pass starts.
    daily_bars_stop_at: time = STOP_AT


def register_jobs(cron: CronTable, deps: Dependencies) -> None:
    async def daily_bars() -> None:
        tally = SweepTally()
        try:
            await sync_daily_bars(
                session_factory=deps.session_factory,
                source=TalliedHistory(SchwabMarketData(deps.schwab.client, deps.calendar), tally),
                limiter=deps.schwab_limiter,
                calendar=deps.calendar,
                clock=deps.clock,
                stop_at=stop_time(deps.clock.now(), deps.daily_bars_stop_at),
            )
        except SchwabAuthError:
            raise  # the daemon pauses Schwab jobs on this
        except Exception as exc:
            await finish_sweep(deps.health, Source.SCHWAB, tally, exc)
            if isinstance(exc, DailyBarsError):
                # A few stocks failed: the next run retries them, and a stock never pings.
                raise QuietFailure(str(exc)) from exc
            raise
        await finish_sweep(deps.health, Source.SCHWAB, tally)

    cron.register(
        Job(
            "daily_bars",
            # 90 minutes after the close; weekends and holidays find nothing due.
            Daily(time(17, 30), EASTERN),
            daily_bars,
            needs_schwab=True,
            run_on_start=True,
            # A night makes one call per Universe listing at the shared 100 a minute, about 2 hours
            # for 13,000 and more when chains share the budget. The pass stops itself at 03:00
            # Central, 10.5 hours after the slot, and a stopped run resumes per symbol; the
            # timeout is a backstop just past that stop.
            timeout=timedelta(hours=11),
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
    # One calendar for the warm job and the poll's lookups, so they share a cache.
    nasdaq = NasdaqEarningsCalendar(deps.http, deps.clock)

    async def warm_earnings_calendar() -> None:
        try:
            fetched = await nasdaq.warm(eastern_day(deps.clock.now()), SCAN_WEEKDAYS)
        except EarningsWarmError as exc:
            await finish_sweep(
                deps.health, Source.NASDAQ, SweepTally(attempted=exc.attempted, failed=exc.failed), exc
            )
            raise QuietFailure(str(exc)) from exc
        if fetched:  # a window that was still fresh made no request, so it says nothing
            await deps.health.report_ok(Source.NASDAQ)

    cron.register(
        Job(
            "earnings_calendar_warm",
            # Under the 6h cache TTL, so the window is refreshed before it lapses.
            Every(120),
            warm_earnings_calendar,
            run_on_start=True,
            # 30 days at 0.25s spacing plus latency; a hung endpoint must not hold the slot.
            timeout=timedelta(minutes=10),
        )
    )
    poll = _popularity_poll(deps, nasdaq)
    # Runs at startup too: a restart mid-boundary finishes that boundary's alerts.
    cron.register(
        Job("popularity_poll", Every(POLL_MINUTES), poll.run, run_on_start=True, timeout=timedelta(minutes=20))
    )


def _popularity_poll(deps: Dependencies, nasdaq: NasdaqEarningsCalendar) -> PopularityPoll:
    edgar = EdgarClient(deps.http, deps.edgar_user_agent, deps.clock)
    earnings = EarningsLookup(
        MonitoredEarningsCalendar(nasdaq, deps.health), MonitoredFilings(edgar, deps.health)
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
