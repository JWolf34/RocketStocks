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

from rs_domain import Clock, TradingCalendar
from rs_ingest.alert_writer import AlertWriter
from rs_ingest.base_rate import base_rate_job
from rs_ingest.cron import CronTable, Daily, Every, Job
from rs_ingest.popularity_poll import POLL_MINUTES, PopularityPoll
from rs_ingest.rate_limit import RateLimiter
from rs_ingest.schwab_session import SchwabSession
from rs_ingest.surge_bars import EASTERN, sync_surge_bars
from rs_ingest.surge_facts import SurgeFactsGatherer
from rs_sources import ApeWisdomClient
from rs_sources.earnings import SCAN_WEEKDAYS, EarningsLookup
from rs_sources.edgar import EdgarClient
from rs_sources.nasdaq import NasdaqEarningsCalendar
from rs_sources.schwab import SchwabMarketData


@dataclass(frozen=True)
class Dependencies:
    session_factory: async_sessionmaker[AsyncSession]
    clock: Clock
    schwab: SchwabSession
    # One budget for every Schwab job: acquire before each request.
    schwab_limiter: RateLimiter
    calendar: TradingCalendar
    http: httpx.AsyncClient
    edgar_user_agent: str
    # Pauses the Schwab jobs; for a job that is not itself needs_schwab but met a dead token.
    trip_token_gate: Callable[[str], Awaitable[None]]


def register_jobs(cron: CronTable, deps: Dependencies) -> None:
    async def surge_bars() -> None:
        await sync_surge_bars(
            session_factory=deps.session_factory,
            source=SchwabMarketData(deps.schwab.client, deps.calendar),
            limiter=deps.schwab_limiter,
            calendar=deps.calendar,
            clock=deps.clock,
        )

    cron.register(
        Job(
            "surge_bars",
            # 90 minutes after the close; weekends and holidays find nothing due.
            Daily(time(17, 30), EASTERN),
            surge_bars,
            needs_schwab=True,
            run_on_start=True,
            # A first run after the history import can walk thousands of symbols.
            timeout=timedelta(hours=3),
        )
    )
    cron.register(base_rate_job())
    # One calendar for the warm job and the poll's lookups, so they share a cache.
    nasdaq = NasdaqEarningsCalendar(deps.http, deps.clock)

    async def warm_earnings_calendar() -> None:
        await nasdaq.warm(deps.clock.now().astimezone(EASTERN).date(), SCAN_WEEKDAYS)

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
    earnings = EarningsLookup(nasdaq, edgar)
    return PopularityPoll(
        source=ApeWisdomClient(deps.http),
        session_factory=deps.session_factory,
        clock=deps.clock,
        writer=AlertWriter(deps.session_factory),
        gatherer=lambda: SurgeFactsGatherer(
            # Read the client per lookup: a re-login rebuilds it.
            quotes=lambda: SchwabMarketData(deps.schwab.client, deps.calendar),
            limiter=deps.schwab_limiter,
            filings=edgar,
            earnings=earnings,
            session_factory=deps.session_factory,
            calendar=deps.calendar,
            on_token_dead=deps.trip_token_gate,
        ),
    )
