"""The daemon's job registry: the one place a ticket adds a scheduled job.

Add a `cron.register(Job(...))` line to `register_jobs`; the job closes over
what it needs from `Dependencies`. Read `deps.schwab.client` inside the run,
never at registration, so a job picks up the client rebuilt after a re-login.
"""

from dataclasses import dataclass
from datetime import time, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import Clock, TradingCalendar
from rs_ingest.base_rate import base_rate_job
from rs_ingest.cron import CronTable, Daily, Job
from rs_ingest.rate_limit import RateLimiter
from rs_ingest.schwab_session import SchwabSession
from rs_ingest.surge_bars import EASTERN, sync_surge_bars
from rs_sources.schwab import SchwabMarketData


@dataclass(frozen=True)
class Dependencies:
    session_factory: async_sessionmaker[AsyncSession]
    clock: Clock
    schwab: SchwabSession
    # One budget for every Schwab job: acquire before each request.
    schwab_limiter: RateLimiter
    calendar: TradingCalendar


def register_jobs(cron: CronTable, deps: Dependencies) -> None:
    """The popularity poll and alert writer arrive here beside surge_bars and the base-rate study."""

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
