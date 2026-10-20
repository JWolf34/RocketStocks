"""The daemon's job registry: the one place a ticket adds a scheduled job.

Add a `cron.register(Job(...))` line to `register_jobs`; the job closes over
what it needs from `Dependencies`. Read `deps.schwab.client` inside the run,
never at registration, so a job picks up the client rebuilt after a re-login.
"""

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import Clock
from rs_ingest.cron import CronTable
from rs_ingest.schwab_session import SchwabSession


@dataclass(frozen=True)
class Dependencies:
    session_factory: async_sessionmaker[AsyncSession]
    clock: Clock
    schwab: SchwabSession


def register_jobs(cron: CronTable, deps: Dependencies) -> None:
    """No jobs yet: the popularity poll, alert writer and surge-bars arrive here."""
