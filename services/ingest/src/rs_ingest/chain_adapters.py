"""Production adapter for ChainScanner's SnapshotStore port (S008 daemon wiring).

SessionSnapshotStore owns transaction scope: one short commit per snapshot, so
each snapshot is durable the moment its train step completes and a later
failure in the same cycle cannot roll it back. The Schwab chain fetch lives in
rs_sources.schwab.
"""

from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import ChainSnapshot, Dataset
from rs_persistence import ChainSnapshotRepository


class SessionSnapshotStore:
    """SnapshotStore port over rs_persistence: session-per-call, commit on
    success (the repository's caller-owns-commit contract)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def insert(self, snapshot: ChainSnapshot) -> int:
        async with self._session_factory() as session:
            async with session.begin():
                return await ChainSnapshotRepository(session).insert(snapshot)

    async def last_accepted_median_quote_times(
        self, dataset: Dataset, now: datetime
    ) -> dict[str, datetime]:
        async with self._session_factory() as session:
            return await ChainSnapshotRepository(session).last_accepted_median_quote_times(
                dataset, now
            )
