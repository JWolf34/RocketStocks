"""popularity_snapshot: idempotent bulk insert and history reads."""

from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import PopularityOrigin, PopularitySnapshot
from rs_persistence.models import PopularitySnapshotRow

# 9 bound parameters per row; stay far below the protocol's 65535 limit.
_INSERT_CHUNK = 5000


def snapshot_from_row(row: PopularitySnapshotRow) -> PopularitySnapshot:
    return PopularitySnapshot(
        polled_at=row.polled_at.astimezone(UTC),
        ticker=row.ticker,
        rank=row.rank,
        mentions=row.mentions,
        upvotes=row.upvotes,
        rank_24h_ago=row.rank_24h_ago,
        mentions_24h_ago=row.mentions_24h_ago,
        fetched_at=row.fetched_at.astimezone(UTC),
        origin=PopularityOrigin(row.origin),
    )


class PopularityRepository:
    """Caller owns the commit."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_many(self, snapshots: Sequence[PopularitySnapshot]) -> int:
        """Insert snapshots, skipping any whose (polled_at, ticker) already exists
        (a re-polled boundary, a re-run import); returns how many were inserted."""
        inserted = 0
        for start in range(0, len(snapshots), _INSERT_CHUNK):
            chunk = snapshots[start : start + _INSERT_CHUNK]
            result = await self._session.scalars(
                pg_insert(PopularitySnapshotRow)
                .values([s.model_dump(mode="python") for s in chunk])
                .on_conflict_do_nothing()
                .returning(PopularitySnapshotRow.ticker)
            )
            inserted += len(result.all())
        return inserted

    async def list_for_ticker(
        self, ticker: str, start: datetime, end: datetime
    ) -> list[PopularitySnapshot]:
        """One ticker's snapshots with polled_at in [start, end), oldest first."""
        rows = await self._session.scalars(
            select(PopularitySnapshotRow)
            .where(
                PopularitySnapshotRow.ticker == ticker,
                PopularitySnapshotRow.polled_at >= start,
                PopularitySnapshotRow.polled_at < end,
            )
            .order_by(PopularitySnapshotRow.polled_at)
        )
        return [snapshot_from_row(row) for row in rows]

    async def list_between(
        self, start: datetime, end: datetime, origin: PopularityOrigin | None = None
    ) -> list[PopularitySnapshot]:
        """Every ticker's snapshots with polled_at in [start, end), ordered by
        (polled_at, ticker), optionally only one origin. Unbounded in size: for
        jobs that replay history; interactive reads go through rs_query."""
        query = (
            select(PopularitySnapshotRow)
            .where(PopularitySnapshotRow.polled_at >= start, PopularitySnapshotRow.polled_at < end)
            .order_by(PopularitySnapshotRow.polled_at, PopularitySnapshotRow.ticker)
        )
        if origin is not None:
            query = query.where(PopularitySnapshotRow.origin == origin.value)
        return [snapshot_from_row(row) for row in await self._session.scalars(query)]
