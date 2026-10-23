"""popularity_snapshot: idempotent bulk insert and reads."""

from collections.abc import Collection, Sequence
from datetime import UTC, datetime

from sqlalchemy import func, select
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
        (a re-polled boundary); returns how many were inserted."""
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

    async def earliest_polled_at(self) -> datetime | None:
        """The oldest stored boundary across all tickers, or None when empty."""
        earliest = await self._session.scalar(select(func.min(PopularitySnapshotRow.polled_at)))
        return None if earliest is None else earliest.astimezone(UTC)

    async def list_between(
        self,
        start: datetime,
        end: datetime,
        min_mentions: int | None = None,
    ) -> list[PopularitySnapshot]:
        """Every ticker's snapshots with polled_at in [start, end), ordered by
        (polled_at, ticker), optionally only those with at least `min_mentions`.
        Unbounded in size: for jobs that replay history."""
        query = (
            select(PopularitySnapshotRow)
            .where(PopularitySnapshotRow.polled_at >= start, PopularitySnapshotRow.polled_at < end)
            .order_by(PopularitySnapshotRow.polled_at, PopularitySnapshotRow.ticker)
        )
        if min_mentions is not None:
            query = query.where(PopularitySnapshotRow.mentions >= min_mentions)
        return [snapshot_from_row(row) for row in await self._session.scalars(query)]

    async def readings(
        self, tickers: Collection[str], start: datetime, end: datetime
    ) -> dict[str, dict[datetime, tuple[int, int]]]:
        """Each of `tickers`' (rank, mentions) reading per boundary with polled_at in
        [start, end); a ticker with none is absent."""
        result: dict[str, dict[datetime, tuple[int, int]]] = {}
        rows = await self._session.execute(
            select(
                PopularitySnapshotRow.ticker,
                PopularitySnapshotRow.polled_at,
                PopularitySnapshotRow.rank,
                PopularitySnapshotRow.mentions,
            ).where(
                PopularitySnapshotRow.ticker.in_(tickers),
                PopularitySnapshotRow.polled_at >= start,
                PopularitySnapshotRow.polled_at < end,
            )
        )
        for ticker, polled_at, rank, mentions in rows:
            result.setdefault(ticker, {})[polled_at.astimezone(UTC)] = (rank, mentions)
        return result

    async def bottom_ranks(self, start: datetime, end: datetime) -> dict[datetime, int]:
        """Each polled boundary in [start, end) with the rank one past the last stored
        there: where a stock absent from that listing stands. A boundary with no stored
        snapshot is not polled and is not listed."""
        rows = await self._session.execute(
            select(PopularitySnapshotRow.polled_at, func.max(PopularitySnapshotRow.rank) + 1)
            .where(PopularitySnapshotRow.polled_at >= start, PopularitySnapshotRow.polled_at < end)
            .group_by(PopularitySnapshotRow.polled_at)
        )
        return {polled_at.astimezone(UTC): bottom for polled_at, bottom in rows}

    async def count_polls(self, start: datetime, end: datetime) -> int:
        """How many distinct boundaries have any snapshot with polled_at in [start, end)."""
        return (
            await self._session.scalar(
                select(func.count(func.distinct(PopularitySnapshotRow.polled_at))).where(
                    PopularitySnapshotRow.polled_at >= start, PopularitySnapshotRow.polled_at < end
                )
            )
            or 0
        )
