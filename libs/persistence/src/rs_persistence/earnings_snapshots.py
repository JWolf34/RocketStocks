"""earnings_snapshot: Nasdaq's earnings calendar stored by snapshot date.

A plain table: the nightly snapshot adds a few thousand rows and the backfill about
half a million once, so it stays far below the sizes that need a hypertable. Writes
are `ON CONFLICT DO NOTHING` (first write wins), so a rerun never rewrites a snapshot.
"""

from collections.abc import Sequence
from datetime import UTC, date

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import EarningsOrigin, EarningsTime, StoredEarnings
from rs_persistence.models import EarningsSnapshotRow

# 11 bound parameters per row; stay far below the protocol's 65535 limit.
_INSERT_CHUNK = 2000


def stored_earnings_from_row(row: EarningsSnapshotRow) -> StoredEarnings:
    return StoredEarnings(
        snapshot_date=row.snapshot_date,
        origin=EarningsOrigin(row.origin),
        ticker=row.ticker,
        report_date=row.report_date,
        time_of_day=None if row.time_of_day is None else EarningsTime(row.time_of_day),
        fiscal_quarter=row.fiscal_quarter,
        eps_forecast=row.eps_forecast,
        estimate_count=row.estimate_count,
        eps_actual=row.eps_actual,
        surprise_pct=row.surprise_pct,
        fetched_at=row.fetched_at.astimezone(UTC),
    )


class EarningsSnapshotRepository:
    """Caller owns the commit."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_many(self, rows: Sequence[StoredEarnings]) -> int:
        """Insert rows, skipping any whose key already exists; returns how many were inserted."""
        inserted = 0
        for start in range(0, len(rows), _INSERT_CHUNK):
            chunk = rows[start : start + _INSERT_CHUNK]
            result = await self._session.scalars(
                pg_insert(EarningsSnapshotRow)
                .values([row.model_dump(mode="python") for row in chunk])
                .on_conflict_do_nothing()
                .returning(EarningsSnapshotRow.ticker)
            )
            inserted += len(result.all())
        return inserted

    async def has_snapshot(self, origin: EarningsOrigin, snapshot_date: date) -> bool:
        found = await self._session.scalar(
            select(EarningsSnapshotRow.ticker)
            .where(EarningsSnapshotRow.origin == origin.value, EarningsSnapshotRow.snapshot_date == snapshot_date)
            .limit(1)
        )
        return found is not None

    async def latest_snapshot_date(self, origin: EarningsOrigin) -> date | None:
        return await self._session.scalar(
            select(func.max(EarningsSnapshotRow.snapshot_date)).where(EarningsSnapshotRow.origin == origin.value)
        )

    async def next_report(
        self, origin: EarningsOrigin, snapshot_date: date, ticker: str, on_or_after: date
    ) -> StoredEarnings | None:
        """The ticker's earliest report date on or after `on_or_after` in one snapshot."""
        row = await self._session.scalar(
            select(EarningsSnapshotRow)
            .where(
                EarningsSnapshotRow.origin == origin.value,
                EarningsSnapshotRow.snapshot_date == snapshot_date,
                EarningsSnapshotRow.ticker == ticker,
                EarningsSnapshotRow.report_date >= on_or_after,
            )
            .order_by(EarningsSnapshotRow.report_date)
            .limit(1)
        )
        return None if row is None else stored_earnings_from_row(row)

    async def report_dates(self, origin: EarningsOrigin, start: date, end: date) -> set[date]:
        """Report dates in [start, end] that any snapshot of `origin` holds a row for."""
        dates = await self._session.scalars(
            select(EarningsSnapshotRow.report_date)
            .where(
                EarningsSnapshotRow.origin == origin.value,
                EarningsSnapshotRow.report_date >= start,
                EarningsSnapshotRow.report_date <= end,
            )
            .distinct()
        )
        return set(dates)
