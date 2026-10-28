"""universe_snapshot and universe_listing: the daily Universe, stored whole by session date."""

from datetime import UTC, date

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from rs_domain import UniverseCounts, UniverseHeader, UniverseListing, UniverseSnapshot
from rs_persistence.models import UniverseListingRow, UniverseSnapshotRow

# 9 bound parameters per row; stay far below the protocol's 65535 limit.
_INSERT_CHUNK = 5000


def _header_from_row(row: UniverseSnapshotRow) -> UniverseHeader:
    return UniverseHeader(
        snapshot_date=row.snapshot_date,
        as_of=row.as_of.astimezone(UTC),
        counts=UniverseCounts(
            nasdaq_listed=row.nasdaq_listed,
            other_listed=row.other_listed,
            options_symbols=row.options_symbols,
            cboe_symbols=row.cboe_symbols,
        ),
        optionable_carried=row.optionable_carried,
    )


class UniverseRepository:
    """Caller owns the commit, so a snapshot's header and listings commit together."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, snapshot: UniverseSnapshot) -> bool:
        """Store the snapshot unless its date already has one; returns whether it was stored.

        The listings go in as a few bulk inserts (the driver caps the parameters of one
        statement) inside the caller's transaction.
        """
        header = snapshot.model_dump(mode="python", exclude={"listings", "counts"}) | snapshot.counts.model_dump()
        stored = await self._session.scalar(
            pg_insert(UniverseSnapshotRow)
            .values(header)
            .on_conflict_do_nothing()
            .returning(UniverseSnapshotRow.snapshot_date)
        )
        if stored is None:
            return False
        for start in range(0, len(snapshot.listings), _INSERT_CHUNK):
            chunk = snapshot.listings[start : start + _INSERT_CHUNK]
            await self._session.execute(
                pg_insert(UniverseListingRow).values(
                    [{"snapshot_date": snapshot.snapshot_date, **listing.model_dump()} for listing in chunk]
                )
            )
        return True

    async def header(self, day: date) -> UniverseHeader | None:
        """The snapshot of `day` without its listings; None when none was taken."""
        row = await self._session.get(UniverseSnapshotRow, day)
        return _header_from_row(row) if row is not None else None

    async def latest_header(self) -> UniverseHeader | None:
        """The newest snapshot without its listings; None before the first."""
        row = await self._session.scalar(
            select(UniverseSnapshotRow).order_by(UniverseSnapshotRow.snapshot_date.desc()).limit(1)
        )
        return _header_from_row(row) if row is not None else None

    async def latest(self) -> UniverseSnapshot | None:
        """The latest Universe: every listing of the newest snapshot with its optionable flag
        and the snapshot's As-of; None before the first."""
        header = await self.latest_header()
        if header is None:
            return None
        rows = await self._session.scalars(
            select(UniverseListingRow)
            .where(UniverseListingRow.snapshot_date == header.snapshot_date)
            .order_by(UniverseListingRow.symbol)
        )
        listings = tuple(
            UniverseListing(
                symbol=row.symbol,
                security_name=row.security_name,
                exchange=row.exchange,
                etf=row.etf,
                financial_status=row.financial_status,
                cqs_symbol=row.cqs_symbol,
                nasdaq_symbol=row.nasdaq_symbol,
                optionable=row.optionable,
            )
            for row in rows
        )
        return UniverseSnapshot(**header.model_dump(), listings=listings)

    async def optionable_symbols(self, day: date) -> set[str]:
        """The symbols flagged optionable in the snapshot of `day`."""
        symbols = await self._session.scalars(
            select(UniverseListingRow.symbol).where(
                UniverseListingRow.snapshot_date == day, UniverseListingRow.optionable
            )
        )
        return set(symbols)

    async def reference_counts(self) -> UniverseCounts | None:
        """What the next night's files are measured against: the listing counts of the newest
        snapshot, and each optionable source's count from the newest snapshot that had a
        usable one. None before the first snapshot."""
        latest = await self.latest_header()
        if latest is None:
            return None
        return UniverseCounts(
            nasdaq_listed=latest.counts.nasdaq_listed,
            other_listed=latest.counts.other_listed,
            options_symbols=await self._latest_count(UniverseSnapshotRow.options_symbols),
            cboe_symbols=await self._latest_count(UniverseSnapshotRow.cboe_symbols),
        )

    async def _latest_count(self, column: InstrumentedAttribute[int | None]) -> int | None:
        return await self._session.scalar(
            select(column).where(column.is_not(None)).order_by(UniverseSnapshotRow.snapshot_date.desc()).limit(1)
        )
