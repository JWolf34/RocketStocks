"""daily_bar, bar_request and unpriced_symbol: idempotent bar writes, coverage
queries, the earliest start asked of the price vendor, and the record of symbols
it cannot price."""

from collections.abc import Collection, Sequence
from datetime import UTC, date, datetime

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import StoredDailyBar, UnpricedSymbol
from rs_persistence.models import BarRequestRow, DailyBarRow, UnpricedSymbolRow

# 9 bound parameters per row; stay far below the protocol's 65535 limit.
_INSERT_CHUNK = 5000


def daily_bar_from_row(row: DailyBarRow) -> StoredDailyBar:
    return StoredDailyBar(
        symbol=row.symbol,
        session_date=row.session_date,
        open=row.open,
        high=row.high,
        low=row.low,
        close=row.close,
        volume=row.volume,
        fetched_at=row.fetched_at.astimezone(UTC),
        adjusted=row.adjusted,
    )


class DailyBarRepository:
    """Caller owns the commit. Bars are validated at the vendor edge (a session
    on the exchange calendar, no epoch-zero stamp) before they reach here."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_many(self, bars: Sequence[StoredDailyBar]) -> int:
        """Insert bars, skipping any whose (symbol, session_date) already exists
        (first write wins); returns how many were inserted."""
        inserted = 0
        for start in range(0, len(bars), _INSERT_CHUNK):
            chunk = bars[start : start + _INSERT_CHUNK]
            result = await self._session.scalars(
                pg_insert(DailyBarRow)
                .values([b.model_dump(mode="python") for b in chunk])
                .on_conflict_do_nothing()
                .returning(DailyBarRow.symbol)
            )
            inserted += len(result.all())
        return inserted

    async def list_range(self, symbol: str, start: date, end: date) -> list[StoredDailyBar]:
        """One symbol's bars with session_date in [start, end] (both inclusive),
        oldest first."""
        rows = await self._session.scalars(
            select(DailyBarRow)
            .where(
                DailyBarRow.symbol == symbol,
                DailyBarRow.session_date >= start,
                DailyBarRow.session_date <= end,
            )
            .order_by(DailyBarRow.session_date)
        )
        return [daily_bar_from_row(row) for row in rows]

    async def latest_session_dates(self, symbols: Collection[str]) -> dict[str, date]:
        """Newest stored session_date per symbol; symbols with no bars are absent."""
        result = await self._session.execute(
            select(DailyBarRow.symbol, func.max(DailyBarRow.session_date))
            .where(DailyBarRow.symbol.in_(symbols))
            .group_by(DailyBarRow.symbol)
        )
        return {symbol: latest for symbol, latest in result}

    async def session_bounds(self, symbols: Collection[str]) -> dict[str, tuple[date, date]]:
        """Oldest and newest stored session_date per symbol; symbols with no bars are absent."""
        result = await self._session.execute(
            select(DailyBarRow.symbol, func.min(DailyBarRow.session_date), func.max(DailyBarRow.session_date))
            .where(DailyBarRow.symbol.in_(symbols))
            .group_by(DailyBarRow.symbol)
        )
        return {symbol: (first, last) for symbol, first, last in result}

    async def earliest_requested(self, symbols: Collection[str]) -> dict[str, date]:
        """Earliest start already requested of the vendor per symbol; never-requested symbols are absent."""
        result = await self._session.execute(
            select(BarRequestRow.symbol, BarRequestRow.earliest_start).where(BarRequestRow.symbol.in_(symbols))
        )
        return {symbol: start for symbol, start in result}

    async def record_request(self, symbol: str, start: date) -> None:
        """Note that the vendor was asked for history from `start`; only ever moves a symbol's start earlier."""
        stmt = pg_insert(BarRequestRow).values(symbol=symbol, earliest_start=start)
        await self._session.execute(
            stmt.on_conflict_do_update(
                index_elements=[BarRequestRow.symbol],
                set_={"earliest_start": func.least(BarRequestRow.earliest_start, stmt.excluded.earliest_start)},
            )
        )

    async def missing_symbols(self, symbols: Collection[str]) -> list[str]:
        """The given symbols that have no stored bars at all, sorted."""
        have = await self.latest_session_dates(symbols)
        return sorted(set(symbols) - have.keys())

    async def record_unpriced(self, symbol: str, reason: str, now: datetime) -> None:
        """Note a failed pricing attempt: the first call sets first_seen, later
        calls only advance last_attempt and replace the reason."""
        stmt = pg_insert(UnpricedSymbolRow).values(
            symbol=symbol, first_seen=now, last_attempt=now, reason=reason
        )
        await self._session.execute(
            stmt.on_conflict_do_update(
                index_elements=[UnpricedSymbolRow.symbol],
                set_={"last_attempt": stmt.excluded.last_attempt, "reason": stmt.excluded.reason},
            )
        )

    async def list_unpriced(self) -> list[UnpricedSymbol]:
        """Every unpriced symbol, oldest first_seen first."""
        rows = await self._session.scalars(
            select(UnpricedSymbolRow).order_by(UnpricedSymbolRow.first_seen, UnpricedSymbolRow.symbol)
        )
        return [
            UnpricedSymbol(
                symbol=row.symbol,
                first_seen=row.first_seen.astimezone(UTC),
                last_attempt=row.last_attempt.astimezone(UTC),
                reason=row.reason,
            )
            for row in rows
        ]

    async def clear_unpriced(self, symbol: str) -> bool:
        """Forget a symbol once it prices; False if it was not recorded."""
        deleted = await self._session.scalar(
            delete(UnpricedSymbolRow)
            .where(UnpricedSymbolRow.symbol == symbol)
            .returning(UnpricedSymbolRow.symbol)
        )
        return deleted is not None
