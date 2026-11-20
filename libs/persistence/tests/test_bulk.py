"""insert_rows: more rows than one statement may bind, with and without the rows already stored."""

from datetime import UTC, date, datetime, timedelta

from sqlalchemy import func, select

from rs_persistence import DailyBarRow
from rs_persistence._bulk import insert_rows

FETCHED = datetime(2026, 7, 7, 21, 0, tzinfo=UTC)
# Nine columns a row: far more rows than one statement carries.
ROWS = 8_000


def _bars(symbols: range) -> list[dict]:
    return [
        {
            "symbol": f"S{n:05d}",
            "session_date": date(2026, 7, 6) + timedelta(days=n % 3),
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": n,
            "fetched_at": FETCHED,
            "adjusted": True,
        }
        for n in symbols
    ]


async def test_rows_past_one_statement_are_all_inserted_and_counted(session_factory):
    async with session_factory() as s:
        assert await insert_rows(s, DailyBarRow, iter(_bars(range(ROWS)))) == ROWS
        await s.commit()

    async with session_factory() as s:
        assert await s.scalar(select(func.count()).select_from(DailyBarRow)) == ROWS


async def test_skipping_existing_rows_counts_only_the_new_ones(session_factory):
    async with session_factory() as s:
        await insert_rows(s, DailyBarRow, _bars(range(0, ROWS, 2)))
        await s.commit()

    async with session_factory() as s:
        assert await insert_rows(s, DailyBarRow, _bars(range(ROWS)), skip_existing=True) == ROWS // 2
        await s.commit()

    async with session_factory() as s:
        assert await s.scalar(select(func.count()).select_from(DailyBarRow)) == ROWS
