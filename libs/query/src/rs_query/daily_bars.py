"""daily_bar reads."""

from datetime import date

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import StoredDailyBar
from rs_persistence import DailyBarRow
from rs_persistence.daily_bars import daily_bar_from_row
from rs_query._page import Page, clamp_limit, decode_cursor, page_of

DEFAULT_LIMIT = 500
MAX_LIMIT = 1000
_KIND = "daily_bars"


async def daily_bars(
    session: AsyncSession,
    symbol: str,
    start: date,
    end: date,
    *,
    limit: int = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> Page[StoredDailyBar]:
    """One symbol's bars with session_date in [start, end] (both inclusive),
    oldest first."""
    limit = clamp_limit(limit, MAX_LIMIT)
    query = (
        select(DailyBarRow)
        .where(
            DailyBarRow.symbol == symbol,
            DailyBarRow.session_date >= start,
            DailyBarRow.session_date <= end,
        )
        .order_by(DailyBarRow.session_date)
        .limit(limit + 1)
    )
    if cursor is not None:
        after = decode_cursor(_KIND, cursor, date.fromisoformat)
        query = query.where(DailyBarRow.session_date > after)
    rows = list((await session.scalars(query)).all())
    return page_of(rows, limit, daily_bar_from_row, lambda r: r.session_date.isoformat(), _KIND)
