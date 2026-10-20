"""popularity_snapshot reads."""

from datetime import UTC, datetime

from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import PopularitySnapshot
from rs_persistence import PopularitySnapshotRow
from rs_persistence.popularity import snapshot_from_row
from rs_query._page import Page, clamp_limit, decode_cursor, page_of, require_utc

DEFAULT_LIMIT = 500
MAX_LIMIT = 2000
_KIND = "popularity_history"


async def popularity_history(
    session: AsyncSession,
    ticker: str,
    start: datetime,
    end: datetime,
    *,
    limit: int = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> Page[PopularitySnapshot]:
    """One ticker's snapshots with polled_at in [start, end), oldest first.
    `start` and `end` must be timezone-aware."""
    start, end = require_utc(start, "start"), require_utc(end, "end")
    limit = clamp_limit(limit, MAX_LIMIT)
    query = (
        select(PopularitySnapshotRow)
        .where(
            PopularitySnapshotRow.ticker == ticker,
            PopularitySnapshotRow.polled_at >= start,
            PopularitySnapshotRow.polled_at < end,
        )
        .order_by(PopularitySnapshotRow.polled_at)
        .limit(limit + 1)
    )
    if cursor is not None:
        after = decode_cursor(_KIND, cursor, datetime.fromisoformat)
        query = query.where(PopularitySnapshotRow.polled_at > after)
    rows = list((await session.scalars(query)).all())
    return page_of(
        rows, limit, snapshot_from_row, lambda r: r.polled_at.astimezone(UTC).isoformat(), _KIND
    )


BETWEEN_DEFAULT_LIMIT = 1000
BETWEEN_MAX_LIMIT = 5000
_BETWEEN_KIND = "popularity_between"


def _parse_between_key(key: object) -> tuple[datetime, str]:
    if not isinstance(key, list) or len(key) != 2 or not isinstance(key[1], str):
        raise TypeError(f"expected [polled_at, ticker], got {key!r}")
    return datetime.fromisoformat(key[0]), key[1]


async def popularity_between(
    session: AsyncSession,
    start: datetime,
    end: datetime,
    *,
    limit: int = BETWEEN_DEFAULT_LIMIT,
    cursor: str | None = None,
) -> Page[PopularitySnapshot]:
    """Every ticker's snapshots with polled_at in [start, end), ordered by
    (polled_at, ticker): the bulk read for history replays. `start` and `end`
    must be timezone-aware."""
    start, end = require_utc(start, "start"), require_utc(end, "end")
    limit = clamp_limit(limit, BETWEEN_MAX_LIMIT)
    row = PopularitySnapshotRow
    query = (
        select(row)
        .where(row.polled_at >= start, row.polled_at < end)
        .order_by(row.polled_at, row.ticker)
        .limit(limit + 1)
    )
    if cursor is not None:
        after = decode_cursor(_BETWEEN_KIND, cursor, _parse_between_key)
        query = query.where(tuple_(row.polled_at, row.ticker) > after)
    rows = list((await session.scalars(query)).all())
    return page_of(
        rows,
        limit,
        snapshot_from_row,
        lambda r: [r.polled_at.astimezone(UTC).isoformat(), r.ticker],
        _BETWEEN_KIND,
    )
