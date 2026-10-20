"""popularity_snapshot reads."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import PopularityOrigin, PopularitySnapshot
from rs_persistence import PopularitySnapshotRow
from rs_query._page import Page, clamp_limit, decode_cursor, page_of, require_utc

DEFAULT_LIMIT = 500
MAX_LIMIT = 2000
_KIND = "popularity_history"


def _to_snapshot(row: PopularitySnapshotRow) -> PopularitySnapshot:
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
        rows, limit, _to_snapshot, lambda r: r.polled_at.astimezone(UTC).isoformat(), _KIND
    )
