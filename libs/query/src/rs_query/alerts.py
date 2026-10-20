"""alerts reads."""

from datetime import UTC

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import Alert, UtcDatetime
from rs_persistence import AlertRow
from rs_persistence.alerts import alert_from_row
from rs_query._page import Page, clamp_limit, decode_cursor, page_of

DEFAULT_LIMIT = 50
MAX_LIMIT = 200
_KIND = "recent_alerts"


class AlertRecord(BaseModel):
    """A stored alert with its log identity: the domain Alert plus the storage id
    and delivery stamp (None until the bot has posted it)."""

    model_config = ConfigDict(frozen=True)

    id: int
    alert: Alert
    posted_at: UtcDatetime | None = None


def _to_record(row: AlertRow) -> AlertRecord:
    return AlertRecord(
        id=row.id,
        alert=alert_from_row(row),
        posted_at=None if row.posted_at is None else row.posted_at.astimezone(UTC),
    )


def _parse_id(key: object) -> int:
    if not isinstance(key, int) or isinstance(key, bool):
        raise TypeError(f"expected an integer key, got {key!r}")
    return key


async def recent_alerts(
    session: AsyncSession,
    ticker: str | None = None,
    *,
    limit: int = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> Page[AlertRecord]:
    """Alerts newest first (by log order, i.e. id), optionally one ticker's."""
    limit = clamp_limit(limit, MAX_LIMIT)
    query = select(AlertRow).order_by(AlertRow.id.desc()).limit(limit + 1)
    if ticker is not None:
        query = query.where(AlertRow.symbol == ticker)
    if cursor is not None:
        before = decode_cursor(_KIND, cursor, _parse_id)
        query = query.where(AlertRow.id < before)
    rows = list((await session.scalars(query)).all())
    return page_of(rows, limit, _to_record, lambda r: r.id, _KIND)
