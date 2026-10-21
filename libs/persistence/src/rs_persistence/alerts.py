"""Alert log: idempotent insert, the bot's delivery cursor, and retry bookkeeping.

An alert is one row plus a transactional NOTIFY, inside the caller's
transaction. NOTIFY is only a wake-up hint (payload = the alert id); the
`posted_at IS NULL` sweep is what makes delivery correct.
"""

from collections.abc import Collection
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import Alert
from rs_persistence.models import AlertRow

# The channel the bot LISTENs on; ingest (writer) and bot (reader) import this
# rather than re-declaring the literal.
NOTIFY_CHANNEL = "alerts"


class PendingAlert(BaseModel):
    """Delivery envelope for one undelivered alert: the storage id and retry count
    ride alongside the domain Alert instead of leaking into it."""

    model_config = ConfigDict(frozen=True)

    id: int
    alert: Alert
    post_attempts: int = 0


def alert_from_row(row: AlertRow) -> Alert:
    return Alert(
        symbol=row.symbol,
        alert_type=row.alert_type,
        triggered_at=row.triggered_at.astimezone(UTC),
        payload=row.payload,
        rule_version=row.rule_version,
        alert_price=row.alert_price,
        quote_time=None if row.quote_time is None else row.quote_time.astimezone(UTC),
        dedup_key=row.dedup_key,
    )


class AlertRepository:
    """Caller owns the commit and MUST commit: `insert` returns a live-looking id
    from the flush, but without a commit the row and the NOTIFY both roll back on
    session close - a lost alert, not a visible error."""

    def __init__(self, session: AsyncSession, notify_channel: str = NOTIFY_CHANNEL) -> None:
        self._session = session
        self._channel = notify_channel

    async def insert(self, alert: Alert) -> int | None:
        """Store the alert and NOTIFY; returns its id, or None when an alert with the
        same `dedup_key` already exists (nothing is written or notified then)."""
        # JSON-safe dump: a payload value json.dumps can't encode (a datetime, a
        # Decimal) is coerced here rather than crashing the insert.
        dumped = alert.model_dump(mode="json")
        alert_id = await self._session.scalar(
            pg_insert(AlertRow)
            .values(
                symbol=alert.symbol,
                alert_type=alert.alert_type,
                triggered_at=alert.triggered_at,
                payload=dumped["payload"],
                rule_version=alert.rule_version,
                alert_price=alert.alert_price,
                quote_time=alert.quote_time,
                dedup_key=alert.dedup_key,
            )
            .on_conflict_do_nothing(index_elements=[AlertRow.dedup_key])
            .returning(AlertRow.id)
        )
        if alert_id is None:
            return None
        # NOTIFY payload is the id only - a wake-up hint; the row is the truth.
        await self._session.execute(select(func.pg_notify(self._channel, str(alert_id))))
        return alert_id

    async def list_decisions(
        self,
        symbols: Collection[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Alert]:
        """Alert rows as domain Alerts (the semantic decisions), id order,
        optionally filtered to symbols and a [start, end) triggered_at window.
        Delivery bookkeeping (id, posted_at, post_attempts) never leaves: the
        replay comparator diffs decisions, not delivery state."""
        query = select(AlertRow).order_by(AlertRow.id)
        if symbols is not None:
            query = query.where(AlertRow.symbol.in_(symbols))
        if start is not None:
            query = query.where(AlertRow.triggered_at >= start)
        if end is not None:
            query = query.where(AlertRow.triggered_at < end)
        rows = (await self._session.scalars(query)).all()
        return [alert_from_row(row) for row in rows]

    async def list_unposted(self, max_attempts: int | None = None) -> list[PendingAlert]:
        """Every undelivered alert, id order, no LIMIT - the bot's sweep drains
        fully. With `max_attempts`, alerts that already failed that many posts are
        excluded (the bot's capped retry); rows are rebuilt as domain Alerts at the
        boundary (UTC-normalized)."""
        query = select(AlertRow).where(AlertRow.posted_at.is_(None)).order_by(AlertRow.id)
        if max_attempts is not None:
            query = query.where(AlertRow.post_attempts < max_attempts)
        rows = (await self._session.scalars(query)).all()
        return [
            PendingAlert(id=row.id, alert=alert_from_row(row), post_attempts=row.post_attempts)
            for row in rows
        ]

    async def mark_posted(self, alert_id: int) -> datetime | None:
        """Stamp the delivery cursor with the DB-side clock (rs_bot holds no client
        clock; the returned stamp feeds the lag log). The posted_at IS NULL guard
        makes it idempotent: None means the row was already marked (or never
        existed) and the first stamp stands - the caller warns rather than
        overwrites."""
        return await self._session.scalar(
            update(AlertRow)
            .where(AlertRow.id == alert_id, AlertRow.posted_at.is_(None))
            .values(posted_at=func.now())
            .returning(AlertRow.posted_at)
        )

    async def increment_post_attempts(self, alert_id: int) -> int | None:
        """Count one failed post; returns the new total, or None when the alert is
        unknown or already posted (nothing to retry)."""
        return await self._session.scalar(
            update(AlertRow)
            .where(AlertRow.id == alert_id, AlertRow.posted_at.is_(None))
            .values(post_attempts=AlertRow.post_attempts + 1)
            .returning(AlertRow.post_attempts)
        )
