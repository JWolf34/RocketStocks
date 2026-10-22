"""Alert log: the insert, the bot's delivery cursor, and retry bookkeeping.

An alert is one row plus a transactional NOTIFY, inside the caller's
transaction. NOTIFY is only a wake-up hint (payload = the alert id); the sweep over
rows still pending or sending is what makes delivery correct. The database allows
one Alert (a row with no `reply_to`) per stock per Eastern day.

Delivery marks only move a row forward, from pending or sending to posted or stale,
and each guard names the states it may leave, so a second mark never overwrites the first.
"""

from datetime import UTC, date, datetime

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import Alert, Delivery
from rs_persistence.models import AlertRow

# The channel the bot LISTENs on; ingest (writer) and bot (reader) import this
# rather than re-declaring the literal.
NOTIFY_CHANNEL = "alerts"

_UNDELIVERED = (Delivery.PENDING, Delivery.SENDING)


class AlertDelivery(BaseModel):
    """Where one Alert stands in delivery: a reply posts into its Alert's thread."""

    model_config = ConfigDict(frozen=True)

    delivery: Delivery
    message_id: int | None = None
    thread_id: int | None = None


class PendingAlert(BaseModel):
    """Delivery envelope for one undelivered alert: the storage id, retry count and
    delivery state ride alongside the domain Alert instead of leaking into it."""

    model_config = ConfigDict(frozen=True)

    id: int
    alert: Alert
    post_attempts: int = 0
    delivery: Delivery = Delivery.PENDING
    sending_at: datetime | None = None


class AlertPost(BaseModel):
    """How one Alert stands: its delivery, when it was triggered, and when it posted (None until it has)."""

    model_config = ConfigDict(frozen=True)

    delivery: Delivery
    triggered_at: datetime
    posted_at: datetime | None = None


def alert_from_row(row: AlertRow) -> Alert:
    return Alert(
        symbol=row.symbol,
        trigger=row.trigger,
        triggered_at=row.triggered_at.astimezone(UTC),
        payload=row.payload,
        rule_version=row.rule_version,
        alert_price=row.alert_price,
        quote_time=None if row.quote_time is None else row.quote_time.astimezone(UTC),
        reply_to=row.reply_to,
    )


class AlertRepository:
    """Caller owns the commit and MUST commit: `insert` returns a live-looking id
    from the flush, but without a commit the row and the NOTIFY both roll back on
    session close - a lost alert, not a visible error."""

    def __init__(self, session: AsyncSession, notify_channel: str = NOTIFY_CHANNEL) -> None:
        self._session = session
        self._channel = notify_channel

    async def insert(self, alert: Alert) -> int:
        """Store the alert and NOTIFY; returns its id. A second Alert for the same
        stock and Eastern day raises IntegrityError and writes and notifies nothing:
        the selector must never ask for it, so it is a bug to surface, not a no-op."""
        # JSON-safe dump: a payload value json.dumps can't encode (a datetime, a
        # Decimal) is coerced here rather than crashing the insert.
        dumped = alert.model_dump(mode="json")
        result = await self._session.execute(
            insert(AlertRow)
            .values(
                symbol=alert.symbol,
                trigger=alert.trigger,
                triggered_at=alert.triggered_at,
                payload=dumped["payload"],
                rule_version=alert.rule_version,
                alert_price=alert.alert_price,
                quote_time=alert.quote_time,
                reply_to=alert.reply_to,
            )
            .returning(AlertRow.id)
        )
        alert_id = result.scalar_one()
        # NOTIFY payload is the id only - a wake-up hint; the row is the truth.
        await self._session.execute(select(func.pg_notify(self._channel, str(alert_id))))
        return alert_id

    async def alert_id_of(self, day: date, symbol: str) -> int | None:
        """The id of the stock's Alert (not a reply) on the Eastern `day`, None when it has none."""
        return await self._session.scalar(
            select(AlertRow.id).where(AlertRow.day == day, AlertRow.reply_to.is_(None), AlertRow.symbol == symbol)
        )

    async def posts_by_day(self, trigger: str, rule_version: str) -> dict[tuple[str, date], AlertPost]:
        """Each stock's Alert (not a reply) of one Trigger version by Eastern day: how its
        delivery stands, when it was triggered and, once posted, when."""
        result = await self._session.execute(
            select(AlertRow.symbol, AlertRow.day, AlertRow.delivery, AlertRow.triggered_at, AlertRow.posted_at).where(
                AlertRow.trigger == trigger,
                AlertRow.rule_version == rule_version,
                AlertRow.reply_to.is_(None),
            )
        )
        return {
            (symbol, day): AlertPost(
                delivery=Delivery(delivery),
                triggered_at=triggered.astimezone(UTC),
                posted_at=None if posted is None else posted.astimezone(UTC),
            )
            for symbol, day, delivery, triggered, posted in result
        }

    async def list_undelivered(self, max_attempts: int | None = None) -> list[PendingAlert]:
        """Every Alert and reply still pending or sending, id order, no LIMIT - the bot's
        sweep drains fully. With `max_attempts`, rows that already failed that many sends
        are excluded (the bot's capped retry); rows are rebuilt as domain Alerts at the
        boundary (UTC-normalized)."""
        query = select(AlertRow).where(AlertRow.delivery.in_(_UNDELIVERED)).order_by(AlertRow.id)
        if max_attempts is not None:
            query = query.where(AlertRow.post_attempts < max_attempts)
        return [
            PendingAlert(
                id=row.id,
                alert=alert_from_row(row),
                post_attempts=row.post_attempts,
                delivery=Delivery(row.delivery),
                sending_at=None if row.sending_at is None else row.sending_at.astimezone(UTC),
            )
            for row in (await self._session.scalars(query)).all()
        ]

    async def delivery_of(self, alert_id: int) -> AlertDelivery | None:
        """Where an Alert stands now, read fresh: a reply's Alert may have posted a moment
        ago in the same sweep. None when there is no such row."""
        row = await self._session.get(AlertRow, alert_id, populate_existing=True)
        if row is None:
            return None
        return AlertDelivery(
            delivery=Delivery(row.delivery), message_id=row.message_id, thread_id=row.thread_id
        )

    async def list_unthreaded(self, posted_since: datetime) -> list[tuple[int, int, str]]:
        """(id, message id, symbol) of each Alert posted at or after `posted_since` whose
        thread was never started, id order. A row posted before messages were stored has
        no message to start one from and is left out."""
        rows = await self._session.execute(
            select(AlertRow.id, AlertRow.message_id, AlertRow.symbol)
            .where(
                AlertRow.delivery == Delivery.POSTED,
                AlertRow.reply_to.is_(None),
                AlertRow.thread_id.is_(None),
                AlertRow.message_id.is_not(None),
                AlertRow.posted_at >= posted_since,
            )
            .order_by(AlertRow.id)
        )
        return [(alert_id, message_id, symbol) for alert_id, message_id, symbol in rows.all()]

    async def mark_sending(self, alert_id: int) -> bool:
        """Mark that a send is about to start, so a crash before its record leaves evidence.
        `sending_at` (DB clock) keeps the first send's time: a later history check looks
        back from it. False when the row is no longer undelivered."""
        stamp = await self._session.scalar(
            update(AlertRow)
            .where(AlertRow.id == alert_id, AlertRow.delivery.in_(_UNDELIVERED))
            .values(delivery=Delivery.SENDING, sending_at=func.coalesce(AlertRow.sending_at, func.now()))
            .returning(AlertRow.sending_at)
        )
        return stamp is not None

    async def mark_posted(self, alert_id: int, message_id: int) -> datetime | None:
        """Record the Discord message and stamp `posted_at` with the DB-side clock (the
        returned stamp feeds the lag log). The undelivered guard makes it idempotent:
        None means the row was already settled (or never existed) and the first record
        stands - the caller warns rather than overwrites."""
        return await self._session.scalar(
            update(AlertRow)
            .where(AlertRow.id == alert_id, AlertRow.delivery.in_(_UNDELIVERED))
            .values(delivery=Delivery.POSTED, posted_at=func.now(), message_id=message_id)
            .returning(AlertRow.posted_at)
        )

    async def mark_stale(self, alert_id: int) -> bool:
        """Record that the row was too old to post and was not. False when it was already
        settled (or never existed)."""
        settled = await self._session.scalar(
            update(AlertRow)
            .where(AlertRow.id == alert_id, AlertRow.delivery.in_(_UNDELIVERED))
            .values(delivery=Delivery.STALE)
            .returning(AlertRow.id)
        )
        return settled is not None

    async def set_thread(self, alert_id: int, thread_id: int) -> None:
        """Store the thread started from a posted Alert's message."""
        await self._session.execute(
            update(AlertRow).where(AlertRow.id == alert_id).values(thread_id=thread_id)
        )

    async def increment_post_attempts(self, alert_id: int) -> int | None:
        """Count one failed post; returns the new total, or None when the alert is
        unknown or already settled (nothing to retry)."""
        return await self._session.scalar(
            update(AlertRow)
            .where(AlertRow.id == alert_id, AlertRow.delivery.in_(_UNDELIVERED))
            .values(post_attempts=AlertRow.post_attempts + 1)
            .returning(AlertRow.post_attempts)
        )
