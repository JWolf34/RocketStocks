"""Minimal repository — only what the S002 round-trip test needs (spec non-goal:
nothing more). Converts between rs_domain.Bar and the StreamBar5m row.

Column names deliberately mirror Bar's field names, so the conversions are
model_dump()-shaped rather than two hand-maintained field lists. Reads
normalize the bucket to UTC before validation (ADR-0004: coerce at the
boundary) — the engine already pins the session timezone, this is the
repository honoring the same contract explicitly.
"""

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import Alert, Bar
from rs_persistence.models import AlertRow, OutboxRow, StreamBar5m

_FIELDS = ("symbol", "bucket", "open", "high", "low", "close", "volume")

NOTIFY_CHANNEL = "alerts"  # Bus-2 wire contract (ADR-0005); bot LISTENs here
OUTBOX_EVENT_TYPE = "alert_created"  # snake_case ledger vocabulary (grilling #6)


class BarRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert(self, bar: Bar) -> None:
        self._session.add(StreamBar5m(**bar.model_dump()))
        await self._session.flush()

    async def get(self, symbol: str, bucket: datetime) -> Bar | None:
        row = await self._session.scalar(
            select(StreamBar5m).where(StreamBar5m.symbol == symbol, StreamBar5m.bucket == bucket)
        )
        if row is None:
            return None
        fields = {name: getattr(row, name) for name in _FIELDS}
        fields["bucket"] = fields["bucket"].astimezone(timezone.utc)
        return Bar(**fields)


class AlertRepository:
    """Writes an alert as one atomic unit — the alerts row, its outbox ledger
    row, and the Bus-2 NOTIFY — all inside the caller's transaction (grilling
    #4/#6). NOTIFY is transactional: a LISTENer sees it only when the caller
    commits, so the alert, its ledger entry, and the wake-up land together or
    not at all. The caller owns the commit (the bar insert is a separate
    transaction — grilling #4)."""

    def __init__(self, session: AsyncSession, notify_channel: str = NOTIFY_CHANNEL) -> None:
        self._session = session
        self._channel = notify_channel

    async def insert(self, alert: Alert) -> int:
        row = AlertRow(
            symbol=alert.symbol,
            alert_type=alert.alert_type,
            triggered_at=alert.triggered_at,
            payload=alert.payload,
        )
        self._session.add(row)
        await self._session.flush()  # assigns the identity id
        alert_id = row.id
        # The ledger row stands alone (grilling #6): full alert content + id, so
        # a downstream reader never has to join back to alerts.
        self._session.add(
            OutboxRow(
                event_type=OUTBOX_EVENT_TYPE,
                payload={"alert_id": alert_id, **alert.model_dump(mode="json")},
            )
        )
        # NOTIFY payload is the id only — a wake-up hint; the row is the truth.
        await self._session.execute(select(func.pg_notify(self._channel, str(alert_id))))
        return alert_id
