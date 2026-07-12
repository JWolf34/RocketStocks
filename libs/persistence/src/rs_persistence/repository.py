"""Minimal repository — only what the S002 round-trip test needs (spec non-goal:
nothing more). Converts between rs_domain.Bar and the StreamBar5m row.

Column names deliberately mirror Bar's field names, so the conversions are
model_dump()-shaped rather than two hand-maintained field lists. Reads
normalize the bucket to UTC before validation (ADR-0004: coerce at the
boundary) — the engine already pins the session timezone, this is the
repository honoring the same contract explicitly.
"""

from datetime import date, datetime, timedelta, timezone

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from rs_domain import Alert, Bar
from rs_persistence.models import AlertRow, OutboxRow, ServiceClientRow, StreamBar5m

_FIELDS = ("symbol", "bucket", "open", "high", "low", "close", "volume")

# Bus-2 wire contract (ADR-0005) — the single published source of truth for
# the channel the bot LISTENs on and the outbox event vocabulary it matches.
# Both are exported from rs_persistence; ingest (writer) and bot (reader) import
# these rather than re-declaring the literals.
NOTIFY_CHANNEL = "alerts"
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
        return None if row is None else self._to_bar(row)

    async def list_for_day(self, symbol: str, day: date) -> list[Bar]:
        """Every bar for a symbol on one UTC calendar day, ordered by bucket. The
        engine pins the session TZ to UTC, so [start, start+1d) is a UTC-day slice
        — the shape compare_bars reconciles against a day of REST candles."""
        start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        rows = (
            await self._session.scalars(
                select(StreamBar5m)
                .where(
                    StreamBar5m.symbol == symbol,
                    StreamBar5m.bucket >= start,
                    StreamBar5m.bucket < start + timedelta(days=1),
                )
                .order_by(StreamBar5m.bucket)
            )
        ).all()
        return [self._to_bar(row) for row in rows]

    async def list_range(self, symbol: str, start: datetime, end: datetime) -> list[Bar]:
        """Bars in the half-open [start, end), oldest-first — the serve read
        (S005). No row cap by design: cap/pagination are a Phase-3
        contract-review input (S005 spec)."""
        rows = (
            await self._session.scalars(
                select(StreamBar5m)
                .where(
                    StreamBar5m.symbol == symbol,
                    StreamBar5m.bucket >= start,
                    StreamBar5m.bucket < end,
                )
                .order_by(StreamBar5m.bucket)
            )
        ).all()
        return [self._to_bar(row) for row in rows]

    @staticmethod
    def _to_bar(row: StreamBar5m) -> Bar:
        fields = {name: getattr(row, name) for name in _FIELDS}
        fields["bucket"] = fields["bucket"].astimezone(timezone.utc)
        return Bar(**fields)


class ServiceClientRepository:
    """API-consumer rows (ADR-0012, rev-003). Speaks key HASHES only — key
    generation and hashing live in rs_serve; this layer never sees a raw key.
    Caller owns the commit (same convention as AlertRepository)."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def provision(self, name: str, api_key_hash: str) -> None:
        self._session.add(ServiceClientRow(name=name, api_key_hash=api_key_hash))
        await self._session.flush()

    async def get_active_by_hash(self, api_key_hash: str) -> ServiceClientRow | None:
        """The auth lookup: unknown and revoked both resolve to None, so the
        middleware's uniform-401 needs no branching (S005: no validity leak)."""
        return await self._session.scalar(
            select(ServiceClientRow).where(
                ServiceClientRow.api_key_hash == api_key_hash,
                ServiceClientRow.revoked_at.is_(None),
            )
        )

    async def revoke(self, name: str) -> bool:
        """Set revoked_at now(); False if no active row matched (unknown name
        or already revoked) so the CLI can report instead of silently no-op."""
        result = await self._session.execute(
            update(ServiceClientRow)
            .where(ServiceClientRow.name == name, ServiceClientRow.revoked_at.is_(None))
            .values(revoked_at=func.now())
        )
        return result.rowcount == 1


class AlertRepository:
    """Writes an alert as one atomic unit — the alerts row, its outbox ledger
    row, and the Bus-2 NOTIFY — all inside the caller's transaction (grilling
    #4/#6). NOTIFY is transactional: a LISTENer sees it only when the caller
    commits, so the alert, its ledger entry, and the wake-up land together or
    not at all. The caller owns the commit (the bar insert is a separate
    transaction — grilling #4).

    Caller MUST commit: `insert` returns a live-looking alert id from the
    flush, but with no subsequent commit the alerts row, the outbox row, AND
    the NOTIFY all silently roll back on session close — a lost alert, not a
    visible error."""

    def __init__(self, session: AsyncSession, notify_channel: str = NOTIFY_CHANNEL) -> None:
        self._session = session
        self._channel = notify_channel

    async def insert(self, alert: Alert) -> int:
        # One JSON-safe dump feeds both JSONB copies: the alerts row and the
        # outbox row store byte-identical payloads through the same serializer,
        # and a payload value the raw json.dumps can't encode (a datetime, a
        # Decimal) is coerced here rather than crashing the alerts insert.
        dumped = alert.model_dump(mode="json")
        row = AlertRow(
            symbol=alert.symbol,
            alert_type=alert.alert_type,
            triggered_at=alert.triggered_at,
            payload=dumped["payload"],
        )
        self._session.add(row)
        await self._session.flush()  # assigns the identity id
        alert_id = row.id
        # The ledger row stands alone (grilling #6): full alert content + id, so
        # a downstream reader never has to join back to alerts.
        self._session.add(
            OutboxRow(
                event_type=OUTBOX_EVENT_TYPE,
                payload={"alert_id": alert_id, **dumped},
            )
        )
        # NOTIFY payload is the id only — a wake-up hint; the row is the truth.
        await self._session.execute(select(func.pg_notify(self._channel, str(alert_id))))
        return alert_id
