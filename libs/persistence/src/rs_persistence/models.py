"""SQLAlchemy declarative models — the persistence shapes (ADR-0004).

These are internal to rs_persistence; repositories convert to/from the
rs_domain Pydantic models so internal fields never leak (ADR-0004).

stream_bars_5m is the detection log (ADR-0021): immutable, never reconciled
against official REST data, hypertable (created via raw DDL in rev-001).
Prices are DOUBLE PRECISION — bit-exact float64 round-trip for replay
(S002 decision); NUMERIC is reserved for future accounting-grade tables.

outbox is an append-only durable event ledger with NO processed-marking —
delivery state belongs to each consumer's cursor (ADR-0005). alerts.posted_at
is the bot's pinned cursor column (ADR-0005 Bus-2 reference contract).
"""

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Identity, Text, text
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class StreamBar5m(Base):
    __tablename__ = "stream_bars_5m"

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    bucket: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    open: Mapped[float]
    high: Mapped[float]
    low: Mapped[float]
    close: Mapped[float]
    volume: Mapped[int] = mapped_column(BigInteger)


class AlertRow(Base):
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    symbol: Mapped[str] = mapped_column(Text)
    alert_type: Mapped[str] = mapped_column(Text)
    triggered_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    posted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class OutboxRow(Base):
    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    event_type: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()")
    )


class ServiceClientRow(Base):
    """One API consumer (ADR-0012); rev-004. Phase 1 reads only api_key_hash +
    revoked_at; scopes and rate-limit columns are enforced from Phase 3."""

    __tablename__ = "service_clients"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    name: Mapped[str] = mapped_column(Text, unique=True)
    api_key_hash: Mapped[str] = mapped_column(Text, unique=True)
    scopes: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    rate_limit_rps: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    rate_limit_burst: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()")
    )
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class SchwabTokenRow(Base):
    """Single-row (id = 1, CHECK-enforced) OAuth token store; rev-002."""

    __tablename__ = "schwab_tokens"

    id: Mapped[int] = mapped_column(primary_key=True)
    token_data: Mapped[dict[str, Any]] = mapped_column(JSONB)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()")
    )
