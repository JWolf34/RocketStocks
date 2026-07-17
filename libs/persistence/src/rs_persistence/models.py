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

from sqlalchemy import (
    CHAR,
    REAL,
    BigInteger,
    Boolean,
    Column,
    Date,
    Identity,
    Integer,
    Numeric,
    SmallInteger,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import ENUM, JSONB, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from rs_domain import Dataset, SourceStatus


class Base(DeclarativeBase):
    pass


# options_chain_snapshot (rev-005, S008) is a Core Table, not an ORM class: the
# hypertable has no primary key (append-only; tombstone rows carry NULL contract
# identity), and the ORM refuses PK-less mappings / silently drops NULL-PK rows.
# Numeric(asdecimal=False): reads come back float, matching the rs_domain shapes
# (detection math is float64; NUMERIC storage keeps quotes exact decimals).
chain_snapshot_table = Table(
    "options_chain_snapshot",
    Base.metadata,
    # identity
    Column("underlying", Text, nullable=False),
    Column("option_symbol", Text),
    Column("expiry", Date),
    Column("strike", Numeric(asdecimal=False)),
    Column("put_call", CHAR(1)),
    # provenance — enum vocabularies come from the rs_domain StrEnums (values
    # written/read as plain strings; rev-005 owns type creation)
    Column(
        "dataset",
        ENUM(*(m.value for m in Dataset), name="dataset", create_type=False),
        nullable=False,
    ),
    Column("cycle_id", TIMESTAMP(timezone=True), nullable=False),
    Column("fetched_at", TIMESTAMP(timezone=True), nullable=False),
    Column("quote_time", TIMESTAMP(timezone=True)),
    Column(
        "source_status",
        ENUM(*(m.value for m in SourceStatus), name="source_status", create_type=False),
        nullable=False,
    ),
    # quote
    Column("bid", Numeric(asdecimal=False)),
    Column("ask", Numeric(asdecimal=False)),
    Column("last", Numeric(asdecimal=False)),
    Column("mark", Numeric(asdecimal=False)),
    Column("bid_size", Integer),
    Column("ask_size", Integer),
    Column("last_size", Integer),
    Column("trade_time", TIMESTAMP(timezone=True)),
    # flow
    Column("volume", BigInteger),
    Column("open_interest", BigInteger),
    # risk
    Column("iv", REAL),
    Column("delta", REAL),
    Column("gamma", REAL),
    Column("theta", REAL),
    Column("vega", REAL),
    Column("rho", REAL),
    # chain context
    Column("underlying_price", Numeric(asdecimal=False)),
    Column("multiplier", SmallInteger),
    Column("expiration_type", CHAR(1)),
    Column("non_standard", Boolean),
)


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
