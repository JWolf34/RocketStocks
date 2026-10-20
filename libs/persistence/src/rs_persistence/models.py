"""SQLAlchemy declarative models — the persistence shapes (ADR-0004).

These are internal to rs_persistence; repositories convert to/from the
rs_domain Pydantic models so internal fields never leak (ADR-0004).

stream_bars_5m is the detection log (ADR-0021): immutable, never reconciled
against official REST data, hypertable (created via raw DDL in rev-001).
Prices are DOUBLE PRECISION — bit-exact float64 round-trip for replay
(S002 decision); NUMERIC is reserved for future accounting-grade tables.

alerts.posted_at is the bot's delivery cursor (its `posted_at IS NULL` sweep);
post_attempts caps its retries.
"""

from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    CHAR,
    REAL,
    BigInteger,
    Boolean,
    Column,
    Date,
    Double,
    Identity,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ENUM, JSONB, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from rs_domain import (
    AuthRequestState,
    BaseRateInput,
    Dataset,
    PopularityOrigin,
    SourceStatus,
)


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
    __table_args__ = (
        # The bot's delivery cursor: a partial index holding only undelivered rows.
        Index("ix_alerts_unposted", "id", postgresql_where=text("posted_at IS NULL")),
        # Idempotent writes: a NULL key (streaming alerts) never conflicts.
        UniqueConstraint("dedup_key", name="uq_alerts_dedup_key"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    symbol: Mapped[str] = mapped_column(Text)
    alert_type: Mapped[str] = mapped_column(Text)
    triggered_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    posted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    rule_version: Mapped[str | None] = mapped_column(Text)
    alert_price: Mapped[float | None] = mapped_column(Double)
    quote_time: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    dedup_key: Mapped[str | None] = mapped_column(Text)
    post_attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))


class PopularitySnapshotRow(Base):
    """popularity_snapshot hypertable; `origin` shares its vocabulary
    with rs_domain.PopularityOrigin."""

    __tablename__ = "popularity_snapshot"
    __table_args__ = (Index("ix_popularity_snapshot_ticker_polled_at", "ticker", "polled_at"),)

    polled_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    ticker: Mapped[str] = mapped_column(Text, primary_key=True)
    rank: Mapped[int] = mapped_column(Integer)
    mentions: Mapped[int] = mapped_column(Integer)
    upvotes: Mapped[int] = mapped_column(Integer)
    rank_24h_ago: Mapped[int | None] = mapped_column(Integer)
    mentions_24h_ago: Mapped[int | None] = mapped_column(Integer)
    fetched_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    origin: Mapped[str] = mapped_column(
        ENUM(*(m.value for m in PopularityOrigin), name="popularity_origin", create_type=False)
    )


class DailyBarRow(Base):
    """daily_bar hypertable, partitioned by session_date."""

    __tablename__ = "daily_bar"

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    session_date: Mapped[date] = mapped_column(Date, primary_key=True)
    open: Mapped[float] = mapped_column(Double)
    high: Mapped[float] = mapped_column(Double)
    low: Mapped[float] = mapped_column(Double)
    close: Mapped[float] = mapped_column(Double)
    volume: Mapped[int] = mapped_column(BigInteger)
    fetched_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    adjusted: Mapped[bool] = mapped_column(Boolean)


class UnpricedSymbolRow(Base):
    __tablename__ = "unpriced_symbol"

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    first_seen: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    last_attempt: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    reason: Mapped[str] = mapped_column(Text)


class BaseRateRow(Base):
    """base_rate: append-only, one row per study run."""

    __tablename__ = "base_rate"
    __table_args__ = (
        Index("ix_base_rate_lookup", "alert_type", "rule_version", "input", "computed_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    alert_type: Mapped[str] = mapped_column(Text)
    rule_version: Mapped[str] = mapped_column(Text)
    computed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    input: Mapped[str] = mapped_column(
        ENUM(*(m.value for m in BaseRateInput), name="base_rate_input", create_type=False)
    )
    n: Mapped[int] = mapped_column(Integer)
    n_unpriced: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    hit_rate_1d: Mapped[float | None] = mapped_column(Double)
    hit_rate_5d: Mapped[float | None] = mapped_column(Double)
    hit_rate_10d: Mapped[float | None] = mapped_column(Double)
    median_excess_1d: Mapped[float | None] = mapped_column(Double)
    median_excess_5d: Mapped[float | None] = mapped_column(Double)
    median_excess_10d: Mapped[float | None] = mapped_column(Double)
    survivorship_caveat: Mapped[str] = mapped_column(Text)


class SchwabAuthRequestRow(Base):
    """schwab_auth_request: the `/setup schwab` handshake. The callback
    URL carries an OAuth code - treat it as a secret and never log it."""

    __tablename__ = "schwab_auth_request"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()")
    )
    requested_by: Mapped[int] = mapped_column(BigInteger)
    state: Mapped[str] = mapped_column(
        ENUM(*(m.value for m in AuthRequestState), name="auth_request_state", create_type=False),
        server_default=text("'requested'"),
    )
    auth_url: Mapped[str | None] = mapped_column(Text)
    callback_url: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    completed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))


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
