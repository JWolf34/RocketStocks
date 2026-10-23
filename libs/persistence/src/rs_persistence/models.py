"""SQLAlchemy declarative models — the persistence shapes.

These are internal to rs_persistence; repositories convert to/from the
rs_domain Pydantic models so internal fields never leak.

alerts.delivery is the bot's delivery cursor (its sweep takes the rows still pending or
sending); post_attempts caps its retries. A row marked sending keeps the `sending_at` of
its first send, and a posted row the Discord `message_id` and, for an Alert, the
`thread_id` of the thread started from it.

`day` on alerts and candidate is the US Eastern calendar day of the row's instant,
computed by the database so it can never disagree with `triggered_at` / `hit_at`.
"""

from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    Computed,
    Date,
    Double,
    ForeignKey,
    Identity,
    Index,
    Integer,
    SmallInteger,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import ENUM, JSONB, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from rs_domain import (
    AuthRequestState,
    Delivery,
    PopularityOrigin,
    Verdict,
)

EASTERN_DAY_OF = "(({column} AT TIME ZONE 'America/New_York')::date)"


class Base(DeclarativeBase):
    pass


class AlertRow(Base):
    __tablename__ = "alerts"
    __table_args__ = (
        # The bot's delivery cursor: a partial index holding only undelivered rows.
        Index("ix_alerts_undelivered", "id", postgresql_where=text("delivery IN ('pending', 'sending')")),
        # One Alert per stock per Eastern day; replies continue an Alert and are exempt.
        Index("uq_alerts_symbol_day", "symbol", "day", unique=True, postgresql_where=text("reply_to IS NULL")),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    symbol: Mapped[str] = mapped_column(Text)
    trigger: Mapped[str] = mapped_column(Text)
    triggered_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    day: Mapped[date] = mapped_column(
        Date, Computed(EASTERN_DAY_OF.format(column="triggered_at"), persisted=True)
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    posted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    rule_version: Mapped[str | None] = mapped_column(Text)
    alert_price: Mapped[float | None] = mapped_column(Double)
    quote_time: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    reply_to: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("alerts.id"))
    post_attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    delivery: Mapped[str] = mapped_column(
        ENUM(*(m.value for m in Delivery), name="alert_delivery", create_type=False),
        server_default=text("'pending'"),
    )
    sending_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    thread_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)


class CandidateRow(Base):
    """candidate: every Trigger hit with the selector's verdict. Rows are decided
    once and never updated; the unique key makes a rerun of a boundary a no-op."""

    __tablename__ = "candidate"
    __table_args__ = (
        Index("uq_candidate_hit", "trigger", "rule_version", "symbol", "hit_at", unique=True),
        # A Trigger's candidates in a time window.
        Index("ix_candidate_trigger_hit_at", "trigger", "hit_at"),
        Index("ix_candidate_symbol_day", "symbol", "day"),
        # What an Eastern day has announced (the budget used, what each Alert carried).
        Index("ix_candidate_announced_day", "day", postgresql_where=text("verdict IN ('posted', 'reply')")),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    trigger: Mapped[str] = mapped_column(Text)
    rule_version: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    hit_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    day: Mapped[date] = mapped_column(Date, Computed(EASTERN_DAY_OF.format(column="hit_at"), persisted=True))
    inputs: Mapped[dict[str, Any]] = mapped_column(JSONB)
    verdict: Mapped[str] = mapped_column(
        ENUM(*(m.value for m in Verdict), name="candidate_verdict", create_type=False)
    )
    decided_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    alert_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("alerts.id"))
    raw_score: Mapped[float | None] = mapped_column(Double)
    strength: Mapped[float | None] = mapped_column(Double)


class PopularitySnapshotRow(Base):
    """popularity_snapshot hypertable; `origin` shares its vocabulary
    with rs_domain.PopularityOrigin."""

    __tablename__ = "popularity_snapshot"
    __table_args__ = (Index("ix_popularity_snapshot_ticker_polled_at", "ticker", "polled_at"),)

    polled_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    ticker: Mapped[str] = mapped_column(Text, primary_key=True)
    rank: Mapped[int] = mapped_column(Integer)
    mentions: Mapped[int] = mapped_column(Integer)
    upvotes: Mapped[int | None] = mapped_column(Integer)
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


class BarRequestRow(Base):
    """The earliest start already requested of the price vendor for a symbol."""

    __tablename__ = "bar_request"

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    earliest_start: Mapped[date] = mapped_column(Date)


class UnpricedSymbolRow(Base):
    __tablename__ = "unpriced_symbol"

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    first_seen: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    last_attempt: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    reason: Mapped[str] = mapped_column(Text)


class OutcomeRow(Base):
    """outcome: what one case did at one horizon. Derived from daily bars and rewritten
    whole for a Trigger version by the nightly job."""

    __tablename__ = "outcome"

    trigger: Mapped[str] = mapped_column(Text, primary_key=True)
    rule_version: Mapped[str] = mapped_column(Text, primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    day: Mapped[date] = mapped_column(Date, primary_key=True)
    horizon: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    case_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    hit_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    live: Mapped[bool] = mapped_column(Boolean)
    entry_session: Mapped[date] = mapped_column(Date)
    exit_session: Mapped[date] = mapped_column(Date)
    stock_return: Mapped[float] = mapped_column(Double)
    benchmark_return: Mapped[float] = mapped_column(Double)
    excess_return: Mapped[float] = mapped_column(Double)
    typical_move: Mapped[float | None] = mapped_column(Double)
    move_multiple: Mapped[float | None] = mapped_column(Double)
    ordinary_share: Mapped[float | None] = mapped_column(Double)


class TrackRecordRow(Base):
    """track_record: one row per Trigger version and horizon, rewritten by the nightly job."""

    __tablename__ = "track_record"

    trigger: Mapped[str] = mapped_column(Text, primary_key=True)
    rule_version: Mapped[str] = mapped_column(Text, primary_key=True)
    horizon: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    computed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    cases: Mapped[int] = mapped_column(Integer)
    live_cases: Mapped[int] = mapped_column(Integer)
    outcomes: Mapped[int] = mapped_column(Integer)
    unpriced: Mapped[int] = mapped_column(Integer)
    split_excluded: Mapped[int] = mapped_column(Integer)
    no_move_size: Mapped[int] = mapped_column(Integer)
    share_at_2x: Mapped[float | None] = mapped_column(Double)
    ordinary_share: Mapped[float | None] = mapped_column(Double)
    median_multiple: Mapped[float | None] = mapped_column(Double)
    median_excess: Mapped[float | None] = mapped_column(Double)


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


class SchwabTokenNoticeRow(Base):
    """schwab_token_notice: which expiry thresholds were already DM'd for a token issue."""

    __tablename__ = "schwab_token_notice"

    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    threshold_hours: Mapped[int] = mapped_column(Integer, primary_key=True)
    sent_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()")
    )


class TriggerRoleRow(Base):
    """trigger_role: the Discord role an Alert pings for a Trigger. A plain id, not a key:
    the role lives in Discord and may be deleted there."""

    __tablename__ = "trigger_role"

    trigger: Mapped[str] = mapped_column(Text, primary_key=True)
    role_id: Mapped[int] = mapped_column(BigInteger)


class SchwabTokenRow(Base):
    """Single-row (id = 1, CHECK-enforced) OAuth token store; rev-002."""

    __tablename__ = "schwab_tokens"

    id: Mapped[int] = mapped_column(primary_key=True)
    token_data: Mapped[dict[str, Any]] = mapped_column(JSONB)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()")
    )
