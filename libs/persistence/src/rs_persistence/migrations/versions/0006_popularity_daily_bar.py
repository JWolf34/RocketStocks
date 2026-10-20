"""rev-006: popularity_snapshot and daily_bar hypertables, unpriced_symbol.

popularity_snapshot is keyed (polled_at, ticker): re-polling a boundary or
re-importing v1 history is `ON CONFLICT DO NOTHING`. daily_bar is keyed
(symbol, session_date). Both compress by age and carry no retention policy
(forward-only data cannot be rebought). Hypertable DDL is raw - Alembic cannot
infer it.

Revision ID: 0006
Revises: 0005
Create Date: 2026-10-20
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ENUM

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE TYPE popularity_origin AS ENUM ('live', 'v1_import')")

    op.create_table(
        "popularity_snapshot",
        sa.Column("polled_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("ticker", sa.Text, nullable=False),
        sa.Column("rank", sa.Integer, nullable=False),
        sa.Column("mentions", sa.Integer, nullable=False),
        sa.Column("upvotes", sa.Integer, nullable=False),
        sa.Column("rank_24h_ago", sa.Integer),
        sa.Column("mentions_24h_ago", sa.Integer),
        sa.Column("fetched_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("origin", ENUM(name="popularity_origin", create_type=False), nullable=False),
        sa.PrimaryKeyConstraint("polled_at", "ticker"),
    )
    op.execute(
        "SELECT create_hypertable('popularity_snapshot', 'polled_at',"
        " chunk_time_interval => INTERVAL '7 days')"
    )
    # Per-ticker history is the read path; the primary key leads with polled_at.
    op.create_index("ix_popularity_snapshot_ticker_polled_at", "popularity_snapshot", ["ticker", "polled_at"])
    op.execute(
        "ALTER TABLE popularity_snapshot SET (timescaledb.compress,"
        " timescaledb.compress_segmentby = 'ticker',"
        " timescaledb.compress_orderby = 'polled_at DESC')"
    )
    op.execute("SELECT add_compression_policy('popularity_snapshot', INTERVAL '14 days')")

    op.create_table(
        "daily_bar",
        sa.Column("symbol", sa.Text, nullable=False),
        sa.Column("session_date", sa.Date, nullable=False),
        sa.Column("open", sa.Double, nullable=False),
        sa.Column("high", sa.Double, nullable=False),
        sa.Column("low", sa.Double, nullable=False),
        sa.Column("close", sa.Double, nullable=False),
        sa.Column("volume", sa.BigInteger, nullable=False),
        sa.Column("fetched_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("adjusted", sa.Boolean, nullable=False),
        sa.PrimaryKeyConstraint("symbol", "session_date"),
    )
    op.execute(
        "SELECT create_hypertable('daily_bar', 'session_date',"
        " chunk_time_interval => INTERVAL '90 days')"
    )
    op.execute(
        "ALTER TABLE daily_bar SET (timescaledb.compress,"
        " timescaledb.compress_segmentby = 'symbol',"
        " timescaledb.compress_orderby = 'session_date DESC')"
    )
    op.execute("SELECT add_compression_policy('daily_bar', INTERVAL '180 days')")

    op.create_table(
        "unpriced_symbol",
        sa.Column("symbol", sa.Text, primary_key=True),
        sa.Column("first_seen", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("last_attempt", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("reason", sa.Text, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("unpriced_symbol")
    op.drop_table("daily_bar")
    op.drop_table("popularity_snapshot")
    op.execute("DROP TYPE popularity_origin")
