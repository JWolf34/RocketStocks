"""rev-001: stream_bars_5m (detection-log hypertable), alerts, outbox.

Hypertable creation is raw DDL — Alembic can't infer it.
chunk_time_interval stays at Timescale's default.
outbox is append-only, no processed-marking.

Revision ID: 0001
Revises:
Create Date: 2026-07-05
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

    op.create_table(
        "stream_bars_5m",
        sa.Column("symbol", sa.Text, nullable=False),
        sa.Column("bucket", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("open", sa.Double, nullable=False),
        sa.Column("high", sa.Double, nullable=False),
        sa.Column("low", sa.Double, nullable=False),
        sa.Column("close", sa.Double, nullable=False),
        sa.Column("volume", sa.BigInteger, nullable=False),
        sa.PrimaryKeyConstraint("symbol", "bucket"),
    )
    op.execute("SELECT create_hypertable('stream_bars_5m', 'bucket')")

    op.create_table(
        "alerts",
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("symbol", sa.Text, nullable=False),
        sa.Column("alert_type", sa.Text, nullable=False),
        sa.Column("triggered_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("posted_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )

    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("event_type", sa.Text, nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
    )


def downgrade() -> None:
    op.drop_table("outbox")
    op.drop_table("alerts")
    op.drop_table("stream_bars_5m")
