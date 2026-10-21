"""Delivery state, so a crash between a send and its record leaves evidence.

alerts: `delivery` says where a row is in being posted (pending, sending, posted or
stale); the bot marks a row `sending` before each send and `sending_at` holds when it
first did. `message_id` is the Discord message the row posted as and `thread_id` the
thread started from an Alert's message. The bot's cursor index covers the rows still
to deliver (pending or sending) instead of those with no `posted_at`, so a stale row
leaves it.

Every existing row with a `posted_at` becomes `posted`; its message id was never
stored, so it stays NULL. The downgrade drops the new columns and type; a stale row
comes back as an undelivered one with no `posted_at`.

Revision ID: 0017
Revises: 0016
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ENUM

revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None

DELIVERIES = ("pending", "sending", "posted", "stale")


def upgrade() -> None:
    states = ", ".join(f"'{d}'" for d in DELIVERIES)
    op.execute(f"CREATE TYPE alert_delivery AS ENUM ({states})")
    op.add_column(
        "alerts",
        sa.Column(
            "delivery",
            ENUM(name="alert_delivery", create_type=False),
            nullable=False,
            server_default="pending",
        ),
    )
    op.add_column("alerts", sa.Column("sending_at", sa.TIMESTAMP(timezone=True)))
    op.add_column("alerts", sa.Column("message_id", sa.BigInteger))
    op.add_column("alerts", sa.Column("thread_id", sa.BigInteger))
    op.execute("UPDATE alerts SET delivery = 'posted' WHERE posted_at IS NOT NULL")

    op.drop_index("ix_alerts_unposted", table_name="alerts")
    op.create_index(
        "ix_alerts_undelivered",
        "alerts",
        ["id"],
        postgresql_where=sa.text("delivery IN ('pending', 'sending')"),
    )


def downgrade() -> None:
    op.drop_index("ix_alerts_undelivered", table_name="alerts")
    op.create_index(
        "ix_alerts_unposted", "alerts", ["id"], postgresql_where=sa.text("posted_at IS NULL")
    )
    op.drop_column("alerts", "thread_id")
    op.drop_column("alerts", "message_id")
    op.drop_column("alerts", "sending_at")
    op.drop_column("alerts", "delivery")
    op.execute("DROP TYPE alert_delivery")
