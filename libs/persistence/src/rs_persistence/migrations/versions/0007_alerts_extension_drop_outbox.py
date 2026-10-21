"""alerts gains rule_version, alert_price, quote_time, dedup_key and
post_attempts; the outbox table is dropped.

dedup_key is unique but nullable (alerts without a rule carry none;
Postgres allows many NULLs under a unique constraint). The bot's posted_at IS
NULL sweep, woken by NOTIFY, is what makes delivery correct, so the outbox
ledger has no reader and goes away.

Revision ID: 0007
Revises: 0006
Create Date: 2026-10-20
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("alerts", sa.Column("rule_version", sa.Text))
    op.add_column("alerts", sa.Column("alert_price", sa.Double))
    op.add_column("alerts", sa.Column("quote_time", sa.TIMESTAMP(timezone=True)))
    op.add_column("alerts", sa.Column("dedup_key", sa.Text))
    op.add_column(
        "alerts",
        sa.Column("post_attempts", sa.Integer, nullable=False, server_default=sa.text("0")),
    )
    op.create_unique_constraint("uq_alerts_dedup_key", "alerts", ["dedup_key"])
    op.drop_table("outbox")


def downgrade() -> None:
    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("event_type", sa.Text, nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.drop_constraint("uq_alerts_dedup_key", "alerts", type_="unique")
    op.drop_column("alerts", "post_attempts")
    op.drop_column("alerts", "dedup_key")
    op.drop_column("alerts", "quote_time")
    op.drop_column("alerts", "alert_price")
    op.drop_column("alerts", "rule_version")
