"""rev-003: partial index on the bot's delivery cursor.

alerts.posted_at IS NULL is the bot's consumer cursor; the sweep's
`SELECT ... WHERE posted_at IS NULL ORDER BY id` hits this index. Partial, so
it holds only undelivered rows - near-empty in steady state.

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-12
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_alerts_unposted",
        "alerts",
        ["id"],
        postgresql_where="posted_at IS NULL",
    )


def downgrade() -> None:
    op.drop_index("ix_alerts_unposted", table_name="alerts")
