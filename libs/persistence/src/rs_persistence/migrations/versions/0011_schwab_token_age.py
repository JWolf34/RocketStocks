"""schwab_token_issue view and schwab_token_notice table.

The bot DMs John before the refresh token expires but must never read the token:
the view exposes only when schwab-py first issued it (creation_timestamp survives
access-token refreshes), and runs with the owner's rights. The notice table
records which thresholds were already sent for which issue, so a restart does not
repeat a DM.

Revision ID: 0011
Revises: 0010
Create Date: 2026-10-20
"""

import sqlalchemy as sa
from alembic import op

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE VIEW schwab_token_issue AS"
        " SELECT to_timestamp((token_data ->> 'creation_timestamp')::bigint) AS issued_at"
        " FROM schwab_tokens"
        " WHERE token_data ->> 'creation_timestamp' IS NOT NULL"
    )
    op.create_table(
        "schwab_token_notice",
        sa.Column("issued_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("threshold_hours", sa.Integer, nullable=False),
        sa.Column("sent_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("issued_at", "threshold_hours"),
    )


def downgrade() -> None:
    op.drop_table("schwab_token_notice")
    op.execute("DROP VIEW schwab_token_issue")
