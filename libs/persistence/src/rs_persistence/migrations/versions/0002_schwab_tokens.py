"""rev-002: schwab_tokens single-row token store.

One row (id = 1), token_data JSONB holding exactly what schwab-py's
token_write_func delivers. `ingest` is the single writer.

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-06
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "schwab_tokens",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("token_data", JSONB, nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("id = 1", name="schwab_tokens_single_row"),
    )


def downgrade() -> None:
    op.drop_table("schwab_tokens")
