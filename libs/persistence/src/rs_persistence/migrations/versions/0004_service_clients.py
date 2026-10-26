"""rev-004: service_clients - per-consumer API keys.

Only api_key_hash + revoked_at were read; revocation is an UPDATE of revoked_at.

Revision ID: 0004
Revises: 0003
Create Date: 2026-07-12
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "service_clients",
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("name", sa.Text, nullable=False, unique=True),
        sa.Column("api_key_hash", sa.Text, nullable=False, unique=True),
        sa.Column("scopes", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("rate_limit_rps", sa.BigInteger, nullable=True),
        sa.Column("rate_limit_burst", sa.BigInteger, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("revoked_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("service_clients")
