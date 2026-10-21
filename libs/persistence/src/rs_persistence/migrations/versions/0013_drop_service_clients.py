"""Drop service_clients.

The API-key store belonged to the removed REST service; nothing reads or writes it.
The downgrade recreates it empty, exactly as rev-004 defined it.

Revision ID: 0013
Revises: 0012
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("service_clients")


def downgrade() -> None:
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
