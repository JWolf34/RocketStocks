"""rev-003: service_clients — per-consumer API keys (ADR-0012, S005).

Full ADR-0012 column set now; Phase 1 reads only api_key_hash + revoked_at
(scope/rate-limit enforcement is Phase 3). Rows are operator-provisioned via
`python -m rs_serve.tools.provision`; revocation is an UPDATE of revoked_at.

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-12
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0003"
down_revision = "0002"
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
