"""import_verification: the recorded result of `rs_backfill verify-offset`.

The v1 popularity import refuses to run unless its --zone equals the latest
row here. Rows are append-only; owner-role access only (no service grant).

Revision ID: 0010
Revises: 0009
Create Date: 2026-10-20
"""

import sqlalchemy as sa
from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "import_verification",
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("verified_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("zone", sa.Text, nullable=False),
        sa.Column("overlap_rows", sa.Integer, nullable=False),
        sa.Column("matched_rows", sa.Integer, nullable=False),
        sa.Column("match_rate", sa.Double, nullable=False),
        sa.Column("overlap_start", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("overlap_end", sa.TIMESTAMP(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("import_verification")
