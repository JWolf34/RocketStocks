"""popularity_snapshot.upvotes becomes nullable.

v1's popularity.upvotes column was never NOT NULL, so imported history can
carry gaps. The live ApeWisdom adapter still requires the field.

Revision ID: 0009
Revises: 0008
Create Date: 2026-10-20
"""

from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("popularity_snapshot", "upvotes", nullable=True)


def downgrade() -> None:
    # Fails loudly if imported rows without upvotes exist.
    op.alter_column("popularity_snapshot", "upvotes", nullable=False)
