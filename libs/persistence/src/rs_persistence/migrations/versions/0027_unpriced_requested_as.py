"""unpriced_symbol.requested_as: the spelling Schwab was asked under.

A symbol is left alone for a week only when it was refused under the spelling the next request
would use, so a corrected spelling asks again at once. Every existing row was asked under the
symbol itself, so that is its backfill; a class share such as `BRK.B` is therefore asked again
under `BRK/B` on the next run.

Revision ID: 0027
Revises: 0026
"""

import sqlalchemy as sa
from alembic import op

revision = "0027"
down_revision = "0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("unpriced_symbol", sa.Column("requested_as", sa.Text, nullable=True))
    op.execute("UPDATE unpriced_symbol SET requested_as = symbol")
    op.alter_column("unpriced_symbol", "requested_as", nullable=False)


def downgrade() -> None:
    op.drop_column("unpriced_symbol", "requested_as")
