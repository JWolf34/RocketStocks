"""bar_request: the earliest start already asked of the price vendor, per symbol.

A symbol whose stored bars begin after the start the job needs is extended
back, unless the vendor was already asked for that start and has nothing
earlier (a listing younger than the lookback). Without this record that
listing would be refetched every night. Symbols fetched before this table
existed have no row, so they are asked once more and then recorded.

Revision ID: 0015
Revises: 0014
"""

import sqlalchemy as sa
from alembic import op

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "bar_request",
        sa.Column("symbol", sa.Text, primary_key=True),
        sa.Column("earliest_start", sa.Date, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("bar_request")
