"""universe_snapshot and universe_listing: the daily Universe.

One snapshot row per session date and one listing row per non-test listing of that
date, stored whole each night with no spans: a listing that leaves is absent from
later dates. Plain tables, not hypertables: about 13,000 rows a day is about 3.3
million rows (a few hundred MB) a year, the bulk write is one fresh day that never
touches a compressed chunk, and reads are one date by primary key.

Revision ID: 0023
Revises: 0022
"""

import sqlalchemy as sa
from alembic import op

revision = "0023"
down_revision = "0022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "universe_snapshot",
        sa.Column("snapshot_date", sa.Date, primary_key=True),
        sa.Column("as_of", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("nasdaq_listed", sa.Integer, nullable=False),
        sa.Column("other_listed", sa.Integer, nullable=False),
        sa.Column("options_symbols", sa.Integer),
        sa.Column("cboe_symbols", sa.Integer),
        sa.Column("optionable_carried", sa.Boolean, nullable=False),
    )
    op.create_table(
        "universe_listing",
        sa.Column(
            "snapshot_date",
            sa.Date,
            sa.ForeignKey("universe_snapshot.snapshot_date", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("symbol", sa.Text, primary_key=True),
        sa.Column("security_name", sa.Text, nullable=False),
        sa.Column("exchange", sa.Text, nullable=False),
        sa.Column("etf", sa.Boolean, nullable=False),
        sa.Column("financial_status", sa.Text),
        sa.Column("cqs_symbol", sa.Text),
        sa.Column("nasdaq_symbol", sa.Text),
        sa.Column("optionable", sa.Boolean, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("universe_listing")
    op.drop_table("universe_snapshot")
