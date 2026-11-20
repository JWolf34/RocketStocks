"""option_open_interest: the morning pass's settled open interest.

One row per contract per session: the open interest Schwab carries once it has settled
(from 07:00 ET), named by the session whose trading it settled, not the date it was fetched.
A hypertable on session_date with no retention policy and the same chunking, compression
segments and ordering as option_contract, whose layout the probe's chains were sized for. A
row holds four columns, so about 1.9 million rows a night (about 9.7 million a five-session
week) compress to a small fraction of the evening's contracts. The morning pass's fetch
records are chain_fetch rows with chain_pass 'morning'.

Revision ID: 0026
Revises: 0025
"""

import sqlalchemy as sa
from alembic import op

revision = "0026"
down_revision = "0025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "option_open_interest",
        sa.Column("session_date", sa.Date, nullable=False),
        sa.Column("underlying", sa.Text, nullable=False),
        sa.Column("option_symbol", sa.Text, nullable=False),
        sa.Column("open_interest", sa.BigInteger, nullable=False),
        sa.PrimaryKeyConstraint("session_date", "underlying", "option_symbol"),
    )
    op.execute(
        "SELECT create_hypertable('option_open_interest', 'session_date',"
        " chunk_time_interval => INTERVAL '7 days')"
    )
    op.execute(
        "ALTER TABLE option_open_interest SET (timescaledb.compress,"
        " timescaledb.compress_segmentby = 'underlying',"
        " timescaledb.compress_orderby = 'option_symbol, session_date DESC')"
    )
    op.execute("SELECT add_compression_policy('option_open_interest', INTERVAL '3 days')")


def downgrade() -> None:
    op.drop_table("option_open_interest")
