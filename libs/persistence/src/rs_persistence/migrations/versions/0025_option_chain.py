"""option_contract and chain_fetch: the nightly options chains.

option_contract is one row per contract per session (the Eastern date the chain was fetched
for), a hypertable on session_date with no retention policy: chains cannot be rebought.
Hypertable DDL is raw; Alembic cannot infer it. chain_fetch is one row per pass, session
and optionable underlying, so every underlying has a record explaining its chain; a
Tombstone is a row with a failure status, and option_contract never holds placeholder rows.

Sizing (the sweep probe, 2026-11-09, 50 sampled optionable listings): contracts per chain
min 24, median 154, p90 1,328, max 2,952, mean 352, and about 5,500 optionable underlyings,
so about 1.9 million rows a night (about 9.7 million a five-session week) and roughly 490
million a year. Chunks of 7 days hold about one week of rows, and segmenting by underlying
leaves a median of about 770 rows per segment in a chunk (a few batches of at most 1,000),
where 1-day chunks would leave about 150. Within a segment, ordering by option symbol puts
each contract's sessions next to each other, so the columns that repeat compress to almost
nothing. The first night's chunk is compressed by hand to check the ratio.

Revision ID: 0025
Revises: 0024
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ENUM

revision = "0025"
down_revision = "0024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE TYPE chain_pass AS ENUM ('evening', 'morning')")
    op.execute("CREATE TYPE chain_fetch_status AS ENUM ('ok', 'empty', 'rejected', 'failed')")

    op.create_table(
        "chain_fetch",
        sa.Column("session_date", sa.Date, nullable=False),
        sa.Column("chain_pass", ENUM(name="chain_pass", create_type=False), nullable=False),
        sa.Column("underlying", sa.Text, nullable=False),
        sa.Column("status", ENUM(name="chain_fetch_status", create_type=False), nullable=False),
        sa.Column("attempts", sa.SmallInteger, nullable=False),
        sa.Column("contract_count", sa.Integer, nullable=False),
        sa.Column("fetched_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("session_date", "chain_pass", "underlying"),
    )

    op.create_table(
        "option_contract",
        sa.Column("session_date", sa.Date, nullable=False),
        sa.Column("underlying", sa.Text, nullable=False),
        sa.Column("option_symbol", sa.Text, nullable=False),
        sa.Column("expiry", sa.Date, nullable=False),
        sa.Column("strike", sa.Double, nullable=False),
        sa.Column("put_call", sa.CHAR(1), nullable=False),
        sa.Column("bid", sa.Double, nullable=False),
        sa.Column("ask", sa.Double, nullable=False),
        sa.Column("last", sa.Double, nullable=False),
        sa.Column("mark", sa.Double, nullable=False),
        sa.Column("bid_size", sa.Integer, nullable=False),
        sa.Column("ask_size", sa.Integer, nullable=False),
        sa.Column("last_size", sa.Integer, nullable=False),
        sa.Column("volume", sa.BigInteger, nullable=False),
        sa.Column("open_interest", sa.BigInteger, nullable=False),
        sa.Column("quote_time", sa.TIMESTAMP(timezone=True)),
        sa.Column("trade_time", sa.TIMESTAMP(timezone=True)),
        sa.Column("iv", sa.Double),
        sa.Column("delta", sa.Double),
        sa.Column("gamma", sa.Double),
        sa.Column("theta", sa.Double),
        sa.Column("vega", sa.Double),
        sa.Column("rho", sa.Double),
        sa.Column("underlying_price", sa.Double),
        sa.Column("multiplier", sa.SmallInteger),
        sa.Column("expiration_type", sa.CHAR(1)),
        sa.Column("non_standard", sa.Boolean),
        sa.PrimaryKeyConstraint("session_date", "underlying", "option_symbol"),
    )
    op.execute(
        "SELECT create_hypertable('option_contract', 'session_date',"
        " chunk_time_interval => INTERVAL '7 days')"
    )
    op.execute(
        "ALTER TABLE option_contract SET (timescaledb.compress,"
        " timescaledb.compress_segmentby = 'underlying',"
        " timescaledb.compress_orderby = 'option_symbol, session_date DESC')"
    )
    op.execute("SELECT add_compression_policy('option_contract', INTERVAL '3 days')")


def downgrade() -> None:
    op.drop_table("option_contract")
    op.drop_table("chain_fetch")
    op.execute("DROP TYPE chain_fetch_status")
    op.execute("DROP TYPE chain_pass")
