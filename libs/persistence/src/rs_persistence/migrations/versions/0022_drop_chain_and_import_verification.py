"""Drop options_chain_snapshot and import_verification.

No job reads or writes the options chain snapshots, and the v1 Popularity import
that import_verification gated is done. Dropping the chain hypertable takes its
compression policy with it, and the `dataset` and `source_status` enums served
only that table.

The downgrade recreates the three objects and the table, its hypertable, its
compression settings and policy exactly as rev-005 left them (no later revision
touched them), and import_verification exactly as rev-010 made it. All empty.

Revision ID: 0022
Revises: 0021
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ENUM

revision = "0022"
down_revision = "0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("options_chain_snapshot")
    op.execute("DROP TYPE source_status")
    op.execute("DROP TYPE dataset")
    op.drop_table("import_verification")


def downgrade() -> None:
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

    op.execute("CREATE TYPE dataset AS ENUM ('curated_5m', 'eod_full')")
    op.execute("CREATE TYPE source_status AS ENUM ('ok', 'partial', 'failed', 'stale', 'skipped')")
    op.create_table(
        "options_chain_snapshot",
        # identity - contract columns nullable: a tombstone row has none
        sa.Column("underlying", sa.Text, nullable=False),
        sa.Column("option_symbol", sa.Text),
        sa.Column("expiry", sa.Date),
        sa.Column("strike", sa.Numeric),
        sa.Column("put_call", sa.CHAR(1)),
        # provenance
        sa.Column("dataset", ENUM(name="dataset", create_type=False), nullable=False),
        sa.Column("cycle_id", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("fetched_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("quote_time", sa.TIMESTAMP(timezone=True)),
        sa.Column("source_status", ENUM(name="source_status", create_type=False), nullable=False),
        # quote
        sa.Column("bid", sa.Numeric),
        sa.Column("ask", sa.Numeric),
        sa.Column("last", sa.Numeric),
        sa.Column("mark", sa.Numeric),
        sa.Column("bid_size", sa.Integer),
        sa.Column("ask_size", sa.Integer),
        sa.Column("last_size", sa.Integer),
        sa.Column("trade_time", sa.TIMESTAMP(timezone=True)),
        # flow
        sa.Column("volume", sa.BigInteger),
        sa.Column("open_interest", sa.BigInteger),
        # risk
        sa.Column("iv", sa.REAL),
        sa.Column("delta", sa.REAL),
        sa.Column("gamma", sa.REAL),
        sa.Column("theta", sa.REAL),
        sa.Column("vega", sa.REAL),
        sa.Column("rho", sa.REAL),
        # chain context
        sa.Column("underlying_price", sa.Numeric),
        sa.Column("multiplier", sa.SmallInteger),
        sa.Column("expiration_type", sa.CHAR(1)),
        sa.Column("non_standard", sa.Boolean),
    )
    op.execute(
        "SELECT create_hypertable('options_chain_snapshot', 'fetched_at',"
        " chunk_time_interval => INTERVAL '1 day')"
    )
    op.execute(
        "ALTER TABLE options_chain_snapshot SET (timescaledb.compress,"
        " timescaledb.compress_segmentby = 'dataset, underlying',"
        " timescaledb.compress_orderby = 'fetched_at')"
    )
    op.execute("SELECT add_compression_policy('options_chain_snapshot', INTERVAL '3 days')")
