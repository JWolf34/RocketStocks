"""Outcomes and the nightly Track record replace the weekly event study.

outcome: one row per case and horizon (1, 5 and 20 sessions), keyed by Trigger
version, stock and Eastern day. It is derived from daily bars and rewritten whole
for a Trigger version by the nightly job.

track_record: one row per Trigger version and horizon, rewritten by the nightly
job; the `reconstructed` and `logged` split, the hit rates, the 10-day columns
and the survivorship caveat are gone. The old rows were measured another way (a
close entry, 1, 5 and 10 days, direction only) and mean nothing next to the new
ones, so the table is dropped and recreated rather than converted; the next
nightly run rebuilds it from the stored Popularity and bars. The downgrade
recreates the old table empty.

Revision ID: 0018
Revises: 0017
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ENUM

revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("track_record")
    op.execute("DROP TYPE track_record_input")

    op.create_table(
        "track_record",
        sa.Column("trigger", sa.Text, nullable=False),
        sa.Column("rule_version", sa.Text, nullable=False),
        sa.Column("horizon", sa.SmallInteger, nullable=False),
        sa.Column("computed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("cases", sa.Integer, nullable=False),
        sa.Column("live_cases", sa.Integer, nullable=False),
        sa.Column("outcomes", sa.Integer, nullable=False),
        sa.Column("unpriced", sa.Integer, nullable=False),
        sa.Column("split_excluded", sa.Integer, nullable=False),
        sa.Column("no_move_size", sa.Integer, nullable=False),
        sa.Column("share_at_2x", sa.Double),
        sa.Column("ordinary_share", sa.Double),
        sa.Column("median_multiple", sa.Double),
        sa.Column("median_excess", sa.Double),
        sa.PrimaryKeyConstraint("trigger", "rule_version", "horizon"),
    )
    op.create_table(
        "outcome",
        sa.Column("trigger", sa.Text, nullable=False),
        sa.Column("rule_version", sa.Text, nullable=False),
        sa.Column("symbol", sa.Text, nullable=False),
        sa.Column("day", sa.Date, nullable=False),
        sa.Column("horizon", sa.SmallInteger, nullable=False),
        sa.Column("case_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("hit_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("live", sa.Boolean, nullable=False),
        sa.Column("entry_session", sa.Date, nullable=False),
        sa.Column("exit_session", sa.Date, nullable=False),
        sa.Column("stock_return", sa.Double, nullable=False),
        sa.Column("benchmark_return", sa.Double, nullable=False),
        sa.Column("excess_return", sa.Double, nullable=False),
        sa.Column("typical_move", sa.Double),
        sa.Column("move_multiple", sa.Double),
        sa.Column("ordinary_share", sa.Double),
        sa.PrimaryKeyConstraint("trigger", "rule_version", "symbol", "day", "horizon"),
    )


def downgrade() -> None:
    op.drop_table("outcome")
    op.drop_table("track_record")

    op.execute("CREATE TYPE track_record_input AS ENUM ('reconstructed', 'logged')")
    op.create_table(
        "track_record",
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("trigger", sa.Text, nullable=False),
        sa.Column("rule_version", sa.Text, nullable=False),
        sa.Column("computed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("input", ENUM(name="track_record_input", create_type=False), nullable=False),
        sa.Column("n", sa.Integer, nullable=False),
        sa.Column("n_unpriced", sa.Integer, nullable=False, server_default=sa.text("0")),
        sa.Column("hit_rate_1d", sa.Double),
        sa.Column("hit_rate_5d", sa.Double),
        sa.Column("hit_rate_10d", sa.Double),
        sa.Column("median_excess_1d", sa.Double),
        sa.Column("median_excess_5d", sa.Double),
        sa.Column("median_excess_10d", sa.Double),
        sa.Column("survivorship_caveat", sa.Text, nullable=False),
    )
    op.create_index(
        "ix_track_record_lookup", "track_record", ["trigger", "rule_version", "input", "computed_at"]
    )
