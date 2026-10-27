"""earnings_snapshot: Nasdaq's earnings calendar as dated snapshots.

One row per snapshot date, origin, ticker and report date. `snapshot_date` is the
Eastern date the row was fetched; `origin` separates the nightly snapshot from the
one-time backfill of history, so a backfill run beside a nightly snapshot never
overwrites it and a reader of "the latest snapshot" asks for 'nightly'. A plain
table: about 4,000 rows a night and roughly half a million from the backfill.

Revision ID: 0024
Revises: 0023
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ENUM

revision = "0024"
down_revision = "0023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE TYPE earnings_origin AS ENUM ('nightly', 'backfill')")
    op.execute("CREATE TYPE earnings_time AS ENUM ('pre_market', 'after_hours')")
    op.create_table(
        "earnings_snapshot",
        sa.Column("origin", ENUM(name="earnings_origin", create_type=False), nullable=False),
        sa.Column("snapshot_date", sa.Date, nullable=False),
        sa.Column("ticker", sa.Text, nullable=False),
        sa.Column("report_date", sa.Date, nullable=False),
        sa.Column("time_of_day", ENUM(name="earnings_time", create_type=False)),
        sa.Column("fiscal_quarter", sa.Text),
        sa.Column("eps_forecast", sa.Double),
        sa.Column("estimate_count", sa.Integer),
        sa.Column("eps_actual", sa.Double),
        sa.Column("surprise_pct", sa.Double),
        sa.Column("fetched_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("origin", "snapshot_date", "ticker", "report_date"),
    )


def downgrade() -> None:
    op.drop_table("earnings_snapshot")
    op.execute("DROP TYPE earnings_time")
    op.execute("DROP TYPE earnings_origin")
