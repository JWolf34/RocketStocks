"""Drop stream_bars_5m.

Nothing writes or reads the 5-minute stream detection log any more.
The downgrade recreates it empty, exactly as rev-001 defined it.

Revision ID: 0012
Revises: 0011
"""

import sqlalchemy as sa
from alembic import op

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("stream_bars_5m")


def downgrade() -> None:
    op.create_table(
        "stream_bars_5m",
        sa.Column("symbol", sa.Text, nullable=False),
        sa.Column("bucket", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("open", sa.Double, nullable=False),
        sa.Column("high", sa.Double, nullable=False),
        sa.Column("low", sa.Double, nullable=False),
        sa.Column("close", sa.Double, nullable=False),
        sa.Column("volume", sa.BigInteger, nullable=False),
        sa.PrimaryKeyConstraint("symbol", "bucket"),
    )
    op.execute("SELECT create_hypertable('stream_bars_5m', 'bucket')")
