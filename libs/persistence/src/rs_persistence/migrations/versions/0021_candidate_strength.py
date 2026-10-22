"""Each candidate keeps its raw score and strength, and the day's announcements are indexed.

candidate: `raw_score` is the hit's distance from the stock's own Popularity Baseline
and `strength` its percentile among the Trigger's hits of the trailing 30 days. Both
are NULL for a candidate decided before they existed, and for a hit whose stock had no
Baseline. `ix_candidate_announced_day` covers the posted and reply candidates of an
Eastern day, which is what the selector reads to know the budget used and what each
stock's Alert already carried.

Revision ID: 0021
Revises: 0020
"""

import sqlalchemy as sa
from alembic import op

revision = "0021"
down_revision = "0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("candidate", sa.Column("raw_score", sa.Double))
    op.add_column("candidate", sa.Column("strength", sa.Double))
    op.create_index(
        "ix_candidate_announced_day",
        "candidate",
        ["day"],
        postgresql_where=sa.text("verdict IN ('posted', 'reply')"),
    )


def downgrade() -> None:
    op.drop_index("ix_candidate_announced_day", table_name="candidate")
    op.drop_column("candidate", "strength")
    op.drop_column("candidate", "raw_score")
