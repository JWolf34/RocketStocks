"""trigger_role: which server role an Alert pings for each Trigger.

One row per Trigger, set by an admin with `/setup roles`. It is user state, so it is a plain
table the operator's backup carries. `role_id` is a Discord role id, not a foreign key: the
role lives in Discord and may be deleted there.

Revision ID: 0019
Revises: 0018
"""

import sqlalchemy as sa
from alembic import op

revision = "0019"
down_revision = "0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "trigger_role",
        sa.Column("trigger", sa.Text, primary_key=True),
        sa.Column("role_id", sa.BigInteger, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("trigger_role")
