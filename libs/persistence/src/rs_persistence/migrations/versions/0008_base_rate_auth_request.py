"""rev-008: base_rate (versioned event-study outcomes) and schwab_auth_request
(the bot/ingest handshake behind `/setup schwab`).

base_rate rows are append-only: each study run inserts a new row and readers
take the latest per (alert_type, rule_version, input).

Revision ID: 0008
Revises: 0007
Create Date: 2026-10-20
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ENUM

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE TYPE base_rate_input AS ENUM ('reconstructed', 'logged')")
    op.execute(
        "CREATE TYPE auth_request_state AS ENUM"
        " ('requested', 'url_ready', 'callback_received', 'done', 'failed', 'expired')"
    )

    op.create_table(
        "base_rate",
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("alert_type", sa.Text, nullable=False),
        sa.Column("rule_version", sa.Text, nullable=False),
        sa.Column("computed_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("input", ENUM(name="base_rate_input", create_type=False), nullable=False),
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
        "ix_base_rate_lookup", "base_rate", ["alert_type", "rule_version", "input", "computed_at"]
    )

    op.create_table(
        "schwab_auth_request",
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("requested_by", sa.BigInteger, nullable=False),
        sa.Column(
            "state",
            ENUM(name="auth_request_state", create_type=False),
            nullable=False,
            server_default=sa.text("'requested'"),
        ),
        sa.Column("auth_url", sa.Text),
        sa.Column("callback_url", sa.Text),
        sa.Column("error", sa.Text),
        sa.Column("completed_at", sa.TIMESTAMP(timezone=True)),
    )


def downgrade() -> None:
    op.drop_table("schwab_auth_request")
    op.drop_table("base_rate")
    op.execute("DROP TYPE auth_request_state")
    op.execute("DROP TYPE base_rate_input")
