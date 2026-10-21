"""Every Trigger hit is a candidate; `alert_type` becomes `trigger`.

alerts: `alert_type` is renamed `trigger` (and track_record's likewise). `day` is
the US Eastern calendar day of `triggered_at`, a stored generated column so the
database computes it for every existing and future row. `reply_to` points a reply
at the Alert it continues. A unique index on (symbol, day) where `reply_to` is
null makes the database hold one Alert per stock per Eastern day. `dedup_key` goes:
a candidate's unique key (trigger, rule version, stock, hit time) is what makes a
rerun idempotent now, and the (symbol, day) index is what stops a second Alert.

candidate: one row per Trigger hit with the selector's verdict, `alert_id` naming
the Alert it produced. Each existing Alert that came from a Trigger (it carries a
rule version) gets one `posted` candidate, its inputs taken from the stored
payload; a staged test Alert has no rule version, is no hit, and gets none.

No existing row is dropped. Two existing Alerts on one stock and Eastern day would
break the one-Alert-a-day rule, so the upgrade stops and names them for the
operator to resolve; nothing is changed until then. The downgrade reverses every
step (the candidates are dropped, the dedup keys rebuilt from the Alerts).

Revision ID: 0015
Revises: 0014
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ENUM, JSONB

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None

EASTERN_DAY = "(({column} AT TIME ZONE 'America/New_York')::date)"
VERDICTS = ("posted", "reply", "below_bar", "over_budget", "repeat", "stale")
# What the Trigger decided on is the payload minus the fields stored beside it.
NON_INPUT_PAYLOAD_KEYS = ("facts", "version", "rule_version", "polled_at")


def _require_one_alert_per_stock_day() -> None:
    shared = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT symbol, " + EASTERN_DAY.format(column="triggered_at") + " AS day,"
                " array_agg(id ORDER BY id) AS ids FROM alerts"
                " GROUP BY symbol, day HAVING count(*) > 1 ORDER BY day, symbol"
            )
        )
        .all()
    )
    if shared:
        found = "; ".join(f"{symbol} on {day}: alert ids {ids}" for symbol, day, ids in shared)
        raise RuntimeError(
            "cannot enforce one Alert per stock per Eastern day, these already share one "
            f"(delete or keep one of each, then upgrade again): {found}"
        )


def upgrade() -> None:
    _require_one_alert_per_stock_day()

    op.alter_column("alerts", "alert_type", new_column_name="trigger")
    op.alter_column("track_record", "alert_type", new_column_name="trigger")
    op.add_column(
        "alerts",
        sa.Column(
            "day",
            sa.Date,
            sa.Computed(EASTERN_DAY.format(column="triggered_at"), persisted=True),
            nullable=False,
        ),
    )
    op.add_column("alerts", sa.Column("reply_to", sa.BigInteger, sa.ForeignKey("alerts.id")))
    op.create_index(
        "uq_alerts_symbol_day",
        "alerts",
        ["symbol", "day"],
        unique=True,
        postgresql_where=sa.text("reply_to IS NULL"),
    )
    op.drop_constraint("uq_alerts_dedup_key", "alerts", type_="unique")
    op.drop_column("alerts", "dedup_key")

    verdicts = ", ".join(f"'{v}'" for v in VERDICTS)
    op.execute(f"CREATE TYPE candidate_verdict AS ENUM ({verdicts})")
    op.create_table(
        "candidate",
        sa.Column("id", sa.BigInteger, sa.Identity(always=True), primary_key=True),
        sa.Column("trigger", sa.Text, nullable=False),
        sa.Column("rule_version", sa.Text, nullable=False),
        sa.Column("symbol", sa.Text, nullable=False),
        sa.Column("hit_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column(
            "day",
            sa.Date,
            sa.Computed(EASTERN_DAY.format(column="hit_at"), persisted=True),
            nullable=False,
        ),
        sa.Column("inputs", JSONB, nullable=False),
        sa.Column(
            "verdict",
            ENUM(name="candidate_verdict", create_type=False),
            nullable=False,
        ),
        sa.Column("decided_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("alert_id", sa.BigInteger, sa.ForeignKey("alerts.id")),
    )
    op.create_index(
        "uq_candidate_hit", "candidate", ["trigger", "rule_version", "symbol", "hit_at"], unique=True
    )
    op.create_index("ix_candidate_trigger_hit_at", "candidate", ["trigger", "hit_at"])
    op.create_index("ix_candidate_symbol_day", "candidate", ["symbol", "day"])

    removed = ", ".join(f"'{key}'" for key in NON_INPUT_PAYLOAD_KEYS)
    op.execute(
        "INSERT INTO candidate (trigger, rule_version, symbol, hit_at, inputs, verdict, decided_at, alert_id)"
        f" SELECT trigger, rule_version, symbol, triggered_at, payload - ARRAY[{removed}], 'posted',"
        " triggered_at, id FROM alerts WHERE rule_version IS NOT NULL ORDER BY id"
    )


def downgrade() -> None:
    op.drop_table("candidate")
    op.execute("DROP TYPE candidate_verdict")

    op.add_column("alerts", sa.Column("dedup_key", sa.Text))
    op.execute(
        "UPDATE alerts SET dedup_key = trigger || ':' || rule_version || ':' || symbol || ':'"
        " || to_char(triggered_at AT TIME ZONE 'UTC', 'YYYYMMDD\"T\"HH24MI\"Z\"')"
        " WHERE rule_version IS NOT NULL AND reply_to IS NULL"
    )
    op.create_unique_constraint("uq_alerts_dedup_key", "alerts", ["dedup_key"])
    op.drop_index("uq_alerts_symbol_day", table_name="alerts")
    op.drop_column("alerts", "reply_to")
    op.drop_column("alerts", "day")
    op.alter_column("track_record", "trigger", new_column_name="alert_type")
    op.alter_column("alerts", "trigger", new_column_name="alert_type")
