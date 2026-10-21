"""Rename to the glossary: attention_surge -> popularity_surge, base_rate -> track_record.

Schema: the base_rate table becomes track_record, with its primary key, lookup
index, identity sequence and base_rate_input enum renamed to match (the enum's
values, reconstructed and logged, do not carry the name and stay).

Stored values: alerts.alert_type and track_record.alert_type 'attention_surge'
becomes 'popularity_surge'; the 'attention_surge:' prefix of alerts.dedup_key
becomes 'popularity_surge:' (so a stored Alert still dedups against the key the
poller now computes for the same event); any 'attention_surge' text inside the
payload of those Alerts is rewritten too (the current payload embeds none).
rule_version is a bare version number and does not change. Stored Alerts stay
readable because every place the old name was stored now holds the new one, so
the bot's render and Track record lookup match them as before.

The downgrade reverses every step.

Revision ID: 0014
Revises: 0013
"""

from alembic import op

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def _rename_schema(old: str, new: str) -> None:
    op.execute(f"ALTER TABLE {old} RENAME TO {new}")
    op.execute(f"ALTER INDEX {old}_pkey RENAME TO {new}_pkey")
    op.execute(f"ALTER INDEX ix_{old}_lookup RENAME TO ix_{new}_lookup")
    op.execute(f"ALTER SEQUENCE {old}_id_seq RENAME TO {new}_id_seq")
    op.execute(f"ALTER TYPE {old}_input RENAME TO {new}_input")


def _rename_values(old: str, new: str) -> None:
    op.execute(f"UPDATE alerts SET alert_type = '{new}' WHERE alert_type = '{old}'")
    op.execute(
        f"UPDATE alerts SET dedup_key = '{new}' || substr(dedup_key, {len(old) + 1})"
        f" WHERE left(dedup_key, {len(old) + 1}) = '{old}:'"
    )
    op.execute(
        f"UPDATE alerts SET payload = replace(payload::text, '{old}', '{new}')::jsonb"
        f" WHERE alert_type = '{new}' AND strpos(payload::text, '{old}') > 0"
    )
    op.execute(f"UPDATE track_record SET alert_type = '{new}' WHERE alert_type = '{old}'")


def upgrade() -> None:
    _rename_schema("base_rate", "track_record")
    _rename_values("attention_surge", "popularity_surge")


def downgrade() -> None:
    _rename_values("popularity_surge", "attention_surge")
    _rename_schema("track_record", "base_rate")
