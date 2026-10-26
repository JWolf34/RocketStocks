"""Migrations apply to a fresh TimescaleDB and the alerts table has the shape the
bot's delivery cursor relies on."""

import pytest
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import inspect, text


@pytest.fixture(scope="session")
def sync_engine(migrated_db_url):
    engine = create_sync_engine(migrated_db_url)
    yield engine
    engine.dispose()


class TestRev001Schema:
    def test_all_tables_created(self, sync_engine):
        tables = set(inspect(sync_engine).get_table_names())
        assert "alerts" in tables
        assert not {"outbox", "stream_bars_5m", "service_clients"} & tables

    def test_alerts_has_posted_at_cursor(self, sync_engine):
        columns = {c["name"] for c in inspect(sync_engine).get_columns("alerts")}
        assert columns == {
            "id",
            "symbol",
            "trigger",
            "triggered_at",
            "day",
            "payload",
            "posted_at",
            "rule_version",
            "alert_price",
            "quote_time",
            "reply_to",
            "post_attempts",
            "delivery",
            "sending_at",
            "message_id",
            "thread_id",
        }

    def test_alerts_undelivered_partial_index(self, sync_engine):
        # The bot cursor's operational index: partial on the rows still to deliver,
        # so it stays near-empty in steady state.
        with sync_engine.connect() as conn:
            rows = conn.execute(
                text("SELECT indexname, indexdef FROM pg_indexes WHERE indexname LIKE 'ix_alerts_%'")
            ).all()
        assert [name for name, _ in rows] == ["ix_alerts_undelivered"]
        assert "'pending'::alert_delivery" in rows[0][1] and "'sending'::alert_delivery" in rows[0][1]
