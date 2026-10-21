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
            "alert_type",
            "triggered_at",
            "payload",
            "posted_at",
            "rule_version",
            "alert_price",
            "quote_time",
            "dedup_key",
            "post_attempts",
        }

    def test_alerts_unposted_partial_index(self, sync_engine):
        # rev-003: the bot cursor's operational index — partial
        # on posted_at IS NULL, so it stays near-empty in steady state.
        with sync_engine.connect() as conn:
            row = conn.execute(
                text("SELECT indexdef FROM pg_indexes WHERE indexname = 'ix_alerts_unposted'")
            ).one_or_none()
        assert row is not None, "ix_alerts_unposted missing (rev-003)"
        assert "WHERE (posted_at IS NULL)" in row[0]
