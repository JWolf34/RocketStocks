"""S002 acceptance: rev-001 migrates a fresh TimescaleDB, stream_bars_5m is a
hypertable, and a domain Bar round-trips through the async session losslessly."""

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import inspect, text

from rs_domain import Bar
from rs_persistence import BarRepository, build_engine, build_session_factory

BUCKET = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)


@pytest.fixture(scope="session")
def sync_engine(migrated_db_url):
    engine = create_sync_engine(migrated_db_url)
    yield engine
    engine.dispose()


class TestRev001Schema:
    def test_all_tables_created(self, sync_engine):
        tables = set(inspect(sync_engine).get_table_names())
        assert {"stream_bars_5m", "alerts", "outbox"} <= tables

    def test_stream_bars_is_hypertable(self, sync_engine):
        with sync_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT hypertable_name FROM timescaledb_information.hypertables "
                    "WHERE hypertable_name = 'stream_bars_5m'"
                )
            ).one_or_none()
        assert row is not None, "stream_bars_5m is not a hypertable"

    def test_alerts_has_posted_at_cursor(self, sync_engine):
        columns = {c["name"] for c in inspect(sync_engine).get_columns("alerts")}
        assert columns == {"id", "symbol", "alert_type", "triggered_at", "payload", "posted_at"}

    def test_outbox_is_minimal_ledger(self, sync_engine):
        columns = {c["name"] for c in inspect(sync_engine).get_columns("outbox")}
        assert columns == {"id", "event_type", "payload", "created_at"}, (
            "outbox must stay an append-only ledger without delivery state (ADR-0005)"
        )


class TestBarRoundTrip:
    async def test_insert_then_get_equal_on_all_fields(self, migrated_db_url):
        bar = Bar(
            symbol="SPY",
            bucket=BUCKET,
            open=560.1,
            high=561.25,
            low=559.9,
            close=560.75,
            volume=1_200_000,
        )
        engine = build_engine(migrated_db_url)
        try:
            factory = build_session_factory(engine)
            async with factory() as session:
                async with session.begin():
                    await BarRepository(session).insert(bar)
            async with factory() as session:
                restored = await BarRepository(session).get("SPY", BUCKET)
        finally:
            await engine.dispose()

        assert restored == bar
        # Field-level bit-exactness — the replay contract (ADR-0023).
        for field in ("open", "high", "low", "close"):
            assert getattr(restored, field).hex() == getattr(bar, field).hex()

    async def test_get_missing_returns_none(self, migrated_db_url):
        engine = build_engine(migrated_db_url)
        try:
            factory = build_session_factory(engine)
            async with factory() as session:
                missing = await BarRepository(session).get("MISSING", BUCKET)
        finally:
            await engine.dispose()
        assert missing is None
