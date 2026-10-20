"""S002 acceptance: rev-001 migrates a fresh TimescaleDB, stream_bars_5m is a
hypertable, and a domain Bar round-trips through the async session losslessly."""

from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import inspect, text

from rs_domain import Bar
from rs_persistence import BarRepository, build_engine, build_session_factory

BUCKET = datetime(2026, 7, 6, 13, 30, tzinfo=UTC)


@pytest.fixture(scope="session")
def sync_engine(migrated_db_url):
    engine = create_sync_engine(migrated_db_url)
    yield engine
    engine.dispose()


class TestRev001Schema:
    def test_all_tables_created(self, sync_engine):
        tables = set(inspect(sync_engine).get_table_names())
        assert {"stream_bars_5m", "alerts"} <= tables
        assert "outbox" not in tables

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
        # rev-003: the bot cursor's operational index (S004 pin #10) — partial
        # on posted_at IS NULL, so it stays near-empty in steady state.
        with sync_engine.connect() as conn:
            row = conn.execute(
                text("SELECT indexdef FROM pg_indexes WHERE indexname = 'ix_alerts_unposted'")
            ).one_or_none()
        assert row is not None, "ix_alerts_unposted missing (rev-003)"
        assert "WHERE (posted_at IS NULL)" in row[0]


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

    async def test_list_for_day_returns_ordered_utc_day_slice(self, migrated_db_url):
        # Unique symbol + day so this never collides with other tests' inserts in
        # the shared session DB (stream_bars_5m is not truncated between tests).
        d0 = datetime(2026, 7, 7, 13, 30, tzinfo=UTC)
        rows = [
            Bar(symbol="LFD", bucket=d0 + timedelta(minutes=10), open=1.0, high=1.0, low=1.0, close=1.0, volume=30),
            Bar(symbol="LFD", bucket=d0, open=1.0, high=1.0, low=1.0, close=1.0, volume=10),
            Bar(symbol="LFD", bucket=d0 + timedelta(minutes=5), open=1.0, high=1.0, low=1.0, close=1.0, volume=20),
            Bar(symbol="LFD", bucket=d0 + timedelta(days=1), open=1.0, high=1.0, low=1.0, close=1.0, volume=99),
            Bar(symbol="OTH", bucket=d0, open=1.0, high=1.0, low=1.0, close=1.0, volume=99),
        ]
        engine = build_engine(migrated_db_url)
        try:
            factory = build_session_factory(engine)
            async with factory() as session:
                async with session.begin():
                    for bar in rows:
                        await BarRepository(session).insert(bar)
            async with factory() as session:
                got = await BarRepository(session).list_for_day("LFD", date(2026, 7, 7))
        finally:
            await engine.dispose()

        # symbol- and day-scoped, sorted by bucket: the out-of-order inserts come
        # back ordered; the next-day LFD bar and the same-day OTH bar are excluded.
        assert [(b.bucket, b.volume) for b in got] == [
            (d0, 10),
            (d0 + timedelta(minutes=5), 20),
            (d0 + timedelta(minutes=10), 30),
        ]
