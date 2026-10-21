"""rev-005 migrates, options_chain_snapshot is a 1-day-chunk hypertable with the
pinned compression policy and no retention, and a ChainSnapshot round-trips
(payload rows and tombstones)."""

from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import inspect, select, text

from rs_domain import ChainSnapshot, Dataset, OptionQuote, SourceStatus
from rs_persistence import (
    ChainSnapshotRepository,
    build_engine,
    build_session_factory,
    chain_snapshot_table,
)

CYCLE = datetime(2026, 7, 6, 14, 35, tzinfo=UTC)

COLUMNS = {
    # identity
    "underlying", "option_symbol", "expiry", "strike", "put_call",
    # provenance
    "dataset", "cycle_id", "fetched_at", "quote_time", "source_status",
    # quote
    "bid", "ask", "last", "mark", "bid_size", "ask_size", "last_size", "trade_time",
    # flow
    "volume", "open_interest",
    # risk
    "iv", "delta", "gamma", "theta", "vega", "rho",
    # chain context
    "underlying_price", "multiplier", "expiration_type", "non_standard",
}


def make_quote(**overrides) -> OptionQuote:
    # Values shaped like a full Schwab chain payload - full payload so
    # the round-trip exercises every nullable column with a real value.
    fields = dict(
        option_symbol="SPY   260717C00560000",
        expiry=date(2026, 7, 17),
        strike=560.0,
        put_call="C",
        quote_time=CYCLE + timedelta(seconds=8),
        bid=6.05,
        ask=6.15,
        last=6.10,
        mark=6.10,
        bid_size=120,
        ask_size=95,
        last_size=2,
        trade_time=CYCLE + timedelta(seconds=5),
        volume=44_120,
        open_interest=61_853,
        iv=0.1431,
        delta=0.5321,
        gamma=0.0412,
        theta=-0.1955,
        vega=0.6410,
        rho=0.2199,
        underlying_price=560.42,
        multiplier=100,
        expiration_type="S",
        non_standard=False,
    )
    fields.update(overrides)
    return OptionQuote(**fields)


def make_snapshot(**overrides) -> ChainSnapshot:
    fields = dict(
        underlying="SPY",
        dataset=Dataset.CURATED_5M,
        cycle_id=CYCLE,
        fetched_at=CYCLE + timedelta(seconds=10),
        source_status=SourceStatus.OK,
        rows=(make_quote(),),
    )
    fields.update(overrides)
    return ChainSnapshot(**fields)


@pytest.fixture(scope="session")
def sync_engine(migrated_db_url):
    engine = create_sync_engine(migrated_db_url)
    yield engine
    engine.dispose()


@pytest.fixture
async def chain_session_factory(migrated_db_url):
    """Async factory over the shared migrated DB, with options_chain_snapshot
    truncated first — the shared session_factory fixture only resets the
    S002/S004 tables."""
    engine = build_engine(migrated_db_url)
    factory = build_session_factory(engine)
    async with factory() as s:
        await s.execute(text("TRUNCATE options_chain_snapshot"))
        await s.commit()
    yield factory
    await engine.dispose()


class TestRev005Schema:
    def test_exact_binding_column_set(self, sync_engine):
        columns = {c["name"] for c in inspect(sync_engine).get_columns("options_chain_snapshot")}
        assert columns == COLUMNS

    def test_membership_columns_not_null_payload_nullable(self, sync_engine):
        nullable = {
            c["name"]: c["nullable"]
            for c in inspect(sync_engine).get_columns("options_chain_snapshot")
        }
        required = {"underlying", "dataset", "cycle_id", "fetched_at", "source_status"}
        assert {name for name, is_nullable in nullable.items() if not is_nullable} == required

    def test_enums_pin_spec_vocabulary(self, sync_engine):
        with sync_engine.connect() as conn:
            datasets = conn.execute(text("SELECT enum_range(NULL::dataset)::text[]")).scalar_one()
            statuses = conn.execute(
                text("SELECT enum_range(NULL::source_status)::text[]")
            ).scalar_one()
        assert datasets == ["curated_5m", "eod_full"]
        assert statuses == ["ok", "partial", "failed", "stale", "skipped"]

    def test_hypertable_with_one_day_chunks(self, sync_engine):
        with sync_engine.connect() as conn:
            interval = conn.execute(
                text(
                    "SELECT time_interval FROM timescaledb_information.dimensions "
                    "WHERE hypertable_name = 'options_chain_snapshot'"
                )
            ).scalar_one_or_none()
        assert interval == timedelta(days=1), "not a hypertable with 1-day chunks"

    def test_compression_policy_three_days_segmented(self, sync_engine):
        with sync_engine.connect() as conn:
            segmentby, orderby = conn.execute(
                text(
                    "SELECT segmentby, orderby "
                    "FROM timescaledb_information.hypertable_compression_settings "
                    "WHERE hypertable::text = 'options_chain_snapshot'"
                )
            ).one()
            compress_after = conn.execute(
                text(
                    "SELECT config ->> 'compress_after' FROM timescaledb_information.jobs "
                    "WHERE proc_name = 'policy_compression' "
                    "AND hypertable_name = 'options_chain_snapshot'"
                )
            ).scalar_one()
        assert [c.strip() for c in segmentby.split(",")] == ["dataset", "underlying"]
        assert orderby is not None and orderby.startswith("fetched_at")
        assert compress_after == "3 days"

    def test_no_retention_policy_forward_only(self, sync_engine):
        with sync_engine.connect() as conn:
            jobs = conn.execute(
                text(
                    "SELECT count(*) FROM timescaledb_information.jobs "
                    "WHERE proc_name = 'policy_retention' "
                    "AND hypertable_name = 'options_chain_snapshot'"
                )
            ).scalar_one()
        assert jobs == 0, "forward-only dataset must have no retention policy (ADR-0020)"


class TestSnapshotRoundTrip:
    async def test_payload_snapshot_round_trips_all_columns(self, chain_session_factory):
        quote = make_quote()
        snapshot = make_snapshot(rows=(quote,))
        async with chain_session_factory() as session:
            async with session.begin():
                written = await ChainSnapshotRepository(session).insert(snapshot)
        assert written == 1

        async with chain_session_factory() as session:
            row = (await session.execute(select(chain_snapshot_table))).one()._mapping

        assert row["underlying"] == "SPY"
        assert row["dataset"] == "curated_5m"
        assert row["source_status"] == "ok"
        assert row["cycle_id"] == snapshot.cycle_id
        assert row["fetched_at"] == snapshot.fetched_at
        # exact-decimal columns (NUMERIC / int / text / bool / timestamptz)
        for field in (
            "option_symbol", "expiry", "strike", "put_call", "quote_time", "bid", "ask",
            "last", "mark", "bid_size", "ask_size", "last_size", "trade_time", "volume",
            "open_interest", "underlying_price", "multiplier", "non_standard",
        ):
            assert row[field] == getattr(quote, field), field
        assert row["expiration_type"] == quote.expiration_type
        # greeks are REAL (float32) by design — compare to storage precision
        for field in ("iv", "delta", "gamma", "theta", "vega", "rho"):
            assert row[field] == pytest.approx(getattr(quote, field), rel=1e-6), field

    async def test_tombstone_writes_one_payload_less_membership_row(self, chain_session_factory):
        tombstone = make_snapshot(source_status=SourceStatus.SKIPPED, rows=())
        async with chain_session_factory() as session:
            async with session.begin():
                written = await ChainSnapshotRepository(session).insert(tombstone)
        assert written == 1

        async with chain_session_factory() as session:
            row = (await session.execute(select(chain_snapshot_table))).one()._mapping
        assert row["underlying"] == "SPY"
        assert row["source_status"] == "skipped"
        assert row["cycle_id"] == tombstone.cycle_id
        payload_columns = COLUMNS - {"underlying", "dataset", "cycle_id", "fetched_at", "source_status"}
        assert all(row[c] is None for c in payload_columns)
