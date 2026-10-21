"""Migrations 0006-0014 against Timescale: the upgrade/downgrade round trip and
the physical layout (hypertables, compression, constraints)."""

import uuid

import pytest
from alembic import command
from sqlalchemy import create_engine, inspect, text

from rs_persistence import migration_config

NEW_TABLES = {"popularity_snapshot", "daily_bar", "unpriced_symbol", "track_record", "schwab_auth_request"}
NEW_TYPES = {"popularity_origin", "track_record_input", "auth_request_state"}
ALERT_COLUMNS_BEFORE = {"id", "symbol", "alert_type", "triggered_at", "payload", "posted_at"}


@pytest.fixture
def scratch_url(migrated_db_url):
    """A separate throwaway database, so the round trip can't disturb the shared one."""
    base = migrated_db_url.rsplit("/", 1)[0]
    name = f"rs_roundtrip_{uuid.uuid4().hex[:10]}"
    admin = create_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        yield f"{base}/{name}"
    finally:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


def _state(url):
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            types = set(
                conn.execute(
                    text("SELECT typname FROM pg_type WHERE typname = ANY(:names)"),
                    {"names": sorted(NEW_TYPES)},
                ).scalars()
            )
        insp = inspect(engine)
        return set(insp.get_table_names()), types, {c["name"] for c in insp.get_columns("alerts")}
    finally:
        engine.dispose()


def test_upgrade_downgrade_round_trip(scratch_url):
    config = migration_config(scratch_url)
    command.upgrade(config, "head")
    tables, types, alert_columns = _state(scratch_url)
    assert NEW_TABLES <= tables
    assert "outbox" not in tables
    assert types == NEW_TYPES
    assert alert_columns >= ALERT_COLUMNS_BEFORE | {"rule_version", "dedup_key", "post_attempts"}

    command.downgrade(config, "0005")
    tables, types, alert_columns = _state(scratch_url)
    assert "outbox" in tables
    assert not tables & NEW_TABLES
    assert types == set()
    assert alert_columns == ALERT_COLUMNS_BEFORE

    command.upgrade(config, "head")
    tables, types, _ = _state(scratch_url)
    assert NEW_TABLES <= tables and types == NEW_TYPES


def test_0012_and_0013_drop_the_dead_tables_and_downgrade_to_empty_ones(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "head")
        assert not {"stream_bars_5m", "service_clients"} & set(inspect(engine).get_table_names())
        assert "options_chain_snapshot" in inspect(engine).get_table_names()

        command.downgrade(config, "0011")
        assert {"stream_bars_5m", "service_clients"} <= set(inspect(engine).get_table_names())
        with engine.connect() as conn:
            hypertable = conn.execute(
                text(
                    "SELECT 1 FROM timescaledb_information.hypertables"
                    " WHERE hypertable_name = 'stream_bars_5m'"
                )
            ).one_or_none()
        assert hypertable is not None

        command.upgrade(config, "head")
        assert not {"stream_bars_5m", "service_clients"} & set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


def test_0014_renames_to_the_glossary_and_rewrites_stored_values_both_ways(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "0013")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO alerts (symbol, alert_type, triggered_at, payload, rule_version, dedup_key)"
                    " VALUES ('GME', 'attention_surge', now(), '{\"note\": \"attention_surge\"}', '1',"
                    "         'attention_surge:1:GME:20261019T1400Z'),"
                    "        ('AMC', 'other', now(), '{\"note\": \"attention_surge\"}', NULL, 'other:AMC')"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO base_rate (alert_type, rule_version, computed_at, input, n,"
                    " survivorship_caveat) VALUES ('attention_surge', '1', now(), 'logged', 7, 'c')"
                )
            )

        command.upgrade(config, "head")
        with engine.connect() as conn:
            alerts = conn.execute(
                text("SELECT symbol, alert_type, dedup_key, payload::text FROM alerts ORDER BY symbol")
            ).all()
            records = conn.execute(text("SELECT alert_type, input::text, n FROM track_record")).all()
            names = set(
                conn.execute(
                    text(
                        "SELECT relname FROM pg_class WHERE relname IN"
                        " ('track_record_pkey', 'ix_track_record_lookup', 'track_record_id_seq')"
                    )
                ).scalars()
            )
        assert alerts == [
            ("AMC", "other", "other:AMC", '{"note": "attention_surge"}'),
            (
                "GME",
                "popularity_surge",
                "popularity_surge:1:GME:20261019T1400Z",
                '{"note": "popularity_surge"}',
            ),
        ]
        assert records == [("popularity_surge", "logged", 7)]
        assert names == {"track_record_pkey", "ix_track_record_lookup", "track_record_id_seq"}
        assert "base_rate" not in inspect(engine).get_table_names()

        command.downgrade(config, "0013")
        with engine.connect() as conn:
            assert conn.execute(
                text("SELECT alert_type, dedup_key FROM alerts WHERE symbol = 'GME'")
            ).one() == ("attention_surge", "attention_surge:1:GME:20261019T1400Z")
            assert conn.execute(text("SELECT alert_type, n FROM base_rate")).one() == (
                "attention_surge",
                7,
            )
    finally:
        engine.dispose()


@pytest.fixture(scope="module")
def sync_engine(migrated_db_url):
    engine = create_engine(migrated_db_url)
    yield engine
    engine.dispose()


@pytest.mark.parametrize("table", ["popularity_snapshot", "daily_bar"])
def test_market_tables_are_compressed_hypertables_without_retention(sync_engine, table):
    with sync_engine.connect() as conn:
        compressed = conn.execute(
            text(
                "SELECT compression_enabled FROM timescaledb_information.hypertables"
                " WHERE hypertable_name = :t"
            ),
            {"t": table},
        ).scalar_one()
        jobs = set(
            conn.execute(
                text("SELECT proc_name FROM timescaledb_information.jobs WHERE hypertable_name = :t"),
                {"t": table},
            ).scalars()
        )
    assert compressed is True
    assert "policy_compression" in jobs
    assert "policy_retention" not in jobs


def test_market_tables_are_keyed_by_symbol_and_time(sync_engine):
    insp = inspect(sync_engine)
    assert insp.get_pk_constraint("popularity_snapshot")["constrained_columns"] == ["polled_at", "ticker"]
    assert insp.get_pk_constraint("daily_bar")["constrained_columns"] == ["symbol", "session_date"]


def test_alerts_dedup_key_is_unique(sync_engine):
    uniques = {
        u["name"]: u["column_names"] for u in inspect(sync_engine).get_unique_constraints("alerts")
    }
    assert uniques == {"uq_alerts_dedup_key": ["dedup_key"]}
