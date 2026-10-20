"""Migrations 0006-0008 against Timescale: the upgrade/downgrade round trip and
the physical layout (hypertables, compression, constraints)."""

import uuid

import pytest
from alembic import command
from sqlalchemy import create_engine, inspect, text

from rs_persistence import migration_config

NEW_TABLES = {"popularity_snapshot", "daily_bar", "unpriced_symbol", "base_rate", "schwab_auth_request"}
NEW_TYPES = {"popularity_origin", "base_rate_input", "auth_request_state"}
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
