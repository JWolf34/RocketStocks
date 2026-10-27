"""Migrations 0006-0024 against Timescale: the upgrade/downgrade round trip and
the physical layout (hypertables, compression, constraints)."""

import uuid
from datetime import UTC, date, datetime

import pytest
from alembic import command
from sqlalchemy import create_engine, inspect, text

from rs_persistence import migration_config

NEW_TABLES = {"popularity_snapshot", "daily_bar", "unpriced_symbol", "track_record", "schwab_auth_request"}
NEW_TYPES = {"popularity_origin", "track_record_input", "auth_request_state"}
# track_record_input is gone at head: the Track record no longer has an input.
HEAD_TYPES = NEW_TYPES - {"track_record_input"}
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
    assert types == HEAD_TYPES
    assert alert_columns >= {
        "id", "symbol", "trigger", "triggered_at", "day", "payload", "posted_at",
        "rule_version", "reply_to", "post_attempts", "delivery", "sending_at", "message_id", "thread_id",
    }
    assert "dedup_key" not in alert_columns

    command.downgrade(config, "0005")
    tables, types, alert_columns = _state(scratch_url)
    assert "outbox" in tables
    assert not tables & NEW_TABLES
    assert types == set()
    assert alert_columns == ALERT_COLUMNS_BEFORE

    command.upgrade(config, "head")
    tables, types, _ = _state(scratch_url)
    assert NEW_TABLES <= tables and types == HEAD_TYPES


def test_0012_and_0013_drop_the_dead_tables_and_downgrade_to_empty_ones(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "head")
        assert not {"stream_bars_5m", "service_clients"} & set(inspect(engine).get_table_names())

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


DROPPED_BY_0022 = {"options_chain_snapshot", "import_verification"}


def _dropped_layout(engine):
    """Everything migration 0022 drops, as the catalog reports it: columns, the hypertable
    dimension, compression settings, the compression policy and the two enums."""
    with engine.connect() as conn:

        def rows(sql):
            return [tuple(r) for r in conn.execute(text(sql))]

        return {
            "columns": rows(
                "SELECT table_name, column_name, data_type, udt_name, is_nullable, column_default,"
                " is_identity, identity_generation, numeric_precision, numeric_scale"
                " FROM information_schema.columns WHERE table_schema = 'public'"
                " AND table_name IN ('options_chain_snapshot', 'import_verification')"
                " ORDER BY table_name, ordinal_position"
            ),
            "constraints": rows(
                "SELECT conrelid::regclass::text, contype, pg_get_constraintdef(oid) FROM pg_constraint"
                " WHERE conrelid IN (SELECT oid FROM pg_class WHERE relnamespace = 'public'::regnamespace"
                " AND relname IN ('options_chain_snapshot', 'import_verification'))"
                " ORDER BY 1, 2, 3"
            ),
            "dimension": rows(
                "SELECT column_name, time_interval FROM timescaledb_information.dimensions"
                " WHERE hypertable_name = 'options_chain_snapshot'"
            ),
            "compression": rows(
                "SELECT segmentby, orderby FROM timescaledb_information.hypertable_compression_settings"
                " WHERE hypertable::text = 'options_chain_snapshot'"
            ),
            "jobs": rows(
                "SELECT proc_name, schedule_interval, config->>'compress_after'"
                " FROM timescaledb_information.jobs WHERE hypertable_name = 'options_chain_snapshot'"
            ),
            "enums": rows(
                "SELECT t.typname, e.enumlabel FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid"
                " WHERE t.typname IN ('dataset', 'source_status') ORDER BY t.typname, e.enumsortorder"
            ),
        }


def test_0022_drops_the_chain_and_import_tables_and_downgrades_to_empty_ones_as_they_were(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "0021")
        before = _dropped_layout(engine)
        assert {c[0] for c in before["columns"]} == DROPPED_BY_0022
        assert before["dimension"] and before["compression"] and before["jobs"] and before["enums"]
        with engine.begin() as conn:  # rows must not stop the drop
            conn.execute(
                text(
                    "INSERT INTO options_chain_snapshot (underlying, dataset, cycle_id, fetched_at, source_status)"
                    " VALUES ('SPY', 'eod_full', now(), now(), 'ok')"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO import_verification (verified_at, zone, overlap_rows, matched_rows,"
                    " match_rate, overlap_start, overlap_end) VALUES (now(), 'z', 1, 1, 1, now(), now())"
                )
            )

        command.upgrade(config, "0022")
        gone = _dropped_layout(engine)
        assert not DROPPED_BY_0022 & set(inspect(engine).get_table_names())
        assert all(not rows for rows in gone.values())

        command.downgrade(config, "0021")
        assert DROPPED_BY_0022 <= set(inspect(engine).get_table_names())
        assert _dropped_layout(engine) == before
        with engine.connect() as conn:
            for table in DROPPED_BY_0022:
                assert conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one() == 0

        command.upgrade(config, "head")
        assert not DROPPED_BY_0022 & set(inspect(engine).get_table_names())
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

        command.upgrade(config, "0014")
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


def test_alerts_hold_one_alert_per_stock_per_eastern_day_and_no_dedup_key(sync_engine):
    insp = inspect(sync_engine)
    assert insp.get_unique_constraints("alerts") == []
    [index] = [i for i in insp.get_indexes("alerts") if i["name"] == "uq_alerts_symbol_day"]
    assert index["unique"] and index["column_names"] == ["symbol", "day"]
    with sync_engine.connect() as conn:
        definition = conn.execute(
            text("SELECT indexdef FROM pg_indexes WHERE indexname = 'uq_alerts_symbol_day'")
        ).scalar_one()
    assert "WHERE (reply_to IS NULL)" in definition


def test_candidates_are_indexed_for_a_triggers_window_and_a_stocks_day(sync_engine):
    indexes = {i["name"]: (i["column_names"], i["unique"]) for i in inspect(sync_engine).get_indexes("candidate")}
    assert indexes["uq_candidate_hit"] == (["trigger", "rule_version", "symbol", "hit_at"], True)
    assert indexes["ix_candidate_trigger_hit_at"] == (["trigger", "hit_at"], False)
    assert indexes["ix_candidate_symbol_day"] == (["symbol", "day"], False)
    with sync_engine.connect() as conn:
        verdicts = conn.execute(text("SELECT unnest(enum_range(NULL::candidate_verdict))::text")).scalars().all()
    assert verdicts == ["posted", "reply", "below_bar", "over_budget", "repeat", "stale"]


SURGE_PAYLOAD = (
    '{"version": 2, "rule_version": "1", "polled_at": "2026-10-20T03:30:00Z", "fired": ["mention_surge"],'
    ' "rank": 40, "mentions": 90, "rank_24h_ago": null, "mentions_24h_ago": 20, "rank_gain": null,'
    ' "mention_ratio": 4.5, "mention_multiplier": 3.0, "mention_min_prior": 15,'
    ' "facts": {"price_move": {"status": "unavailable", "reason": "x"}}}'
)


def test_0016_keeps_every_alert_backfills_day_and_one_posted_candidate_per_trigger_alert(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "0015")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO alerts (symbol, alert_type, triggered_at, payload, rule_version, dedup_key)"
                    " VALUES ('GME', 'popularity_surge', '2026-10-20 03:30+00', CAST(:payload AS jsonb), '1',"
                    "         'popularity_surge:1:GME:20261020T0330Z'),"
                    "        ('TEST', 'staged_test', '2026-10-20 15:00+00', '{}', NULL, NULL)"
                ),
                {"payload": SURGE_PAYLOAD},
            )
            conn.execute(
                text(
                    "INSERT INTO track_record (alert_type, rule_version, computed_at, input, n,"
                    " survivorship_caveat) VALUES ('popularity_surge', '1', now(), 'logged', 7, 'c')"
                )
            )

        command.upgrade(config, "0016")
        with engine.connect() as conn:
            alerts = conn.execute(text("SELECT id, symbol, trigger, day, reply_to FROM alerts ORDER BY id")).all()
            candidates = conn.execute(
                text(
                    "SELECT trigger, rule_version, symbol, hit_at, day, inputs, verdict::text, decided_at, alert_id"
                    " FROM candidate"
                )
            ).all()
            record = conn.execute(text("SELECT trigger, n FROM track_record")).one()
        # 23:30 EDT on the 19th is still the 19th in New York.
        assert [(a.symbol, a.trigger, a.day, a.reply_to) for a in alerts] == [
            ("GME", "popularity_surge", date(2026, 10, 19), None),
            ("TEST", "staged_test", date(2026, 10, 20), None),
        ]
        [candidate] = candidates
        gme = datetime(2026, 10, 20, 3, 30, tzinfo=UTC)
        assert candidate == (
            "popularity_surge", "1", "GME", gme, date(2026, 10, 19),
            {
                "fired": ["mention_surge"], "rank": 40, "mentions": 90, "rank_24h_ago": None,
                "mentions_24h_ago": 20, "rank_gain": None, "mention_ratio": 4.5,
                "mention_multiplier": 3.0, "mention_min_prior": 15,
            },
            "posted", gme, alerts[0].id,
        )
        assert record == ("popularity_surge", 7)

        command.downgrade(config, "0015")
        with engine.connect() as conn:
            assert conn.execute(text("SELECT symbol, alert_type, dedup_key FROM alerts ORDER BY id")).all() == [
                ("GME", "popularity_surge", "popularity_surge:1:GME:20261020T0330Z"),
                ("TEST", "staged_test", None),
            ]
            assert conn.execute(text("SELECT alert_type FROM track_record")).scalar_one() == "popularity_surge"
        assert "candidate" not in inspect(engine).get_table_names()

        command.upgrade(config, "head")
        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM candidate")).scalar_one() == 1
    finally:
        engine.dispose()


def test_0016_stops_and_names_two_alerts_that_share_a_stock_and_eastern_day(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "0015")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO alerts (symbol, alert_type, triggered_at, payload) VALUES"
                    " ('GME', 'popularity_surge', '2026-10-19 15:00+00', '{}'),"
                    " ('GME', 'popularity_surge', '2026-10-20 02:00+00', '{}'),"  # 22:00 EDT the same day
                    " ('AMC', 'popularity_surge', '2026-10-19 15:00+00', '{}')"
                )
            )

        with pytest.raises(RuntimeError, match=r"GME on 2026-10-19: alert ids \[1, 2\]"):
            command.upgrade(config, "head")

        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM alerts")).scalar_one() == 3
            assert "alert_type" in {c["name"] for c in inspect(conn).get_columns("alerts")}
    finally:
        engine.dispose()


def test_0016_downgrade_marks_an_unposted_reply_handled_for_the_previous_bot(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "0016")
        with engine.begin() as conn:
            alert_id = conn.execute(
                text(
                    "INSERT INTO alerts (symbol, trigger, triggered_at, payload, posted_at)"
                    " VALUES ('GME', 't', '2026-10-20 15:00+00', '{}', '2026-10-20 15:01+00') RETURNING id"
                )
            ).scalar_one()
            conn.execute(
                text(
                    "INSERT INTO alerts (symbol, trigger, triggered_at, payload, reply_to) VALUES"
                    " ('GME', 't', '2026-10-20 16:00+00', '{}', :alert),"
                    " ('GME', 't', '2026-10-20 17:00+00', '{}', :alert)"
                ),
                {"alert": alert_id},
            )
            conn.execute(
                text("UPDATE alerts SET posted_at = '2026-10-20 17:01+00' WHERE triggered_at = '2026-10-20 17:00+00'")
            )

        command.downgrade(config, "0015")
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT triggered_at, posted_at FROM alerts ORDER BY triggered_at")
            ).all()
            old_cursor = conn.execute(text("SELECT count(*) FROM alerts WHERE posted_at IS NULL")).scalar_one()
        # The unposted reply reads as handled (its own time); the posted ones are untouched.
        assert rows == [
            (datetime(2026, 10, 20, 15, tzinfo=UTC), datetime(2026, 10, 20, 15, 1, tzinfo=UTC)),
            (datetime(2026, 10, 20, 16, tzinfo=UTC), datetime(2026, 10, 20, 16, tzinfo=UTC)),
            (datetime(2026, 10, 20, 17, tzinfo=UTC), datetime(2026, 10, 20, 17, 1, tzinfo=UTC)),
        ]
        assert old_cursor == 0
    finally:
        engine.dispose()


def test_0015_adds_bar_request_and_downgrade_drops_it(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "head")
        assert [c["name"] for c in inspect(engine).get_columns("bar_request")] == ["symbol", "earliest_start"]
        assert inspect(engine).get_pk_constraint("bar_request")["constrained_columns"] == ["symbol"]

        command.downgrade(config, "0014")
        assert "bar_request" not in inspect(engine).get_table_names()
    finally:
        engine.dispose()


def test_0017_marks_posted_rows_posted_and_downgrade_returns_the_old_cursor(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "0016")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO alerts (symbol, trigger, triggered_at, payload, posted_at) VALUES"
                    " ('AAA', 't', '2026-10-19 15:00+00', '{}', '2026-10-19 15:01+00'),"
                    " ('BBB', 't', '2026-10-19 15:00+00', '{}', NULL),"
                    " ('CCC', 't', '2026-10-19 15:00+00', '{}', NULL)"
                )
            )

        command.upgrade(config, "head")
        with engine.begin() as conn:
            conn.execute(text("UPDATE alerts SET delivery = 'stale' WHERE symbol = 'CCC'"))
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT symbol, delivery::text, sending_at, message_id, thread_id FROM alerts ORDER BY id")
            ).all()
            index = conn.execute(
                text("SELECT indexdef FROM pg_indexes WHERE indexname = 'ix_alerts_undelivered'")
            ).scalar_one()
        assert rows == [
            ("AAA", "posted", None, None, None),
            ("BBB", "pending", None, None, None),
            ("CCC", "stale", None, None, None),
        ]
        assert "WHERE (delivery = ANY (ARRAY['pending'::alert_delivery, 'sending'::alert_delivery]))" in index

        command.downgrade(config, "0016")
        insp = inspect(engine)
        assert not {"delivery", "sending_at", "message_id", "thread_id"} & {c["name"] for c in insp.get_columns("alerts")}
        assert {i["name"] for i in insp.get_indexes("alerts")} >= {"ix_alerts_unposted"}
        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM pg_type WHERE typname = 'alert_delivery'")).scalar_one() == 0
            # The previous bot posts every row without posted_at: the stale one must not be among them.
            old_cursor = conn.execute(text("SELECT symbol FROM alerts WHERE posted_at IS NULL ORDER BY id")).scalars().all()
            stale = conn.execute(
                text("SELECT posted_at = triggered_at FROM alerts WHERE symbol = 'CCC'")
            ).scalar_one()
            assert conn.execute(text("SELECT count(*) FROM alerts")).scalar_one() == 3
        assert old_cursor == ["BBB"]
        assert stale is True

        command.upgrade(config, "head")
    finally:
        engine.dispose()


def test_0018_swaps_the_track_record_for_outcomes_and_downgrade_restores_the_old_shape(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "0017")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO track_record (trigger, rule_version, computed_at, input, n, survivorship_caveat)"
                    " VALUES ('popularity_surge', '2', now(), 'logged', 40, 'caveat')"
                )
            )

        command.upgrade(config, "0018")
        insp = inspect(engine)
        assert insp.get_pk_constraint("track_record")["constrained_columns"] == [
            "trigger", "rule_version", "horizon",
        ]
        assert insp.get_pk_constraint("outcome")["constrained_columns"] == [
            "trigger", "rule_version", "symbol", "day", "horizon",
        ]
        columns = {c["name"] for c in insp.get_columns("track_record")}
        assert not columns & {"input", "n", "hit_rate_1d", "hit_rate_10d", "median_excess_10d", "survivorship_caveat"}
        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM track_record")).scalar_one() == 0  # old study rows are gone
            assert conn.execute(text("SELECT count(*) FROM pg_type WHERE typname = 'track_record_input'")).scalar_one() == 0

        command.downgrade(config, "0017")
        insp = inspect(engine)
        assert "outcome" not in insp.get_table_names()
        assert {c["name"] for c in insp.get_columns("track_record")} >= {"input", "hit_rate_10d", "survivorship_caveat"}

        command.upgrade(config, "head")
        assert "outcome" in inspect(engine).get_table_names()
    finally:
        engine.dispose()


def test_0019_adds_trigger_role_and_downgrade_drops_it(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "0018")
        assert "trigger_role" not in inspect(engine).get_table_names()

        command.upgrade(config, "0019")
        insp = inspect(engine)
        assert {c["name"]: c["nullable"] for c in insp.get_columns("trigger_role")} == {
            "trigger": False,
            "role_id": False,
        }
        assert insp.get_pk_constraint("trigger_role")["constrained_columns"] == ["trigger"]

        command.downgrade(config, "0018")
        assert "trigger_role" not in inspect(engine).get_table_names()

        command.upgrade(config, "head")
    finally:
        engine.dispose()


def test_0020_flags_the_stored_bars_adjusted_and_downgrade_flags_them_unadjusted(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "0019")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO daily_bar (symbol, session_date, open, high, low, close, volume, fetched_at, adjusted)"
                    " VALUES ('AAA', '2026-10-19', 10, 11, 9, 10.5, 1000, now(), false),"
                    " ('AAA', '2026-10-20', 10, 11, 9, 10.6, 1000, now(), false),"
                    " ('BBB', '2026-10-20', 20, 21, 19, 20.5, 1000, now(), false)"
                )
            )

        command.upgrade(config, "0020")
        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM daily_bar WHERE adjusted")).scalar_one() == 3

        command.downgrade(config, "0019")
        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM daily_bar WHERE NOT adjusted")).scalar_one() == 3

        command.upgrade(config, "head")
    finally:
        engine.dispose()


COMPRESSED_BAR_SYMBOLS = 150
COMPRESSED_BAR_DAYS = 700  # 105,000 bars, over Timescale's 100,000 decompressed tuples per transaction


def _seed_compressed_bars(conn, *, adjusted: bool) -> set[str]:
    """Years of old bars in compressed chunks, plus one recent bar in a chunk the compression
    policy has not reached. Returns the chunks that are compressed."""
    conn.execute(
        text(
            "INSERT INTO daily_bar (symbol, session_date, open, high, low, close, volume, fetched_at, adjusted)"
            " SELECT 'S' || s, DATE '2020-01-01' + d, 10, 11, 9, 10.5, 1000, now(), :adjusted"
            " FROM generate_series(1, :symbols) s, generate_series(0, :days - 1) d"
        ),
        {"symbols": COMPRESSED_BAR_SYMBOLS, "days": COMPRESSED_BAR_DAYS, "adjusted": adjusted},
    )
    conn.execute(
        text(
            "INSERT INTO daily_bar (symbol, session_date, open, high, low, close, volume, fetched_at, adjusted)"
            " VALUES ('NEW', CURRENT_DATE, 10, 11, 9, 10.5, 1000, now(), :adjusted)"
        ),
        {"adjusted": adjusted},
    )
    conn.execute(
        text(
            "SELECT compress_chunk(c, if_not_compressed => true)"
            " FROM show_chunks('daily_bar', older_than => DATE '2024-01-01') c"
        )
    )
    return _compressed_bar_chunks(conn)


def _compressed_bar_chunks(conn) -> set[str]:
    return set(
        conn.execute(
            text(
                "SELECT chunk_name FROM timescaledb_information.chunks"
                " WHERE hypertable_name = 'daily_bar' AND is_compressed"
            )
        ).scalars()
    )


def _partially_compressed_bar_chunks(conn) -> int:
    """Chunks holding rows outside their compressed form, which is how an UPDATE leaves them."""
    return conn.execute(
        text(
            "SELECT count(*) FROM _timescaledb_catalog.chunk c"
            " JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id"
            " WHERE h.table_name = 'daily_bar' AND c.status & 8 <> 0"
        )
    ).scalar_one()


@pytest.mark.parametrize(("start", "target", "adjusted"), [("0019", "0020", True), ("0020", "0019", False)])
def test_0020_flags_more_compressed_bars_than_one_transaction_may_decompress(
    scratch_url, start, target, adjusted
):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, start)
        with engine.begin() as conn:
            compressed = _seed_compressed_bars(conn, adjusted=not adjusted)
        assert len(compressed) > 1

        if adjusted:
            command.upgrade(config, target)
        else:
            command.downgrade(config, target)

        with engine.connect() as conn:
            total = COMPRESSED_BAR_SYMBOLS * COMPRESSED_BAR_DAYS + 1
            flagged = conn.execute(
                text("SELECT count(*) FROM daily_bar WHERE adjusted = :a"), {"a": adjusted}
            ).scalar_one()
            assert flagged == total
            assert _compressed_bar_chunks(conn) == compressed
            assert _partially_compressed_bar_chunks(conn) == 0
    finally:
        engine.dispose()


def test_0021_adds_candidate_scores_and_keeps_existing_candidates_unscored(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "0020")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO candidate (trigger, rule_version, symbol, hit_at, inputs, verdict, decided_at)"
                    " VALUES ('t', '2', 'AAA', '2026-10-19 15:00+00', '{}', 'repeat', '2026-10-19 15:00+00')"
                )
            )

        command.upgrade(config, "head")
        with engine.connect() as conn:
            assert conn.execute(text("SELECT raw_score, strength FROM candidate")).all() == [(None, None)]
            index = conn.execute(
                text("SELECT indexdef FROM pg_indexes WHERE indexname = 'ix_candidate_announced_day'")
            ).scalar_one()
        assert "(day) WHERE (verdict = ANY (ARRAY['posted'::candidate_verdict, 'reply'::candidate_verdict]))" in index

        command.downgrade(config, "0020")
        insp = inspect(engine)
        assert not {"raw_score", "strength"} & {c["name"] for c in insp.get_columns("candidate")}
        assert "ix_candidate_announced_day" not in {i["name"] for i in insp.get_indexes("candidate")}

        command.upgrade(config, "head")
    finally:
        engine.dispose()


def test_0023_adds_the_universe_tables_and_downgrade_drops_them(scratch_url):
    config = migration_config(scratch_url)
    engine = create_engine(scratch_url)
    try:
        command.upgrade(config, "0022")
        assert not {"universe_snapshot", "universe_listing"} & set(inspect(engine).get_table_names())

        command.upgrade(config, "0023")
        insp = inspect(engine)
        assert insp.get_pk_constraint("universe_snapshot")["constrained_columns"] == ["snapshot_date"]
        assert insp.get_pk_constraint("universe_listing")["constrained_columns"] == ["snapshot_date", "symbol"]
        assert {c["name"]: c["nullable"] for c in insp.get_columns("universe_listing")} == {
            "snapshot_date": False,
            "symbol": False,
            "security_name": False,
            "exchange": False,
            "etf": False,
            "financial_status": True,
            "cqs_symbol": True,
            "nasdaq_symbol": True,
            "optionable": False,
        }

        command.downgrade(config, "0022")
        assert not {"universe_snapshot", "universe_listing"} & set(inspect(engine).get_table_names())

        command.upgrade(config, "head")
    finally:
        engine.dispose()
