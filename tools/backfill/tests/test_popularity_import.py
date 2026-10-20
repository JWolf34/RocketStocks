"""verify-offset and popularity-import on synthetic data only: a planted zone is
converted into v1-style naive rows and mirrored as v2 live rows."""
# ruff: noqa: F811 - fixture params named after imported fixtures are how pytest injects them.

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text

from rs_backfill.__main__ import main
from rs_backfill.dump import V1Row
from rs_backfill.importer import import_rows, to_snapshot
from rs_backfill.verify import LiveIndex, render_report, verify_offset
from rs_backfill.zones import CANDIDATE_ZONES, resolve_zone
from rs_domain import PopularityOrigin
from rs_persistence import PopularityRepository
from rs_persistence.testing import migrated_db_url, session_factory  # noqa: F401

STEP = timedelta(minutes=30)
TICKERS = [f"T{i:02d}" for i in range(8)]


def _history(start: datetime, steps: int) -> list[tuple[datetime, str, int, int, int]]:
    """(boundary, ticker, rank, mentions, mentions_24h_ago); the numbers move every
    step, so a wrong offset lines up with different values."""
    return [
        (
            start + STEP * step,
            ticker,
            i + 1,
            20 + (i * 37 + step * 11) % 300,
            15 + (i * 17 + step * 5) % 200,
        )
        for step in range(steps)
        for i, ticker in enumerate(TICKERS)
    ]


def _v1_rows(history, zone_name: str) -> list[V1Row]:
    """What v1 would have stored: naive local wall-clock, first write per
    (datetime, rank) wins (the repeated fall-back hour collides on v1's key)."""
    zone = resolve_zone(zone_name)
    rows: dict[tuple[datetime, int], V1Row] = {}
    for at, ticker, rank, mentions, m24 in history:
        naive = at.astimezone(zone).replace(tzinfo=None)
        rows.setdefault((naive, rank), V1Row(naive, rank, ticker, mentions, 5, rank + 1, m24))
    return list(rows.values())


def _live(history) -> LiveIndex:
    index = LiveIndex()
    for at, ticker, rank, mentions, m24 in history:
        index.add(at, ticker, rank, mentions, m24)
    return index


# Three days across the 2026-03-08 spring-forward (08:00Z, 36 hours in): local
# zones and fixed offsets diverge halfway through, so each planted zone has
# exactly one perfect match.
SPRING = datetime(2026, 3, 6, 20, tzinfo=UTC)
SPRING_HISTORY = _history(SPRING, 144)


@pytest.mark.parametrize("planted", CANDIDATE_ZONES)
def test_the_verifier_picks_the_planted_zone(planted):
    report = verify_offset(_v1_rows(SPRING_HISTORY, planted), _live(SPRING_HISTORY))

    assert report.winner is not None
    assert report.winner.zone == planted
    assert report.winner.rate == 1.0
    others = [c for c in report.candidates if c.zone != planted]
    assert all(c.rate < 0.6 for c in others)  # the DST split leaves at most half


def _corrupt(rows: list[V1Row], every: int) -> list[V1Row]:
    return [replace(r, mentions=(r.mentions or 0) + 1) if i % every == 0 else r for i, r in enumerate(rows)]


def test_a_98_percent_match_is_rejected_and_99_percent_passes():
    rows = _v1_rows(SPRING_HISTORY, "America/Chicago")

    rejected = verify_offset(_corrupt(rows, 50), _live(SPRING_HISTORY))  # 2% wrong
    assert rejected.winner is None
    assert "no candidate reached 99%" in rejected.reason
    assert render_report(rejected).count("NOT VERIFIED") == 1

    passed = verify_offset(_corrupt(rows, 200), _live(SPRING_HISTORY))  # 0.5% wrong
    assert passed.winner is not None and passed.winner.zone == "America/Chicago"
    assert passed.winner.rate == pytest.approx(0.995, abs=0.001)


def test_a_poll_one_boundary_off_still_matches():
    """v1 and v2 poll independently, so a row can land one bucket from its twin."""
    rows = _v1_rows(SPRING_HISTORY, "America/Chicago")
    jittered = [
        replace(r, at=r.at + STEP) if i % 3 == 0 and r.at.day != 8 else r for i, r in enumerate(rows)
    ]
    report = verify_offset(jittered, _live(SPRING_HISTORY))
    assert report.winner is not None and report.winner.zone == "America/Chicago"
    assert report.winner.rate == 1.0


def test_too_little_overlap_cannot_verify_anything():
    history = SPRING_HISTORY[: 8 * 20]
    report = verify_offset(_v1_rows(history, "UTC"), _live(history))
    assert report.winner is None
    assert "overlap too small" in report.reason


def test_no_overlap_at_all_is_not_verified():
    far = _history(datetime(2024, 1, 1, tzinfo=UTC), 100)
    report = verify_offset(_v1_rows(far, "UTC"), _live(SPRING_HISTORY))
    assert report.winner is None


# A summer-only overlap cannot tell America/Chicago from a fixed -05:00.
SUMMER = datetime(2026, 7, 1, 12, tzinfo=UTC)
SUMMER_HISTORY = _history(SUMMER, 144)


def _with_spring_day(rows: list[V1Row], hours: list[int]) -> list[V1Row]:
    """Extra v1 rows on the 2026-03-08 wall clock, at the given hours only."""
    return rows + [
        V1Row(datetime(2026, 3, 8, hour, minute), 1, "ZZ", 50, 5, 2, 40)
        for hour in hours
        for minute in (0, 30)
    ]


def test_a_summer_only_overlap_without_dst_evidence_is_ambiguous():
    report = verify_offset(_v1_rows(SUMMER_HISTORY, "America/Chicago"), _live(SUMMER_HISTORY))
    assert report.winner is None
    assert "ambiguous" in report.reason
    assert "America/Chicago" in report.reason and "-05:00" in report.reason


@pytest.mark.parametrize(
    "planted, expected_gap",
    [("America/Chicago", True), ("-05:00", False)],
)
def test_the_spring_forward_hour_breaks_the_tie(planted, expected_gap):
    hours = [1, 3] if expected_gap else [1, 2, 3]
    rows = _with_spring_day(_v1_rows(SUMMER_HISTORY, planted), hours)
    report = verify_offset(rows, _live(SUMMER_HISTORY))

    assert report.dst.looks_local is expected_gap
    assert report.winner is not None and report.winner.zone == planted


def test_a_local_zone_with_rows_in_its_nonexistent_hour_is_contradicted():
    rows = _with_spring_day(_v1_rows(SPRING_HISTORY, "America/Chicago"), [2])
    report = verify_offset(rows, _live(SPRING_HISTORY))
    assert report.winner is None
    assert "contradicts the DST evidence" in report.reason


def test_the_repeated_hour_is_reported_lost_under_a_local_zone():
    fall = _history(datetime(2025, 11, 1, 12, tzinfo=UTC), 144)
    report = verify_offset(_v1_rows(fall, "America/Chicago"), _live(fall))

    assert report.winner is not None and report.winner.zone == "America/Chicago"
    assert report.dst.repeated_hour_lost("America/Chicago") == [
        datetime(2025, 11, 2, 7, 0, tzinfo=UTC),
        datetime(2025, 11, 2, 7, 30, tzinfo=UTC),
    ]
    assert report.dst.repeated_hour_lost("-06:00") is None
    assert "07:00Z, 07:30Z" in render_report(report)


# --- mapping and the idempotent import ------------------------------------

ROW = V1Row(datetime(2026, 10, 1, 7, 30), 3, " gme ", 120, 800, 9, 40)


def test_to_snapshot_maps_v1_to_utc_and_marks_the_origin():
    snap = to_snapshot(ROW, "America/Chicago")
    assert not isinstance(snap, str)
    assert snap.polled_at == datetime(2026, 10, 1, 12, 30, tzinfo=UTC)
    assert snap.fetched_at == snap.polled_at
    assert snap.ticker == "GME"
    assert snap.origin is PopularityOrigin.V1_IMPORT
    assert (snap.rank, snap.mentions, snap.upvotes, snap.rank_24h_ago, snap.mentions_24h_ago) == (
        3, 120, 800, 9, 40,
    )  # fmt: skip


@pytest.mark.parametrize(
    "row, reason",
    [
        (replace(ROW, ticker=None), "missing ticker"),
        (replace(ROW, ticker="  "), "missing ticker"),
        (replace(ROW, mentions=None), "missing rank or mentions"),
        (replace(ROW, rank=0), "value out of range"),
        (replace(ROW, mentions=-1), "value out of range"),
        (replace(ROW, at=datetime(2026, 3, 8, 2, 30)), "wall-clock time does not exist in the zone"),
        (replace(ROW, at=datetime(2026, 10, 1, 7, 31)), "not on a half-hour boundary"),
    ],
)
def test_unusable_rows_are_rejected_with_a_reason(row, reason):
    assert to_snapshot(row, "America/Chicago") == reason


async def test_a_missing_upvotes_value_is_imported_as_none():
    snap = to_snapshot(replace(ROW, upvotes=None), "UTC")
    assert not isinstance(snap, str) and snap.upvotes is None


async def test_import_runs_once_and_a_rerun_inserts_nothing(session_factory):
    rows = _v1_rows(_history(SPRING, 20), "America/Chicago")
    rows.append(replace(ROW, mentions=None))

    first = await import_rows(rows, "America/Chicago", session_factory, batch_size=50)
    again = await import_rows(rows, "America/Chicago", session_factory, batch_size=50)

    assert (first.parsed, first.inserted, first.already_present) == (len(rows), len(rows) - 1, 0)
    assert dict(first.rejected) == {"missing rank or mentions": 1}
    assert (again.parsed, again.inserted, again.already_present) == (len(rows), 0, len(rows) - 1)

    async with session_factory() as s:
        stored = await PopularityRepository(s).list_between(SPRING - timedelta(days=1), SPRING + timedelta(days=1))
    assert len(stored) == len(rows) - 1
    assert {snap.origin for snap in stored} == {PopularityOrigin.V1_IMPORT}


async def test_an_existing_live_row_wins_over_the_import(session_factory):
    rows = _v1_rows(_history(SPRING, 1), "UTC")
    first = to_snapshot(rows[0], "UTC")
    assert not isinstance(first, str)
    live = first.model_copy(update={"origin": PopularityOrigin.LIVE, "mentions": 999})
    async with session_factory() as s:
        await PopularityRepository(s).insert_many([live])
        await s.commit()

    stats = await import_rows(rows, "UTC", session_factory)

    assert (stats.inserted, stats.already_present) == (len(rows) - 1, 1)
    async with session_factory() as s:
        (kept,) = await PopularityRepository(s).list_for_ticker(
            first.ticker, SPRING, SPRING + STEP
        )
    assert (kept.origin, kept.mentions) == (PopularityOrigin.LIVE, 999)


# --- the CLI end to end ---------------------------------------------------

V1_ONLY = datetime(2026, 3, 5, 12, tzinfo=UTC)  # 40 steps, ending before the live window
V1_ONLY_STEPS = 40


@pytest.fixture
def db(migrated_db_url, monkeypatch):
    """The shared migrated DB, emptied; RS_DB_URL pointed at it."""
    engine = create_engine(migrated_db_url)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE popularity_snapshot, import_verification RESTART IDENTITY"))
    monkeypatch.setenv("RS_DB_URL", migrated_db_url)
    yield engine
    engine.dispose()


def _seed_live(engine, history) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO popularity_snapshot (polled_at, ticker, rank, mentions, upvotes,"
                " rank_24h_ago, mentions_24h_ago, fetched_at, origin)"
                " VALUES (:at, :ticker, :rank, :mentions, 5, :prior, :m24, :at, 'live')"
            ),
            [
                {"at": at, "ticker": t, "rank": rank, "mentions": mentions, "prior": rank + 1, "m24": m24}
                for at, t, rank, mentions, m24 in history
            ],
        )


def _write_dump(path, rows: list[V1Row]):
    lines = ["COPY public.popularity (datetime, rank, ticker, name, mentions, upvotes, rank_24h_ago, mentions_24h_ago) FROM stdin;"]
    for r in rows:
        lines.append(
            "\t".join(
                [r.at.isoformat(sep=" "), str(r.rank), str(r.ticker), "x", str(r.mentions), str(r.upvotes), str(r.rank_24h_ago), str(r.mentions_24h_ago)]
            )
        )
    lines.append("\\.")
    path.write_text("\n".join(lines) + "\n")
    return path


def _count(engine, where: str = "true") -> int:
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT count(*) FROM popularity_snapshot WHERE {where}")).scalar_one()


def _full_history():
    """v1 holds a stretch before v2 started polling, then the live overlap."""
    return _history(V1_ONLY, V1_ONLY_STEPS) + SPRING_HISTORY


def test_verify_then_import_then_rerun(db, tmp_path, capsys):
    history = _full_history()
    _seed_live(db, SPRING_HISTORY)
    dump = _write_dump(tmp_path / "popularity.sql", _v1_rows(history, "America/Chicago"))
    now = datetime(2026, 10, 20, 9, tzinfo=UTC)

    class FixedClock:
        def now(self) -> datetime:
            return now

    assert main(["popularity-import", "--dump", str(dump), "--zone", "America/Chicago"]) == 1
    assert "run verify-offset first" in capsys.readouterr().err
    assert _count(db, "origin = 'v1_import'") == 0

    assert main(["verify-offset", "--dump", str(dump)], clock=FixedClock()) == 0
    out = capsys.readouterr().out
    assert "VERIFIED: America/Chicago" in out and "DST cross-check" in out
    with db.connect() as conn:
        zone, rate, verified_at = conn.execute(
            text("SELECT zone, match_rate, verified_at FROM import_verification")
        ).one()
    assert (zone, rate, verified_at) == ("America/Chicago", 1.0, now)

    assert main(["popularity-import", "--dump", str(dump), "--zone", "UTC"]) == 1
    assert "not the verified zone America/Chicago" in capsys.readouterr().err
    assert _count(db, "origin = 'v1_import'") == 0

    assert main(["popularity-import", "--dump", str(dump), "--zone", "America/Chicago"]) == 0
    first_out = capsys.readouterr().out
    imported = _count(db, "origin = 'v1_import'")
    assert imported == V1_ONLY_STEPS * len(TICKERS)  # the overlap stays live
    assert f"inserted {imported}" in first_out

    assert main(["popularity-import", "--dump", str(dump), "--zone", "America/Chicago"]) == 0
    assert "inserted 0" in capsys.readouterr().out
    assert _count(db, "origin = 'v1_import'") == imported


def test_a_dump_matching_only_98_percent_records_nothing_and_blocks_the_import(db, tmp_path, capsys):
    _seed_live(db, SPRING_HISTORY)
    rows = _corrupt(_v1_rows(_full_history(), "America/Chicago"), 50)
    dump = _write_dump(tmp_path / "popularity.sql", rows)

    assert main(["verify-offset", "--dump", str(dump)]) == 1
    assert "NOT VERIFIED" in capsys.readouterr().out
    with db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM import_verification")).scalar_one() == 0

    assert main(["popularity-import", "--dump", str(dump), "--zone", "America/Chicago"]) == 1
    assert _count(db, "origin = 'v1_import'") == 0


def test_verify_without_live_rows_says_so(db, tmp_path, capsys):
    dump = _write_dump(tmp_path / "popularity.sql", _v1_rows(_history(SPRING, 4), "UTC"))
    assert main(["verify-offset", "--dump", str(dump)]) == 1
    assert "no live popularity rows" in capsys.readouterr().err


def test_an_unreadable_dump_exits_2(db, tmp_path, capsys):
    _seed_live(db, SPRING_HISTORY)
    empty = tmp_path / "empty.sql"
    empty.write_text("-- nothing\n")
    assert main(["verify-offset", "--dump", str(empty)]) == 2
    assert "no COPY block" in capsys.readouterr().err


def test_an_unknown_zone_and_a_missing_dsn_exit_2(db, tmp_path, capsys, monkeypatch):
    dump = tmp_path / "popularity.sql"
    dump.write_text("-- nothing\n")
    assert main(["popularity-import", "--dump", str(dump), "--zone", "Mars/Olympus"]) == 2
    assert "unknown zone" in capsys.readouterr().err
    monkeypatch.delenv("RS_DB_URL")
    assert main(["popularity-import", "--dump", str(dump), "--zone", "UTC"]) == 2
