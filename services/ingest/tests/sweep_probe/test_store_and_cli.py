"""The output directory's durability and the command line."""

import json

from rs_ingest.tools.sweep_probe.cli import DEFAULT_SAMPLE_SIZE, main
from rs_ingest.tools.sweep_probe.store import CallRecord, Kind, Outcome, Plan, ProbeDir

SYMBOLS = "Company Name, Stock Symbol, DPM Name\n" + "".join(
    f'"COMPANY {n}","S{n:03d}","Someone"\n' for n in range(120)
)


def record(key: str, outcome: Outcome = Outcome.HTTP) -> CallRecord:
    return CallRecord(key, Kind.CHAIN, "SPY", outcome, at="2026-10-28T20:05:00+00:00", status=200)


def test_a_record_cut_short_by_a_crash_is_ignored_and_the_rest_are_kept(tmp_path):
    out = ProbeDir(tmp_path)
    out.append(record("chain/A/full"))
    out.append(record("chain/B/full"))
    with (tmp_path / "records.jsonl").open("a") as handle:
        handle.write('{"key":"chain/C/full","kind":"ch')  # the process died mid-write

    reopened = ProbeDir(tmp_path)

    assert [r.key for r in reopened.records] == ["chain/A/full", "chain/B/full"]


def test_a_record_appended_after_a_torn_line_is_not_lost(tmp_path):
    (tmp_path / "records.jsonl").write_text('{"key":"chain/C/full","kind":"ch')

    ProbeDir(tmp_path).append(record("chain/D/full"))

    assert [r.key for r in ProbeDir(tmp_path).records] == ["chain/D/full"]


def test_the_last_record_of_a_key_wins_and_only_no_token_is_retried(tmp_path):
    out = ProbeDir(tmp_path)
    out.append(record("k", Outcome.UNAVAILABLE))
    assert out.terminal("k") is None
    out.append(record("k", Outcome.HTTP))

    assert ProbeDir(tmp_path).terminal("k").outcome == Outcome.HTTP


def test_raw_payloads_are_stored_gzipped_under_their_key(tmp_path):
    out = ProbeDir(tmp_path)

    relative = out.write_raw("chain/BRK_B/full", b'{"a": 1}')

    assert relative == "raw/chain/BRK_B/full.gz"
    assert (tmp_path / relative).read_bytes()[:2] == b"\x1f\x8b"
    assert out.read_raw(relative) == b'{"a": 1}'


def test_a_plan_survives_a_round_trip_with_its_small_tickers(tmp_path, harness):
    plan = Plan.from_json(harness.plan(small=None).to_json())
    assert plan.small_tickers is None

    chosen = plan.with_small(("AAA", "BBB"))

    assert Plan.from_json(chosen.to_json()) == chosen
    assert chosen.timed_tickers == ("AAPL", "AAA", "BBB")


def test_dry_run_prints_the_plan_and_writes_nothing(tmp_path, capsys):
    symbols = tmp_path / "symbols.csv"
    symbols.write_text(SYMBOLS)
    out = tmp_path / "never-created"

    assert main(["run", "--out", str(out), "--symbols-file", str(symbols), "--dry-run"]) == 0

    plan = json.loads(capsys.readouterr().out)
    assert len(plan["sample_listings"]) == DEFAULT_SAMPLE_SIZE
    assert len(plan["samples"]) >= 20
    assert plan["named_chains"][-1] == "BRK/B"
    assert not out.exists()


def test_the_same_seed_draws_the_same_sample_and_another_seed_a_different_one(tmp_path, capsys):
    symbols = tmp_path / "symbols.csv"
    symbols.write_text(SYMBOLS)

    def drawn(seed: str) -> list[str]:
        main(["run", "--out", str(tmp_path / "o"), "--symbols-file", str(symbols), "--seed", seed, "--dry-run"])
        return json.loads(capsys.readouterr().out)["sample_listings"]

    assert drawn("3") == drawn("3")
    assert drawn("3") != drawn("4")


def test_summary_of_a_directory_without_a_run_says_so(tmp_path, capsys):
    assert main(["summary", str(tmp_path)]) == 0

    assert "nothing to summarise" in capsys.readouterr().out


def test_a_start_whose_next_morning_is_not_a_session_is_refused(tmp_path, capsys):
    symbols = tmp_path / "symbols.csv"
    symbols.write_text(SYMBOLS)

    code = main(["run", "--out", str(tmp_path / "o"), "--symbols-file", str(symbols), "--dry-run", "--start", "2026-10-30T17:00"])

    logged = capsys.readouterr().out
    assert code == 1
    assert "the next morning, 2026-10-31, is not a trading session" in logged
    assert "the next valid start is 2026-11-02T16:05" in logged
    assert '"sample_listings"' not in logged


def test_run_stops_with_the_reason_when_the_token_cannot_be_read(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("RS_DB_URL", "postgresql+psycopg://nobody:x@127.0.0.1:1/none")
    monkeypatch.setenv("SCHWAB_API_KEY", "key")
    monkeypatch.setenv("SCHWAB_API_SECRET", "secret")
    monkeypatch.setenv("RS_EDGAR_USER_AGENT", "RocketStocks test@example.com")
    out = tmp_path / "never-created"

    code = main(["run", "--out", str(out)])

    assert code == 1
    assert "could not be read from the database" in capsys.readouterr().out
    assert not out.exists()

