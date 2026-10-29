"""The probe run in virtual time against a scripted Schwab client."""

import gzip
import json
from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy.exc import OperationalError

from rs_domain import EASTERN
from rs_ingest.tools.sweep_probe.planning import build_plan
from rs_ingest.tools.sweep_probe.store import Outcome
from rs_ingest.tools.sweep_probe.symbols import Listing
from rs_sources import ExchangeCalendar
from rs_sources.schwab import SchwabAuthError, TokenMissingError

TODAY = date(2026, 10, 28)


def by_key(harness):
    return {r.key: r for r in harness.out.records}


def timed_calls(harness, symbols):
    return [c for c in harness.schwab.calls if c.symbol in symbols]


async def test_a_chain_that_fails_twice_is_split_into_three_expiry_ranges_the_last_open_ended(harness, wire):
    def on_chain(symbol, start, end):
        if symbol == "SPY" and start is None and end is None:
            return wire.status(502, "bad gateway")
        return wire.ok(wire.sized_chain(symbol, 40 if start else 3))

    harness.schwab.on_chain = on_chain
    plan = harness.plan(named=("SPY",), fixed=(), small=())

    await harness.runner(plan).run()

    spy = [c for c in harness.schwab.calls if c.symbol == "SPY"]
    assert [(c.start, c.end) for c in spy] == [
        (None, None),
        (None, None),
        (TODAY, TODAY + timedelta(days=30)),
        (TODAY + timedelta(days=31), TODAY + timedelta(days=180)),
        (TODAY + timedelta(days=181), None),
    ]
    records = by_key(harness)
    assert records["chain/SPY/full"].status == 502
    assert (records["chain/SPY/full"].attempts, records["chain/SPY/full"].first_failure) == (2, "502")
    assert [records[f"chain/SPY/split{n}"].status for n in (1, 2, 3)] == [200, 200, 200]
    assert records["chain/SPY/split3"].to_date is None
    assert records["chain/SPY/split2"].contracts == 40
    assert gzip.decompress((harness.directory / records["chain/SPY/full"].raw).read_bytes()) == b"bad gateway"


async def test_a_502_that_clears_on_the_retry_is_not_split(harness, wire):
    answers = iter([502])
    harness.schwab.on_chain = lambda symbol, start, end: (
        wire.status(next(answers, 200)) if symbol == "SPY" else wire.ok(wire.sized_chain(symbol, 3))
    )

    await harness.runner(harness.plan(named=("SPY",), fixed=(), small=())).run()

    assert [(c.symbol, c.start, c.end) for c in harness.schwab.calls] == [("SPY", None, None)] * 2
    full = by_key(harness)["chain/SPY/full"]
    assert (full.status, full.attempts, full.first_failure) == (200, 2, "502")


async def test_a_range_that_fails_twice_is_a_failure_and_the_other_ranges_are_still_fetched(harness, wire):
    def on_chain(symbol, start, end):
        if start is None or start == TODAY + timedelta(days=31):
            return wire.status(504)
        return wire.ok(wire.sized_chain(symbol, 5))

    harness.schwab.on_chain = on_chain

    await harness.runner(harness.plan(named=("SPY",), fixed=(), small=())).run()

    calls = [(c.start, c.end) for c in harness.schwab.calls]
    assert calls.count((TODAY + timedelta(days=31), TODAY + timedelta(days=180))) == 2
    records = by_key(harness)
    assert [records[f"chain/SPY/split{n}"].status for n in (1, 2, 3)] == [200, 504, 200]
    assert "chain/SPY/split4" not in records


async def test_a_timed_sample_goes_down_the_same_ladder(harness, wire):
    harness.schwab.on_chain = lambda symbol, start, end: (
        wire.status(502) if start is None else wire.ok(wire.sized_chain(symbol, 4))
    )
    plan = harness.plan(samples=(harness.first,), fixed=("AAPL",), small=())

    await harness.runner(plan).run()

    stamp = "20261028T200500Z"
    records = by_key(harness)
    assert sorted(records) == [f"timed/{stamp}/AAPL/{part}" for part in ("full", "split1", "split2", "split3")]
    assert records[f"timed/{stamp}/AAPL/split2"].target == harness.first.isoformat()
    assert len(harness.schwab.calls) == 5


async def test_a_sample_that_falls_due_goes_before_the_next_range_of_a_ladder(harness, wire):
    harness.clock._now = harness.first - timedelta(seconds=4)
    harness.schwab.on_chain = lambda symbol, start, end: (
        wire.status(502) if symbol == "SPY" and start is None else wire.ok(wire.sized_chain(symbol, 3))
    )
    plan = harness.plan(samples=(harness.first,), named=("SPY",), fixed=("AAPL",), small=())

    await harness.runner(plan).run()

    assert [c.symbol for c in harness.schwab.calls] == ["SPY", "SPY", "SPY", "AAPL", "SPY", "SPY"]
    assert by_key(harness)["timed/20261028T200500Z/AAPL/full"].late_s < 5


async def test_a_timeout_also_goes_down_the_ladder(harness, wire):
    def on_chain(symbol, start, end):
        if start is None and end is None:
            raise TimeoutError
        return wire.ok(wire.sized_chain(symbol, 7))

    harness.schwab.on_chain = on_chain

    await harness.runner(harness.plan(named=("QQQ",), fixed=(), small=())).run()

    records = by_key(harness)
    assert records["chain/QQQ/full"].outcome == Outcome.TIMEOUT
    assert records["chain/QQQ/full"].raw is None
    assert [records[f"chain/QQQ/split{n}"].contracts for n in (1, 2, 3)] == [7, 7, 7]


async def test_a_400_or_an_empty_chain_is_recorded_and_not_split_or_retried(harness, wire):
    harness.schwab.on_chain = lambda symbol, start, end: wire.status(400) if symbol == "BAD" else wire.ok(wire.chain([]))

    await harness.runner(harness.plan(named=("BAD", "EMPTY"), fixed=(), small=())).run()

    assert [c.symbol for c in harness.schwab.calls] == ["BAD", "EMPTY"]
    records = by_key(harness)
    assert records["chain/BAD/full"].status == 400
    assert records["chain/EMPTY/full"].contracts == 0


async def test_class_share_spellings_are_all_tried_and_the_one_that_worked_is_recorded(harness, wire):
    answers = {"BRK/B": 200, "BRK.B": 400, "BRKB": 400, "BF/B": 400, "BF.B": 200}
    harness.schwab.on_chain = lambda symbol, start, end: (
        wire.ok(wire.sized_chain(symbol, 30)) if answers[symbol] == 200 else wire.status(400)
    )
    plan = harness.plan(named=("BRK/B",), spelling=("BRK.B", "BRKB"), listings=("BF.B",), fixed=(), small=())

    await harness.runner(plan).run()

    assert [c.symbol for c in harness.schwab.calls] == ["BRK/B", "BRK.B", "BRKB", "BF/B", "BF.B"]
    records = by_key(harness)
    assert {k: r.status for k, r in records.items()} == {
        "chain/BRK_B/full": 200,
        "chain/BRK.B/full": 400,
        "chain/BRKB/full": 400,
        "chain/BF_B/full": 400,
        "chain/BF.B/full": 200,
    }


async def test_bar_depth_records_the_earliest_candle_and_counts_apart_the_epoch_zero_ones(harness, wire):
    # 1990-01-02 12:00 UTC, then a 2026 candle, then an epoch-zero artefact.
    harness.schwab.on_history = lambda symbol: wire.ok(wire.candles(631_281_600_000, 1_790_000_000_000, 0))

    await harness.runner(harness.plan(bars=("SPY", "BRK.B"), fixed=(), small=())).run()

    record = by_key(harness)["bars/SPY"]
    assert (record.status, record.candles, record.earliest, record.epoch_zero) == (200, 3, "1990-01-02", 1)
    assert by_key(harness)["bars/BRK.B"].raw is not None


async def test_one_failed_request_is_recorded_and_the_run_goes_on(harness, wire):
    def on_chain(symbol, start, end):
        if symbol == "BOOM":
            raise RuntimeError("connection reset")
        return wire.ok(wire.sized_chain(symbol, 3))

    harness.schwab.on_chain = on_chain

    await harness.runner(harness.plan(named=("BOOM", "NEXT"), fixed=(), small=())).run()

    records = by_key(harness)
    assert records["chain/BOOM/full"].outcome == Outcome.ERROR
    assert "connection reset" in records["chain/BOOM/full"].detail
    assert records["chain/NEXT/full"].status == 200


async def test_a_dead_refresh_token_is_recorded_for_that_call_and_the_run_goes_on(harness, wire):
    def on_chain(symbol, start, end):
        if symbol == "DEAD":
            raise SchwabAuthError("Schwab refresh token rejected: invalid_grant")
        return wire.ok(wire.sized_chain(symbol, 3))

    harness.schwab.on_chain = on_chain

    await harness.runner(harness.plan(named=("DEAD", "NEXT"), fixed=(), small=())).run()

    records = by_key(harness)
    assert records["chain/DEAD/full"].outcome == Outcome.UNAVAILABLE
    assert records["chain/NEXT/full"].status == 200


async def test_a_missing_token_mid_run_costs_only_the_samples_it_covers(harness, wire):
    first = harness.first
    harness.token_missing_between = (first + timedelta(hours=1), first + timedelta(hours=2, minutes=30))
    plan = harness.plan(fixed=("AAPL",), small=())  # samples at +0h, +2h, +3h

    await harness.runner(plan).run()

    outcomes = {r.target: r.outcome for r in harness.out.records if r.kind == "timed"}
    assert outcomes == {
        first.isoformat(): Outcome.HTTP,
        (first + timedelta(hours=2)).isoformat(): Outcome.UNAVAILABLE,
        (first + timedelta(hours=3)).isoformat(): Outcome.HTTP,
    }


@pytest.mark.parametrize(
    "error",
    [
        SchwabAuthError("dead"),
        TokenMissingError("no Schwab token in the database"),
        OperationalError("select token", {}, Exception("database is down")),
    ],
    ids=["dead", "missing", "database"],
)
async def test_a_restart_retries_the_calls_that_had_no_token_but_misses_slots_that_passed(harness, wire, error):
    harness.client_error = error
    plan = harness.plan(named=("SPY",), fixed=("AAPL",), small=())
    await harness.runner(plan).run()
    assert {r.outcome for r in harness.out.records} == {Outcome.UNAVAILABLE}

    harness.client_error = None
    await harness.clock.sleep(30 * 60)  # the operator re-logs in half an hour after the last slot
    await harness.runner(plan).run()

    records = by_key(harness)
    assert records["chain/SPY/full"].outcome == Outcome.HTTP
    assert {r.outcome for r in records.values() if r.kind == "timed"} == {Outcome.MISSED}
    assert [c.symbol for c in harness.schwab.calls] == ["SPY"]


async def test_the_probe_never_exceeds_twenty_calls_a_minute(harness, wire):
    plan = harness.plan(named=tuple(f"N{n}" for n in range(30)), fixed=("AAPL", "MSFT"), small=())

    await harness.runner(plan).run()

    times = [c.at for c in harness.schwab.calls]
    assert len(times) == 30 + 3 * 2
    assert all(b - a >= timedelta(seconds=3) for a, b in zip(times, times[1:], strict=False))
    assert all(times[i + 20] - times[i] >= timedelta(seconds=60) for i in range(len(times) - 20))


async def test_samples_are_taken_when_due_and_a_due_sample_goes_before_the_rest_of_the_survey(harness, wire):
    harness.clock._now = harness.first - timedelta(seconds=7)
    listings = tuple(f"L{n}" for n in range(10))
    plan = harness.plan(samples=(harness.first,), listings=listings, fixed=("AAPL",), small=("SMALL",))

    await harness.runner(plan).run()

    symbols = [c.symbol for c in harness.schwab.calls]
    assert symbols.index("AAPL") < symbols.index("L9")
    assert all(c.at >= harness.first for c in harness.schwab.calls if c.symbol in ("AAPL", "SMALL"))
    assert symbols[:3] == ["L0", "L1", "L2"]


async def test_the_client_is_rebuilt_for_every_sample_but_reused_across_a_survey(harness, wire):
    plan = harness.plan(named=tuple(f"N{n}" for n in range(10)), fixed=("AAPL",), small=("SMALL",))

    await harness.runner(plan).run()

    # One for the survey (10 calls in 30 seconds), one for each of the three samples.
    assert harness.builds == 4


async def test_a_401_drops_the_client_and_the_call_is_tried_again_later(harness, wire):
    answers = iter([401])
    harness.schwab.on_chain = lambda symbol, start, end: (
        wire.status(next(answers, 200)) if symbol == "A" else wire.ok(wire.sized_chain(symbol, 3))
    )

    await harness.runner(harness.plan(named=("A", "B"), fixed=(), small=())).run()

    assert by_key(harness)["chain/A/full"].status == 200
    assert [c.symbol for c in harness.schwab.calls if c.kind == "chain"][:3] == ["A", "B", "A"]
    assert harness.builds == 3  # A, then B on a rebuilt client, then A again after the wait


async def test_a_survey_that_found_no_token_is_tried_again_once_the_token_is_back(harness, wire):
    harness.token_missing_between = (harness.clock.now(), harness.first - timedelta(minutes=30))
    plan = harness.plan(named=("SPY", "QQQ"), fixed=(), small=())

    await harness.runner(plan).run()

    records = by_key(harness)
    assert (records["chain/SPY/full"].status, records["chain/QQQ/full"].status) == (200, 200)
    assert all(c.at >= harness.first - timedelta(minutes=30) for c in harness.schwab.calls)


async def test_a_restart_skips_written_calls_and_marks_samples_that_passed_by_ten_minutes_missed(harness, wire):
    harness.clock._now = harness.first - timedelta(seconds=1)
    plan = harness.plan(fixed=("AAPL", "MSFT"), small=("SMALL",))
    harness.schwab.die_on_call = 2  # AAPL is written, then the process dies fetching MSFT
    with pytest.raises(harness.ProcessDied):
        await harness.runner(plan).run()
    assert [(r.symbol, r.outcome) for r in harness.out.records] == [("AAPL", Outcome.HTTP)]

    harness.schwab.die_on_call = None
    calls_before = len(harness.schwab.calls)
    harness.clock._now = harness.first + timedelta(minutes=25)
    await harness.runner(plan).run()

    records = [r for r in harness.out.records if r.kind == "timed"]
    first_slot = [r for r in records if r.target == harness.first.isoformat()]
    assert [(r.symbol, r.outcome) for r in first_slot] == [("AAPL", Outcome.HTTP), ("MSFT", Outcome.MISSED), ("SMALL", Outcome.MISSED)]
    assert first_slot[1].late_s == pytest.approx(1500, abs=1)
    assert len(harness.schwab.calls) - calls_before == 2 * 3  # the two later slots, all three tickers
    assert sum(1 for r in records if r.symbol == "AAPL" and r.target == harness.first.isoformat()) == 1


async def test_a_finished_run_restarted_makes_no_calls_at_all(harness, wire):
    plan = harness.plan(named=("SPY",), bars=("SPY",), listings=("L0",), fixed=("AAPL",), small=("SMALL",))
    await harness.runner(plan).run()
    calls, builds, records = len(harness.schwab.calls), harness.builds, len(harness.out.records)

    await harness.runner(plan).run()

    assert (len(harness.schwab.calls), harness.builds, len(harness.out.records)) == (calls, builds, records)


async def test_the_small_tickers_are_the_smallest_sampled_chains_and_stay_chosen(harness, wire):
    sizes = {"AAA": 500, "BBB": 30, "CCC": 25, "DDD": 5, "EEE": 100}
    harness.schwab.on_chain = lambda symbol, start, end: wire.ok(wire.sized_chain(symbol, sizes.get(symbol, 50)))
    plan = harness.plan(listings=tuple(sizes), fixed=("AAPL",), small=None)

    await harness.runner(plan).run()

    saved = harness.out.load_plan()
    assert saved.small_tickers == ("CCC", "BBB")
    assert {c.symbol for c in harness.schwab.calls if c.kind == "chain"} >= {"CCC", "BBB"}
    assert {r.symbol for r in harness.out.records if r.kind == "timed"} == {"AAPL", "CCC", "BBB"}


async def test_without_two_qualifying_chains_the_first_sampled_listings_fill_in(harness, wire):
    harness.schwab.on_chain = lambda symbol, start, end: wire.ok(wire.sized_chain(symbol, 4))
    plan = harness.plan(listings=("ZED", "BF.B", "ABE"), fixed=("AAPL",), small=None)

    await harness.runner(plan).run()

    assert harness.out.load_plan().small_tickers == ("ZED", "BF/B")


async def test_the_whole_schedule_runs_in_virtual_time_within_the_budget(harness, wire):
    listings = [Listing(f"S{n:03d}", "x") for n in range(80)]
    harness.clock._now = datetime(2026, 10, 28, 14, 0, tzinfo=EASTERN).astimezone(UTC)
    plan = build_plan(
        now=harness.clock.now(), calendar=ExchangeCalendar(), listings=listings, seed=5, sample_size=50, start=None
    )
    harness.out.save_plan(plan)

    await harness.runner(plan).run()

    records = harness.out.records
    timed = [r for r in records if r.kind == "timed"]
    assert len(plan.samples) == 22
    assert len(timed) == 22 * 5
    assert {r.outcome for r in timed} == {Outcome.HTTP}
    assert max(r.late_s for r in timed) < 60
    assert len([r for r in records if r.kind == "chain"]) == 9 + 2 + 50
    assert len([r for r in records if r.kind == "bars"]) == 7
    assert all(
        later.at - earlier.at >= timedelta(seconds=60)
        for earlier, later in zip(harness.schwab.calls, harness.schwab.calls[20:], strict=False)
    )
    assert json.loads((harness.directory / "plan.json").read_text())["small_tickers"]
