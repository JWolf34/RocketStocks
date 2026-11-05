"""The summary's answers, from sample sets crafted to have a known answer."""

import re
from datetime import UTC, date, datetime, timedelta

from rs_ingest.tools.sweep_probe.payloads import Snapshot
from rs_ingest.tools.sweep_probe.summary import (
    Comparison,
    _oi_answers,
    build_summary,
    compare,
    volume_verdict,
)

SECRET_PRICE = 4321.125
# The ladder's first two ranges start here, the third after them.
TODAY_NEAR = date(2026, 10, 28)
TODAY_MID = date(2026, 11, 28)


def samples(harness):
    """16:05 ET, 18:05 ET, then 02:05 and 03:05 ET the next morning."""
    first = harness.first
    return (first, first + timedelta(hours=2), first + timedelta(hours=10), first + timedelta(hours=11))


def script_overnight(harness, wire):
    """AAPL keeps its volume, quotes move at 18:05 and open interest moves at the third sample;
    NVDA's volume resets at the third sample and its open interest moves at the fourth."""

    def on_chain(symbol, start, end):
        elapsed = harness.clock.now() - harness.first
        evening, morning, later = (elapsed >= timedelta(hours=h) for h in (1, 9, 10.5))
        if symbol == "AAPL":
            oi = 1100 if morning else 1000
            contracts = [
                wire.contract(f"AAPL{n}", volume=volume, oi=oi, bid=SECRET_PRICE + (1 if evening else 0), quote_time=5 + evening)
                for n, volume in enumerate((100, 50, 0, 10))
            ]
            if morning:
                contracts = contracts[:3] + [wire.contract("AAPLNEW", volume=0, oi=5)]
            return wire.ok(wire.chain(contracts, underlying=SECRET_PRICE + (2 if evening else 0)))
        contracts = [
            wire.contract(f"NVDA{n}", volume=0 if morning else 20, oi=700 + (3 if later else 0)) for n in range(5)
        ]
        return wire.ok(wire.chain(contracts, underlying=SECRET_PRICE))

    harness.schwab.on_chain = on_chain


async def run_overnight(harness, wire):
    script_overnight(harness, wire)
    plan = harness.plan(samples=samples(harness), fixed=("AAPL", "NVDA"), small=())
    harness.out.save_plan(plan)
    await harness.runner(plan).run()
    return plan


def sections(summary, symbol, other):
    """(against the first sample, against the 18:05 sample) of one ticker's block."""
    block = summary.split(f"-- {symbol}")[1].split(f"-- {other}")[0]
    _, against_first, against_evening = re.split(r"vs (?:first|18:05) sample \(", block)
    return against_first, against_evening


async def test_the_summary_says_whether_volume_persists_or_resets_against_both_references(harness, wire):
    await run_overnight(harness, wire)

    summary = build_summary(harness.directory)

    for section in sections(summary, "AAPL", "NVDA"):
        assert re.search(r"volume at \S+ \S+ ET vs (first|18:05) sample: persists \(unchanged 2/2", section)
    for section in sections(summary + "-- END", "NVDA", "END"):
        assert re.search(r"vs (first|18:05) sample: resets to zero \(unchanged 0/5 \(0\.000\), zero 5/5 \(1\.000\)", section)


async def test_the_summary_says_which_sample_open_interest_changes_at_over_contracts_in_both(harness, wire):
    await run_overnight(harness, wire)

    summary = build_summary(harness.directory)

    aapl = sections(summary, "AAPL", "NVDA")
    nvda = sections(summary + "-- END", "NVDA", "END")
    # 10-28 20:05Z is 16:05 ET; +10h is 02:05 ET and +11h is 03:05 ET the next day.
    for section in aapl:
        assert "open interest: first difference at 10-29 02:05 ET" in section
        assert "90% of the change seen by 10-29 02:05 ET; at 10-29 03:05 ET 3/3 (1.000) of common contracts differ" in section
    for section in nvda:
        assert "open interest: first difference at 10-29 03:05 ET" in section
        assert "non-zero samples): 10-29 03:05=5" in section


async def test_the_summary_says_which_quotes_underlying_and_volatility_change_after_the_close(harness, wire):
    await run_overnight(harness, wire)

    summary = build_summary(harness.directory)

    against_first, against_evening = sections(summary, "AAPL", "NVDA")
    assert "bid: first difference at 10-28 18:05 ET" in against_first
    assert "underlying price: first difference at 10-28 18:05 ET; differs at 3 of 3 later samples" in against_first
    assert "implied volatility: first difference none" in against_first
    # Nothing moves between 18:05 and the morning but the open interest.
    assert "bid: first difference none" in against_evening
    assert "underlying price: first difference none" in against_evening


async def test_the_summary_counts_the_contracts_present_on_only_one_side(harness, wire):
    await run_overnight(harness, wire)

    summary = build_summary(harness.directory)

    for section in sections(summary, "AAPL", "NVDA"):
        assert re.search(r"contracts on one side only at 10-29 03:05 ET: 1 only in the \S+( sample)?, 1 only in the sample", section)
        assert re.search(r"10-29 03:05 +4 +3 +1 +1 ", section)  # contracts, common, ref only, new


async def test_the_summary_carries_no_prices_and_no_contract_symbols(harness, wire):
    await run_overnight(harness, wire)

    summary = build_summary(harness.directory)

    assert "4321" not in summary
    assert not re.search(r"AAPL\d|NVDA\d|AAPLNEW", summary)


async def test_a_sample_answered_in_expiry_ranges_is_compared_over_the_contracts_it_has_in_common(harness, wire):
    contracts = [f"AAPL{n}" for n in range(4)]

    def on_chain(symbol, start, end):
        if harness.clock.now() < harness.first + timedelta(hours=1):
            return wire.ok(wire.chain([wire.contract(c, volume=5, oi=1) for c in contracts]))
        if start is None:
            return wire.status(502)
        part = {TODAY_NEAR: contracts[:2], TODAY_MID: contracts[2:3]}.get(start, contracts[3:])
        return wire.ok(wire.chain([wire.contract(c, volume=5, oi=1) for c in part]))

    harness.schwab.on_chain = on_chain
    plan = harness.plan(samples=samples(harness)[:2], fixed=("AAPL",), small=())
    harness.out.save_plan(plan)

    await harness.runner(plan).run()

    summary = build_summary(harness.directory)
    assert re.search(r"10-28 18:05 +4p +4 +0 +0 +4 ", summary)
    assert "volume at 10-28 18:05 ET vs first sample: persists (unchanged 4/4" in summary


async def test_the_summary_of_a_run_cut_short_says_what_is_missing(harness, wire):
    script_overnight(harness, wire)
    plan = harness.plan(
        samples=samples(harness), named=("SPY",), listings=("L0", "L1"), bars=("IBM",), fixed=("AAPL",), small=()
    )
    harness.out.save_plan(plan)
    harness.schwab.die_on_call = 5  # the four survey calls, then the first sample's AAPL
    try:
        await harness.runner(plan).run()
    except harness.ProcessDied:
        pass

    summary = build_summary(harness.directory)

    assert "survey: 4 of 4 tasks have a record" in summary
    assert "timed samples: 0 of 4 complete across 1 tickers" in summary
    assert "10-28 16:05 ET: ok 0, no record 1, missed 0, no token 0, empty 0, failed 0" in summary
    assert "no usable sample (10-28 16:05 ET (not fetched); 10-28 18:05 ET (not fetched);" in summary


async def test_the_summary_names_survey_tasks_that_were_never_fetched(harness, wire):
    plan = harness.plan(named=("SPY", "QQQ"), listings=("L0",), bars=("IBM",), fixed=("AAPL",), small=())
    harness.out.save_plan(plan)

    summary = build_summary(harness.directory)

    assert "survey: 0 of 4 tasks have a record" in summary
    assert "no record: SPY, QQQ, IBM, L0" in summary
    assert "SPY      not fetched" in summary


def test_a_directory_without_a_plan_has_nothing_to_summarise(tmp_path):
    assert "nothing to summarise" in build_summary(tmp_path)


async def test_the_chain_section_gives_sizes_failures_and_the_ladder_without_contract_rows(harness, wire):
    def on_chain(symbol, start, end):
        if symbol in ("SPY", "L1") and start is None:
            return wire.status(502)
        return wire.ok(wire.sized_chain(symbol, 10 + len(symbol), truncated=False))

    harness.schwab.on_chain = on_chain
    plan = harness.plan(named=("SPY", "BRK/B"), listings=("L0", "L1", "BF.B"), fixed=(), small=())
    harness.out.save_plan(plan)
    await harness.runner(plan).run()

    summary = build_summary(harness.directory)

    assert re.search(r"SPY +full: 502 0\.00MB", summary)
    assert re.search(r"<=30d: 200 13 contracts 1 expiry", summary)
    assert "as a chain: in parts, 39 contracts, 3 expiries" in summary
    assert "fetched 3 of 3; chains: in parts: 1, whole: 2" in summary
    assert "3 usable chains (1 answered in parts, summed and flagged):" in summary
    # L0 and BF/B answer whole with 12 and 14 contracts; L1 answers in three ranges of 12 each.
    assert "contracts: min 12, median 14, p90 36, max 36, mean 21; total 62" in summary
    assert "expiries: min 1, median 1, p90 3, max 3, mean 2" in summary
    assert "chain status in the responses: SUCCESS: 5" in summary
    assert "isChainTruncated true on 0 of 3 chains" in summary
    assert "1 chains went down the ladder (fetched again, then in expiry ranges); range results:" in summary
    assert "<=30d: 200: 1" in summary
    assert "sizes when summed: L1 36 contracts" in summary
    assert "class share spelling - BF.B: BF/B 200" in summary


async def test_empty_chains_are_reported_and_left_out_of_the_spread(harness, wire):
    def on_chain(symbol, start, end):
        if symbol == "FAILED":
            return wire.ok({**wire.sized_chain(symbol, 9), "status": "FAILED"})
        if symbol == "ZERO":
            return wire.ok(wire.chain([]))
        return wire.ok(wire.sized_chain(symbol, 30))

    harness.schwab.on_chain = on_chain
    plan = harness.plan(listings=("FAILED", "ZERO", "REAL"), fixed=(), small=())
    harness.out.save_plan(plan)
    await harness.runner(plan).run()

    summary = build_summary(harness.directory)

    assert "fetched 3 of 3; chains: empty: 2, whole: 1" in summary
    assert "empty (a 200 with status FAILED or no contracts; left out of the spreads): FAILED, ZERO" in summary
    assert "contracts: min 30, median 30, p90 30, max 30, mean 30; total 30" in summary
    assert "chain status in the responses: FAILED: 1, SUCCESS: 2" in summary


async def test_the_bars_section_gives_depth_and_both_spellings(harness, wire):
    harness.schwab.on_history = lambda symbol: (
        wire.ok(wire.candles(631_281_600_000, 1_790_000_000_000))
        if symbol != "BRK/B"
        else wire.status(400, "bad symbol")
    )
    plan = harness.plan(bars=("SPY", "BRK.B", "BRK/B"), fixed=(), small=())
    harness.out.save_plan(plan)
    await harness.runner(plan).run()

    summary = build_summary(harness.directory)

    assert "candles 2, earliest 1990-01-02" in summary.split("BRK.B")[1]
    assert re.search(r"BRK/B +400", summary)


def test_volume_verdicts_need_nearly_everything_to_agree():
    assert volume_verdict(Comparison(volume_positive=100, volume_same=95)) == "persists"
    assert volume_verdict(Comparison(volume_positive=100, volume_zero=96)) == "resets to zero"
    assert volume_verdict(Comparison(volume_positive=100, volume_same=60, volume_zero=30)) == "mixed"
    assert volume_verdict(Comparison()) == "no contract had volume at the reference sample"


def test_compare_counts_over_the_contracts_present_in_both(wire):
    before = wire.chain([wire.contract("A", volume=5, oi=1), wire.contract("B", volume=5, oi=1), wire.contract("GONE")])
    later = wire.chain([wire.contract("A", volume=9, oi=2), wire.contract("B", volume=0, oi=1), wire.contract("NEW")])

    result = compare(Snapshot.of(before), Snapshot.of(later))

    assert (result.common, result.reference_only, result.sample_only) == (2, 1, 1)
    assert (result.volume_up, result.volume_down, result.volume_zero) == (1, 1, 1)
    assert result.oi_changed == 1
    assert result.underlying_changed is False


async def test_an_unusable_first_sample_hands_the_reference_to_the_next_usable_one(harness, wire):
    script_overnight(harness, wire)
    plan = harness.plan(samples=samples(harness), fixed=("AAPL",), small=())
    harness.out.save_plan(plan)
    harness.token_missing_between = (harness.first - timedelta(hours=2), harness.first + timedelta(hours=1))
    await harness.runner(plan).run()

    summary = build_summary(harness.directory)

    assert "unusable, so not a reference: 10-28 16:05 ET (no usable token)" in summary
    assert "first sample 10-28 18:05 ET:" in summary
    assert "vs 18:05 sample" not in summary
    assert "volume at 10-29 03:05 ET vs first sample: persists" in summary


def test_open_interest_settling_is_measured_against_the_largest_change_seen():
    moment = datetime(2026, 10, 29, 10, 0, tzinfo=UTC)
    series = [(moment + timedelta(hours=n), Comparison(common=10, oi_changed=c)) for n, c in enumerate((0, 8, 10, 0))]

    answer = _oi_answers("first sample", series)[0]

    assert "first difference at 10-29 07:00 ET" in answer
    assert "90% of the change seen by 10-29 08:00 ET" in answer
