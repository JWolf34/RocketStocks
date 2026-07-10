"""compare_bars (S003 B7b): the pure stream-vs-REST comparison and the REST
candle mapping. The DB read (BarRepository.list_for_day) is tested in
libs/persistence; the live Schwab REST call is exercised in the B8 acceptance run."""

from datetime import datetime, timedelta, timezone

import pytest

from rs_ingest.tools.compare_bars import (
    PRICE_TOLERANCE,
    Ohlcv,
    compare,
    fetch_rest_candles,
    format_report,
)

B0 = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)


def bkt(i: int) -> datetime:
    return B0 + timedelta(minutes=5 * i)


def test_matched_within_tolerance_passes():
    stream = {bkt(0): Ohlcv(100.0, 100.5, 99.5, 100.2, 1000.0)}
    rest = {bkt(0): Ohlcv(100.05, 100.48, 99.51, 100.19, 900.0)}  # open off most, by 0.05
    result = compare(stream, rest)

    assert result.within_tolerance()
    diff = result.matched[0]
    assert diff.worst_field == "open"
    assert diff.price_dev == pytest.approx(0.05)
    assert diff.volume_pct == pytest.approx(100 * (1000 - 900) / 900)  # reported, not gated


def test_price_deviation_over_tolerance_fails():
    stream = {bkt(0): Ohlcv(100.0, 100.0, 100.0, 100.0, 10.0)}
    rest = {bkt(0): Ohlcv(100.0, 100.4, 100.0, 100.0, 10.0)}  # high off by 0.40 > 0.25
    result = compare(stream, rest)

    assert not result.within_tolerance()
    assert result.max_price_dev == pytest.approx(0.40)
    assert result.matched[0].worst_field == "high"


def test_max_price_dev_is_the_worst_across_buckets():
    stream = {bkt(0): Ohlcv(100, 100, 100, 100, 1), bkt(1): Ohlcv(100, 100, 100, 100, 1)}
    rest = {bkt(0): Ohlcv(100, 100.10, 100, 100, 1), bkt(1): Ohlcv(100, 100, 100, 100.30, 1)}
    assert compare(stream, rest).max_price_dev == pytest.approx(0.30)


def test_unmatched_buckets_reported_but_do_not_fail_the_price_gate():
    stream = {bkt(0): Ohlcv(100, 100, 100, 100, 1), bkt(1): Ohlcv(100, 100, 100, 100, 1)}
    rest = {bkt(1): Ohlcv(100, 100, 100, 100, 1), bkt(2): Ohlcv(100, 100, 100, 100, 1)}
    result = compare(stream, rest)

    assert result.stream_only == [bkt(0)]
    assert result.rest_only == [bkt(2)]
    assert len(result.matched) == 1
    assert result.within_tolerance()  # coverage is reported, not gated (PRICE_TOLERANCE unused here)
    assert PRICE_TOLERANCE == 0.25


def test_total_misalignment_is_not_a_vacuous_pass():
    # bars on both sides but zero overlapping buckets -> FAIL, not a 0.0-dev pass
    stream = {bkt(0): Ohlcv(100, 100, 100, 100, 1)}
    rest = {bkt(1): Ohlcv(100, 100, 100, 100, 1)}
    result = compare(stream, rest)
    assert result.matched == []
    assert result.max_price_dev == 0.0
    assert not result.within_tolerance()  # data exists but nothing matched


def test_empty_both_sides_is_a_vacuous_pass():
    # genuinely nothing to compare (wrong/non-session day) -> vacuously PASS
    assert compare({}, {}).within_tolerance()


def test_zero_rest_volume_yields_none_pct_not_zero_division():
    stream = {bkt(0): Ohlcv(100, 100, 100, 100, 5)}
    rest = {bkt(0): Ohlcv(100, 100, 100, 100, 0)}
    assert compare(stream, rest).matched[0].volume_pct is None


def test_format_report_shows_verdict_counts_and_coverage():
    stream = {bkt(0): Ohlcv(100, 100, 100, 100, 1)}
    rest = {bkt(0): Ohlcv(100, 100.4, 100, 100, 1), bkt(1): Ohlcv(100, 100, 100, 100, 1)}
    report = format_report(compare(stream, rest), "SPY", B0.date())

    assert "FAIL" in report  # 0.40 > tolerance
    assert "rest-only" in report  # bkt(1) has no stream bar
    assert "SPY" in report


class _FakeResp:
    def __init__(self, candles):
        self._candles = candles

    def raise_for_status(self):
        pass

    def json(self):
        return {"candles": self._candles}


class _FakeClient:
    def __init__(self, candles):
        self._candles = candles
        self.calls = []

    async def get_price_history_every_five_minutes(self, symbol, **kwargs):
        self.calls.append((symbol, kwargs))
        return _FakeResp(self._candles)


async def test_fetch_rest_candles_maps_epoch_ms_and_fields():
    ms = int(B0.timestamp() * 1000)
    client = _FakeClient([{"datetime": ms, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100}])

    rest = await fetch_rest_candles(client, "SPY", B0.date())

    assert rest == {B0: Ohlcv(1.0, 2.0, 0.5, 1.5, 100.0)}  # epoch ms -> UTC bucket, fields mapped
    assert client.calls[0][0] == "SPY"
    assert client.calls[0][1]["need_extended_hours_data"] is False  # RTH-only, matches the stream


async def test_fetch_rest_candles_keeps_only_the_requested_day():
    # Schwab returns adjacent sessions for a period=1-day request (observed 2026-07-09:
    # a 7/9 query yields 7/8 + 7/9). fetch_rest_candles must keep only the requested UTC day,
    # or prior-session candles leak in as bogus "rest-only" coverage.
    prev = B0 - timedelta(days=1)
    client = _FakeClient(
        [
            {"datetime": int(prev.timestamp() * 1000), "open": 9, "high": 9, "low": 9, "close": 9, "volume": 1},
            {"datetime": int(B0.timestamp() * 1000), "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100},
        ]
    )

    rest = await fetch_rest_candles(client, "SPY", B0.date())

    assert set(rest) == {B0}  # the prior-day candle is dropped
    assert rest[B0] == Ohlcv(1.0, 2.0, 0.5, 1.5, 100.0)
