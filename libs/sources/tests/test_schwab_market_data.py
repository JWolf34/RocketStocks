"""Schwab quote and daily history through a fake schwab-py client.

Payloads are synthetic. Dates: 2026-07-06 (Mon) .. 2026-07-10 (Fri) are
sessions; 2026-07-03 (Fri) is the Independence Day holiday; 2026-07-04/05 is
a weekend.
"""

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import pytest
from authlib.integrations.base_client import OAuthError

from rs_sources import ExchangeCalendar
from rs_sources.schwab import (
    RejectReason,
    SchwabAuthError,
    SchwabMarketData,
    SchwabResponseError,
)

ET = ZoneInfo("America/New_York")


def millis(day: date, hour: int = 0) -> int:
    """Epoch ms for an ET wall time (daily candles are stamped inside their ET day)."""
    return int(datetime(day.year, day.month, day.day, hour, tzinfo=ET).timestamp() * 1000)


def candle(day: date, close: float = 101.0, **overrides):
    raw = {"open": 100.0, "high": 102.0, "low": 99.0, "close": close, "volume": 1_000, "datetime": millis(day)}
    return {**raw, **overrides}


class FakeResponse:
    def __init__(self, payload=None, status_code=200):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSchwabClient:
    """Records calls and answers with a canned response (or raises it)."""

    def __init__(self, response):
        self.response = response
        self.history_calls = []
        self.quote_calls = []

    def _answer(self):
        if isinstance(self.response, Exception):
            raise self.response
        return self.response

    async def get_quotes(self, symbols, **kwargs):
        self.quote_calls.append((symbols, kwargs))
        return self._answer()

    async def get_price_history_every_day(self, symbol, **kwargs):
        self.history_calls.append((symbol, kwargs))
        return self._answer()


@pytest.fixture(scope="module")
def calendar():
    return ExchangeCalendar()


def history_of(candles, symbol="SPY"):
    return FakeResponse({"symbol": symbol, "empty": not candles, "candles": candles})


def market(response, calendar):
    client = FakeSchwabClient(response)
    return SchwabMarketData(client, calendar), client


class TestQuote:
    def quote_payload(self, **overrides):
        raw = {
            "lastPrice": 523.45,
            "closePrice": 520.1,
            "quoteTime": int(datetime(2026, 7, 6, 15, 30, tzinfo=UTC).timestamp() * 1000),
            "totalVolume": 41_000_000,
        }
        return FakeResponse({"SPY": {"symbol": "SPY", "quote": {**raw, **overrides}}})

    async def test_returns_price_previous_close_time_and_volume(self, calendar):
        data, client = market(self.quote_payload(), calendar)
        quote = await data.quote(" spy ")
        assert client.quote_calls == [(["SPY"], {})]
        assert quote.symbol == "SPY"
        assert quote.last_price == 523.45
        assert quote.previous_close == 520.1
        assert quote.quote_time == datetime(2026, 7, 6, 15, 30, tzinfo=UTC)
        assert quote.volume == 41_000_000

    async def test_unknown_symbol_is_none(self, calendar):
        data, _ = market(FakeResponse({"errors": {"invalidSymbols": ["ZZZZ"]}}), calendar)
        assert await data.quote("ZZZZ") is None

    @pytest.mark.parametrize(
        "overrides",
        [{"quoteTime": 0}, {"lastPrice": 0}, {"closePrice": -1.0}, {"totalVolume": None}, {"lastPrice": "n/a"}],
    )
    async def test_unusable_quote_raises(self, calendar, overrides):
        data, _ = market(self.quote_payload(**overrides), calendar)
        with pytest.raises(SchwabResponseError, match="unusable quote"):
            await data.quote("SPY")

    async def test_missing_field_raises(self, calendar):
        data, _ = market(FakeResponse({"SPY": {"quote": {"lastPrice": 5.0}}}), calendar)
        with pytest.raises(SchwabResponseError):
            await data.quote("SPY")

    async def test_non_ok_status_raises_with_status_code(self, calendar):
        data, _ = market(FakeResponse({}, status_code=503), calendar)
        with pytest.raises(SchwabResponseError) as info:
            await data.quote("SPY")
        assert info.value.status_code == 503

    async def test_dead_refresh_token_is_a_schwab_auth_error(self, calendar):
        data, _ = market(OAuthError(error="invalid_grant"), calendar)
        with pytest.raises(SchwabAuthError):
            await data.quote("SPY")


class TestDailyHistoryRequest:
    async def test_bounds_are_aware_eastern_full_days_without_extended_hours(self, calendar):
        data, client = market(history_of([]), calendar)
        await data.daily_history("spy", date(2026, 7, 6), date(2026, 7, 6))
        ((symbol, kwargs),) = client.history_calls
        assert symbol == "SPY"
        start, end = kwargs["start_datetime"], kwargs["end_datetime"]
        assert start.utcoffset() is not None and end.utcoffset() is not None
        assert start.astimezone(ET).date() == date(2026, 7, 6) == end.astimezone(ET).date()
        assert end > start  # a single-day request is never zero-width (Schwab 400s on that)
        assert kwargs["need_extended_hours_data"] is False

    async def test_start_after_end_is_rejected_before_any_call(self, calendar):
        data, client = market(history_of([]), calendar)
        with pytest.raises(ValueError):
            await data.daily_history("SPY", date(2026, 7, 7), date(2026, 7, 6))
        assert client.history_calls == []


class TestDailyHistoryValidation:
    async def test_clean_bars_come_back_in_session_order(self, calendar):
        candles = [candle(date(2026, 7, 7), close=103.0), candle(date(2026, 7, 6))]
        data, _ = market(history_of(candles), calendar)
        result = await data.daily_history("SPY", date(2026, 7, 6), date(2026, 7, 10))
        assert [b.session_date for b in result.bars] == [date(2026, 7, 6), date(2026, 7, 7)]
        assert result.bars[1].close == 103.0
        assert result.bars[0].volume == 1_000
        assert result.rejects == ()

    async def test_session_date_is_the_eastern_date(self, calendar):
        # 05:00 UTC in July is 01:00 ET the same calendar day.
        stamp = int(datetime(2026, 7, 6, 5, tzinfo=UTC).timestamp() * 1000)
        data, _ = market(history_of([candle(date(2026, 7, 6), datetime=stamp)]), calendar)
        result = await data.daily_history("SPY", date(2026, 7, 6), date(2026, 7, 6))
        assert [b.session_date for b in result.bars] == [date(2026, 7, 6)]

    @pytest.mark.parametrize("day", [date(2026, 7, 3), date(2026, 7, 4)], ids=["holiday", "weekend"])
    async def test_non_session_date_is_rejected_and_returned(self, calendar, day):
        data, _ = market(history_of([candle(day), candle(date(2026, 7, 6))]), calendar)
        result = await data.daily_history("SPY", date(2026, 7, 3), date(2026, 7, 10))
        assert [b.session_date for b in result.bars] == [date(2026, 7, 6)]
        (reject,) = result.rejects
        assert reject.reason is RejectReason.NOT_A_SESSION
        assert reject.session_date == day
        assert reject.symbol == "SPY"

    async def test_epoch_zero_is_rejected_not_a_crash(self, calendar):
        data, _ = market(history_of([candle(date(2026, 7, 6), datetime=0)]), calendar)
        result = await data.daily_history("SPY", date(2026, 7, 6), date(2026, 7, 10))
        assert result.bars == ()
        (reject,) = result.rejects
        assert reject.reason is RejectReason.EPOCH_ZERO
        assert reject.timestamp_ms == 0

    async def test_dates_outside_the_calendar_range_are_rejected(self, calendar):
        # 1969 and 2100 are outside the calendar's supported range, which raises there.
        stamps = [-86_400_000 * 365, millis(date(2100, 1, 4))]
        data, _ = market(history_of([candle(date(2026, 7, 6), datetime=s) for s in stamps]), calendar)
        result = await data.daily_history("SPY", date(2026, 7, 6), date(2026, 7, 10))
        assert result.bars == ()
        assert [r.reason for r in result.rejects] == [RejectReason.NOT_A_SESSION] * 2

    async def test_session_outside_the_requested_window_is_rejected(self, calendar):
        data, _ = market(history_of([candle(date(2026, 7, 8)), candle(date(2026, 7, 6))]), calendar)
        result = await data.daily_history("SPY", date(2026, 7, 6), date(2026, 7, 7))
        assert [b.session_date for b in result.bars] == [date(2026, 7, 6)]
        assert [r.reason for r in result.rejects] == [RejectReason.OUTSIDE_WINDOW]

    async def test_second_candle_for_a_session_is_a_duplicate(self, calendar):
        candles = [candle(date(2026, 7, 6)), candle(date(2026, 7, 6), close=999.0)]
        data, _ = market(history_of(candles), calendar)
        result = await data.daily_history("SPY", date(2026, 7, 6), date(2026, 7, 6))
        assert [b.close for b in result.bars] == [101.0]
        assert [r.reason for r in result.rejects] == [RejectReason.DUPLICATE_SESSION]

    @pytest.mark.parametrize(
        "bad",
        [{"close": 0}, {"open": -1.0}, {"volume": -5}, {"high": float("nan")}, {"volume": None}],
    )
    async def test_malformed_candle_is_rejected_and_the_rest_kept(self, calendar, bad):
        candles = [candle(date(2026, 7, 6), **bad), candle(date(2026, 7, 7))]
        data, _ = market(history_of(candles), calendar)
        result = await data.daily_history("SPY", date(2026, 7, 6), date(2026, 7, 10))
        assert [b.session_date for b in result.bars] == [date(2026, 7, 7)]
        assert [r.reason for r in result.rejects] == [RejectReason.MALFORMED]

    async def test_candle_missing_its_timestamp_is_malformed(self, calendar):
        raw = candle(date(2026, 7, 6))
        del raw["datetime"]
        data, _ = market(history_of([raw]), calendar)
        result = await data.daily_history("SPY", date(2026, 7, 6), date(2026, 7, 10))
        assert [r.reason for r in result.rejects] == [RejectReason.MALFORMED]

    async def test_empty_history_is_empty_not_an_error(self, calendar):
        data, _ = market(FakeResponse({"symbol": "ZZZZ", "empty": True}), calendar)
        result = await data.daily_history("ZZZZ", date(2026, 7, 6), date(2026, 7, 10))
        assert result.bars == () and result.rejects == ()

    async def test_non_ok_status_raises_with_status_code(self, calendar):
        data, _ = market(FakeResponse({}, status_code=400), calendar)
        with pytest.raises(SchwabResponseError) as info:
            await data.daily_history("SPY", date(2026, 7, 6), date(2026, 7, 6))
        assert info.value.status_code == 400

    async def test_non_json_body_raises(self, calendar):
        data, _ = market(FakeResponse(ValueError("bad json")), calendar)
        with pytest.raises(SchwabResponseError, match="not JSON"):
            await data.daily_history("SPY", date(2026, 7, 6), date(2026, 7, 6))

    async def test_dead_refresh_token_is_a_schwab_auth_error(self, calendar):
        data, _ = market(OAuthError(error="invalid_grant"), calendar)
        with pytest.raises(SchwabAuthError):
            await data.daily_history("SPY", date(2026, 7, 6), date(2026, 7, 6))
