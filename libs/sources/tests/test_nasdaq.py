"""Nasdaq earnings-calendar adapter tests over httpx.MockTransport. Payloads are synthetic."""

import asyncio
from datetime import UTC, date, datetime

import httpx
import pytest

from rs_domain import EarningsTime
from rs_sources.errors import SourceError
from rs_sources.nasdaq import EarningsListing, NasdaqEarningsCalendar

NO_RECORD = {
    "data": None,
    "message": None,
    "status": {"rCode": 200, "bCodeMessage": [{"code": 1002, "errorMessage": "No record found."}]},
}


def past_row(symbol="AAA", **overrides):
    row = {
        "symbol": symbol,
        "name": "Alpha Co",
        "marketCap": "$1,000,000",
        "time": "time-after-hours",
        "fiscalQuarterEnding": "Sep/2026",
        "epsForecast": "$1.20",
        "noOfEsts": "7",
        "eps": "$1.33",
        "surprise": "10.83",
    }
    return row | overrides


def forward_row(symbol="AAA", **overrides):
    row = {
        "symbol": symbol,
        "name": "Alpha Co",
        "marketCap": "$1,000,000",
        "time": "time-pre-market",
        "fiscalQuarterEnding": "Dec/2026",
        "epsForecast": "$0.64",
        "noOfEsts": "12",
        "lastYearRptDt": "11/20/2025",
        "lastYearEPS": "$0.62",
    }
    return row | overrides


def day_payload(*rows):
    return {"data": {"asOf": "x", "headers": {"symbol": "Symbol"} if rows else None, "rows": list(rows) or None}}


class FakeNasdaq:
    def __init__(self, days=None, failing=()):
        self.days = days or {}
        self.failing = set(failing)
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        day = request.url.params["date"]
        if day in self.failing:
            return httpx.Response(500)
        return httpx.Response(200, json=self.days.get(day, day_payload()))


def make_calendar(fake, clock, **kwargs):
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake.handler))
    return NasdaqEarningsCalendar(http, clock, min_interval=0, **kwargs)


async def fetch(clock, *rows, day=date(2026, 10, 22)):
    fake = FakeNasdaq({day.isoformat(): day_payload(*rows)})
    return await make_calendar(fake, clock).day(day)


async def test_a_past_day_parses_forecast_actual_and_surprise(clock):
    result = await fetch(clock, past_row("aaa "))

    assert result.listings == (
        EarningsListing(
            ticker="AAA",
            report_date=date(2026, 10, 22),
            time_of_day=EarningsTime.AFTER_HOURS,
            fiscal_quarter="2026-09",
            eps_forecast=1.2,
            estimate_count=7,
            eps_actual=1.33,
            surprise_pct=10.83,
        ),
    )
    assert result.day == date(2026, 10, 22)
    assert result.fetched_at == clock.now()
    assert dict(result.unparsed) == {}


async def test_a_forward_day_has_no_actuals_and_that_is_not_a_null(clock):
    result = await fetch(clock, forward_row())

    [listing] = result.listings
    assert (listing.time_of_day, listing.eps_forecast, listing.estimate_count) == (EarningsTime.PRE_MARKET, 0.64, 12)
    assert listing.fiscal_quarter == "2026-12"
    assert listing.eps_actual is None and listing.surprise_pct is None
    assert dict(result.unparsed) == {}


@pytest.mark.parametrize(
    ("wire", "expected"),
    [
        ("$3.33", 3.33),
        ("$2", 2.0),
        ("($0.12)", -0.12),
        ("($1,234.50)", -1234.5),
        ("-123.6", -123.6),
        ("5.82", 5.82),
        ("$0.00", 0.0),
    ],
)
async def test_amounts_parse_with_parentheses_as_negative(clock, wire, expected):
    result = await fetch(clock, past_row(eps=wire, surprise=wire, epsForecast=wire))

    [listing] = result.listings
    assert (listing.eps_actual, listing.surprise_pct, listing.eps_forecast) == (expected, expected, expected)
    assert dict(result.unparsed) == {}


@pytest.mark.parametrize("wire", ["N/A", "", "  ", "n/a", "abc", "$", "()", "nan", "inf", "1_0", "1.2.3", None, 7])
async def test_unparseable_amounts_are_null_and_counted_per_field(clock, wire):
    result = await fetch(clock, past_row(eps=wire, surprise=wire, epsForecast=wire, noOfEsts=wire))

    [listing] = result.listings
    assert listing.eps_actual is None and listing.surprise_pct is None
    assert listing.eps_forecast is None and listing.estimate_count is None
    expected = {"eps_actual": 1, "surprise_pct": 1, "eps_forecast": 1, "estimate_count": 1}
    # 7 is a number but not text: Nasdaq serves display text, so anything else is unreadable too.
    assert dict(result.unparsed) == expected


@pytest.mark.parametrize(("wire", "expected"), [("7", 7), ("12", 12), ("0", 0), ("N/A", None), ("2.5", None), ("-1", None)])
async def test_estimate_counts_are_whole_non_negative_numbers(clock, wire, expected):
    [listing] = (await fetch(clock, past_row(noOfEsts=wire))).listings

    assert listing.estimate_count == expected


@pytest.mark.parametrize(
    ("wire", "expected"), [("Sep/2026", "2026-09"), ("Jan/2008", "2008-01"), ("Dec/2026", "2026-12"), ("Foo/2026", None), ("2026", None), ("", None)]
)
async def test_fiscal_quarters_parse_to_the_month_they_end(clock, wire, expected):
    result = await fetch(clock, past_row(fiscalQuarterEnding=wire))

    assert result.listings[0].fiscal_quarter == expected
    assert dict(result.unparsed) == ({} if expected else {"fiscal_quarter": 1})


async def test_unparsed_values_are_summed_over_the_rows_of_a_day(clock):
    result = await fetch(clock, past_row("AAA", eps="N/A"), past_row("BBB", eps="N/A", surprise="N/A"), past_row("CCC"))

    assert dict(result.unparsed) == {"eps_actual": 2, "surprise_pct": 1}


@pytest.mark.parametrize(
    ("wire", "expected", "counted"),
    [
        ("time-pre-market", EarningsTime.PRE_MARKET, False),
        ("time-after-hours", EarningsTime.AFTER_HOURS, False),
        ("time-not-supplied", None, False),
        ("time-something-new", None, True),
    ],
)
async def test_time_of_day_mapping(clock, wire, expected, counted):
    result = await fetch(clock, past_row(time=wire))

    assert result.listings[0].time_of_day == expected
    assert dict(result.unparsed) == ({"time_of_day": 1} if counted else {})


async def test_a_ticker_listed_twice_in_a_day_keeps_the_first(clock):
    result = await fetch(clock, past_row("AAA", eps="$1.00"), past_row("aaa", eps="$9.00"), past_row("BBB"))

    assert [(item.ticker, item.eps_actual) for item in result.listings] == [("AAA", 1.0), ("BBB", 1.33)]


async def test_weekend_rows_null_is_an_empty_day(clock):
    result = await fetch(clock)

    assert result.listings == ()


async def test_no_record_found_is_an_empty_day(clock):
    fake = FakeNasdaq({"2026-12-25": NO_RECORD})

    result = await make_calendar(fake, clock).day(date(2026, 12, 25))

    assert result.listings == ()


@pytest.mark.parametrize(
    "payload",
    [
        {"data": None, "status": {"rCode": 400, "bCodeMessage": None}},
        {"data": None, "status": {"rCode": 200, "bCodeMessage": [{"code": 1001}]}},
        {"data": None},
        {"data": {"rows": 5}},
        {"data": {"rows": {"symbol": "AAA"}}},
        {"data": {}},
        {"data": 7},
        {"nothing": True},
        [],
    ],
)
async def test_a_payload_of_the_wrong_shape_as_a_whole_is_a_source_error(clock, payload):
    fake = FakeNasdaq({"2026-10-22": payload})

    with pytest.raises(SourceError):
        await make_calendar(fake, clock).day(date(2026, 10, 22))


@pytest.mark.parametrize("bad", [{"symbol": None}, {"symbol": ""}, {"symbol": "  "}, {"symbol": 7}, None, "AAA", 5])
async def test_a_row_without_a_usable_symbol_is_skipped_and_counted_and_the_rest_of_the_day_stays(clock, bad):
    row = bad if not isinstance(bad, dict) else past_row() | bad
    result = await fetch(clock, past_row("AAA"), row, past_row("BBB"))

    assert [item.ticker for item in result.listings] == ["AAA", "BBB"]
    assert dict(result.unparsed) == {"symbol": 1}


async def test_a_row_with_no_symbol_key_at_all_is_skipped(clock):
    row = {k: v for k, v in past_row().items() if k != "symbol"}

    result = await fetch(clock, row, past_row("BBB"))

    assert [item.ticker for item in result.listings] == ["BBB"]
    assert dict(result.unparsed) == {"symbol": 1}


@pytest.mark.parametrize(
    ("key", "field"),
    [
        ("time", "time_of_day"),
        ("fiscalQuarterEnding", "fiscal_quarter"),
        ("epsForecast", "eps_forecast"),
        ("noOfEsts", "estimate_count"),
        ("eps", "eps_actual"),
        ("surprise", "surprise_pct"),
    ],
)
async def test_a_missing_value_is_null_and_counted_and_never_costs_the_day(clock, key, field):
    row = {k: v for k, v in past_row().items() if k != key}

    result = await fetch(clock, row, past_row("BBB"))

    assert [item.ticker for item in result.listings] == ["AAA", "BBB"]
    assert dict(result.unparsed) == {field: 1}


async def test_a_row_with_every_figure_missing_still_lists_the_ticker(clock):
    result = await fetch(clock, {"symbol": "aaa"})

    [listing] = result.listings
    assert (listing.ticker, listing.report_date, listing.eps_forecast, listing.eps_actual) == (
        "AAA",
        date(2026, 10, 22),
        None,
        None,
    )
    assert set(result.unparsed) == {"time_of_day", "fiscal_quarter", "eps_forecast", "estimate_count", "eps_actual", "surprise_pct"}


async def test_an_unhashable_time_value_is_null_and_counted(clock):
    result = await fetch(clock, forward_row() | {"time": ["x"]})

    assert result.listings[0].time_of_day is None
    assert dict(result.unparsed) == {"time_of_day": 1}


async def test_a_failed_request_is_a_source_error_with_its_status(clock):
    fake = FakeNasdaq(failing={"2026-10-22"})

    with pytest.raises(SourceError) as raised:
        await make_calendar(fake, clock).day(date(2026, 10, 22))

    assert raised.value.status_code == 500


async def test_request_asks_for_the_date_with_a_browser_user_agent(clock):
    fake = FakeNasdaq()

    await make_calendar(fake, clock).day(date(2026, 10, 22))

    [request] = fake.requests
    assert request.url.params["date"] == "2026-10-22"
    assert request.headers["user-agent"].startswith("Mozilla/5.0")


async def test_fetch_time_is_when_the_answer_arrived(clock):
    fake = FakeNasdaq({"2026-10-22": day_payload(past_row())})
    calendar = make_calendar(fake, clock)

    result = await calendar.day(date(2026, 10, 22))

    assert result.fetched_at == datetime(2026, 10, 19, 15, 0, tzinfo=UTC)


async def test_requests_are_spaced_by_the_minimum_interval(clock):
    loop = asyncio.get_running_loop()
    stamps: list[float] = []
    fake = FakeNasdaq()
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: (stamps.append(loop.time()), fake.handler(request))[1])
    )
    calendar = NasdaqEarningsCalendar(http, clock, min_interval=0.05)

    for day in (date(2026, 10, 20), date(2026, 10, 21), date(2026, 10, 22), date(2026, 10, 23)):
        await calendar.day(day)

    gaps = [b - a for a, b in zip(stamps, stamps[1:], strict=False)]
    assert len(stamps) == 4
    assert all(gap >= 0.04 for gap in gaps)
