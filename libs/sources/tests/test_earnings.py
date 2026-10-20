"""Next-earnings resolution: hand-written fakes at the two source seams, plus one
composition test through the real adapters."""

from datetime import date, datetime, timezone

import httpx
import pytest

from rs_sources.earnings import EarningsLookup, NextEarnings, _add_quarter
from rs_sources.edgar import EdgarClient, Filing8K
from rs_sources.errors import SourceError
from rs_sources.nasdaq import EarningsTime, NasdaqEarningsCalendar, ScheduledEarnings

AS_OF = datetime(2026, 10, 19, 15, 0, tzinfo=timezone.utc)


class FakeNasdaq:
    def __init__(self, result=None, error=False):
        self.result, self.error = result, error
        self.calls: list[tuple[str, date, int]] = []

    async def next_for(self, ticker, start, max_weekdays):
        self.calls.append((ticker, start, max_weekdays))
        if self.error:
            raise SourceError("nasdaq down")
        return self.result


class FakeFilings:
    def __init__(self, filings=(), error=False):
        self.filings, self.error = list(filings), error

    async def recent_8ks(self, ticker, as_of, limit=5):
        if self.error:
            raise SourceError("edgar down")
        return self.filings


def eight_k(filed, items=("2.02", "9.01"), form="8-K"):
    return Filing8K(
        accession_number=f"acc-{filed}-{form}",
        filing_date=filed,
        accepted_at=datetime.combine(filed, datetime.min.time(), tzinfo=timezone.utc),
        form=form,
        items=items,
        url="https://example.invalid/doc",
    )


async def test_nasdaq_date_is_returned_unestimated_with_time_of_day():
    scheduled = ScheduledEarnings(ticker="AAA", date=date(2026, 10, 29), time_of_day=EarningsTime.AFTER_HOURS)
    lookup = EarningsLookup(FakeNasdaq(scheduled), FakeFilings(error=True), scan_weekdays=12)

    result = await lookup.next_earnings("AAA", AS_OF)

    assert result == NextEarnings(date=date(2026, 10, 29), time_of_day=EarningsTime.AFTER_HOURS, estimated=False)


async def test_scan_starts_on_the_eastern_date():
    nasdaq = FakeNasdaq()
    lookup = EarningsLookup(nasdaq, FakeFilings(), scan_weekdays=12)

    # 02:00 UTC on the 20th is still the evening of the 19th in New York.
    await lookup.next_earnings("AAA", datetime(2026, 10, 20, 2, 0, tzinfo=timezone.utc))

    assert nasdaq.calls == [("AAA", date(2026, 10, 19), 12)]


async def test_nasdaq_failure_falls_back_to_last_item_202_plus_a_quarter():
    filings = FakeFilings([eight_k(date(2026, 9, 1), items=("5.02",)), eight_k(date(2026, 7, 30)), eight_k(date(2026, 4, 30))])
    lookup = EarningsLookup(FakeNasdaq(error=True), filings)

    result = await lookup.next_earnings("AAA", AS_OF)

    assert result == NextEarnings(date=date(2026, 10, 30), time_of_day=None, estimated=True)


async def test_nasdaq_without_a_listing_also_falls_back():
    lookup = EarningsLookup(FakeNasdaq(None), FakeFilings([eight_k(date(2026, 7, 30))]))

    result = await lookup.next_earnings("AAA", AS_OF)

    assert result.estimated
    assert result.date == date(2026, 10, 30)


async def test_estimate_ignores_amendments():
    filings = FakeFilings([eight_k(date(2026, 8, 20), form="8-K/A"), eight_k(date(2026, 7, 30))])

    result = await EarningsLookup(FakeNasdaq(error=True), filings).next_earnings("AAA", AS_OF)

    assert result.date == date(2026, 10, 30)


async def test_no_data_anywhere_is_an_explicit_unknown():
    lookup = EarningsLookup(FakeNasdaq(error=True), FakeFilings([eight_k(date(2026, 9, 1), items=("5.02",))]))

    result = await lookup.next_earnings("AAA", AS_OF)

    assert result == NextEarnings.unknown()
    assert result.date is None
    assert not result.estimated


async def test_both_sources_down_is_unknown_not_an_error():
    lookup = EarningsLookup(FakeNasdaq(error=True), FakeFilings(error=True))

    assert await lookup.next_earnings("AAA", AS_OF) == NextEarnings.unknown()


async def test_estimate_already_in_the_past_is_unknown():
    lookup = EarningsLookup(FakeNasdaq(error=True), FakeFilings([eight_k(date(2026, 6, 1))]))

    assert await lookup.next_earnings("AAA", AS_OF) == NextEarnings.unknown()


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        (date(2026, 7, 30), date(2026, 10, 30)),
        (date(2026, 11, 30), date(2027, 2, 28)),
        (date(2027, 11, 30), date(2028, 2, 29)),
        (date(2026, 12, 15), date(2027, 3, 15)),
    ],
)
def test_add_quarter_is_calendar_arithmetic_with_month_end_clamping(day, expected):
    assert _add_quarter(day) == expected


async def test_composed_through_real_adapters_nasdaq_outage_yields_estimate(clock):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.nasdaq.com":
            return httpx.Response(500)
        if request.url.path == "/files/company_tickers.json":
            return httpx.Response(200, json={"0": {"cik_str": 111, "ticker": "AAA", "title": "Alpha Co"}})
        return httpx.Response(
            200,
            json={
                "filings": {
                    "recent": {
                        "accessionNumber": ["0000000111-26-000004"],
                        "filingDate": ["2026-07-30"],
                        "acceptanceDateTime": ["2026-07-30T20:30:00.000Z"],
                        "form": ["8-K"],
                        "items": ["2.02,9.01"],
                        "primaryDocument": ["q2.htm"],
                    }
                }
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    lookup = EarningsLookup(
        NasdaqEarningsCalendar(http, clock, min_interval=0),
        EdgarClient(http, "RocketStocksTest ops@example.invalid", clock, min_interval=0),
    )

    result = await lookup.next_earnings("AAA", AS_OF)

    assert result == NextEarnings(date=date(2026, 10, 30), estimated=True)
