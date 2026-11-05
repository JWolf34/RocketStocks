"""Next-earnings resolution: hand-written fakes at the two source seams, plus one
composition test through the real EDGAR adapter."""

from datetime import UTC, date, datetime

import httpx
import pytest

from rs_domain import EarningsOrigin, EarningsTime, StoredEarnings
from rs_sources.earnings import EarningsLookup, NextEarnings
from rs_sources.edgar import EdgarClient, Filing8K
from rs_sources.errors import SourceError

AS_OF = datetime(2026, 10, 19, 15, 0, tzinfo=UTC)


def listed(report_date, time_of_day=None):
    return StoredEarnings(
        snapshot_date=date(2026, 10, 18),
        origin=EarningsOrigin.NIGHTLY,
        ticker="AAA",
        report_date=report_date,
        time_of_day=time_of_day,
        fiscal_quarter="2026-09",
        eps_forecast=1.0,
        estimate_count=3,
        eps_actual=None,
        surprise_pct=None,
        fetched_at=datetime(2026, 10, 19, 0, 30, tzinfo=UTC),
    )


class FakeCalendar:
    """The stored calendar: `result` is what the latest fresh snapshot lists, None when it does not."""

    def __init__(self, result=None):
        self.result = result
        self.calls: list[tuple[str, date]] = []

    async def next_listed(self, ticker, today):
        self.calls.append((ticker, today))
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
        accepted_at=datetime.combine(filed, datetime.min.time(), tzinfo=UTC),
        form=form,
        items=items,
        url="https://example.invalid/doc",
    )


async def test_a_listed_date_is_returned_unestimated_with_time_of_day():
    calendar = FakeCalendar(listed(date(2026, 10, 29), EarningsTime.AFTER_HOURS))
    lookup = EarningsLookup(calendar, FakeFilings(error=True))

    result = await lookup.next_earnings("AAA", AS_OF)

    assert result == NextEarnings(date=date(2026, 10, 29), time_of_day=EarningsTime.AFTER_HOURS, estimated=False)


async def test_the_lookup_asks_for_the_eastern_date():
    calendar = FakeCalendar()
    lookup = EarningsLookup(calendar, FakeFilings())

    # 02:00 UTC on the 20th is still the evening of the 19th in New York.
    await lookup.next_earnings("AAA", datetime(2026, 10, 20, 2, 0, tzinfo=UTC))

    assert calendar.calls == [("AAA", date(2026, 10, 19))]


async def test_a_ticker_the_calendar_does_not_list_falls_back_to_last_item_202_plus_a_quarter():
    filings = FakeFilings([eight_k(date(2026, 9, 1), items=("5.02",)), eight_k(date(2026, 7, 30)), eight_k(date(2026, 4, 30))])
    lookup = EarningsLookup(FakeCalendar(None), filings)

    result = await lookup.next_earnings("AAA", AS_OF)

    assert result == NextEarnings(date=date(2026, 10, 30), time_of_day=None, estimated=True)


async def test_estimate_ignores_amendments():
    filings = FakeFilings([eight_k(date(2026, 8, 20), form="8-K/A"), eight_k(date(2026, 7, 30))])

    result = await EarningsLookup(FakeCalendar(), filings).next_earnings("AAA", AS_OF)

    assert result.date == date(2026, 10, 30)


async def test_no_data_anywhere_is_an_explicit_unknown():
    lookup = EarningsLookup(FakeCalendar(), FakeFilings([eight_k(date(2026, 9, 1), items=("5.02",))]))

    result = await lookup.next_earnings("AAA", AS_OF)

    assert result == NextEarnings.unknown()
    assert result.date is None
    assert not result.estimated


async def test_both_sources_down_is_unknown_not_an_error():
    lookup = EarningsLookup(FakeCalendar(), FakeFilings(error=True))

    assert await lookup.next_earnings("AAA", AS_OF) == NextEarnings.unknown()


async def test_estimate_already_in_the_past_is_unknown():
    lookup = EarningsLookup(FakeCalendar(), FakeFilings([eight_k(date(2026, 6, 1))]))

    assert await lookup.next_earnings("AAA", AS_OF) == NextEarnings.unknown()


@pytest.mark.parametrize(
    ("filed", "expected"),
    [
        (date(2026, 7, 30), date(2026, 10, 30)),
        (date(2026, 11, 30), date(2027, 2, 28)),
        (date(2027, 11, 30), date(2028, 2, 29)),
        (date(2026, 12, 15), date(2027, 3, 15)),
    ],
)
async def test_estimate_is_calendar_quarter_arithmetic_with_month_end_clamping(filed, expected):
    lookup = EarningsLookup(FakeCalendar(), FakeFilings([eight_k(filed)]))

    result = await lookup.next_earnings("AAA", datetime.combine(filed, datetime.min.time(), tzinfo=UTC))

    assert result == NextEarnings(date=expected, estimated=True)


async def test_naive_as_of_is_refused():
    lookup = EarningsLookup(FakeCalendar(), FakeFilings())

    with pytest.raises(ValueError, match="timezone-aware"):
        await lookup.next_earnings("AAA", datetime(2026, 10, 19, 15, 0))


async def test_composed_through_the_real_edgar_adapter_an_unlisted_ticker_yields_the_estimate(clock):
    def handler(request: httpx.Request) -> httpx.Response:
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
        FakeCalendar(), EdgarClient(http, "RocketStocksTest ops@example.invalid", clock, min_interval=0)
    )

    result = await lookup.next_earnings("AAA", AS_OF)

    assert result == NextEarnings(date=date(2026, 10, 30), estimated=True)
