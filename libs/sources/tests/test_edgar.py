"""EDGAR adapter tests over httpx.MockTransport. Payloads are synthetic."""

from datetime import UTC, date, datetime, timedelta

import httpx
import pytest

from rs_sources.edgar import EdgarClient
from rs_sources.errors import SourceError

AS_OF = datetime(2026, 10, 19, 15, 0, tzinfo=UTC)
USER_AGENT = "RocketStocksTest ops@example.invalid"

TICKERS = {
    "0": {"cik_str": 111, "ticker": "AAA", "title": "Alpha Co"},
    "1": {"cik_str": 222, "ticker": "BRK-B", "title": "Class Share Co"},
}


def filing(accession, form, items, filed, accepted, document="doc.htm"):
    return dict(
        accessionNumber=accession,
        form=form,
        items=items,
        filingDate=filed,
        acceptanceDateTime=accepted,
        primaryDocument=document,
    )


def submissions(*rows):
    """EDGAR serves the recent block column-wise."""
    names = ["accessionNumber", "filingDate", "acceptanceDateTime", "form", "items", "primaryDocument"]
    return {"filings": {"recent": {name: [row[name] for row in rows] for name in names}}}


MIXED_FORMS = submissions(
    filing("0000000111-26-000005", "4", "", "2026-10-15", "2026-10-15T21:00:00.000Z", "form4.xml"),
    filing("0000000111-26-000004", "8-K", "2.02,9.01", "2026-10-14", "2026-10-14T20:30:00.000Z", "q3.htm"),
    filing("0000000111-26-000003", "10-Q", "", "2026-08-01", "2026-08-01T20:30:00.000Z", "q.htm"),
    filing("0000000111-26-000002", "8-K/A", "5.02", "2026-07-10", "2026-07-10T13:00:00.000Z", ""),
    filing("0000000111-26-000001", "8-K", "5.02", "2026-06-01", "2026-06-01T12:00:00.000Z", "exec.htm"),
)


class FakeEdgar:
    def __init__(self, tickers=TICKERS, subs=MIXED_FORMS):
        self.tickers, self.subs = tickers, subs
        self.requests: list[httpx.Request] = []
        self.fail_tickers = False

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/files/company_tickers.json":
            if self.fail_tickers:
                return httpx.Response(503)
            return httpx.Response(200, json=self.tickers)
        if request.url.path == "/submissions/CIK0000000111.json":
            return httpx.Response(200, json=self.subs)
        return httpx.Response(404)

    def paths(self) -> list[str]:
        return [r.url.path for r in self.requests]


def make_client(fake, clock, **kwargs):
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake.handler))
    return EdgarClient(http, USER_AGENT, clock, min_interval=0, **kwargs)


async def test_unlisted_ticker_has_no_cik_and_no_submissions_call(clock):
    fake = FakeEdgar()
    client = make_client(fake, clock)

    assert await client.cik("ZZZZ") is None
    assert await client.recent_8ks("ZZZZ", AS_OF) == []
    assert fake.paths() == ["/files/company_tickers.json"]


async def test_class_share_ticker_with_dot_resolves_to_hyphenated_entry(clock):
    client = make_client(FakeEdgar(), clock)

    assert await client.cik("brk.b") == 222


async def test_only_8k_forms_are_returned_newest_first(clock):
    client = make_client(FakeEdgar(), clock)

    filings = await client.recent_8ks("AAA", AS_OF)

    assert [(f.form, f.accession_number) for f in filings] == [
        ("8-K", "0000000111-26-000004"),
        ("8-K/A", "0000000111-26-000002"),
        ("8-K", "0000000111-26-000001"),
    ]


async def test_filing_carries_items_dates_and_document_link(clock):
    client = make_client(FakeEdgar(), clock)

    results = (await client.recent_8ks("AAA", AS_OF))[0]

    assert results.items == ("2.02", "9.01")
    assert results.filing_date == date(2026, 10, 14)
    assert results.accepted_at == datetime(2026, 10, 14, 20, 30, tzinfo=UTC)
    assert results.url == "https://www.sec.gov/Archives/edgar/data/111/000000011126000004/q3.htm"


async def test_filing_without_primary_document_links_to_the_index(clock):
    client = make_client(FakeEdgar(), clock)

    amendment = (await client.recent_8ks("AAA", AS_OF))[1]

    assert amendment.url.endswith("/000000011126000002/0000000111-26-000002-index.htm")


async def test_limit_keeps_the_newest(clock):
    client = make_client(FakeEdgar(), clock)

    filings = await client.recent_8ks("AAA", AS_OF, limit=1)

    assert [f.accession_number for f in filings] == ["0000000111-26-000004"]


async def test_filings_accepted_after_as_of_are_excluded(clock):
    client = make_client(FakeEdgar(), clock)

    filings = await client.recent_8ks("AAA", datetime(2026, 10, 14, 20, 29, tzinfo=UTC))

    assert [f.accession_number for f in filings] == ["0000000111-26-000002", "0000000111-26-000001"]


async def test_malformed_row_is_skipped_not_fatal(clock):
    subs = submissions(
        filing("0000000111-26-000009", "8-K", "2.02", "not-a-date", "2026-10-14T20:30:00.000Z"),
        filing("0000000111-26-000008", "8-K", "5.02", "2026-10-13", "2026-10-13T20:30:00.000Z"),
    )
    client = make_client(FakeEdgar(subs=subs), clock)

    filings = await client.recent_8ks("AAA", AS_OF)

    assert [f.accession_number for f in filings] == ["0000000111-26-000008"]


async def test_unexpected_submissions_shape_is_a_source_error(clock):
    client = make_client(FakeEdgar(subs={"filings": {}}), clock)

    with pytest.raises(SourceError):
        await client.recent_8ks("AAA", AS_OF)


async def test_http_failure_is_a_source_error(clock):
    fake = FakeEdgar()
    fake.fail_tickers = True
    client = make_client(fake, clock)

    with pytest.raises(SourceError):
        await client.cik("AAA")


async def test_declared_user_agent_is_sent(clock):
    fake = FakeEdgar()
    client = make_client(fake, clock)

    await client.recent_8ks("AAA", AS_OF)

    assert {r.headers["user-agent"] for r in fake.requests} == {USER_AGENT}


def test_blank_user_agent_is_refused(clock):
    with pytest.raises(ValueError, match="User-Agent"):
        EdgarClient(httpx.AsyncClient(), "  ", clock)


async def test_ticker_map_is_cached_until_it_expires(clock):
    fake = FakeEdgar()
    client = make_client(fake, clock, tickers_ttl=timedelta(hours=24))

    await client.cik("AAA")
    await client.cik("BRK-B")
    clock.advance(timedelta(hours=23))
    await client.cik("AAA")
    assert fake.paths().count("/files/company_tickers.json") == 1

    clock.advance(timedelta(hours=2))
    await client.cik("AAA")
    assert fake.paths().count("/files/company_tickers.json") == 2


async def test_stale_ticker_map_keeps_serving_when_refresh_fails(clock):
    fake = FakeEdgar()
    client = make_client(fake, clock, tickers_ttl=timedelta(hours=1))
    assert await client.cik("AAA") == 111

    fake.fail_tickers = True
    clock.advance(timedelta(hours=2))

    assert await client.cik("AAA") == 111


async def test_naive_as_of_is_refused(clock):
    client = make_client(FakeEdgar(), clock)

    with pytest.raises(ValueError, match="timezone-aware"):
        await client.recent_8ks("AAA", datetime(2026, 10, 19, 15, 0))
