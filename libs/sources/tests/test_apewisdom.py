"""ApeWisdom adapter behaviour through its public client, against a fake HTTP transport.

Payloads are synthetic: the real API returns numbers as strings, so rows use
strings where the live shape does.
"""

import logging

import httpx
import pytest

from rs_sources import ApeWisdomClient, ApeWisdomError, ApeWisdomRow


def raw(rank, ticker, mentions="20", upvotes="40", rank_24h_ago="9", mentions_24h_ago="10"):
    return {
        "rank": str(rank),
        "ticker": ticker,
        "name": f"{ticker} Inc",
        "mentions": mentions,
        "upvotes": upvotes,
        "rank_24h_ago": rank_24h_ago,
        "mentions_24h_ago": mentions_24h_ago,
    }


def page(results, pages=1, number=1):
    return {"count": 999, "pages": pages, "current_page": number, "results": results}


def ok(body):
    return httpx.Response(200, json=body)


class FakeApeWisdom:
    """Serves queued outcomes per page number (the last one repeats); records requests."""

    def __init__(self, **outcomes):
        self.outcomes = {int(key.removeprefix("page")): list(value) for key, value in outcomes.items()}
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        queue = self.outcomes[int(request.url.path.rsplit("/", 1)[1])]
        outcome = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    @property
    def pages_requested(self) -> list[int]:
        return [int(r.url.path.rsplit("/", 1)[1]) for r in self.requests]


def make_client(fake, sleeps=None, **kwargs):
    async def sleep(seconds):
        if sleeps is not None:
            sleeps.append(seconds)

    http = httpx.AsyncClient(transport=httpx.MockTransport(fake))
    return ApeWisdomClient(http, sleep=sleep, **kwargs)


async def test_fetches_every_page_in_order():
    fake = FakeApeWisdom(
        page1=[ok(page([raw(1, "AMD"), raw(2, "NVDA")], pages=3, number=1))],
        page2=[ok(page([raw(3, "TSLA")], pages=3, number=2))],
        page3=[ok(page([raw(4, "GME")], pages=3, number=3))],
    )

    rows = await make_client(fake).fetch_all()

    assert [r.ticker for r in rows] == ["AMD", "NVDA", "TSLA", "GME"]
    assert fake.pages_requested == [1, 2, 3]
    assert str(fake.requests[0].url) == "https://apewisdom.io/api/v1.0/filter/all-stocks/page/1"


async def test_row_is_typed_and_drops_name():
    fake = FakeApeWisdom(
        page1=[ok(page([raw(1, "AMD", mentions="92", upvotes="310", rank_24h_ago="4", mentions_24h_ago="30")]))]
    )

    (row,) = await make_client(fake).fetch_all()

    assert row == ApeWisdomRow(rank=1, ticker="AMD", mentions=92, upvotes=310, rank_24h_ago=4, mentions_24h_ago=30)
    assert not hasattr(row, "name")


async def test_normalises_ticker_and_tolerates_null_24h_fields():
    fake = FakeApeWisdom(page1=[ok(page([raw(7, " gme ", rank_24h_ago=None, mentions_24h_ago=None)]))])

    (row,) = await make_client(fake).fetch_all()

    assert (row.ticker, row.rank_24h_ago, row.mentions_24h_ago) == ("GME", None, None)


@pytest.mark.parametrize(
    "bad",
    [
        raw(0, "ZERO"),
        raw(-3, "NEG"),
        raw(5, "BADM", mentions="-1"),
        raw(5, "BADH", mentions_24h_ago="-4"),
        raw(5, "BADR", rank_24h_ago="-1"),
        raw(5, "BADS", rank_24h_ago="x"),
        raw(5, "BADV", upvotes="x"),
        raw(5, ""),
        raw(5, "NOMEN", mentions=None),
        raw("x", "JUNK"),
        "not-a-row",
    ],
)
async def test_malformed_row_is_rejected_and_the_rest_kept(bad):
    fake = FakeApeWisdom(page1=[ok(page([raw(1, "AMD"), bad, raw(3, "GME")]))])

    rows = await make_client(fake).fetch_all()

    assert [r.ticker for r in rows] == ["AMD", "GME"]


async def test_unranked_24h_ago_and_negative_upvotes_are_kept():
    entrant = raw(2, "NEWB", upvotes="-1", rank_24h_ago="0", mentions_24h_ago=None)
    fake = FakeApeWisdom(page1=[ok(page([raw(1, "AMD"), entrant, raw(3, "GME", rank_24h_ago=0)]))])

    rows = {r.ticker: r for r in await make_client(fake).fetch_all()}

    assert set(rows) == {"AMD", "NEWB", "GME"}
    assert (rows["NEWB"].rank_24h_ago, rows["NEWB"].upvotes, rows["NEWB"].mentions_24h_ago) == (None, -1, None)
    assert rows["GME"].rank_24h_ago is None


async def test_rejects_are_one_warning_per_fetch_with_row_detail_at_debug(caplog):
    bad = [raw(0, f"B{i}") for i in range(50)] + [raw(5, "NOMEN", mentions=None)]
    fake = FakeApeWisdom(page1=[ok(page([raw(1, "AMD"), *bad]))])

    with caplog.at_level(logging.DEBUG, logger="rs_sources.apewisdom"):
        rows = await make_client(fake).fetch_all()

    assert [r.ticker for r in rows] == ["AMD"]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "rejected 51" in warnings[0].getMessage() and "rank" in warnings[0].getMessage()
    assert sum(r.levelno == logging.DEBUG for r in caplog.records) == 51


async def test_duplicate_ticker_across_pages_keeps_first():
    fake = FakeApeWisdom(
        page1=[ok(page([raw(1, "AMD")], pages=2))],
        page2=[ok(page([raw(2, "amd", mentions="99"), raw(3, "GME")], pages=2, number=2))],
    )

    rows = await make_client(fake).fetch_all()

    assert [(r.ticker, r.rank) for r in rows] == [("AMD", 1), ("GME", 3)]


async def test_transient_failure_is_retried_with_growing_backoff():
    fake = FakeApeWisdom(
        page1=[httpx.ConnectTimeout("slow"), httpx.Response(503), ok(page([raw(1, "AMD")]))],
    )
    sleeps: list[float] = []

    rows = await make_client(fake, sleeps, backoff_seconds=2.0).fetch_all()

    assert [r.ticker for r in rows] == ["AMD"]
    assert sleeps == [2.0, 4.0]


async def test_total_failure_raises_typed_error_after_bounded_attempts():
    fake = FakeApeWisdom(page1=[httpx.ConnectError("down")])

    with pytest.raises(ApeWisdomError):
        await make_client(fake, max_attempts=3).fetch_all()

    assert fake.pages_requested == [1, 1, 1]


async def test_failure_on_a_later_page_fails_the_whole_fetch():
    fake = FakeApeWisdom(
        page1=[ok(page([raw(1, "AMD")], pages=2))],
        page2=[httpx.Response(500)],
    )

    with pytest.raises(ApeWisdomError):
        await make_client(fake).fetch_all()


async def test_client_error_is_not_retried():
    fake = FakeApeWisdom(page1=[httpx.Response(404)])

    with pytest.raises(ApeWisdomError):
        await make_client(fake).fetch_all()

    assert fake.pages_requested == [1]


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text="<html>maintenance</html>"),
        ok({"pages": 1}),
        ok(["not", "an", "object"]),
    ],
)
async def test_unusable_body_fails_the_fetch(response):
    with pytest.raises(ApeWisdomError):
        await make_client(FakeApeWisdom(page1=[response])).fetch_all()


@pytest.mark.parametrize("pages", [0, None, "3", 10_000])
async def test_implausible_page_count_fails_the_fetch(pages):
    fake = FakeApeWisdom(page1=[ok({"pages": pages, "results": [raw(1, "AMD")]})])

    with pytest.raises(ApeWisdomError):
        await make_client(fake).fetch_all()


async def test_undecodable_response_is_retried():
    fake = FakeApeWisdom(page1=[httpx.DecodingError("bad gzip"), ok(page([raw(1, "AMD")]))])

    rows = await make_client(fake).fetch_all()

    assert [r.ticker for r in rows] == ["AMD"]
