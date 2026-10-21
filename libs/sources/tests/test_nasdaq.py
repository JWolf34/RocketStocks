"""Nasdaq earnings-calendar adapter tests over httpx.MockTransport. Payloads are synthetic."""

import asyncio
from datetime import date, timedelta

import httpx
import pytest

from rs_sources.errors import SourceError
from rs_sources.nasdaq import EarningsTime, EarningsWarmError, NasdaqEarningsCalendar


def day_payload(*rows):
    return {"data": {"rows": [{"symbol": s, "time": t} for s, t in rows] or None}, "status": {"rCode": 200}}


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

    def dates(self) -> list[str]:
        return [r.url.params["date"] for r in self.requests]


def make_calendar(fake, clock, **kwargs):
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake.handler))
    return NasdaqEarningsCalendar(http, clock, min_interval=0, **kwargs)


async def test_scans_forward_to_the_first_listing_and_stops(clock):
    fake = FakeNasdaq({"2026-10-22": day_payload(("AAA", "time-pre-market")), "2026-10-23": day_payload(("AAA", "time-pre-market"))})
    calendar = make_calendar(fake, clock)

    found = await calendar.next_for("aaa", date(2026, 10, 20), max_weekdays=10)

    assert found is not None
    assert (found.ticker, found.date, found.time_of_day) == ("AAA", date(2026, 10, 22), EarningsTime.PRE_MARKET)
    assert fake.dates() == ["2026-10-20", "2026-10-21", "2026-10-22"]


@pytest.mark.parametrize(
    ("wire", "expected"),
    [
        ("time-pre-market", EarningsTime.PRE_MARKET),
        ("time-after-hours", EarningsTime.AFTER_HOURS),
        ("time-not-supplied", None),
    ],
)
async def test_time_of_day_mapping(clock, wire, expected):
    fake = FakeNasdaq({"2026-10-20": day_payload(("AAA", wire))})

    found = await make_calendar(fake, clock).next_for("AAA", date(2026, 10, 20), max_weekdays=1)

    assert found is not None
    assert found.time_of_day == expected


async def test_weekends_are_not_requested(clock):
    fake = FakeNasdaq()
    calendar = make_calendar(fake, clock)

    # 2026-10-23 is a Friday.
    assert await calendar.next_for("AAA", date(2026, 10, 23), max_weekdays=3) is None

    assert fake.dates() == ["2026-10-23", "2026-10-26", "2026-10-27"]


async def test_ticker_absent_from_the_window_is_none(clock):
    fake = FakeNasdaq({"2026-10-21": day_payload(("OTHER", "time-pre-market"))})

    assert await make_calendar(fake, clock).next_for("AAA", date(2026, 10, 20), max_weekdays=3) is None


async def test_failed_day_raises_rather_than_claiming_absence(clock):
    fake = FakeNasdaq(failing={"2026-10-21"})

    with pytest.raises(SourceError):
        await make_calendar(fake, clock).next_for("AAA", date(2026, 10, 20), max_weekdays=3)


async def test_payload_without_data_is_a_source_error(clock):
    fake = FakeNasdaq({"2026-10-20": {"data": None, "status": {"rCode": 400}}})

    with pytest.raises(SourceError):
        await make_calendar(fake, clock).next_for("AAA", date(2026, 10, 20), max_weekdays=1)


async def test_days_are_cached_across_tickers_until_ttl(clock):
    fake = FakeNasdaq({"2026-10-20": day_payload(("AAA", "time-pre-market"), ("BBB", "time-after-hours"))})
    calendar = make_calendar(fake, clock, cache_ttl=timedelta(hours=6))

    await calendar.next_for("AAA", date(2026, 10, 20), max_weekdays=1)
    await calendar.next_for("BBB", date(2026, 10, 20), max_weekdays=1)
    assert len(fake.requests) == 1

    clock.advance(timedelta(hours=7))
    await calendar.next_for("AAA", date(2026, 10, 20), max_weekdays=1)
    assert len(fake.requests) == 2


async def test_request_uses_a_browser_user_agent(clock):
    fake = FakeNasdaq()

    await make_calendar(fake, clock).next_for("AAA", date(2026, 10, 20), max_weekdays=1)

    assert fake.requests[0].headers["user-agent"].startswith("Mozilla/5.0")


async def test_day_requests_are_spaced_by_the_minimum_interval(clock):
    loop = asyncio.get_running_loop()
    stamps: list[float] = []
    fake = FakeNasdaq()
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: (stamps.append(loop.time()), fake.handler(request))[1])
    )
    calendar = NasdaqEarningsCalendar(http, clock, min_interval=0.05)

    await calendar.next_for("AAA", date(2026, 10, 20), max_weekdays=4)

    gaps = [b - a for a, b in zip(stamps, stamps[1:], strict=False)]
    assert len(stamps) == 4
    assert all(gap >= 0.04 for gap in gaps)


async def test_warm_fills_the_cache_so_a_late_listing_is_found_without_requests(clock):
    fake = FakeNasdaq({"2026-11-02": day_payload(("LATE", "time-pre-market"))})
    calendar = make_calendar(fake, clock)

    fetched = await calendar.warm(date(2026, 10, 20), max_weekdays=10)
    requests_after_warm = len(fake.requests)
    found = await calendar.next_for("late", date(2026, 10, 20), max_weekdays=10)
    absent = await calendar.next_for("none", date(2026, 10, 20), max_weekdays=10)

    assert fetched == 10
    assert found is not None
    assert found.date == date(2026, 11, 2)
    assert absent is None
    assert len(fake.requests) == requests_after_warm


async def test_warm_skips_days_younger_than_half_the_ttl_and_refreshes_older_ones(clock):
    fake = FakeNasdaq()
    calendar = make_calendar(fake, clock, cache_ttl=timedelta(hours=6))
    await calendar.warm(date(2026, 10, 20), max_weekdays=3)

    clock.advance(timedelta(hours=2))
    assert await calendar.warm(date(2026, 10, 20), max_weekdays=3) == 0
    assert len(fake.requests) == 3

    clock.advance(timedelta(hours=2))
    assert await calendar.warm(date(2026, 10, 20), max_weekdays=3) == 3
    assert len(fake.requests) == 6


async def test_warm_tries_every_day_then_raises_and_keeps_the_good_days(clock):
    fake = FakeNasdaq({"2026-10-22": day_payload(("AAA", None))}, failing={"2026-10-21"})
    calendar = make_calendar(fake, clock)

    with pytest.raises(EarningsWarmError) as raised:
        await calendar.warm(date(2026, 10, 20), max_weekdays=3)

    assert (raised.value.attempted, raised.value.failed) == (3, 1)
    assert fake.dates() == ["2026-10-20", "2026-10-21", "2026-10-22"]
    count = len(fake.requests)
    # The failed day is still unknown, so "not listed" is not provable from the cache.
    with pytest.raises(SourceError):
        await calendar.next_for("zzz", date(2026, 10, 20), max_weekdays=3)
    assert fake.dates()[count:] == ["2026-10-21"]
