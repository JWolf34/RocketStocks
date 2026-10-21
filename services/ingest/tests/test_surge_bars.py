"""surge_bars against a real database, the real exchange calendar and the real
Schwab adapter over a fake schwab-py client: which symbols get bars, what is
rejected, what is recorded unpriced, and that a rerun fetches nothing."""

from collections import Counter
from datetime import UTC, date, datetime, time, timedelta

import pytest
from sqlalchemy import func, select

from rs_analysis import popularity_surge
from rs_domain import PopularityOrigin, PopularitySnapshot
from rs_ingest.rate_limit import RateLimiter
from rs_ingest.surge_bars import (
    BENCHMARK,
    EASTERN,
    SurgeBarsError,
    first_event_dates,
    lookback_start,
    sync_surge_bars,
)
from rs_persistence import DailyBarRepository, DailyBarRow, PopularityRepository
from rs_sources import ExchangeCalendar
from rs_sources.schwab import SchwabAuthError, SchwabMarketData, SchwabResponseError

CALENDAR = ExchangeCalendar()
# Tuesday 2026-10-20, 18:00 ET: two hours past the close, so that day's candle is settled.
NOW = datetime(2026, 10, 20, 22, 0, tzinfo=UTC)
LAST_SESSION = date(2026, 10, 20)
FETCHED = datetime(2026, 10, 14, 15, 0, 5, tzinfo=UTC)


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self.current = now

    def now(self) -> datetime:
        return self.current


class FakeResponse:
    def __init__(self, payload=None, status_code=200):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def _millis(day: date) -> int:
    return int(datetime.combine(day, time.min, tzinfo=EASTERN).timestamp() * 1000)


class FakeSchwab:
    """schwab-py's price-history call over a scripted market: every symbol
    prices every session in the window from its listing date on; `junk` adds
    candles the adapter must reject; `broken` answers an error instead."""

    def __init__(self) -> None:
        self.listed: dict[str, date | None] = {}
        self.junk: dict[str, list[dict]] = {}
        self.broken: dict[str, int | Exception] = {}
        self.calls: list[tuple[str, date, date]] = []

    def price(self, *symbols: str, listed: date | None = date(2000, 1, 1)) -> "FakeSchwab":
        for symbol in symbols:
            self.listed[symbol] = listed
        return self

    async def get_price_history_every_day(self, symbol, start_datetime, end_datetime, **kwargs):
        start, end = start_datetime.date(), end_datetime.date()
        self.calls.append((symbol, start, end))
        failure = self.broken.get(symbol)
        if isinstance(failure, Exception):
            raise failure
        if failure is not None:
            return FakeResponse(status_code=failure)
        listed = self.listed.get(symbol)
        sessions = [] if listed is None else [d for d in CALENDAR.sessions_in_range(start, end) if d >= listed]
        candles = [
            {"datetime": _millis(d), "open": 10.0 + i, "high": 12.0 + i, "low": 9.0 + i, "close": 11.0 + i, "volume": 1000 + i}
            for i, d in enumerate(sessions)
        ]
        return FakeResponse({"candles": candles + self.junk.get(symbol, []), "symbol": symbol})

    def symbols_called(self) -> list[str]:
        return [symbol for symbol, _, _ in self.calls]


class Budget:
    """A limiter in virtual time that records how long it made callers wait."""

    def __init__(self) -> None:
        self.t = 0.0
        self.waited = 0.0

    async def sleep(self, seconds: float) -> None:
        self.waited += seconds
        self.t += seconds

    def limiter(self, calls_per_minute: int = 60) -> RateLimiter:
        return RateLimiter(calls_per_minute, now=lambda: self.t, sleep=self.sleep)


def _snap(ticker: str, polled_at: datetime, *, mentions: int = 100, prior: int = 20, **kw) -> PopularitySnapshot:
    fields = {
        "polled_at": polled_at,
        "ticker": ticker,
        "rank": 40,
        "mentions": mentions,
        "upvotes": 500,
        "rank_24h_ago": 45,
        "mentions_24h_ago": prior,
        "fetched_at": FETCHED,
        "origin": PopularityOrigin.LIVE,
    }
    return PopularitySnapshot(**(fields | kw))


AAA_EVENT = datetime(2026, 10, 14, 15, 0, tzinfo=UTC)
BBB_EVENT = datetime(2026, 9, 30, 15, 0, tzinfo=UTC)
ZZZ_EVENT = datetime(2026, 10, 15, 15, 0, tzinfo=UTC)


async def _seed(session_factory) -> None:
    snapshots = [
        _snap("AAA", AAA_EVENT),
        _snap("BBB", BBB_EVENT, origin=PopularityOrigin.V1_IMPORT),
        _snap("ZZZ", ZZZ_EVENT),
        # Popular but never a surge: no bars wanted.
        _snap("CCC", AAA_EVENT, mentions=100, prior=90),
        # Below the minimum mentions: ignored whatever the ratio.
        _snap("DDD", AAA_EVENT, mentions=12, prior=2),
    ]
    async with session_factory() as s:
        await PopularityRepository(s).insert_many(snapshots)
        await s.commit()


async def _run(session_factory, schwab: FakeSchwab, clock: FakeClock, budget: Budget | None = None):
    return await sync_surge_bars(
        session_factory=session_factory,
        source=SchwabMarketData(schwab, CALENDAR),
        limiter=(budget or Budget()).limiter(),
        calendar=CALENDAR,
        clock=clock,
    )


async def _bars(session_factory, symbol: str) -> list[date]:
    async with session_factory() as s:
        rows = await DailyBarRepository(s).list_range(symbol, date(2000, 1, 1), date(2100, 1, 1))
    return [r.session_date for r in rows]


async def _unpriced(session_factory):
    async with session_factory() as s:
        return {u.symbol: u for u in await DailyBarRepository(s).list_unpriced()}


async def test_stores_bars_for_event_tickers_and_spy_only(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "CCC", "DDD", BENCHMARK)

    report = await _run(session_factory, schwab, FakeClock(NOW))

    assert sorted(schwab.symbols_called()) == ["AAA", "BBB", BENCHMARK, "ZZZ"]
    assert await _bars(session_factory, "AAA") == CALENDAR.sessions_in_range(
        lookback_start(CALENDAR, date(2026, 10, 14)), LAST_SESSION
    )
    # An imported (v1) event is as good as a live one.
    assert (await _bars(session_factory, "BBB"))[0] == lookback_start(CALENDAR, date(2026, 9, 30))
    assert await _bars(session_factory, "CCC") == []
    assert await _bars(session_factory, "DDD") == []
    # The benchmark spans the earliest event onwards.
    spy = await _bars(session_factory, BENCHMARK)
    assert spy[0] == lookback_start(CALENDAR, date(2026, 9, 30))
    assert spy[-1] == LAST_SESSION
    # ZZZ is asked about too, but Schwab has no history for it.
    assert (report.needed, report.fetched, report.unpriced) == (4, 4, 1)


async def test_stamps_each_bar_with_fetch_time_and_adjustment(session_factory):
    await _seed(session_factory)
    await _run(session_factory, FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK), FakeClock(NOW))

    async with session_factory() as s:
        [bar, *_] = await DailyBarRepository(s).list_range("AAA", date(2026, 10, 20), date(2026, 10, 20))
    assert bar.fetched_at == NOW
    assert bar.adjusted is False


async def test_rejects_non_session_and_epoch_zero_bars_without_storing_them(session_factory):
    await _seed(session_factory)
    saturday = date(2026, 10, 17)
    schwab = FakeSchwab().price("AAA", "BBB", BENCHMARK)
    schwab.junk["AAA"] = [
        {"datetime": _millis(saturday), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
        {"datetime": 0, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
    ]

    report = await _run(session_factory, schwab, FakeClock(NOW))

    assert report.rejected == Counter({"not_a_session": 1, "epoch_zero": 1})
    assert saturday not in await _bars(session_factory, "AAA")
    assert date(1970, 1, 1) not in await _bars(session_factory, "AAA")
    async with session_factory() as s:
        assert await s.scalar(select(func.count()).select_from(DailyBarRow).where(DailyBarRow.session_date < date(2000, 1, 1))) == 0


async def test_records_an_unpriceable_ticker_once_and_does_not_retry_it_for_a_week(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", BENCHMARK).price("ZZZ", listed=None)
    clock = FakeClock(NOW)

    report = await _run(session_factory, schwab, clock)
    first = (await _unpriced(session_factory))["ZZZ"]
    assert (first.reason, first.first_seen, first.last_attempt) == ("no_history", NOW, NOW)
    assert report.unpriced == 1
    assert await _bars(session_factory, "ZZZ") == []

    # The next session: everything else is behind by a day, the unpriced symbol is left alone.
    schwab.calls.clear()
    clock.current = NOW + timedelta(days=1)
    report = await _run(session_factory, schwab, clock)
    assert "ZZZ" not in schwab.symbols_called()
    assert report.deferred == 1 and report.unpriced == 0
    assert (await _unpriced(session_factory))["ZZZ"] == first


async def test_retries_an_unpriced_ticker_after_a_week_and_clears_it_once_it_prices(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", BENCHMARK).price("ZZZ", listed=None)
    clock = FakeClock(NOW)
    await _run(session_factory, schwab, clock)

    clock.current = NOW + timedelta(days=7)  # Tuesday 2026-10-27
    schwab.price("ZZZ")
    schwab.calls.clear()
    await _run(session_factory, schwab, clock)

    assert "ZZZ" in schwab.symbols_called()
    assert await _unpriced(session_factory) == {}
    assert (await _bars(session_factory, "ZZZ"))[-1] == date(2026, 10, 27)


async def test_a_still_unpriceable_ticker_keeps_first_seen_and_advances_last_attempt(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", BENCHMARK).price("ZZZ", listed=None)
    clock = FakeClock(NOW)
    await _run(session_factory, schwab, clock)

    clock.current = NOW + timedelta(days=7)
    await _run(session_factory, schwab, clock)

    retried = (await _unpriced(session_factory))["ZZZ"]
    assert (retried.first_seen, retried.last_attempt) == (NOW, clock.current)


async def test_a_ticker_whose_bars_are_all_rejected_is_unpriced_with_the_reasons(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", BENCHMARK).price("ZZZ", listed=None)
    schwab.junk["ZZZ"] = [{"datetime": 0, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]

    await _run(session_factory, schwab, FakeClock(NOW))

    assert (await _unpriced(session_factory))["ZZZ"].reason == "all_rejected:epoch_zero"


async def test_a_rerun_with_nothing_new_makes_no_call_and_changes_nothing(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", BENCHMARK).price("ZZZ", listed=None)
    clock = FakeClock(NOW)
    await _run(session_factory, schwab, clock)
    schwab.calls.clear()

    report = await _run(session_factory, schwab, clock)

    assert schwab.calls == []
    assert report.bars_inserted == 0
    assert report.deferred == 1  # ZZZ waits out its week


async def test_the_next_session_fetches_only_the_new_day(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)
    clock = FakeClock(NOW)
    await _run(session_factory, schwab, clock)
    schwab.calls.clear()

    clock.current = NOW + timedelta(days=1)  # Wed 2026-10-21 after the close
    report = await _run(session_factory, schwab, clock)

    assert sorted(schwab.calls) == sorted((s, date(2026, 10, 21), date(2026, 10, 21)) for s in ("AAA", "BBB", "ZZZ", BENCHMARK))
    assert report.bars_inserted == 4


async def test_nothing_is_fetched_before_the_session_has_settled(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)
    # 16:30 ET Tuesday: the session closed 30 minutes ago, so Monday is the last settled one.
    await _run(session_factory, schwab, FakeClock(datetime(2026, 10, 20, 20, 30, tzinfo=UTC)))

    assert all(end == date(2026, 10, 19) for _, _, end in schwab.calls)


async def test_a_weekend_run_ends_at_friday_and_holds_nothing_further(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)
    saturday = FakeClock(datetime(2026, 10, 24, 22, 0, tzinfo=UTC))
    await _run(session_factory, schwab, saturday)
    schwab.calls.clear()

    await _run(session_factory, schwab, FakeClock(datetime(2026, 10, 25, 22, 0, tzinfo=UTC)))

    assert schwab.calls == []
    assert (await _bars(session_factory, "AAA"))[-1] == date(2026, 10, 23)


async def test_an_older_event_discovered_later_back_fills_its_lookback(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)
    clock = FakeClock(NOW)
    await _run(session_factory, schwab, clock)
    schwab.calls.clear()
    # The v1 import lands an earlier event for a ticker already carrying bars.
    older = datetime(2026, 8, 12, 15, 0, tzinfo=UTC)
    async with session_factory() as s:
        await PopularityRepository(s).insert_many([_snap("AAA", older, origin=PopularityOrigin.V1_IMPORT)])
        await s.commit()

    await _run(session_factory, schwab, clock)

    start = lookback_start(CALENDAR, date(2026, 8, 12))
    assert sorted(schwab.calls) == [("AAA", start, LAST_SESSION), (BENCHMARK, start, LAST_SESSION)]
    assert (await _bars(session_factory, "AAA"))[0] == start

    schwab.calls.clear()
    await _run(session_factory, schwab, clock)
    assert schwab.calls == []


async def test_a_listing_younger_than_the_lookback_is_not_refetched_forever(session_factory):
    await _seed(session_factory)
    # BBB listed two sessions before its event: the lookback is mostly empty, and stays so.
    schwab = FakeSchwab().price("AAA", "ZZZ", BENCHMARK).price("BBB", listed=date(2026, 9, 28))
    clock = FakeClock(NOW)
    await _run(session_factory, schwab, clock)
    assert (await _bars(session_factory, "BBB"))[0] == date(2026, 9, 28)
    schwab.calls.clear()

    await _run(session_factory, schwab, clock)

    assert schwab.calls == []


async def test_a_weekend_event_on_a_listing_that_starts_monday_is_not_refetched_forever(session_factory):
    saturday = datetime(2026, 10, 17, 15, 0, tzinfo=UTC)
    async with session_factory() as s:
        await PopularityRepository(s).insert_many([_snap("NEW", saturday)])
        await s.commit()
    schwab = FakeSchwab().price(BENCHMARK).price("NEW", listed=date(2026, 10, 19))
    clock = FakeClock(NOW)
    await _run(session_factory, schwab, clock)
    assert (await _bars(session_factory, "NEW"))[0] == date(2026, 10, 19)
    schwab.calls.clear()

    await _run(session_factory, schwab, clock)

    assert schwab.calls == []


async def test_the_lookback_is_thirty_sessions_before_the_event(session_factory):
    start = lookback_start(CALENDAR, date(2026, 10, 14))
    assert len(CALENDAR.sessions_in_range(start, date(2026, 10, 13))) == 30


async def test_a_stored_symbol_schwab_stops_answering_for_is_not_a_job_failure(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)
    clock = FakeClock(NOW)
    await _run(session_factory, schwab, clock)
    schwab.broken["AAA"] = 404  # delisted since

    clock.current = NOW + timedelta(days=1)
    await _run(session_factory, schwab, clock)

    assert "AAA" not in await _unpriced(session_factory)  # its earlier bars are still good


async def test_a_failing_symbol_is_reported_but_does_not_stop_the_others(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)
    schwab.broken["BBB"] = 503

    with pytest.raises(SurgeBarsError, match="1 of 4 symbols failed: BBB"):
        await _run(session_factory, schwab, FakeClock(NOW))

    assert await _bars(session_factory, "BBB") == []
    assert await _bars(session_factory, "AAA") != []
    assert await _bars(session_factory, "ZZZ") != []
    assert "BBB" not in await _unpriced(session_factory)  # transient, not unpriceable


async def test_a_symbol_schwab_refuses_outright_is_recorded_unpriced(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", BENCHMARK)
    schwab.broken["ZZZ"] = 400

    await _run(session_factory, schwab, FakeClock(NOW))

    assert (await _unpriced(session_factory))["ZZZ"].reason == "http_400"


async def test_being_over_budget_stops_the_run_and_keeps_what_was_stored(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)
    schwab.broken["AAA"] = 429

    with pytest.raises(SchwabResponseError, match="429"):
        await _run(session_factory, schwab, FakeClock(NOW))

    assert schwab.symbols_called() == [BENCHMARK, "AAA"]
    assert await _bars(session_factory, BENCHMARK) != []


async def test_a_dead_token_propagates_so_the_daemon_can_pause_schwab_jobs(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)

    class Dead:
        async def daily_history(self, symbol, start, end):
            raise SchwabAuthError("refresh token rejected")

    with pytest.raises(SchwabAuthError):
        await sync_surge_bars(
            session_factory=session_factory,
            source=Dead(),
            limiter=Budget().limiter(),
            calendar=CALENDAR,
            clock=FakeClock(NOW),
        )
    assert schwab.calls == []


async def test_every_call_waits_on_the_shared_budget(session_factory):
    await _seed(session_factory)
    budget = Budget()
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)

    await _run(session_factory, schwab, FakeClock(NOW), budget)

    # 4 calls at 1 per second: the first is free, the other three each wait a second.
    assert len(schwab.calls) == 4
    assert budget.waited == pytest.approx(3.0)


async def test_with_no_events_only_the_benchmark_is_tracked(session_factory):
    schwab = FakeSchwab().price(BENCHMARK)

    report = await _run(session_factory, schwab, FakeClock(NOW))

    assert schwab.symbols_called() == [BENCHMARK]
    assert (await _bars(session_factory, BENCHMARK))[-1] == LAST_SESSION
    assert report.needed == 1


async def test_first_event_dates_agree_with_the_rule_replay(session_factory):
    history = []
    for hours in range(0, 24 * 21, 6):  # AAA surges repeatedly; each hit is its own event
        at = datetime(2026, 9, 1, tzinfo=UTC) + timedelta(hours=hours)
        history.append(_snap("AAA", at, origin=PopularityOrigin.V1_IMPORT if hours % 12 else PopularityOrigin.LIVE))
        history.append(_snap("QUIET", at, mentions=100, prior=100))
    history.append(_snap("LATE", datetime(2026, 10, 18, 3, 0, tzinfo=UTC)))  # 23:00 ET on the 17th
    async with session_factory() as s:
        await PopularityRepository(s).insert_many(history)
        await s.commit()

    got = await first_event_dates(session_factory, NOW)

    replayed = popularity_surge.replay(history)
    expected = {}
    for event in replayed:
        expected.setdefault(event.ticker, event.polled_at.astimezone(EASTERN).date())
    assert got == expected
    assert got == {"AAA": date(2026, 8, 31), "LATE": date(2026, 10, 17)}


async def test_first_event_dates_is_empty_with_no_history(session_factory):
    assert await first_event_dates(session_factory, NOW) == {}
