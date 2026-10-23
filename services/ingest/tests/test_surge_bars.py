"""surge_bars against a real database, the real exchange calendar and the real
Schwab adapter over a fake schwab-py client: which symbols get bars, what is
rejected, what is recorded unpriced, and that a rerun fetches nothing."""

from collections import Counter
from datetime import UTC, date, datetime, time, timedelta

import pytest
from sqlalchemy import func, select, text

from rs_analysis import popularity_surge
from rs_domain import EASTERN, PopularityOrigin, PopularitySnapshot, StoredDailyBar
from rs_ingest import surge_bars
from rs_ingest.rate_limit import RateLimiter
from rs_ingest.surge_bars import (
    BENCHMARK,
    LOOKBACK_SESSIONS,
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
    prices every session in the window from its listing date on, the same
    price whenever it is asked; `scale` multiplies a symbol's prices (a split
    after the bars were stored); `junk` adds candles the adapter must reject;
    `broken` answers an error instead."""

    def __init__(self) -> None:
        self.listed: dict[str, date | None] = {}
        self.junk: dict[str, list[dict]] = {}
        self.broken: dict[str, int | Exception] = {}
        self.scale: dict[str, float] = {}
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
        k = self.scale.get(symbol, 1.0)
        candles = [
            {
                "datetime": _millis(d),
                "open": k * (10.0 + d.toordinal() % 17),
                "high": k * (12.0 + d.toordinal() % 17),
                "low": k * (9.0 + d.toordinal() % 17),
                "close": k * (11.0 + d.toordinal() % 17),
                "volume": 1000 + d.toordinal() % 17,
            }
            for d in sessions
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


async def test_stamps_each_bar_with_fetch_time_and_stores_it_adjusted(session_factory):
    await _seed(session_factory)
    await _run(session_factory, FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK), FakeClock(NOW))

    async with session_factory() as s:
        [bar, *_] = await DailyBarRepository(s).list_range("AAA", date(2026, 10, 20), date(2026, 10, 20))
    assert bar.fetched_at == NOW
    assert bar.adjusted is True


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

    # The tail fetch starts three sessions back so that it overlaps the stored bars.
    assert sorted(schwab.calls) == sorted((s, date(2026, 10, 16), date(2026, 10, 21)) for s in ("AAA", "BBB", "ZZZ", BENCHMARK))
    assert report.bars_inserted == 4
    assert report.repaired == 0


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


async def test_the_lookback_covers_the_typical_move_window_with_margin():
    assert LOOKBACK_SESSIONS >= 250 + 10


async def test_the_lookback_is_that_many_sessions_before_any_event_day():
    # Weekdays, weekends and holiday weeks alike: the calendar-day span never comes up short.
    for offset in range(0, 370):
        day = date(2026, 1, 1) + timedelta(days=offset)
        start = lookback_start(CALENDAR, day)
        before = CALENDAR.sessions_in_range(start, day - timedelta(days=1))
        assert len(before) >= LOOKBACK_SESSIONS, day
        assert len(CALENDAR.sessions_in_range(start + timedelta(days=1), day - timedelta(days=1))) < len(before)


async def test_a_year_of_bars_is_fetched_before_the_first_event(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)

    await _run(session_factory, schwab, FakeClock(NOW))

    aaa = await _bars(session_factory, "AAA")
    assert len([d for d in aaa if d < date(2026, 10, 14)]) >= 250
    assert aaa[-1] == LAST_SESSION


async def _store_with_the_old_thirty_session_lookback(session_factory, schwab: FakeSchwab, clock: FakeClock, monkeypatch):
    """The state a box running the earlier job holds: short lookbacks and no request record."""
    with monkeypatch.context() as old:
        old.setattr(surge_bars, "LOOKBACK_SESSIONS", 30)
        old.setattr(surge_bars, "LOOKBACK_CALENDAR_DAYS", 70)
        await _run(session_factory, schwab, clock)
    async with session_factory() as s:
        await s.execute(text("DELETE FROM bar_request"))
        await s.commit()
    schwab.calls.clear()


async def test_bars_stored_with_a_short_lookback_are_extended_back_once(session_factory, monkeypatch):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)
    clock = FakeClock(NOW)
    await _store_with_the_old_thirty_session_lookback(session_factory, schwab, clock, monkeypatch)
    assert len(await _bars(session_factory, "AAA")) < 60

    report = await _run(session_factory, schwab, clock)

    wanted = {s: lookback_start(CALENDAR, d) for s, d in {"AAA": date(2026, 10, 14), "BBB": date(2026, 9, 30), "ZZZ": date(2026, 10, 15)}.items()}
    wanted[BENCHMARK] = wanted["BBB"]
    assert sorted(schwab.calls) == sorted((s, start, LAST_SESSION) for s, start in wanted.items())
    for symbol, start in wanted.items():
        assert (await _bars(session_factory, symbol))[0] == start
    assert report.bars_inserted > 0

    schwab.calls.clear()
    await _run(session_factory, schwab, clock)
    assert schwab.calls == []


async def test_a_listing_younger_than_the_lookback_is_asked_once_and_never_again(session_factory, monkeypatch):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "ZZZ", BENCHMARK).price("BBB", listed=date(2026, 9, 28))
    clock = FakeClock(NOW)
    await _store_with_the_old_thirty_session_lookback(session_factory, schwab, clock, monkeypatch)

    await _run(session_factory, schwab, clock)
    # BBB was asked from the full lookback once and has nothing earlier than its listing.
    assert [c for c in schwab.calls if c[0] == "BBB"] == [("BBB", lookback_start(CALENDAR, date(2026, 9, 30)), LAST_SESSION)]
    assert (await _bars(session_factory, "BBB"))[0] == date(2026, 9, 28)

    schwab.calls.clear()
    clock.current = NOW + timedelta(days=1)
    await _run(session_factory, schwab, clock)
    # Only the tail: its short history is complete, not a gap.
    assert [c for c in schwab.calls if c[0] == "BBB"] == [("BBB", date(2026, 10, 16), date(2026, 10, 21))]
    assert (await _bars(session_factory, "BBB"))[0] == date(2026, 9, 28)


async def test_the_benchmark_already_covering_the_earliest_need_is_not_refetched(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)
    clock = FakeClock(NOW)
    # SPY was stored earlier, from before the earliest need, with no request on record.
    async with session_factory() as s:
        history = await SchwabMarketData(schwab, CALENDAR).daily_history(BENCHMARK, date(2025, 1, 2), LAST_SESSION)
        await DailyBarRepository(s).insert_many(surge_bars._to_stored(history, FETCHED))
        await s.commit()
    schwab.calls.clear()

    await _run(session_factory, schwab, clock)

    assert BENCHMARK not in schwab.symbols_called()
    assert (await _bars(session_factory, BENCHMARK))[0] == date(2025, 1, 2)


async def test_a_long_catch_up_logs_its_plan_and_progress(session_factory, monkeypatch, caplog):
    await _seed(session_factory)
    monkeypatch.setattr(surge_bars, "PROGRESS_EVERY", 2)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)

    with caplog.at_level("INFO", logger="rs_ingest.surge_bars"):
        await _run(session_factory, schwab, FakeClock(NOW))

    messages = [r.getMessage() for r in caplog.records]
    assert "surge_bars: 4 of 4 symbols to fetch, one call each" in messages
    assert "surge_bars: 2 of 4 symbols done" in messages


async def test_the_run_says_it_started_before_the_trigger_replay_that_can_take_minutes(session_factory, caplog):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)

    with caplog.at_level("INFO", logger="rs_ingest.surge_bars"):
        await _run(session_factory, schwab, FakeClock(NOW))

    messages = [r.getMessage() for r in caplog.records]
    assert messages[0] == "surge_bars: replaying the Popularity Trigger to find the stocks to track"


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


# --- bars stored before a split are repaired by overlap -----------------------------------------


async def _closes(session_factory, symbol: str) -> dict[date, float]:
    async with session_factory() as s:
        rows = await DailyBarRepository(s).list_range(symbol, date(2000, 1, 1), date(2100, 1, 1))
    return {r.session_date: r.close for r in rows}


async def _stored_then_next_session(session_factory) -> tuple[FakeSchwab, FakeClock, dict[date, float]]:
    """Bars stored through Tuesday, the clock moved to the next session's close."""
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)
    clock = FakeClock(NOW)
    await _run(session_factory, schwab, clock)
    before = await _closes(session_factory, "AAA")
    schwab.calls.clear()
    clock.current = NOW + timedelta(days=1)  # Wed 2026-10-21 after the close
    return schwab, clock, before


async def test_an_overlap_that_matches_makes_no_repair(session_factory):
    schwab, clock, before = await _stored_then_next_session(session_factory)

    report = await _run(session_factory, schwab, clock)

    assert report.repaired == 0
    assert sorted(schwab.calls) == sorted((s, date(2026, 10, 16), date(2026, 10, 21)) for s in ("AAA", "BBB", "ZZZ", BENCHMARK))
    after = await _closes(session_factory, "AAA")
    assert {d: c for d, c in after.items() if d in before} == before
    assert max(after) == date(2026, 10, 21)


@pytest.mark.parametrize("drift, repaired", [(1.002, 0), (1.05, 1)])
async def test_a_close_is_a_disagreement_only_beyond_the_tolerance(session_factory, drift, repaired):
    schwab, clock, _ = await _stored_then_next_session(session_factory)
    schwab.scale["AAA"] = drift  # a vendor correction, small or not

    report = await _run(session_factory, schwab, clock)

    assert report.repaired == repaired


async def test_a_reverse_split_after_storage_replaces_the_stored_bars_with_a_fresh_fetch(session_factory, caplog):
    schwab, clock, before = await _stored_then_next_session(session_factory)
    # A bar older than the lookback, left by an earlier job: the fresh fetch will not have it.
    stale = StoredDailyBar(
        symbol="AAA", session_date=date(2025, 1, 2), open=11.0, high=12.0, low=10.0, close=11.0,
        volume=1, fetched_at=FETCHED, adjusted=True,
    )
    async with session_factory() as s:
        await DailyBarRepository(s).insert_many([stale])
        await s.commit()
    schwab.scale["AAA"] = 10.0  # 1-for-10 reverse split: every close is now ten times the stored one

    with caplog.at_level("WARNING", logger="rs_ingest.surge_bars"):
        report = await _run(session_factory, schwab, clock)

    need = lookback_start(CALENDAR, date(2026, 10, 14))
    assert [c for c in schwab.calls if c[0] == "AAA"] == [
        ("AAA", date(2026, 10, 16), date(2026, 10, 21)),  # the tail with its overlap
        ("AAA", need, date(2026, 10, 21)),  # the full fetch it triggered
    ]
    after = await _closes(session_factory, "AAA")
    assert sorted(after) == CALENDAR.sessions_in_range(need, date(2026, 10, 21))
    assert all(after[d] == pytest.approx(10 * before[d]) for d in before if d in after)
    assert report.repaired == 1
    assert any("AAA" in r.getMessage() and "replacing" in r.getMessage() for r in caplog.records)
    # Only the split symbol was touched, and a rerun finds nothing to do.
    assert sorted(c for c in schwab.calls if c[0] != "AAA") == sorted(
        (s, date(2026, 10, 16), date(2026, 10, 21)) for s in ("BBB", "ZZZ", BENCHMARK)
    )
    schwab.calls.clear()
    await _run(session_factory, schwab, clock)
    assert schwab.calls == []


async def test_a_split_found_while_extending_back_needs_no_second_call(session_factory, monkeypatch):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)
    clock = FakeClock(NOW)
    await _store_with_the_old_thirty_session_lookback(session_factory, schwab, clock, monkeypatch)
    before = await _closes(session_factory, "AAA")
    schwab.scale["AAA"] = 0.1  # 10-for-1 forward split since

    await _run(session_factory, schwab, clock)

    need = lookback_start(CALENDAR, date(2026, 10, 14))
    assert [c for c in schwab.calls if c[0] == "AAA"] == [("AAA", need, LAST_SESSION)]
    after = await _closes(session_factory, "AAA")
    assert sorted(after) == CALENDAR.sessions_in_range(need, LAST_SESSION)
    assert all(after[d] == pytest.approx(0.1 * before[d]) for d in before)


async def test_an_empty_answer_does_not_wipe_stored_bars(session_factory):
    schwab, clock, before = await _stored_then_next_session(session_factory)
    schwab.listed["AAA"] = None  # delisted or halted: Schwab answers with nothing

    report = await _run(session_factory, schwab, clock)

    assert report.repaired == 0
    assert await _closes(session_factory, "AAA") == before
    assert "AAA" not in await _unpriced(session_factory)


async def test_an_answer_without_the_overlap_sessions_is_not_a_disagreement_but_is_reported_unverified(
    session_factory, caplog
):
    schwab, clock, before = await _stored_then_next_session(session_factory)
    schwab.scale["AAA"] = 10.0
    plain = schwab.get_price_history_every_day

    async def only_the_new_session(symbol, start_datetime, end_datetime, **kwargs):
        # The vendor has nothing for the sessions before Wednesday: nothing to compare against.
        if symbol != "AAA":
            return await plain(symbol, start_datetime, end_datetime, **kwargs)
        return await plain(symbol, start_datetime.replace(day=21), end_datetime, **kwargs)

    schwab.get_price_history_every_day = only_the_new_session

    with caplog.at_level("WARNING", logger="rs_ingest.surge_bars"):
        report = await _run(session_factory, schwab, clock)

    assert report.repaired == 0
    after = await _closes(session_factory, "AAA")
    assert {d: c for d, c in after.items() if d in before} == before  # nothing was deleted
    # The unverifiable tail is visible, not silent, and costs no second call.
    assert report.unverified == 1
    assert any(
        "AAA" in r.getMessage() and "unverified" in r.getMessage() and "2026-10-20" in r.getMessage()
        for r in caplog.records
    )
    assert [c[0] for c in schwab.calls].count("AAA") == 1


async def test_an_answer_with_overlap_is_not_unverified(session_factory):
    schwab, clock, _ = await _stored_then_next_session(session_factory)

    report = await _run(session_factory, schwab, clock)

    assert report.unverified == 0


class _FailsFullFetch:
    """The tail answers, the fresh full fetch behind it does not."""

    def __init__(self, inner: SchwabMarketData, fails_from: date) -> None:
        self.inner, self.fails_from = inner, fails_from

    async def daily_history(self, symbol: str, start: date, end: date):
        if symbol == "AAA" and start < self.fails_from:
            raise SchwabResponseError(503, "unavailable")
        return await self.inner.daily_history(symbol, start, end)


async def test_a_failed_repair_keeps_the_stored_bars_and_the_new_tail_out_until_it_succeeds(session_factory):
    schwab, clock, before = await _stored_then_next_session(session_factory)
    schwab.scale["AAA"] = 10.0

    with pytest.raises(SurgeBarsError, match="1 of 4 symbols failed: AAA"):
        await sync_surge_bars(
            session_factory=session_factory,
            source=_FailsFullFetch(SchwabMarketData(schwab, CALENDAR), date(2026, 10, 16)),
            limiter=Budget().limiter(),
            calendar=CALENDAR,
            clock=clock,
        )

    # No new-scale bar sits beside the old-scale ones.
    assert await _closes(session_factory, "AAA") == before

    report = await _run(session_factory, schwab, clock)
    assert report.repaired == 1
    assert max(await _closes(session_factory, "AAA")) == date(2026, 10, 21)
