"""daily_bars against a real database, the real exchange calendar and the real
Schwab adapter over a fake schwab-py client: which symbols get bars and in what order,
the 1990 seed, the stop time and the resume, what is rejected, what is recorded unpriced,
and that a rerun fetches nothing."""

from collections import Counter
from datetime import UTC, date, datetime, time, timedelta

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from rs_analysis import popularity_surge
from rs_domain import (
    EASTERN,
    PopularityOrigin,
    PopularitySnapshot,
    StoredDailyBar,
    UniverseCounts,
    UniverseListing,
    UniverseSnapshot,
)
from rs_ingest import daily_bars
from rs_ingest.daily_bars import (
    BENCHMARK,
    DailyBarsError,
    stop_time,
    sync_daily_bars,
    triggered_tickers,
)
from rs_ingest.rate_limit import RateLimiter
from rs_persistence import (
    DailyBarRepository,
    DailyBarRow,
    PopularityRepository,
    UniverseRepository,
)
from rs_sources import ExchangeCalendar
from rs_sources.schwab import SchwabAuthError, SchwabMarketData, SchwabResponseError

CALENDAR = ExchangeCalendar()
CALENDAR_FIRST_SESSION = date(1990, 1, 2)
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
        first = max(start, CALENDAR_FIRST_SESSION)  # the calendar library starts at the 1990 seed's first session
        sessions = [] if listed is None else [d for d in CALENDAR.sessions_in_range(first, end) if d >= listed]
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


# Most tests run on a short seed so the fake vendor serves weeks, not decades; the seed tests
# put the real 1990 start back.
SHORT_SEED = date(2026, 8, 3)


@pytest.fixture(autouse=True)
def short_seed(monkeypatch):
    monkeypatch.setattr(daily_bars, "SEED_START", SHORT_SEED)


def _listing(symbol: str) -> UniverseListing:
    return UniverseListing(
        symbol=symbol,
        security_name=f"{symbol} Inc.",
        exchange="Q",
        etf=False,
        financial_status="N",
        cqs_symbol=None,
        nasdaq_symbol=None,
        optionable=False,
    )


async def _universe(session_factory, *symbols: str, day: date = date(2026, 10, 19)) -> None:
    snapshot = UniverseSnapshot(
        snapshot_date=day,
        as_of=datetime.combine(day, time(22, 1), tzinfo=UTC),
        counts=UniverseCounts(nasdaq_listed=len(symbols), other_listed=0, options_symbols=0, cboe_symbols=0),
        optionable_carried=False,
        listings=tuple(_listing(symbol) for symbol in symbols),
    )
    async with session_factory() as s:
        await UniverseRepository(s).save(snapshot)
        await s.commit()


async def _run(
    session_factory,
    schwab: FakeSchwab,
    clock: FakeClock,
    budget: Budget | None = None,
    *,
    stop_at: datetime | None = None,
):
    return await sync_daily_bars(
        session_factory=session_factory,
        source=SchwabMarketData(schwab, CALENDAR),
        limiter=(budget or Budget()).limiter(),
        calendar=CALENDAR,
        clock=clock,
        stop_at=stop_at or stop_time(clock.now()),
    )


async def _bars(session_factory, symbol: str) -> list[date]:
    async with session_factory() as s:
        rows = await DailyBarRepository(s).list_range(symbol, date(1980, 1, 1), date(2100, 1, 1))
    return [r.session_date for r in rows]


async def _unpriced(session_factory):
    async with session_factory() as s:
        return {u.symbol: u for u in await DailyBarRepository(s).list_unpriced()}


def _sessions(start: date, end: date = LAST_SESSION) -> list[date]:
    return CALENDAR.sessions_in_range(start, end)


# --- scope and order -----------------------------------------------------------------------------


async def test_fetches_the_benchmark_then_triggered_stocks_then_the_rest_of_the_universe(session_factory):
    await _seed(session_factory)
    # BBB was Triggered and has since left the Universe; EEE and FFF never were Triggered.
    await _universe(session_factory, "ZZZ", "FFF", "AAA", "EEE", "CCC", "DDD")
    schwab = FakeSchwab().price("AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "ZZZ", BENCHMARK)

    report = await _run(session_factory, schwab, FakeClock(NOW))

    assert schwab.symbols_called() == [BENCHMARK, "AAA", "BBB", "ZZZ", "CCC", "DDD", "EEE", "FFF"]
    assert (report.needed, report.fetched) == (8, 8)
    for symbol in ("AAA", "CCC", "FFF"):
        assert await _bars(session_factory, symbol) == _sessions(SHORT_SEED)


async def test_without_a_universe_only_the_benchmark_and_triggered_stocks_are_fetched(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "CCC", "DDD", "ZZZ", BENCHMARK)

    await _run(session_factory, schwab, FakeClock(NOW))

    assert schwab.symbols_called() == [BENCHMARK, "AAA", "BBB", "ZZZ"]


async def test_with_no_events_and_no_universe_only_the_benchmark_is_fetched(session_factory):
    schwab = FakeSchwab().price(BENCHMARK)

    report = await _run(session_factory, schwab, FakeClock(NOW))

    assert schwab.symbols_called() == [BENCHMARK]
    assert (await _bars(session_factory, BENCHMARK))[-1] == LAST_SESSION
    assert report.needed == 1


async def test_the_latest_universe_is_the_scope(session_factory):
    await _universe(session_factory, "OLD", day=date(2026, 10, 16))
    await _universe(session_factory, "NEW", day=date(2026, 10, 19))
    schwab = FakeSchwab().price("OLD", "NEW", BENCHMARK)

    await _run(session_factory, schwab, FakeClock(NOW))

    assert schwab.symbols_called() == [BENCHMARK, "NEW"]


async def test_bars_of_a_listing_that_left_the_universe_are_kept_and_no_longer_asked_for(session_factory):
    await _universe(session_factory, "GONE", "STAY", day=date(2026, 10, 19))
    schwab = FakeSchwab().price("GONE", "STAY", BENCHMARK)
    clock = FakeClock(NOW)
    await _run(session_factory, schwab, clock)
    kept = await _bars(session_factory, "GONE")
    await _universe(session_factory, "STAY", day=date(2026, 10, 20))
    schwab.calls.clear()

    clock.current = NOW + timedelta(days=1)
    await _run(session_factory, schwab, clock)

    assert schwab.symbols_called() == [BENCHMARK, "STAY"]
    assert await _bars(session_factory, "GONE") == kept != []


# --- the price-history spelling ----------------------------------------------------------------


def test_a_universe_symbol_is_asked_for_under_its_own_spelling_by_default():
    assert daily_bars.price_history_symbol("BRK.B") == "BRK.B"
    assert daily_bars.price_history_symbol("AAPL") == "AAPL"


async def test_the_request_spelling_comes_from_one_function_and_bars_keep_the_universe_symbol(
    session_factory, monkeypatch
):
    monkeypatch.setattr(daily_bars, "price_history_symbol", lambda symbol: symbol.replace(".", "/"))
    await _universe(session_factory, "BRK.B")
    schwab = FakeSchwab().price("BRK/B", BENCHMARK)

    await _run(session_factory, schwab, FakeClock(NOW))

    assert schwab.symbols_called() == [BENCHMARK, "BRK/B"]
    assert await _bars(session_factory, "BRK.B") == _sessions(SHORT_SEED)
    assert await _bars(session_factory, "BRK/B") == []
    async with session_factory() as s:
        assert await DailyBarRepository(s).earliest_requested(["BRK.B", "BRK/B"]) == {"BRK.B": SHORT_SEED}


# --- the seed ------------------------------------------------------------------------------------


async def test_a_symbol_with_no_bars_asks_from_1990_and_gets_what_schwab_has(session_factory, monkeypatch):
    monkeypatch.setattr(daily_bars, "SEED_START", date(1990, 1, 1))
    await _universe(session_factory, "OLD", "MID", "NEW")
    schwab = FakeSchwab().price(BENCHMARK, listed=date(1993, 1, 29))
    schwab.price("OLD", listed=date(1990, 1, 2)).price("MID", listed=date(2005, 6, 1))
    schwab.price("NEW", listed=date(2026, 9, 28))

    await _run(session_factory, schwab, FakeClock(NOW))

    assert sorted(schwab.calls) == sorted(
        (symbol, date(1990, 1, 1), LAST_SESSION) for symbol in ("OLD", "MID", "NEW", BENCHMARK)
    )
    old = await _bars(session_factory, "OLD")
    assert old == _sessions(date(1990, 1, 2))
    assert len(old) > 9000  # a seed is about 9,000 daily bars a symbol
    assert await _bars(session_factory, "MID") == _sessions(date(2005, 6, 1))
    assert await _bars(session_factory, "NEW") == _sessions(date(2026, 9, 28))


async def test_a_seeded_symbol_is_never_seeded_again_and_the_next_night_is_only_a_tail(
    session_factory, monkeypatch
):
    monkeypatch.setattr(daily_bars, "SEED_START", date(1990, 1, 1))
    await _universe(session_factory, "OLD", "NEW")
    schwab = FakeSchwab().price(BENCHMARK, "OLD", listed=date(1990, 1, 2)).price("NEW", listed=date(2026, 9, 28))
    clock = FakeClock(NOW)
    await _run(session_factory, schwab, clock)
    schwab.calls.clear()

    await _run(session_factory, schwab, clock)
    assert schwab.calls == []

    clock.current = NOW + timedelta(days=1)
    report = await _run(session_factory, schwab, clock)
    # Even the listing younger than the seed: it was asked once and has nothing earlier.
    assert sorted(schwab.calls) == sorted(
        (symbol, date(2026, 10, 16), date(2026, 10, 21)) for symbol in ("OLD", "NEW", BENCHMARK)
    )
    assert report.bars_inserted == 3


async def _store_with_a_later_seed(session_factory, schwab: FakeSchwab, clock: FakeClock, monkeypatch):
    """The state a box running the earlier job holds: shorter histories and no request record."""
    with monkeypatch.context() as earlier:
        earlier.setattr(daily_bars, "SEED_START", date(2026, 9, 14))
        await _run(session_factory, schwab, clock)
    async with session_factory() as s:
        await s.execute(text("DELETE FROM bar_request"))
        await s.commit()
    schwab.calls.clear()


async def test_bars_stored_with_a_shorter_history_are_extended_back_once(session_factory, monkeypatch):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)
    clock = FakeClock(NOW)
    await _store_with_a_later_seed(session_factory, schwab, clock, monkeypatch)
    assert (await _bars(session_factory, "AAA"))[0] == date(2026, 9, 14)

    report = await _run(session_factory, schwab, clock)

    assert sorted(schwab.calls) == sorted((s, SHORT_SEED, LAST_SESSION) for s in ("AAA", "BBB", "ZZZ", BENCHMARK))
    for symbol in ("AAA", "BBB", "ZZZ", BENCHMARK):
        assert await _bars(session_factory, symbol) == _sessions(SHORT_SEED)
    assert report.bars_inserted > 0

    schwab.calls.clear()
    await _run(session_factory, schwab, clock)
    assert schwab.calls == []


async def test_the_benchmark_already_covering_the_seed_is_not_refetched(session_factory):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)
    clock = FakeClock(NOW)
    # SPY was stored earlier, from before the seed, with no request on record.
    async with session_factory() as s:
        history = await SchwabMarketData(schwab, CALENDAR).daily_history(BENCHMARK, date(2025, 1, 2), LAST_SESSION)
        await DailyBarRepository(s).insert_many(daily_bars._to_stored(history, BENCHMARK, FETCHED))
        await s.commit()
    schwab.calls.clear()

    await _run(session_factory, schwab, clock)

    assert BENCHMARK not in schwab.symbols_called()
    assert (await _bars(session_factory, BENCHMARK))[0] == date(2025, 1, 2)


# --- the stop time and the resume ------------------------------------------------------------------


def test_the_pass_stops_at_the_next_3am_central():
    # Wednesday 2026-10-21 03:00 CDT is 08:00 UTC.
    assert stop_time(NOW) == datetime(2026, 10, 21, 8, 0, tzinfo=UTC)
    assert stop_time(datetime(2026, 10, 21, 7, 59, tzinfo=UTC)) == datetime(2026, 10, 21, 8, 0, tzinfo=UTC)
    assert stop_time(datetime(2026, 10, 21, 8, 0, tzinfo=UTC)) == datetime(2026, 10, 22, 8, 0, tzinfo=UTC)
    # After the clocks fall back (2026-11-01) 03:00 CST is 09:00 UTC.
    assert stop_time(datetime(2026, 11, 1, 5, 0, tzinfo=UTC)) == datetime(2026, 11, 1, 9, 0, tzinfo=UTC)
    # The clock time is configurable: 04:30 CDT is 09:30 UTC.
    assert stop_time(NOW, time(4, 30)) == datetime(2026, 10, 21, 9, 30, tzinfo=UTC)


class Ticking:
    """The Schwab adapter, with the clock moving on `step` after every call."""

    def __init__(self, inner: SchwabMarketData, clock: FakeClock, step: timedelta) -> None:
        self.inner, self.clock, self.step = inner, clock, step

    async def daily_history(self, symbol: str, start: date, end: date):
        try:
            return await self.inner.daily_history(symbol, start, end)
        finally:
            self.clock.current += self.step


async def _ticking_run(session_factory, schwab, clock, step: timedelta, stop_at: datetime):
    return await sync_daily_bars(
        session_factory=session_factory,
        source=Ticking(SchwabMarketData(schwab, CALENDAR), clock, step),
        limiter=Budget().limiter(),
        calendar=CALENDAR,
        clock=clock,
        stop_at=stop_at,
    )


async def test_the_pass_stops_at_the_stop_time_and_a_rerun_resumes_where_it_left_off(session_factory):
    await _universe(session_factory, "A1", "A2", "A3", "A4", "A5")
    schwab = FakeSchwab().price("A1", "A2", "A3", "A4", "A5", BENCHMARK)
    clock = FakeClock(NOW)

    # One hour a call against a stop 2.5 hours out: three calls are made, the fourth never is.
    report = await _ticking_run(
        session_factory, schwab, clock, timedelta(hours=1), NOW + timedelta(hours=2, minutes=30)
    )

    assert schwab.symbols_called() == [BENCHMARK, "A1", "A2"]
    assert report.left == 3 and report.fetched == 3
    assert await _bars(session_factory, "A3") == []

    # Later in the same session's evening: the symbols done are skipped, the rest are seeded.
    schwab.calls.clear()
    clock.current = NOW + timedelta(hours=5)
    report = await _run(session_factory, schwab, clock)

    assert schwab.calls == [(symbol, SHORT_SEED, LAST_SESSION) for symbol in ("A3", "A4", "A5")]
    assert report.left == 0
    assert await _bars(session_factory, "A5") == _sessions(SHORT_SEED)


async def test_a_pass_started_at_its_stop_time_makes_no_call(session_factory):
    await _universe(session_factory, "A1")
    schwab = FakeSchwab().price("A1", BENCHMARK)

    report = await _run(session_factory, schwab, FakeClock(NOW), stop_at=NOW)

    assert schwab.calls == []
    assert report.left == 2


async def test_a_stop_between_a_tail_and_the_refetch_it_asks_for_leaves_the_stored_bars_untouched(session_factory):
    schwab, clock, before = await _stored_then_next_session(session_factory)
    schwab.scale["AAA"] = 10.0  # split since: the tail will ask for a full refetch

    # An hour a call against a stop 90 minutes out: SPY, then AAA's tail; the refetch never starts.
    report = await _ticking_run(
        session_factory, schwab, clock, timedelta(hours=1), clock.now() + timedelta(minutes=90)
    )

    assert schwab.symbols_called() == [BENCHMARK, "AAA"]
    assert report.left == 3
    assert await _closes(session_factory, "AAA") == before  # the stale tail was not written either


# --- what is stored ------------------------------------------------------------------------------


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


async def test_a_listing_younger_than_the_seed_is_asked_once(session_factory):
    await _seed(session_factory)
    # BBB listed two sessions before its event: the seed window is mostly empty, and stays so.
    schwab = FakeSchwab().price("AAA", "ZZZ", BENCHMARK).price("BBB", listed=date(2026, 9, 28))
    clock = FakeClock(NOW)
    await _run(session_factory, schwab, clock)
    assert (await _bars(session_factory, "BBB"))[0] == date(2026, 9, 28)
    schwab.calls.clear()

    await _run(session_factory, schwab, clock)

    assert schwab.calls == []


async def test_a_long_catch_up_logs_its_plan_and_progress(session_factory, monkeypatch, caplog):
    await _seed(session_factory)
    monkeypatch.setattr(daily_bars, "PROGRESS_EVERY", 2)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)

    with caplog.at_level("INFO", logger="rs_ingest.daily_bars"):
        await _run(session_factory, schwab, FakeClock(NOW))

    messages = [r.getMessage() for r in caplog.records]
    assert "daily_bars: 4 of 4 symbols to fetch, one call each" in messages
    assert "daily_bars: 2 of 4 symbols done" in messages


async def test_the_run_says_it_started_before_the_trigger_replay_that_can_take_minutes(session_factory, caplog):
    await _seed(session_factory)
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)

    with caplog.at_level("INFO", logger="rs_ingest.daily_bars"):
        await _run(session_factory, schwab, FakeClock(NOW))

    messages = [r.getMessage() for r in caplog.records]
    assert messages[0] == "daily_bars: replaying the Popularity Trigger to find the stocks to fetch first"


# --- failures ------------------------------------------------------------------------------------


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

    with pytest.raises(DailyBarsError, match="1 of 4 symbols failed: BBB"):
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
        await sync_daily_bars(
            session_factory=session_factory,
            source=Dead(),
            limiter=Budget().limiter(),
            calendar=CALENDAR,
            clock=FakeClock(NOW),
            stop_at=stop_time(NOW),
        )
    assert schwab.calls == []


async def test_a_run_a_dead_token_killed_is_safe_to_rerun_and_repeats_no_work(session_factory):
    await _universe(session_factory, "A1", "A2", "A3")
    schwab = FakeSchwab().price("A1", "A2", "A3", BENCHMARK)
    inner = SchwabMarketData(schwab, CALENDAR)

    class DiesOnThird:
        calls = 0

        async def daily_history(self, symbol, start, end):
            self.calls += 1
            if self.calls == 3:
                raise SchwabAuthError("refresh token rejected")
            return await inner.daily_history(symbol, start, end)

    with pytest.raises(SchwabAuthError):
        await sync_daily_bars(
            session_factory=session_factory,
            source=DiesOnThird(),
            limiter=Budget().limiter(),
            calendar=CALENDAR,
            clock=FakeClock(NOW),
            stop_at=stop_time(NOW),
        )
    schwab.calls.clear()

    await _run(session_factory, schwab, FakeClock(NOW))

    assert schwab.symbols_called() == ["A2", "A3"]  # SPY and A1 were committed before the token died
    assert await _bars(session_factory, "A3") == _sessions(SHORT_SEED)


async def test_every_call_waits_on_the_shared_budget(session_factory):
    await _seed(session_factory)
    budget = Budget()
    schwab = FakeSchwab().price("AAA", "BBB", "ZZZ", BENCHMARK)

    await _run(session_factory, schwab, FakeClock(NOW), budget)

    # 4 calls at 1 per second: the first is free, the other three each wait a second.
    assert len(schwab.calls) == 4
    assert budget.waited == pytest.approx(3.0)


async def test_triggered_tickers_agree_with_the_rule_replay(session_factory):
    history = []
    for hours in range(0, 24 * 21, 6):  # AAA surges repeatedly; each hit is its own event
        at = datetime(2026, 9, 1, tzinfo=UTC) + timedelta(hours=hours)
        history.append(_snap("AAA", at, origin=PopularityOrigin.V1_IMPORT if hours % 12 else PopularityOrigin.LIVE))
        history.append(_snap("QUIET", at, mentions=100, prior=100))
    history.append(_snap("LATE", datetime(2026, 10, 18, 3, 0, tzinfo=UTC)))  # 23:00 ET on the 17th
    async with session_factory() as s:
        await PopularityRepository(s).insert_many(history)
        await s.commit()

    got = await triggered_tickers(session_factory, NOW)

    assert got == {event.ticker for event in popularity_surge.replay(history)} == {"AAA", "LATE"}


async def test_triggered_tickers_is_empty_with_no_history(session_factory):
    assert await triggered_tickers(session_factory, NOW) == set()


# --- bars stored before a split are repaired by overlap -----------------------------------------


async def _closes(session_factory, symbol: str) -> dict[date, float]:
    async with session_factory() as s:
        rows = await DailyBarRepository(s).list_range(symbol, date(1980, 1, 1), date(2100, 1, 1))
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

    assert report.repaired == 0 and report.reused == 0
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
    # A bar older than the seed, left by an earlier job: the fresh fetch will not have it.
    stale = StoredDailyBar(
        symbol="AAA", session_date=date(2025, 1, 2), open=11.0, high=12.0, low=10.0, close=11.0,
        volume=1, fetched_at=FETCHED, adjusted=True,
    )
    async with session_factory() as s:
        await DailyBarRepository(s).insert_many([stale])
        await s.commit()
    schwab.scale["AAA"] = 10.0  # 1-for-10 reverse split: every close is now ten times the stored one

    with caplog.at_level("WARNING", logger="rs_ingest.daily_bars"):
        report = await _run(session_factory, schwab, clock)

    assert [c for c in schwab.calls if c[0] == "AAA"] == [
        ("AAA", date(2026, 10, 16), date(2026, 10, 21)),  # the tail with its overlap
        ("AAA", SHORT_SEED, date(2026, 10, 21)),  # the full fetch it triggered
    ]
    after = await _closes(session_factory, "AAA")
    assert sorted(after) == _sessions(SHORT_SEED, date(2026, 10, 21))
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
    await _store_with_a_later_seed(session_factory, schwab, clock, monkeypatch)
    before = await _closes(session_factory, "AAA")
    schwab.scale["AAA"] = 0.1  # 10-for-1 forward split since

    await _run(session_factory, schwab, clock)

    assert [c for c in schwab.calls if c[0] == "AAA"] == [("AAA", SHORT_SEED, LAST_SESSION)]
    after = await _closes(session_factory, "AAA")
    assert sorted(after) == _sessions(SHORT_SEED)
    assert all(after[d] == pytest.approx(0.1 * before[d]) for d in before)


async def test_an_empty_answer_does_not_wipe_stored_bars(session_factory):
    schwab, clock, before = await _stored_then_next_session(session_factory)
    schwab.listed["AAA"] = None  # delisted or halted: Schwab answers with nothing

    report = await _run(session_factory, schwab, clock)

    assert report.repaired == 0 and report.reused == 0
    assert await _closes(session_factory, "AAA") == before
    assert "AAA" not in await _unpriced(session_factory)


async def test_a_tail_sharing_no_session_with_the_stored_bars_replaces_the_whole_history(session_factory, caplog):
    schwab, clock, before = await _stored_then_next_session(session_factory)
    # Another company took the ticker on Wednesday: Schwab has nothing of the old one's sessions.
    schwab.listed["AAA"] = date(2026, 10, 21)

    with caplog.at_level("WARNING", logger="rs_ingest.daily_bars"):
        report = await _run(session_factory, schwab, clock)

    assert [c for c in schwab.calls if c[0] == "AAA"] == [
        ("AAA", date(2026, 10, 16), date(2026, 10, 21)),
        ("AAA", SHORT_SEED, date(2026, 10, 21)),  # the full fetch behind the tail
    ]
    assert await _bars(session_factory, "AAA") == [date(2026, 10, 21)]
    assert (report.reused, report.repaired) == (1, 0)
    assert any("AAA" in r.getMessage() and "no session in common" in r.getMessage() for r in caplog.records)
    # The others carried on, and the replaced symbol is not replaced again the night after.
    assert await _closes(session_factory, BENCHMARK) != {}
    schwab.calls.clear()
    clock.current += timedelta(days=1)
    await _run(session_factory, schwab, clock)
    assert [c for c in schwab.calls if c[0] == "AAA"] == [("AAA", date(2026, 10, 19), date(2026, 10, 22))]
    assert (await _bars(session_factory, "AAA"))[0] == date(2026, 10, 21)


async def test_an_answer_with_overlap_is_not_a_reused_ticker(session_factory):
    schwab, clock, _ = await _stored_then_next_session(session_factory)

    report = await _run(session_factory, schwab, clock)

    assert report.reused == 0


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

    with pytest.raises(DailyBarsError, match="1 of 4 symbols failed: AAA"):
        await sync_daily_bars(
            session_factory=session_factory,
            source=_FailsFullFetch(SchwabMarketData(schwab, CALENDAR), date(2026, 10, 16)),
            limiter=Budget().limiter(),
            calendar=CALENDAR,
            clock=clock,
            stop_at=stop_time(clock.now()),
        )

    # No new-scale bar sits beside the old-scale ones.
    assert await _closes(session_factory, "AAA") == before

    report = await _run(session_factory, schwab, clock)
    assert report.repaired == 1
    assert max(await _closes(session_factory, "AAA")) == date(2026, 10, 21)


# --- compressed history --------------------------------------------------------------------------
#
# Timescale aborts a transaction that decompresses more than 100,000 tuples. Fourteen seeded
# symbols hold about 130,000 bars, so a write that touched them all at once would fail; each
# symbol's write may only touch its own.


def _held_like_the_fake(symbol: str, sessions: list[date], k: float = 1.0) -> list[StoredDailyBar]:
    """Bars with exactly the prices FakeSchwab serves, so that an overlap check finds no drift."""
    return [
        StoredDailyBar(
            symbol=symbol,
            session_date=d,
            open=k * (10.0 + d.toordinal() % 17),
            high=k * (12.0 + d.toordinal() % 17),
            low=k * (9.0 + d.toordinal() % 17),
            close=k * (11.0 + d.toordinal() % 17),
            volume=1000 + d.toordinal() % 17,
            fetched_at=FETCHED,
            adjusted=True,
        )
        for d in sessions
    ]


async def _compress_every_chunk(session_factory) -> None:
    async with session_factory() as s:
        await s.execute(text("SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('daily_bar') c"))
        await s.commit()


async def test_a_seed_a_tail_and_two_replacements_each_stay_under_the_decompression_cap(
    session_factory, monkeypatch
):
    monkeypatch.setattr(daily_bars, "SEED_START", date(1990, 1, 1))
    symbols = [f"S{n:02d}" for n in range(14)]
    await _universe(session_factory, *symbols)
    listed = date(1990, 1, 2)
    schwab = FakeSchwab().price(BENCHMARK, *symbols, listed=listed)
    clock = FakeClock(NOW)

    # The seed night: one call and one write per symbol, 9,000 bars and more each.
    await _run(session_factory, schwab, clock)
    async with session_factory() as s:
        assert await s.scalar(select(func.count()).select_from(DailyBarRow)) > 100_000
    assert await _bars(session_factory, "S13") == _sessions(listed)

    # The history sits compressed, and the cap is live in this database: one statement over
    # every symbol's bars is refused.
    await _compress_every_chunk(session_factory)
    async with session_factory() as s:
        with pytest.raises(DBAPIError, match="decompress"):
            await s.execute(text("UPDATE daily_bar SET volume = volume + 1"))
        await s.rollback()

    # The next night: a tail into every symbol's compressed chunks, a reverse split that replaces
    # one symbol's whole history, and a reused ticker that replaces another's.
    schwab.calls.clear()
    schwab.scale["S00"] = 10.0
    schwab.listed["S01"] = date(2026, 10, 21)
    clock.current = NOW + timedelta(days=1)
    report = await _run(session_factory, schwab, clock)

    assert (report.repaired, report.reused) == (1, 1)
    for symbol in [*symbols[2:], BENCHMARK]:
        assert await _bars(session_factory, symbol) == _sessions(listed, date(2026, 10, 21))
    assert await _closes(session_factory, "S00") == {
        b.session_date: b.close for b in _held_like_the_fake("S00", _sessions(listed, date(2026, 10, 21)), 10.0)
    }
    assert await _bars(session_factory, "S01") == [date(2026, 10, 21)]


async def test_a_tail_into_a_symbols_compressed_chunks_adds_only_the_new_sessions(session_factory, monkeypatch):
    monkeypatch.setattr(daily_bars, "SEED_START", date(2024, 1, 2))
    await _universe(session_factory, "AAA")
    async with session_factory() as s:
        repo = DailyBarRepository(s)
        await repo.insert_many(_held_like_the_fake("AAA", _sessions(date(2024, 1, 2), LAST_SESSION)))
        await repo.record_request("AAA", date(2024, 1, 2))
        await s.commit()
    await _compress_every_chunk(session_factory)
    schwab = FakeSchwab().price("AAA", BENCHMARK, listed=date(2024, 1, 2))

    report = await _run(session_factory, schwab, FakeClock(NOW + timedelta(days=1)))

    assert [c for c in schwab.calls if c[0] == "AAA"] == [("AAA", date(2026, 10, 16), date(2026, 10, 21))]
    assert await _bars(session_factory, "AAA") == _sessions(date(2024, 1, 2), date(2026, 10, 21))
    assert report.repaired == 0 and report.reused == 0
