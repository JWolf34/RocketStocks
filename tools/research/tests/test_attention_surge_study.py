"""The attention-surge event study end to end against a real database: synthetic
snapshots, bars and alerts in, a base-rate row `rs_query` reads out, with every
statistic worked out by hand below.

Session index i counts from Monday 2026-08-03 (i = 0); the stored sessions run
from i = -5 (Mon Jul 27) to i = 19 (Fri Aug 28). SPY closes 100 + i.
"""

import asyncio
from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import select

from rs_domain import (
    Alert,
    BaseRate,
    BaseRateInput,
    PopularityOrigin,
    PopularitySnapshot,
    StoredDailyBar,
)
from rs_persistence import (
    AlertRepository,
    BaseRateRow,
    DailyBarRepository,
    PopularityRepository,
)
from rs_query import current_base_rate
from rs_research import attention_surge
from rs_research.__main__ import main
from rs_research.attention_surge import StudyError

AUG_3 = date(2026, 8, 3)
NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


class FixedClock:
    def now(self) -> datetime:
        return NOW


def _sessions() -> dict[int, date]:
    days: dict[int, date] = {}
    day, i = date(2026, 7, 27), -5
    while day <= date(2026, 8, 28):
        if day.weekday() < 5:
            days[i] = day
            i += 1
        day += timedelta(days=1)
    return days


SESSIONS = _sessions()

# Close price by session index; CCC is deliberately absent (no bars).
CLOSES = {
    "SPY": lambda i: 100.0 + i,
    "AAA": lambda i: 50.0 + 2 * i,
    "BBB": lambda i: 80.0 - i,
    "DDD": lambda i: 10.0,
    "EEE": lambda i: 10.0,
    "FFF": lambda i: 100.0 + 3 * i,
    "GGG": lambda i: 100.0,
}


def excess(entry: float, exit_: float, spy_entry: float, spy_exit: float) -> float:
    return (exit_ / entry - 1) - (spy_exit / spy_entry - 1)


def _bars() -> list[StoredDailyBar]:
    fetched = datetime(2026, 8, 29, tzinfo=UTC)
    return [
        StoredDailyBar(
            symbol=symbol,
            session_date=day,
            open=price(i),
            high=price(i),
            low=price(i),
            close=price(i),
            volume=1000,
            fetched_at=fetched,
            adjusted=True,
        )
        for symbol, price in CLOSES.items()
        for i, day in SESSIONS.items()
    ]


def _surge(ticker: str, at: datetime, mentions: int = 100) -> PopularitySnapshot:
    return PopularitySnapshot(
        polled_at=at,
        ticker=ticker,
        rank=100,
        mentions=mentions,
        upvotes=0,
        rank_24h_ago=None,
        mentions_24h_ago=20,
        fetched_at=at + timedelta(seconds=20),
        origin=PopularityOrigin.LIVE,
    )


def _utc(day: int, hour: int, minute: int = 0, month: int = 8) -> datetime:
    return datetime(2026, month, day, hour, minute, tzinfo=UTC)


SNAPSHOTS = [
    _surge("AAA", _utc(3, 14), mentions=20),  # quiet: no event
    _surge("AAA", _utc(3, 15)),  # event, entry Tue Aug 4 (i = 1)
    _surge("AAA", _utc(3, 16)),  # inside the 24h cooldown: no second event
    _surge("BBB", _utc(5, 15)),  # entry Thu Aug 6 (i = 3)
    _surge("CCC", _utc(4, 15)),  # no bars: unpriced
    _surge("DDD", _utc(27, 15)),  # entry Fri Aug 28, the last stored session: no horizon yet
    _surge("EEE", _utc(28, 15)),  # entry session has not happened: not measured
    _surge("FFF", _utc(8, 12)),  # Saturday: entry Mon Aug 10 (i = 5)
    _surge("GGG", _utc(4, 15)),  # entry Wed Aug 5 (i = 2)
]


def _alert(symbol: str, at: datetime, price: float | None, **overrides: object) -> Alert:
    fields: dict[str, object] = {
        "symbol": symbol,
        "alert_type": "attention_surge",
        "triggered_at": at,
        "rule_version": "1",
        "alert_price": price,
        "quote_time": at,
        "dedup_key": f"{symbol}:{at.isoformat()}:{price}",
    }
    return Alert(**(fields | overrides))


ALERTS = [
    _alert("AAA", _utc(3, 15), 51.0),  # 11:00 ET Monday: reference is Friday's close (i = -1)
    _alert("BBB", _utc(5, 21), 78.0),  # 17:00 ET: reference is that day's close (i = 2)
    _alert("FFF", _utc(8, 12), 114.0),  # Saturday: reference is Friday's close (i = 4)
    _alert("GGG", _utc(5, 15), None),  # no alert price: unpriced
    _alert("CCC", _utc(5, 15), 9.0),  # no bars to measure an exit: unpriced
    _alert("AAA", _utc(6, 15), 60.0, alert_type="large_move"),  # other signal: ignored
    _alert("AAA", _utc(6, 16), 60.0, rule_version="0"),  # other rule version: ignored
]


async def _seed(session_factory, snapshots=SNAPSHOTS, alerts=ALERTS, bars=None) -> None:
    async with session_factory() as s:
        await PopularityRepository(s).insert_many(snapshots)
        await DailyBarRepository(s).insert_many(_bars() if bars is None else bars)
        repository = AlertRepository(s)
        for alert in alerts:
            await repository.insert(alert)
        await s.commit()


async def _rows(session_factory) -> list[BaseRateRow]:
    async with session_factory() as s:
        return list((await s.scalars(select(BaseRateRow).order_by(BaseRateRow.id))).all())


def _by_input(rates: list[BaseRate]) -> dict[BaseRateInput, BaseRate]:
    return {rate.input: rate for rate in rates}


async def test_reconstructed_base_rate_is_stored_and_read_back_by_rs_query(session_factory):
    await _seed(session_factory)

    result = await attention_surge.run(session_factory, FixedClock())

    async with session_factory() as s:
        stored = await current_base_rate(s, "attention_surge", "1")
    assert stored is not None
    assert stored == _by_input(result.rates)[BaseRateInput.RECONSTRUCTED]
    assert (stored.input, stored.computed_at) == (BaseRateInput.RECONSTRUCTED, NOW)
    # AAA, BBB, FFF, GGG and DDD are entered (EEE has no entry session yet); CCC has no bars.
    assert (stored.n, stored.n_unpriced) == (5, 1)
    aaa = [excess(52, 54, 101, 102), excess(52, 62, 101, 106), excess(52, 72, 101, 111)]
    bbb = [excess(77, 76, 103, 104), excess(77, 72, 103, 108), excess(77, 67, 103, 113)]
    fff = [excess(115, 118, 105, 106), excess(115, 130, 105, 110), excess(115, 145, 105, 115)]
    ggg = [excess(100, 100, 102, 103), excess(100, 100, 102, 107), excess(100, 100, 102, 112)]
    # DDD has no exit session stored, so it adds to N but to no horizon.
    assert stored.hit_rate_1d == 0.5  # AAA and FFF beat SPY; BBB and GGG did not
    assert stored.hit_rate_5d == 0.5
    assert stored.hit_rate_10d == 0.5
    median = lambda values: sorted(values)[1] / 2 + sorted(values)[2] / 2  # noqa: E731
    assert stored.median_excess_1d == pytest.approx(median([aaa[0], bbb[0], fff[0], ggg[0]]))
    assert stored.median_excess_5d == pytest.approx(median([aaa[1], bbb[1], fff[1], ggg[1]]))
    assert stored.median_excess_10d == pytest.approx(median([aaa[2], bbb[2], fff[2], ggg[2]]))
    assert stored.median_excess_1d == pytest.approx(0.003380, abs=1e-6)
    assert "1 of 6" in stored.survivorship_caveat


async def test_logged_base_rate_enters_at_the_alert_price(session_factory):
    await _seed(session_factory)

    result = await attention_surge.run(session_factory, FixedClock())

    logged = _by_input(result.rates)[BaseRateInput.LOGGED]
    assert (logged.n, logged.n_unpriced) == (3, 2)  # a missing alert price, and a symbol with no bars
    aaa = [excess(51, 50, 99, 100), excess(51, 58, 99, 104), excess(51, 68, 99, 109)]
    bbb = [excess(78, 77, 102, 103), excess(78, 73, 102, 107), excess(78, 68, 102, 112)]
    fff = [excess(114, 115, 104, 105), excess(114, 127, 104, 109), excess(114, 142, 104, 114)]
    for day, hits in ((1, 0.0), (5, 2 / 3), (10, 2 / 3)):
        k = (1, 5, 10).index(day)
        assert getattr(logged, f"hit_rate_{day}d") == pytest.approx(hits)
        assert getattr(logged, f"median_excess_{day}d") == pytest.approx(sorted([aaa[k], bbb[k], fff[k]])[1])
    assert logged.median_excess_1d == pytest.approx(-0.022625, abs=1e-6)
    assert logged.median_excess_10d == pytest.approx(0.149460, abs=1e-6)
    assert "2 of 5" in logged.survivorship_caveat


async def test_a_logged_row_is_only_shown_once_it_has_enough_alerts(session_factory):
    await _seed(session_factory)
    await attention_surge.run(session_factory, FixedClock())

    inputs = [row.input for row in await _rows(session_factory)]

    assert sorted(inputs) == ["logged", "reconstructed"]
    async with session_factory() as s:
        shown = await current_base_rate(s, "attention_surge", "1")
    assert shown is not None
    assert shown.input is BaseRateInput.RECONSTRUCTED  # 3 logged alerts is below the minimum N


async def test_the_same_database_and_clock_give_the_same_rows(session_factory):
    await _seed(session_factory)

    first = await attention_surge.compute(session_factory, FixedClock())
    second = await attention_surge.compute(session_factory, FixedClock())

    assert first == second


async def test_a_rerun_appends_rows_and_readers_take_the_latest(session_factory):
    await _seed(session_factory)

    await attention_surge.run(session_factory, FixedClock())
    await attention_surge.run(session_factory, FixedClock())

    assert len(await _rows(session_factory)) == 4


async def test_events_whose_ticker_has_no_bars_are_counted_not_dropped(session_factory):
    spy_only = [b for b in _bars() if b.symbol == "SPY"]
    await _seed(session_factory, alerts=[], bars=spy_only)

    result = await attention_surge.compute(session_factory, FixedClock())

    # Every replayed event is unpriced, so nothing is measured and nothing is written.
    assert result.rates == []
    assert result.events == 7


async def test_an_event_before_the_stored_benchmark_history_is_unpriced(session_factory):
    early = _surge("AAA", _utc(1, 15, month=7))  # three weeks before the first stored SPY session
    await _seed(session_factory, snapshots=[early, *SNAPSHOTS], alerts=[])

    result = await attention_surge.compute(session_factory, FixedClock())

    [reconstructed] = result.rates
    assert (reconstructed.n, reconstructed.n_unpriced) == (5, 2)


async def test_nothing_is_written_when_there_is_nothing_to_measure(session_factory):
    result = await attention_surge.run(session_factory, FixedClock())

    assert result.rates == []
    assert await _rows(session_factory) == []


async def test_missing_benchmark_bars_fail_loudly(session_factory):
    await _seed(session_factory, bars=[b for b in _bars() if b.symbol != "SPY"])

    with pytest.raises(StudyError, match="SPY"):
        await attention_surge.run(session_factory, FixedClock())
    assert await _rows(session_factory) == []


async def test_snapshots_after_the_clock_are_not_replayed(session_factory):
    await _seed(session_factory, alerts=[])

    class EarlyClock:
        def now(self) -> datetime:
            return _utc(4, 12)  # only AAA's Aug 3 events exist yet

    result = await attention_surge.compute(session_factory, EarlyClock())

    assert result.events == 1


async def test_cli_runs_the_study_and_exits_zero(session_factory, migrated_db_url, monkeypatch):
    await _seed(session_factory)
    monkeypatch.setenv("RS_RESEARCH_DB_URL", migrated_db_url)

    code = await asyncio.to_thread(main, ["event-study", "attention_surge"], FixedClock())

    assert code == 0
    assert [row.input for row in await _rows(session_factory)] == ["reconstructed", "logged"]


async def test_cli_exits_one_when_the_study_fails(session_factory, migrated_db_url, monkeypatch):
    await _seed(session_factory, bars=[])
    monkeypatch.setenv("RS_RESEARCH_DB_URL", migrated_db_url)

    code = await asyncio.to_thread(main, ["event-study", "attention_surge"], FixedClock())

    assert code == 1
    assert await _rows(session_factory) == []


def test_cli_requires_its_own_dsn(monkeypatch, capsys):
    monkeypatch.delenv("RS_RESEARCH_DB_URL", raising=False)
    monkeypatch.setenv("RS_DB_URL", "postgresql+psycopg://wider:role@localhost/db")

    assert main(["event-study", "attention_surge"]) == 2
    assert "RS_RESEARCH_DB_URL" in capsys.readouterr().err


def test_cli_rejects_an_unknown_study():
    with pytest.raises(SystemExit):
        main(["event-study", "nope"])


async def test_imported_rows_without_upvotes_replay_like_any_other(session_factory):
    imported = [
        s.model_copy(update={"upvotes": None, "origin": PopularityOrigin.V1_IMPORT}) for s in SNAPSHOTS
    ]
    await _seed(session_factory, snapshots=imported, alerts=[])

    result = await attention_surge.compute(session_factory, FixedClock())

    [reconstructed] = result.rates
    assert (reconstructed.n, reconstructed.n_unpriced) == (5, 1)
