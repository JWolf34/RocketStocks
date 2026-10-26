"""DailyBarRepository: upsert-ignore bars, coverage queries, unpriced symbols."""

from datetime import UTC, date, datetime, timedelta

from sqlalchemy import func, select

from rs_domain import StoredDailyBar
from rs_persistence import DailyBarRepository, DailyBarRow

FETCHED = datetime(2026, 7, 7, 21, 0, tzinfo=UTC)
D0 = date(2026, 7, 6)


def _bar(symbol: str, session_date: date = D0, **overrides) -> StoredDailyBar:
    fields = {
        "symbol": symbol,
        "session_date": session_date,
        "open": 10.5,
        "high": 11.25,
        "low": 10.0,
        "close": 11.0,
        "volume": 1_200_000,
        "fetched_at": FETCHED,
        "adjusted": True,
    }
    return StoredDailyBar(**(fields | overrides))


async def _count(session_factory) -> int:
    async with session_factory() as s:
        return await s.scalar(select(func.count()).select_from(DailyBarRow))


async def test_insert_many_round_trips_bit_exact(session_factory):
    bar = _bar("AAA", open=10.1, high=10.30000000000001)
    async with session_factory() as s:
        assert await DailyBarRepository(s).insert_many([bar]) == 1
        await s.commit()

    async with session_factory() as s:
        [got] = await DailyBarRepository(s).list_range("AAA", D0, D0)
    assert got == bar
    assert got.high.hex() == bar.high.hex()


async def test_inserting_the_same_bars_twice_changes_nothing(session_factory):
    bars = [_bar("AAA"), _bar("AAA", D0 + timedelta(days=1)), _bar("BBB")]
    async with session_factory() as s:
        assert await DailyBarRepository(s).insert_many(bars) == 3
        await s.commit()
    async with session_factory() as s:
        refetched = [_bar("AAA", close=99.0), *bars[1:], _bar("CCC")]
        assert await DailyBarRepository(s).insert_many(refetched) == 1
        await s.commit()

    assert await _count(session_factory) == 4
    async with session_factory() as s:
        [kept] = await DailyBarRepository(s).list_range("AAA", D0, D0)
    assert kept.close == 11.0  # first write wins


async def test_list_range_is_inclusive_ordered_and_scoped(session_factory):
    days = [D0 + timedelta(days=i) for i in range(4)]
    async with session_factory() as s:
        await DailyBarRepository(s).insert_many(
            [
                _bar("AAA", days[2]),
                _bar("AAA", days[0]),
                _bar("AAA", days[1]),
                _bar("AAA", days[3]),
                _bar("BBB", days[1]),
            ]
        )
        await s.commit()

    async with session_factory() as s:
        got = await DailyBarRepository(s).list_range("AAA", days[1], days[2])
    assert [b.session_date for b in got] == [days[1], days[2]]


async def test_latest_session_dates_and_missing_symbols(session_factory):
    async with session_factory() as s:
        await DailyBarRepository(s).insert_many(
            [_bar("AAA"), _bar("AAA", D0 + timedelta(days=3)), _bar("BBB", D0 + timedelta(days=1))]
        )
        await s.commit()

    async with session_factory() as s:
        repo = DailyBarRepository(s)
        assert await repo.latest_session_dates(["AAA", "BBB", "ZZZ"]) == {
            "AAA": D0 + timedelta(days=3),
            "BBB": D0 + timedelta(days=1),
        }
        assert await repo.missing_symbols(["ZZZ", "AAA", "YYY"]) == ["YYY", "ZZZ"]
        assert await repo.missing_symbols([]) == []


async def test_session_bounds_give_oldest_and_newest_per_symbol(session_factory):
    async with session_factory() as s:
        await DailyBarRepository(s).insert_many(
            [_bar("AAA", D0 + timedelta(days=3)), _bar("AAA"), _bar("AAA", D0 + timedelta(days=1)), _bar("BBB")]
        )
        await s.commit()

    async with session_factory() as s:
        repo = DailyBarRepository(s)
        assert await repo.session_bounds(["AAA", "BBB", "ZZZ"]) == {
            "AAA": (D0, D0 + timedelta(days=3)),
            "BBB": (D0, D0),
        }
        assert await repo.session_bounds([]) == {}


async def test_record_unpriced_keeps_first_seen_and_advances_last_attempt(session_factory):
    first = datetime(2026, 7, 7, 22, 0, tzinfo=UTC)
    later = first + timedelta(days=7)
    async with session_factory() as s:
        repo = DailyBarRepository(s)
        await repo.record_unpriced("DEAD", "empty history", first)
        await repo.record_unpriced("DEAD", "still empty", later)
        await repo.record_unpriced("OTHER", "delisted", later)
        await s.commit()

    async with session_factory() as s:
        got = await DailyBarRepository(s).list_unpriced()
    assert [(u.symbol, u.first_seen, u.last_attempt, u.reason) for u in got] == [
        ("DEAD", first, later, "still empty"),
        ("OTHER", later, later, "delisted"),
    ]


async def test_clear_unpriced(session_factory):
    async with session_factory() as s:
        repo = DailyBarRepository(s)
        await repo.record_unpriced("DEAD", "empty history", FETCHED)
        assert await repo.clear_unpriced("DEAD") is True
        assert await repo.clear_unpriced("DEAD") is False
        assert await repo.list_unpriced() == []


async def test_record_request_only_ever_moves_a_symbols_start_earlier(session_factory):
    async with session_factory() as s:
        repo = DailyBarRepository(s)
        await repo.record_request("AAA", D0)
        await repo.record_request("AAA", D0 - timedelta(days=30))
        await repo.record_request("AAA", D0 + timedelta(days=5))
        await repo.record_request("BBB", D0)
        await s.commit()

    async with session_factory() as s:
        repo = DailyBarRepository(s)
        assert await repo.earliest_requested(["AAA", "BBB", "NEVER"]) == {
            "AAA": D0 - timedelta(days=30),
            "BBB": D0,
        }
        assert await repo.earliest_requested([]) == {}


async def test_replace_symbol_swaps_the_whole_series_and_leaves_other_symbols(session_factory):
    old = [_bar("AAA", D0), _bar("AAA", D0 + timedelta(days=1)), _bar("BBB", D0)]
    async with session_factory() as s:
        await DailyBarRepository(s).insert_many(old)
        await s.commit()

    fresh = [_bar("AAA", D0 + timedelta(days=1), close=110.0), _bar("AAA", D0 + timedelta(days=2), close=111.0)]
    async with session_factory() as s:
        assert await DailyBarRepository(s).replace_symbol("AAA", fresh) == 2
        await s.commit()

    async with session_factory() as s:
        repo = DailyBarRepository(s)
        assert await repo.list_range("AAA", D0, D0 + timedelta(days=9)) == fresh  # D0 is gone, new close wins
        assert await repo.list_range("BBB", D0, D0 + timedelta(days=9)) == [old[2]]


async def test_replace_symbol_rolls_back_with_the_caller_transaction(session_factory):
    async with session_factory() as s:
        await DailyBarRepository(s).insert_many([_bar("AAA")])
        await s.commit()

    async with session_factory() as s:
        await DailyBarRepository(s).replace_symbol("AAA", [_bar("AAA", close=5.0)])
        await s.rollback()

    async with session_factory() as s:
        [kept] = await DailyBarRepository(s).list_range("AAA", D0, D0)
    assert kept.close == 11.0
