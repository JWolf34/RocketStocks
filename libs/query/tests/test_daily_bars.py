from datetime import UTC, date, datetime, timedelta

import pytest

from rs_domain import PopularityOrigin, PopularitySnapshot, StoredDailyBar
from rs_persistence import DailyBarRepository, PopularityRepository
from rs_query import InvalidCursor, daily_bars, popularity_history
from rs_query.daily_bars import MAX_LIMIT

D0 = date(2026, 6, 1)
FETCHED = datetime(2026, 7, 6, 21, 0, tzinfo=UTC)


def _bar(symbol: str, i: int) -> StoredDailyBar:
    # Calendar days, not sessions: the repository trusts the vendor edge to validate.
    return StoredDailyBar(
        symbol=symbol,
        session_date=D0 + timedelta(days=i),
        open=10.0 + i,
        high=11.0 + i,
        low=9.0 + i,
        close=10.5 + i,
        volume=1000 + i,
        fetched_at=FETCHED,
        adjusted=True,
    )


async def _seed(session_factory, bars):
    async with session_factory() as s:
        await DailyBarRepository(s).insert_many(bars)
        await s.commit()


async def test_returns_inclusive_range_oldest_first(session_factory):
    bars = [_bar("AAA", i) for i in range(5)]
    await _seed(session_factory, [*bars, _bar("SPY", 2)])

    async with session_factory() as s:
        page = await daily_bars(s, "AAA", D0 + timedelta(days=1), D0 + timedelta(days=3))

    assert page.items == tuple(bars[1:4])
    assert page.next_cursor is None


async def test_pagination_round_trip_visits_every_row_once(session_factory):
    bars = [_bar("AAA", i) for i in range(7)]
    await _seed(session_factory, bars)

    seen: list[StoredDailyBar] = []
    cursor = None
    async with session_factory() as s:
        while True:
            page = await daily_bars(s, "AAA", D0, D0 + timedelta(days=30), limit=3, cursor=cursor)
            seen.extend(page.items)
            cursor = page.next_cursor
            if cursor is None:
                break

    assert seen == bars


async def test_limit_above_the_cap_is_capped(session_factory):
    count = MAX_LIMIT + 3
    await _seed(session_factory, [_bar("AAA", i) for i in range(count)])

    async with session_factory() as s:
        page = await daily_bars(s, "AAA", D0, D0 + timedelta(days=count), limit=MAX_LIMIT * 5)

    assert len(page.items) == MAX_LIMIT
    assert page.next_cursor is not None


async def test_rejects_a_bad_limit_and_a_cursor_from_another_query(session_factory):
    await _seed(session_factory, [_bar("AAA", i) for i in range(3)])
    async with session_factory() as s:
        snaps = [
            PopularitySnapshot(
                polled_at=FETCHED - timedelta(hours=1, minutes=30 * i),
                ticker="AAA",
                rank=1,
                mentions=20,
                upvotes=5,
                fetched_at=FETCHED,
                origin=PopularityOrigin.LIVE,
            )
            for i in range(2)
        ]
        await PopularityRepository(s).insert_many(snaps)
        await s.commit()
        with pytest.raises(ValueError, match="limit"):
            await daily_bars(s, "AAA", D0, D0, limit=-1)
        with pytest.raises(InvalidCursor):
            await daily_bars(s, "AAA", D0, D0, cursor="not-a-cursor")
        # A popularity cursor must not be accepted by a bars query.
        foreign = (await popularity_history(s, "AAA", FETCHED - timedelta(days=1), FETCHED, limit=1)).next_cursor
        assert foreign is not None
        with pytest.raises(InvalidCursor):
            await daily_bars(s, "AAA", D0, D0 + timedelta(days=5), cursor=foreign)
