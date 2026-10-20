from datetime import UTC, datetime, timedelta

import pytest

from rs_domain import PopularityOrigin, PopularitySnapshot
from rs_persistence import PopularityRepository
from rs_query import InvalidCursor, popularity_between, popularity_history
from rs_query.popularity import BETWEEN_MAX_LIMIT

T0 = datetime(2026, 7, 6, 14, 0, tzinfo=UTC)
STEP = timedelta(minutes=30)


def _snap(ticker: str, i: int) -> PopularitySnapshot:
    return PopularitySnapshot(
        polled_at=T0 + i * STEP,
        ticker=ticker,
        rank=3 + i,
        mentions=120,
        upvotes=800,
        rank_24h_ago=9,
        mentions_24h_ago=None,
        fetched_at=T0 + i * STEP + timedelta(seconds=7),
        origin=PopularityOrigin.LIVE,
    )


async def _seed(session_factory, snaps):
    async with session_factory() as s:
        await PopularityRepository(s).insert_many(snaps)
        await s.commit()


async def test_returns_every_tickers_window_by_time_then_ticker(session_factory):
    snaps = [_snap("BBB", 0), _snap("AAA", 0), _snap("AAA", 1), _snap("BBB", 1), _snap("AAA", 2)]
    await _seed(session_factory, snaps)

    async with session_factory() as s:
        page = await popularity_between(s, T0, T0 + 2 * STEP)

    assert [(p.polled_at, p.ticker) for p in page.items] == [
        (T0, "AAA"),
        (T0, "BBB"),
        (T0 + STEP, "AAA"),
        (T0 + STEP, "BBB"),
    ]  # [start, end): the 2*STEP row is excluded
    assert page.next_cursor is None


async def test_cursor_that_splits_a_shared_timestamp_loses_and_repeats_nothing(session_factory):
    snaps = [_snap(t, i) for i in range(3) for t in ("AAA", "BBB", "CCC")]
    await _seed(session_factory, snaps)

    seen: list[PopularitySnapshot] = []
    cursor = None
    async with session_factory() as s:
        while True:
            page = await popularity_between(s, T0, T0 + 10 * STEP, limit=2, cursor=cursor)
            seen.extend(page.items)
            cursor = page.next_cursor
            if cursor is None:
                break

    assert sorted(seen, key=lambda p: (p.polled_at, p.ticker)) == seen
    assert len(seen) == len({(p.polled_at, p.ticker) for p in seen}) == len(snaps)


async def test_limit_above_the_cap_is_capped_and_continues_via_cursor(session_factory):
    count = BETWEEN_MAX_LIMIT + 3
    await _seed(session_factory, [_snap("AAA", i) for i in range(count)])

    async with session_factory() as s:
        page = await popularity_between(s, T0, T0 + (count + 1) * STEP, limit=BETWEEN_MAX_LIMIT * 10)
        rest = await popularity_between(
            s, T0, T0 + (count + 1) * STEP, limit=BETWEEN_MAX_LIMIT * 10, cursor=page.next_cursor
        )

    assert len(page.items) == BETWEEN_MAX_LIMIT
    assert len(rest.items) == 3
    assert rest.next_cursor is None


async def test_rejects_bad_inputs(session_factory):
    async with session_factory() as s:
        with pytest.raises(ValueError, match="limit"):
            await popularity_between(s, T0, T0 + STEP, limit=0)
        with pytest.raises(ValueError, match="timezone-aware"):
            await popularity_between(s, datetime(2026, 7, 6), T0 + STEP)
        with pytest.raises(InvalidCursor):
            await popularity_between(s, T0, T0 + STEP, cursor="not-a-cursor")


async def test_a_cursor_from_another_query_is_rejected(session_factory):
    await _seed(session_factory, [_snap("AAA", i) for i in range(3)])
    async with session_factory() as s:
        other = await popularity_history(s, "AAA", T0, T0 + 10 * STEP, limit=1)
        assert other.next_cursor is not None
        with pytest.raises(InvalidCursor):
            await popularity_between(s, T0, T0 + 10 * STEP, cursor=other.next_cursor)
