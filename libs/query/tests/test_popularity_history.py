from datetime import UTC, datetime, timedelta

import pytest

from rs_domain import PopularityOrigin, PopularitySnapshot
from rs_persistence import PopularityRepository
from rs_query import InvalidCursor, popularity_history
from rs_query.popularity import MAX_LIMIT

T0 = datetime(2026, 7, 6, 14, 0, tzinfo=UTC)
STEP = timedelta(minutes=30)


def _snap(ticker: str, i: int, **overrides) -> PopularitySnapshot:
    fields = {
        "polled_at": T0 + i * STEP,
        "ticker": ticker,
        "rank": 3 + i,
        "mentions": 120,
        "upvotes": 800,
        "rank_24h_ago": 9,
        "mentions_24h_ago": None,
        "fetched_at": T0 + i * STEP + timedelta(seconds=7),
        "origin": PopularityOrigin.LIVE,
    }
    return PopularitySnapshot(**(fields | overrides))


async def _seed(session_factory, snaps):
    async with session_factory() as s:
        await PopularityRepository(s).insert_many(snaps)
        await s.commit()


async def test_returns_one_tickers_window_oldest_first_as_domain_models(session_factory):
    snaps = [_snap("AAA", i) for i in range(4)]
    await _seed(session_factory, [*snaps, _snap("BBB", 1)])

    async with session_factory() as s:
        page = await popularity_history(s, "AAA", T0 + STEP, T0 + 3 * STEP)

    assert page.items == (snaps[1], snaps[2])  # [start, end): end excluded
    assert page.next_cursor is None


async def test_pagination_round_trip_visits_every_row_once(session_factory):
    snaps = [_snap("AAA", i) for i in range(7)]
    await _seed(session_factory, snaps)

    seen: list[PopularitySnapshot] = []
    cursor = None
    pages = 0
    async with session_factory() as s:
        while True:
            page = await popularity_history(s, "AAA", T0, T0 + 10 * STEP, limit=3, cursor=cursor)
            seen.extend(page.items)
            pages += 1
            cursor = page.next_cursor
            if cursor is None:
                break

    assert seen == snaps
    assert pages == 3


async def test_exact_multiple_of_limit_ends_without_a_dangling_cursor(session_factory):
    await _seed(session_factory, [_snap("AAA", i) for i in range(4)])

    async with session_factory() as s:
        first = await popularity_history(s, "AAA", T0, T0 + 10 * STEP, limit=2)
        assert first.next_cursor is not None
        last = await popularity_history(s, "AAA", T0, T0 + 10 * STEP, limit=2, cursor=first.next_cursor)

    assert len(last.items) == 2
    assert last.next_cursor is None


async def test_limit_above_the_cap_is_capped_and_continues_via_cursor(session_factory):
    count = MAX_LIMIT + 5
    await _seed(session_factory, [_snap("AAA", i) for i in range(count)])

    async with session_factory() as s:
        page = await popularity_history(s, "AAA", T0, T0 + (count + 1) * STEP, limit=MAX_LIMIT * 10)
        rest = await popularity_history(
            s, "AAA", T0, T0 + (count + 1) * STEP, limit=MAX_LIMIT * 10, cursor=page.next_cursor
        )

    assert len(page.items) == MAX_LIMIT
    assert len(rest.items) == 5
    assert rest.next_cursor is None


async def test_rejects_bad_inputs(session_factory):
    async with session_factory() as s:
        with pytest.raises(ValueError, match="limit"):
            await popularity_history(s, "AAA", T0, T0 + STEP, limit=0)
        with pytest.raises(ValueError, match="timezone-aware"):
            await popularity_history(s, "AAA", datetime(2026, 7, 6), T0 + STEP)
        with pytest.raises(InvalidCursor):
            await popularity_history(s, "AAA", T0, T0 + STEP, cursor="not-a-cursor")
