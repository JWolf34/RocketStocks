"""PopularityRepository: idempotent inserts (criterion: the same snapshots twice
leave counts unchanged), reads, and the DB enum matching the domain vocabulary."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select, text

from rs_domain import PopularityOrigin, PopularitySnapshot
from rs_persistence import PopularityRepository, PopularitySnapshotRow

T0 = datetime(2026, 7, 6, 14, 0, tzinfo=UTC)
FETCHED = datetime(2026, 7, 6, 14, 0, 7, tzinfo=UTC)


def _snap(ticker: str, polled_at: datetime = T0, **overrides) -> PopularitySnapshot:
    fields = {
        "polled_at": polled_at,
        "ticker": ticker,
        "rank": 3,
        "mentions": 120,
        "upvotes": 800,
        "rank_24h_ago": 9,
        "mentions_24h_ago": 40,
        "fetched_at": FETCHED,
        "origin": PopularityOrigin.LIVE,
    }
    return PopularitySnapshot(**(fields | overrides))


async def _count(session_factory) -> int:
    async with session_factory() as s:
        return await s.scalar(select(func.count()).select_from(PopularitySnapshotRow))


async def test_insert_many_round_trips_every_field(session_factory):
    snaps = [
        _snap("AAA"),
        _snap("BBB", rank_24h_ago=None, mentions_24h_ago=None, origin=PopularityOrigin.V1_IMPORT),
    ]
    async with session_factory() as s:
        assert await PopularityRepository(s).insert_many(snaps) == 2
        await s.commit()

    async with session_factory() as s:
        got = await PopularityRepository(s).list_between(T0, T0 + timedelta(minutes=30))
    assert got == snaps


async def test_inserting_the_same_snapshots_twice_changes_nothing(session_factory):
    snaps = [_snap("AAA"), _snap("BBB"), _snap("AAA", T0 + timedelta(minutes=30))]
    async with session_factory() as s:
        assert await PopularityRepository(s).insert_many(snaps) == 3
        await s.commit()
    async with session_factory() as s:
        # A re-poll of the same boundary with different numbers is still a no-op.
        again = [*snaps, _snap("BBB", mentions=999), _snap("CCC")]
        assert await PopularityRepository(s).insert_many(again) == 1
        await s.commit()

    assert await _count(session_factory) == 4
    async with session_factory() as s:
        kept = await s.scalar(
            select(PopularitySnapshotRow.mentions).where(PopularitySnapshotRow.ticker == "BBB")
        )
    assert kept == 120  # first write wins


async def test_duplicates_inside_one_batch_insert_once(session_factory):
    async with session_factory() as s:
        assert await PopularityRepository(s).insert_many([_snap("AAA"), _snap("AAA")]) == 1
        await s.commit()


async def test_empty_batch_inserts_nothing(session_factory):
    async with session_factory() as s:
        assert await PopularityRepository(s).insert_many([]) == 0


async def test_reinserting_into_a_compressed_chunk_is_still_a_noop(session_factory):
    old = datetime(2025, 1, 1, tzinfo=UTC)
    async with session_factory() as s:
        await PopularityRepository(s).insert_many(
            [_snap("AAA", old, origin=PopularityOrigin.V1_IMPORT)]
        )
        await s.commit()
    async with session_factory() as s:
        chunks = (await s.execute(text("SELECT show_chunks('popularity_snapshot')"))).scalars().all()
        for chunk in chunks:
            await s.execute(text(f"SELECT compress_chunk('{chunk}', if_not_compressed => true)"))
        await s.commit()

    async with session_factory() as s:
        # A v1 import rerun lands in a compressed chunk: the duplicate is skipped, the new row kept.
        added = await PopularityRepository(s).insert_many(
            [
                _snap("AAA", old, origin=PopularityOrigin.V1_IMPORT),
                _snap("AAA", old + timedelta(minutes=30), origin=PopularityOrigin.V1_IMPORT),
            ]
        )
        await s.commit()
    assert added == 1
    assert await _count(session_factory) == 2


async def test_earliest_polled_at_is_the_oldest_boundary_or_none(session_factory):
    async with session_factory() as s:
        assert await PopularityRepository(s).earliest_polled_at() is None
        later = T0 + timedelta(hours=2)
        await PopularityRepository(s).insert_many([_snap("AAA", later), _snap("BBB"), _snap("AAA", T0 + timedelta(hours=1))])
        await s.commit()

    async with session_factory() as s:
        assert await PopularityRepository(s).earliest_polled_at() == T0


async def test_db_origin_enum_matches_the_domain_vocabulary(session_factory):
    async with session_factory() as s:
        labels = (
            (
                await s.execute(
                    text(
                        "SELECT enumlabel FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid"
                        " WHERE t.typname = 'popularity_origin' ORDER BY e.enumsortorder"
                    )
                )
            )
            .scalars()
            .all()
        )
    assert labels == [m.value for m in PopularityOrigin]


async def test_upvotes_may_be_absent_on_imported_rows(session_factory):
    snap = _snap("AAA", upvotes=None, origin=PopularityOrigin.V1_IMPORT)
    async with session_factory() as s:
        assert await PopularityRepository(s).insert_many([snap]) == 1
        await s.commit()
    async with session_factory() as s:
        assert await PopularityRepository(s).list_between(T0, T0 + timedelta(minutes=30)) == [snap]


async def test_list_between_can_keep_only_snapshots_with_enough_mentions(session_factory):
    async with session_factory() as s:
        await PopularityRepository(s).insert_many([_snap("AAA", mentions=14), _snap("BBB", mentions=15)])
        await s.commit()

    async with session_factory() as s:
        got = await PopularityRepository(s).list_between(T0, T0 + timedelta(minutes=30), min_mentions=15)
    assert [snap.ticker for snap in got] == ["BBB"]


async def test_readings_are_the_rank_and_mentions_of_the_named_tickers_per_boundary_in_the_window(session_factory):
    async with session_factory() as s:
        await PopularityRepository(s).insert_many(
            [
                _snap("AAA", T0 + timedelta(minutes=30), rank=5, mentions=50),
                _snap("AAA", T0, rank=4, mentions=40),
                _snap("AAA", T0 + timedelta(minutes=60), rank=6, mentions=60),  # outside the window
                _snap("BBB", T0, rank=9, mentions=10),
                _snap("CCC", T0, rank=1, mentions=99),  # not asked for
            ]
        )
        await s.commit()

    async with session_factory() as s:
        got = await PopularityRepository(s).readings({"AAA", "BBB", "ZZZ"}, T0, T0 + timedelta(minutes=60))
    assert got == {
        "AAA": {T0: (4, 40), T0 + timedelta(minutes=30): (5, 50)},
        "BBB": {T0: (9, 10)},
    }


async def test_bottom_ranks_are_one_past_the_last_stored_at_each_polled_boundary_only(session_factory):
    async with session_factory() as s:
        await PopularityRepository(s).insert_many(
            [
                _snap("AAA", T0, rank=4),
                _snap("BBB", T0, rank=9),
                _snap("AAA", T0 + timedelta(minutes=30), rank=2),
                _snap("AAA", T0 + timedelta(hours=3), rank=7),  # outside the window
            ]
        )
        await s.commit()

    async with session_factory() as s:
        got = await PopularityRepository(s).bottom_ranks(T0, T0 + timedelta(hours=1))
    # The boundary at 14:00 + 1h has no snapshot at all: it is downtime, not listed.
    assert got == {T0: 10, T0 + timedelta(minutes=30): 3}


async def test_count_polls_counts_distinct_boundaries_not_rows(session_factory):
    async with session_factory() as s:
        await PopularityRepository(s).insert_many(
            [
                _snap("AAA", T0),
                _snap("BBB", T0),
                _snap("AAA", T0 + timedelta(minutes=30)),
                _snap("AAA", T0 + timedelta(hours=2)),
            ]
        )
        await s.commit()

    async with session_factory() as s:
        repo = PopularityRepository(s)
        assert await repo.count_polls(T0, T0 + timedelta(hours=1)) == 2
        assert await repo.count_polls(T0 + timedelta(hours=1), T0 + timedelta(hours=2)) == 0
