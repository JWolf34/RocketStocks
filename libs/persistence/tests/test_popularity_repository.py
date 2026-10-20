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


async def test_list_for_ticker_is_half_open_ordered_and_scoped(session_factory):
    async with session_factory() as s:
        await PopularityRepository(s).insert_many(
            [
                _snap("AAA", T0 + timedelta(minutes=60)),
                _snap("AAA", T0),
                _snap("AAA", T0 + timedelta(minutes=30)),
                _snap("BBB", T0),
            ]
        )
        await s.commit()

    async with session_factory() as s:
        got = await PopularityRepository(s).list_for_ticker("AAA", T0, T0 + timedelta(minutes=60))
    assert [(g.ticker, g.polled_at) for g in got] == [
        ("AAA", T0),
        ("AAA", T0 + timedelta(minutes=30)),
    ]


async def test_earliest_polled_at_is_the_oldest_boundary_or_none(session_factory):
    async with session_factory() as s:
        assert await PopularityRepository(s).earliest_polled_at() is None
        later = T0 + timedelta(hours=2)
        await PopularityRepository(s).insert_many([_snap("AAA", later), _snap("BBB"), _snap("AAA", T0 + timedelta(hours=1))])
        await s.commit()

    async with session_factory() as s:
        assert await PopularityRepository(s).earliest_polled_at() == T0


async def test_list_between_filters_by_origin(session_factory):
    async with session_factory() as s:
        await PopularityRepository(s).insert_many(
            [_snap("BBB"), _snap("AAA", origin=PopularityOrigin.V1_IMPORT)]
        )
        await s.commit()

    async with session_factory() as s:
        repo = PopularityRepository(s)
        end = T0 + timedelta(hours=1)
        assert [g.ticker for g in await repo.list_between(T0, end)] == ["AAA", "BBB"]
        live = await repo.list_between(T0, end, origin=PopularityOrigin.LIVE)
        assert [g.ticker for g in live] == ["BBB"]


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


async def test_extent_is_per_origin_and_none_when_empty(session_factory):
    later = T0 + timedelta(hours=2)
    async with session_factory() as s:
        repo = PopularityRepository(s)
        assert await repo.extent(PopularityOrigin.LIVE) is None
        await repo.insert_many(
            [
                _snap("AAA"),
                _snap("AAA", later),
                _snap("BBB", T0 - timedelta(days=1), origin=PopularityOrigin.V1_IMPORT),
            ]
        )
        await s.commit()
    async with session_factory() as s:
        repo = PopularityRepository(s)
        assert await repo.extent(PopularityOrigin.LIVE) == (T0, later)
        assert await repo.extent(PopularityOrigin.V1_IMPORT) == (T0 - timedelta(days=1),) * 2
