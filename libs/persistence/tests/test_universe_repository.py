"""UniverseRepository: a snapshot is stored whole once per session date and read back as the latest Universe."""

from datetime import UTC, date, datetime

from sqlalchemy import func, select

from rs_domain import UniverseCounts, UniverseListing, UniverseSnapshot
from rs_persistence import UniverseListingRow, UniverseRepository

AS_OF = datetime(2026, 10, 26, 22, 1, tzinfo=UTC)


def _listing(symbol: str, **overrides) -> UniverseListing:
    fields = {
        "symbol": symbol,
        "security_name": f"{symbol} Inc.",
        "exchange": "Q",
        "etf": False,
        "financial_status": "N",
        "cqs_symbol": None,
        "nasdaq_symbol": None,
        "optionable": False,
    }
    return UniverseListing(**(fields | overrides))


def _snapshot(day: date, listings, **overrides) -> UniverseSnapshot:
    fields = {
        "snapshot_date": day,
        "as_of": AS_OF,
        "counts": UniverseCounts(nasdaq_listed=2, other_listed=1, options_symbols=1, cboe_symbols=2),
        "optionable_carried": False,
        "listings": tuple(listings),
    }
    return UniverseSnapshot(**(fields | overrides))


async def test_a_saved_snapshot_reads_back_as_the_latest_universe(session_factory):
    listings = [
        _listing("BBB", optionable=True),
        _listing("AAA", etf=True, financial_status="D"),
        _listing("BRK.B", exchange="N", financial_status=None, cqs_symbol="BRK.B", nasdaq_symbol="BRK-B"),
    ]
    async with session_factory() as s:
        assert await UniverseRepository(s).latest() is None
        assert await UniverseRepository(s).save(_snapshot(date(2026, 10, 26), listings))
        await s.commit()

    async with session_factory() as s:
        latest = await UniverseRepository(s).latest()

    assert latest is not None
    assert (latest.snapshot_date, latest.as_of, latest.optionable_carried) == (date(2026, 10, 26), AS_OF, False)
    assert latest.counts == UniverseCounts(nasdaq_listed=2, other_listed=1, options_symbols=1, cboe_symbols=2)
    assert list(latest.listings) == sorted(listings, key=lambda x: x.symbol)


async def test_the_latest_universe_is_the_newest_date(session_factory):
    async with session_factory() as s:
        repo = UniverseRepository(s)
        await repo.save(_snapshot(date(2026, 10, 27), [_listing("NEW")]))
        await repo.save(_snapshot(date(2026, 10, 26), [_listing("OLD")]))
        await s.commit()

        latest = await repo.latest()
        header = await repo.header(date(2026, 10, 26))
        missing = await repo.header(date(2026, 10, 25))

    assert latest is not None and [x.symbol for x in latest.listings] == ["NEW"]
    assert header is not None and header.snapshot_date == date(2026, 10, 26)
    assert missing is None


async def test_saving_a_date_that_has_a_snapshot_changes_nothing(session_factory):
    async with session_factory() as s:
        repo = UniverseRepository(s)
        assert await repo.save(_snapshot(date(2026, 10, 26), [_listing("AAA")]))
        await s.commit()

        assert not await repo.save(_snapshot(date(2026, 10, 26), [_listing("AAA"), _listing("ZZZ")]))
        await s.commit()

        assert await s.scalar(select(func.count()).select_from(UniverseListingRow)) == 1


async def test_a_realistic_snapshot_is_stored_in_one_transaction(session_factory):
    listings = [_listing(f"S{n:05d}", optionable=n % 2 == 0) for n in range(13_000)]
    async with session_factory() as s:
        await UniverseRepository(s).save(_snapshot(date(2026, 10, 26), listings))
        await s.commit()

    async with session_factory() as s:
        repo = UniverseRepository(s)
        latest = await repo.latest()
        optionable = await repo.optionable_symbols(date(2026, 10, 26))

    assert latest is not None and len(latest.listings) == 13_000
    assert len(optionable) == 6_500 and "S00002" in optionable and "S00003" not in optionable


async def test_reference_counts_take_each_optionable_source_from_the_newest_night_it_worked(session_factory):
    async with session_factory() as s:
        repo = UniverseRepository(s)
        assert await repo.reference_counts() is None
        await repo.save(
            _snapshot(
                date(2026, 10, 23),
                [_listing("AAA")],
                counts=UniverseCounts(nasdaq_listed=10, other_listed=20, options_symbols=7, cboe_symbols=8),
            )
        )
        await repo.save(
            _snapshot(
                date(2026, 10, 26),
                [_listing("AAA")],
                counts=UniverseCounts(nasdaq_listed=11, other_listed=21, options_symbols=None, cboe_symbols=9),
                optionable_carried=True,
            )
        )
        await s.commit()

        counts = await repo.reference_counts()

    assert counts == UniverseCounts(nasdaq_listed=11, other_listed=21, options_symbols=7, cboe_symbols=9)
