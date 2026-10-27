"""EarningsSnapshotRepository: first-write-wins inserts, snapshot lookups and the days a backfill has."""

from datetime import UTC, date, datetime

from sqlalchemy import func, select

from rs_domain import EarningsOrigin, EarningsTime, StoredEarnings
from rs_persistence import EarningsSnapshotRepository, EarningsSnapshotRow

FETCHED = datetime(2026, 10, 27, 0, 31, tzinfo=UTC)
TONIGHT = date(2026, 10, 26)


def _row(ticker: str, report_date: date, **overrides) -> StoredEarnings:
    fields = {
        "snapshot_date": TONIGHT,
        "origin": EarningsOrigin.NIGHTLY,
        "ticker": ticker,
        "report_date": report_date,
        "time_of_day": EarningsTime.AFTER_HOURS,
        "fiscal_quarter": "2026-09",
        "eps_forecast": 1.25,
        "estimate_count": 7,
        "eps_actual": None,
        "surprise_pct": None,
        "fetched_at": FETCHED,
    }
    return StoredEarnings(**(fields | overrides))


async def _insert(session_factory, rows) -> int:
    async with session_factory() as s:
        inserted = await EarningsSnapshotRepository(s).insert_many(rows)
        await s.commit()
    return inserted


async def test_a_row_round_trips_with_its_nulls(session_factory):
    row = _row("AAA", date(2026, 10, 29), eps_forecast=-0.12, time_of_day=None, estimate_count=None)
    await _insert(session_factory, [row])

    async with session_factory() as s:
        got = await EarningsSnapshotRepository(s).next_report(
            EarningsOrigin.NIGHTLY, TONIGHT, "AAA", date(2026, 10, 27)
        )

    assert got == row
    assert got.time_of_day is None and got.estimate_count is None


async def test_the_same_snapshot_stored_twice_keeps_the_first_write(session_factory):
    first = [_row("AAA", date(2026, 10, 29)), _row("BBB", date(2026, 10, 29))]
    assert await _insert(session_factory, first) == 2

    again = [_row("AAA", date(2026, 10, 29), eps_forecast=9.0), _row("CCC", date(2026, 10, 29))]
    assert await _insert(session_factory, again) == 1

    async with session_factory() as s:
        assert await s.scalar(select(func.count()).select_from(EarningsSnapshotRow)) == 3
        kept = await EarningsSnapshotRepository(s).next_report(
            EarningsOrigin.NIGHTLY, TONIGHT, "AAA", date(2026, 10, 27)
        )
    assert kept is not None and kept.eps_forecast == 1.25


async def test_a_backfill_row_coexists_with_a_nightly_row_of_the_same_key(session_factory):
    nightly = _row("AAA", date(2026, 10, 20))
    backfill = _row("AAA", date(2026, 10, 20), origin=EarningsOrigin.BACKFILL, eps_actual=1.3)
    assert await _insert(session_factory, [nightly, backfill]) == 2

    async with session_factory() as s:
        repository = EarningsSnapshotRepository(s)
        assert await repository.latest_snapshot_date(EarningsOrigin.NIGHTLY) == TONIGHT
        assert await repository.has_snapshot(EarningsOrigin.BACKFILL, TONIGHT)
        got = await repository.next_report(EarningsOrigin.NIGHTLY, TONIGHT, "AAA", date(2026, 10, 20))
    assert got is not None and got.eps_actual is None


async def test_latest_and_has_snapshot_are_per_origin(session_factory):
    async with session_factory() as s:
        repository = EarningsSnapshotRepository(s)
        assert await repository.latest_snapshot_date(EarningsOrigin.NIGHTLY) is None
        assert not await repository.has_snapshot(EarningsOrigin.NIGHTLY, TONIGHT)

    await _insert(
        session_factory,
        [
            _row("AAA", date(2026, 10, 29), snapshot_date=date(2026, 10, 22)),
            _row("AAA", date(2026, 10, 29)),
            _row("OLD", date(2008, 1, 15), origin=EarningsOrigin.BACKFILL, snapshot_date=date(2026, 10, 27)),
        ],
    )

    async with session_factory() as s:
        repository = EarningsSnapshotRepository(s)
        assert await repository.latest_snapshot_date(EarningsOrigin.NIGHTLY) == TONIGHT
        assert await repository.latest_snapshot_date(EarningsOrigin.BACKFILL) == date(2026, 10, 27)
        assert await repository.has_snapshot(EarningsOrigin.NIGHTLY, date(2026, 10, 22))
        assert not await repository.has_snapshot(EarningsOrigin.NIGHTLY, date(2026, 10, 27))


async def test_next_report_is_the_earliest_date_on_or_after_the_given_day(session_factory):
    await _insert(
        session_factory,
        [
            _row("AAA", date(2026, 10, 22)),
            _row("AAA", date(2026, 10, 29)),
            _row("AAA", date(2027, 1, 28)),
            _row("BBB", date(2026, 10, 28)),
        ],
    )

    async with session_factory() as s:
        repository = EarningsSnapshotRepository(s)
        on_the_day = await repository.next_report(EarningsOrigin.NIGHTLY, TONIGHT, "AAA", date(2026, 10, 29))
        later = await repository.next_report(EarningsOrigin.NIGHTLY, TONIGHT, "AAA", date(2026, 10, 30))
        past_all = await repository.next_report(EarningsOrigin.NIGHTLY, TONIGHT, "AAA", date(2027, 1, 29))
        other_night = await repository.next_report(
            EarningsOrigin.NIGHTLY, date(2026, 10, 25), "AAA", date(2026, 10, 22)
        )

    assert on_the_day is not None and on_the_day.report_date == date(2026, 10, 29)
    assert later is not None and later.report_date == date(2027, 1, 28)
    assert past_all is None and other_night is None


async def test_report_dates_lists_the_days_one_origin_has_rows_for(session_factory):
    await _insert(
        session_factory,
        [
            _row("AAA", date(2008, 1, 15), origin=EarningsOrigin.BACKFILL),
            _row("BBB", date(2008, 1, 15), origin=EarningsOrigin.BACKFILL),
            _row("AAA", date(2008, 1, 17), origin=EarningsOrigin.BACKFILL),
            _row("AAA", date(2008, 1, 16)),
            _row("AAA", date(2008, 2, 1), origin=EarningsOrigin.BACKFILL),
        ],
    )

    async with session_factory() as s:
        days = await EarningsSnapshotRepository(s).report_dates(
            EarningsOrigin.BACKFILL, date(2008, 1, 1), date(2008, 1, 31)
        )

    assert days == {date(2008, 1, 15), date(2008, 1, 17)}
