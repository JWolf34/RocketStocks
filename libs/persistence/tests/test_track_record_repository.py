"""TrackRecordRepository: append-only study results."""

from datetime import UTC, datetime

from sqlalchemy import func, select, text

from rs_domain import TrackRecord, TrackRecordInput
from rs_persistence import TrackRecordRepository, TrackRecordRow

COMPUTED = datetime(2026, 7, 6, 3, 0, tzinfo=UTC)


def _rate(**overrides) -> TrackRecord:
    fields = {
        "alert_type": "popularity_surge",
        "rule_version": "1",
        "computed_at": COMPUTED,
        "input": TrackRecordInput.RECONSTRUCTED,
        "n": 212,
        "n_unpriced": 9,
        "hit_rate_1d": 0.48,
        "hit_rate_5d": 0.45,
        "hit_rate_10d": 0.44,
        "median_excess_1d": -0.002,
        "median_excess_5d": -0.006,
        "median_excess_10d": -0.011,
        "survivorship_caveat": "9 unpriced symbols excluded",
    }
    return TrackRecord(**(fields | overrides))


def _to_domain(row: TrackRecordRow) -> TrackRecord:
    return TrackRecord(**{c.name: getattr(row, c.name) for c in TrackRecordRow.__table__.columns if c.name != "id"})


async def test_insert_stores_every_field(session_factory):
    rate = _rate()
    async with session_factory() as s:
        rate_id = await TrackRecordRepository(s).insert(rate)
        await s.commit()

    async with session_factory() as s:
        row = await s.get(TrackRecordRow, rate_id)
    assert row is not None
    assert _to_domain(row) == rate


async def test_unmeasured_horizons_store_null_and_zero_n_is_valid(session_factory):
    rate = _rate(n=0, n_unpriced=0, hit_rate_10d=None, median_excess_10d=None)
    async with session_factory() as s:
        rate_id = await TrackRecordRepository(s).insert(rate)
        await s.commit()
    async with session_factory() as s:
        row = await s.get(TrackRecordRow, rate_id)
    assert row is not None
    assert (row.n, row.hit_rate_10d, row.median_excess_10d) == (0, None, None)


async def test_rerun_appends_a_row(session_factory):
    async with session_factory() as s:
        repo = TrackRecordRepository(s)
        first = await repo.insert(_rate())
        second = await repo.insert(_rate(computed_at=datetime(2026, 7, 13, 3, 0, tzinfo=UTC)))
        await s.commit()
    assert first != second
    async with session_factory() as s:
        assert await s.scalar(select(func.count()).select_from(TrackRecordRow)) == 2


async def test_db_input_enum_matches_the_domain_vocabulary(session_factory):
    async with session_factory() as s:
        labels = (
            (
                await s.execute(
                    text(
                        "SELECT enumlabel FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid"
                        " WHERE t.typname = 'track_record_input' ORDER BY e.enumsortorder"
                    )
                )
            )
            .scalars()
            .all()
        )
    assert labels == [m.value for m in TrackRecordInput]
