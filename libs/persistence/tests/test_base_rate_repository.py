"""BaseRateRepository: append-only study results."""

from datetime import UTC, datetime

from sqlalchemy import func, select, text

from rs_domain import BaseRate, BaseRateInput
from rs_persistence import BaseRateRepository, BaseRateRow

COMPUTED = datetime(2026, 7, 6, 3, 0, tzinfo=UTC)


def _rate(**overrides) -> BaseRate:
    fields = {
        "alert_type": "attention_surge",
        "rule_version": "1",
        "computed_at": COMPUTED,
        "input": BaseRateInput.RECONSTRUCTED,
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
    return BaseRate(**(fields | overrides))


def _to_domain(row: BaseRateRow) -> BaseRate:
    return BaseRate(**{c.name: getattr(row, c.name) for c in BaseRateRow.__table__.columns if c.name != "id"})


async def test_insert_stores_every_field(session_factory):
    rate = _rate()
    async with session_factory() as s:
        rate_id = await BaseRateRepository(s).insert(rate)
        await s.commit()

    async with session_factory() as s:
        row = await s.get(BaseRateRow, rate_id)
    assert row is not None
    assert _to_domain(row) == rate


async def test_unmeasured_horizons_store_null_and_zero_n_is_valid(session_factory):
    rate = _rate(n=0, n_unpriced=0, hit_rate_10d=None, median_excess_10d=None)
    async with session_factory() as s:
        rate_id = await BaseRateRepository(s).insert(rate)
        await s.commit()
    async with session_factory() as s:
        row = await s.get(BaseRateRow, rate_id)
    assert row is not None
    assert (row.n, row.hit_rate_10d, row.median_excess_10d) == (0, None, None)


async def test_rerun_appends_a_row(session_factory):
    async with session_factory() as s:
        repo = BaseRateRepository(s)
        first = await repo.insert(_rate())
        second = await repo.insert(_rate(computed_at=datetime(2026, 7, 13, 3, 0, tzinfo=UTC)))
        await s.commit()
    assert first != second
    async with session_factory() as s:
        assert await s.scalar(select(func.count()).select_from(BaseRateRow)) == 2


async def test_db_input_enum_matches_the_domain_vocabulary(session_factory):
    async with session_factory() as s:
        labels = (
            (
                await s.execute(
                    text(
                        "SELECT enumlabel FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid"
                        " WHERE t.typname = 'base_rate_input' ORDER BY e.enumsortorder"
                    )
                )
            )
            .scalars()
            .all()
        )
    assert labels == [m.value for m in BaseRateInput]
