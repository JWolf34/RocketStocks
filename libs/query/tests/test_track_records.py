from datetime import UTC, datetime

from rs_domain import TrackRecord
from rs_persistence import TrackRecordRepository
from rs_query import track_records

COMPUTED = datetime(2026, 7, 6, 3, 0, tzinfo=UTC)


def _record(horizon: int, trigger: str = "popularity_surge", rule_version: str = "2", **overrides) -> TrackRecord:
    fields = {
        "trigger": trigger,
        "rule_version": rule_version,
        "horizon": horizon,
        "computed_at": COMPUTED,
        "cases": 60,
        "live_cases": 5,
        "outcomes": 45,
        "unpriced": 2,
        "share_at_2x": 0.2,
        "ordinary_share": 0.05,
        "median_multiple": 0.8,
        "median_excess": -0.004,
    }
    return TrackRecord(**(fields | overrides))


async def _seed(session_factory, *records: TrackRecord) -> None:
    async with session_factory() as s:
        await TrackRecordRepository(s).upsert(records)
        await s.commit()


async def _read(session_factory, trigger: str = "popularity_surge", rule_version: str = "2") -> list[TrackRecord]:
    async with session_factory() as s:
        return await track_records(s, trigger, rule_version)


async def test_a_versions_horizons_come_back_in_horizon_order(session_factory):
    records = [_record(20), _record(1), _record(5)]
    await _seed(session_factory, *records)

    assert await _read(session_factory) == sorted(records, key=lambda r: r.horizon)


async def test_another_trigger_or_rule_version_is_not_mixed_in(session_factory):
    mine = _record(5)
    await _seed(session_factory, mine, _record(5, rule_version="1"), _record(5, trigger="large_move"))

    assert await _read(session_factory) == [mine]


async def test_an_unmeasured_version_has_no_track_record(session_factory):
    assert await _read(session_factory) == []
