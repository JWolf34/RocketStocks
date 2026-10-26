"""TrackRecordRepository: a Trigger version's Outcomes and Track record are rewritten, not appended."""

from datetime import UTC, date, datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from rs_domain import Outcome, TrackRecord
from rs_persistence import OutcomeRow, TrackRecordRepository, TrackRecordRow

COMPUTED = datetime(2026, 7, 6, 3, 0, tzinfo=UTC)
CASE_AT = datetime(2026, 7, 6, 15, 0, tzinfo=UTC)


def _outcome(**overrides) -> Outcome:
    fields = {
        "trigger": "popularity_surge",
        "rule_version": "2",
        "symbol": "ABC",
        "day": date(2026, 7, 6),
        "horizon": 5,
        "case_at": CASE_AT,
        "hit_at": CASE_AT,
        "live": True,
        "entry_session": date(2026, 7, 7),
        "exit_session": date(2026, 7, 13),
        "stock_return": -0.04,
        "benchmark_return": 0.01,
        "excess_return": -0.05,
        "typical_move": 0.03,
        "move_multiple": 1.7,
        "ordinary_share": 0.06,
    }
    return Outcome(**(fields | overrides))


def _record(**overrides) -> TrackRecord:
    fields = {
        "trigger": "popularity_surge",
        "rule_version": "2",
        "horizon": 5,
        "computed_at": COMPUTED,
        "cases": 40,
        "live_cases": 12,
        "outcomes": 31,
        "unpriced": 4,
        "split_excluded": 1,
        "no_move_size": 3,
        "share_at_2x": 0.25,
        "ordinary_share": 0.07,
        "median_multiple": 0.9,
        "median_excess": -0.006,
    }
    return TrackRecord(**(fields | overrides))


async def _stored_outcomes(session_factory) -> list[Outcome]:
    query = select(OutcomeRow).order_by(OutcomeRow.rule_version, OutcomeRow.symbol, OutcomeRow.horizon)
    async with session_factory() as s:
        rows = (await s.scalars(query)).all()
    return [Outcome(**{c.name: getattr(row, c.name) for c in OutcomeRow.__table__.columns}) for row in rows]


async def _stored_records(session_factory) -> list[TrackRecord]:
    async with session_factory() as s:
        rows = (await s.scalars(select(TrackRecordRow).order_by(TrackRecordRow.horizon))).all()
    columns = TrackRecordRow.__table__.columns
    return [TrackRecord(**{c.name: getattr(row, c.name) for c in columns}) for row in rows]


async def test_outcomes_round_trip_every_field(session_factory):
    outcomes = [_outcome(), _outcome(horizon=1, typical_move=None, move_multiple=None, ordinary_share=None)]
    async with session_factory() as s:
        await TrackRecordRepository(s).replace_outcomes("popularity_surge", "2", outcomes)
        await s.commit()

    assert await _stored_outcomes(session_factory) == [outcomes[1], outcomes[0]]


async def test_replacing_a_version_swaps_its_outcomes_and_leaves_other_versions_alone(session_factory):
    async with session_factory() as s:
        repo = TrackRecordRepository(s)
        await repo.replace_outcomes("popularity_surge", "2", [_outcome(symbol="OLD")])
        await repo.replace_outcomes("popularity_surge", "1", [_outcome(rule_version="1")])
        await repo.replace_outcomes("popularity_surge", "2", [_outcome(symbol="NEW")])
        await s.commit()

    stored = await _stored_outcomes(session_factory)
    assert [(o.rule_version, o.symbol) for o in stored] == [("1", "ABC"), ("2", "NEW")]


async def test_replacing_with_nothing_clears_the_version(session_factory):
    async with session_factory() as s:
        repo = TrackRecordRepository(s)
        await repo.replace_outcomes("popularity_surge", "2", [_outcome()])
        await repo.replace_outcomes("popularity_surge", "2", [])
        await s.commit()

    assert await _stored_outcomes(session_factory) == []


async def test_a_rewrite_that_fails_leaves_the_stored_outcomes(session_factory):
    async with session_factory() as s:
        await TrackRecordRepository(s).replace_outcomes("popularity_surge", "2", [_outcome()])
        await s.commit()

    with pytest.raises(IntegrityError):
        async with session_factory() as s:
            repo = TrackRecordRepository(s)
            # The same case and horizon twice breaks the key after the delete ran.
            await repo.replace_outcomes("popularity_surge", "2", [_outcome(symbol="NEW"), _outcome(symbol="NEW")])
            await s.commit()

    assert [o.symbol for o in await _stored_outcomes(session_factory)] == ["ABC"]


async def test_track_records_round_trip_every_field_and_null(session_factory):
    empty = _record(
        horizon=20, outcomes=0, share_at_2x=None, ordinary_share=None, median_multiple=None, median_excess=None
    )
    records = [_record(horizon=1), empty]
    async with session_factory() as s:
        await TrackRecordRepository(s).upsert(records)
        await s.commit()

    assert await _stored_records(session_factory) == records


async def test_a_rerun_replaces_the_row_instead_of_adding_one(session_factory):
    later = _record(computed_at=datetime(2026, 7, 7, 3, 0, tzinfo=UTC), outcomes=35, median_excess=0.001)
    async with session_factory() as s:
        repo = TrackRecordRepository(s)
        await repo.upsert([_record()])
        await repo.upsert([later])
        await s.commit()

    assert await _stored_records(session_factory) == [later]
    async with session_factory() as s:
        assert await s.scalar(select(func.count()).select_from(TrackRecordRow)) == 1


def test_too_few_cases_below_thirty_outcomes():
    assert _record(outcomes=29).too_few_cases
    assert not _record(outcomes=30).too_few_cases


def test_too_few_move_sizes_counts_only_outcomes_with_a_move_size():
    assert _record(outcomes=40, no_move_size=11).too_few_move_sizes
    assert not _record(outcomes=40, no_move_size=10).too_few_move_sizes
    assert _record(outcomes=29, no_move_size=0).too_few_move_sizes
