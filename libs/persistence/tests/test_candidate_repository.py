"""CandidateRepository: a hit is decided once, and the Eastern day is the database's."""

from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from rs_domain import Alert, Candidate, Verdict
from rs_persistence import AlertRepository, CandidateRepository, CandidateRow

HIT = datetime(2026, 7, 7, 3, 30, tzinfo=UTC)  # 23:30 EDT on July 6

CANDIDATE = Candidate(
    trigger="popularity_surge",
    rule_version="2",
    symbol="ABC",
    hit_at=HIT,
    inputs={"fired": ["mention_surge"], "rank": 40},
    verdict=Verdict.REPEAT,
    decided_at=HIT + timedelta(seconds=20),
)


async def _insert(session_factory, candidate: Candidate) -> int | None:
    async with session_factory() as s:
        candidate_id = await CandidateRepository(s).insert(candidate)
        await s.commit()
    return candidate_id


async def _window(session_factory, trigger: str, start: datetime, end: datetime) -> list[Candidate]:
    async with session_factory() as s:
        return await CandidateRepository(s).list_window(trigger, start, end)


async def test_a_candidate_round_trips_with_its_eastern_day(session_factory):
    candidate_id = await _insert(session_factory, CANDIDATE)

    assert candidate_id is not None
    async with session_factory() as s:
        row = await s.get(CandidateRow, candidate_id)
    assert row.day == date(2026, 7, 6)
    assert await _window(session_factory, "popularity_surge", HIT, HIT + timedelta(minutes=30)) == [CANDIDATE]


async def test_the_same_hit_of_the_same_rule_version_is_stored_once(session_factory):
    first = await _insert(session_factory, CANDIDATE)
    second = await _insert(session_factory, CANDIDATE.model_copy(update={"verdict": Verdict.POSTED}))

    assert first is not None
    assert second is None
    [kept] = await _window(session_factory, "popularity_surge", HIT, HIT + timedelta(minutes=30))
    assert kept.verdict is Verdict.REPEAT  # the first decision stands


async def test_a_hit_is_decided_once_stored_for_its_rule_version(session_factory):
    async with session_factory() as s:
        assert not await CandidateRepository(s).is_decided(CANDIDATE)
    await _insert(session_factory, CANDIDATE)

    async with session_factory() as s:
        repo = CandidateRepository(s)
        assert await repo.is_decided(CANDIDATE)
        assert not await repo.is_decided(CANDIDATE.model_copy(update={"rule_version": "3"}))
        assert not await repo.is_decided(CANDIDATE.model_copy(update={"symbol": "XYZ"}))


async def test_another_rule_version_symbol_or_time_is_a_different_hit(session_factory):
    ids = [
        await _insert(session_factory, CANDIDATE),
        await _insert(session_factory, CANDIDATE.model_copy(update={"rule_version": "3"})),
        await _insert(session_factory, CANDIDATE.model_copy(update={"symbol": "XYZ"})),
        await _insert(session_factory, CANDIDATE.model_copy(update={"hit_at": HIT + timedelta(minutes=30)})),
    ]

    assert None not in ids
    assert len(set(ids)) == 4


async def test_every_verdict_can_be_stored(session_factory):
    for i, verdict in enumerate(Verdict):
        stored = await _insert(
            session_factory, CANDIDATE.model_copy(update={"verdict": verdict, "hit_at": HIT + timedelta(hours=i)})
        )
        assert stored is not None

    got = await _window(session_factory, "popularity_surge", HIT, HIT + timedelta(days=1))
    assert [c.verdict for c in got] == list(Verdict)


async def test_a_candidate_names_the_alert_it_produced(session_factory):
    async with session_factory() as s:
        alert_id = await AlertRepository(s).insert(
            Alert(symbol="ABC", trigger="popularity_surge", triggered_at=HIT, rule_version="2")
        )
        await CandidateRepository(s).insert(
            CANDIDATE.model_copy(update={"verdict": Verdict.POSTED, "alert_id": alert_id})
        )
        await s.commit()

    [stored] = await _window(session_factory, "popularity_surge", HIT, HIT + timedelta(minutes=30))
    assert stored.alert_id == alert_id


async def test_a_candidate_cannot_name_an_alert_that_does_not_exist(session_factory):
    with pytest.raises(IntegrityError):
        await _insert(session_factory, CANDIDATE.model_copy(update={"alert_id": 999}))


async def test_the_window_is_one_triggers_hits_start_inclusive_end_exclusive(session_factory):
    for offset in (-30, 0, 30):
        await _insert(session_factory, CANDIDATE.model_copy(update={"hit_at": HIT + timedelta(minutes=offset)}))
    await _insert(session_factory, CANDIDATE.model_copy(update={"trigger": "other"}))

    got = await _window(session_factory, "popularity_surge", HIT, HIT + timedelta(minutes=30))

    assert [c.hit_at for c in got] == [HIT]


async def test_a_rolled_back_candidate_leaves_nothing(session_factory):
    async with session_factory() as s:
        await CandidateRepository(s).insert(CANDIDATE)
        await s.rollback()

    async with session_factory() as s:
        assert (await s.scalars(select(CandidateRow))).all() == []


async def test_first_hits_by_day_takes_the_earliest_per_stock_and_eastern_day(session_factory):
    evening = HIT  # 23:30 EDT on July 6
    hits = [
        ("ABC", "2", evening + timedelta(minutes=30)),  # 00:00 EDT on July 7: a new Eastern day
        ("ABC", "2", evening),
        ("ABC", "2", evening - timedelta(hours=1)),
        ("ABC", "1", evening - timedelta(hours=5)),  # another rule version
        ("XYZ", "2", evening),
    ]
    for symbol, version, hit_at in hits:
        await _insert(
            session_factory, CANDIDATE.model_copy(update={"symbol": symbol, "rule_version": version, "hit_at": hit_at})
        )

    async with session_factory() as s:
        first = await CandidateRepository(s).first_hits_by_day("popularity_surge", "2")

    assert first == {
        ("ABC", date(2026, 7, 6)): evening - timedelta(hours=1),
        ("ABC", date(2026, 7, 7)): evening + timedelta(minutes=30),
        ("XYZ", date(2026, 7, 6)): evening,
    }
