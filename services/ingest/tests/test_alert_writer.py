"""Durable write of a candidate and its Alert: one transaction, transient
failures retry, then ops hears the content."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import DataError

from rs_domain import Alert, Candidate, Verdict
from rs_ingest.alert_writer import AlertWriteError, AlertWriter, RetryPolicy, Stored
from rs_persistence import AlertRow, CandidateRow

T0 = datetime(2026, 10, 19, 14, 0, tzinfo=UTC)


def candidate(verdict: Verdict = Verdict.POSTED, at: datetime = T0) -> Candidate:
    return Candidate(
        trigger="popularity_surge",
        rule_version="1",
        symbol="ABC",
        hit_at=at,
        inputs={"fired": ["mention_surge"]},
        verdict=verdict,
        decided_at=at + timedelta(seconds=20),
    )


def alert(at: datetime = T0) -> Alert:
    return Alert(
        symbol="ABC",
        trigger="popularity_surge",
        triggered_at=at,
        payload={"note": "unit"},
        rule_version="1",
    )


async def count(session_factory, model) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(model))


async def test_a_posted_candidate_is_stored_with_the_alert_it_produced(session_factory, notices):
    writer = AlertWriter(session_factory)

    stored = await writer.write(candidate(), alert())

    assert stored is not None
    assert stored.alert_id is not None
    async with session_factory() as session:
        row = await session.get(CandidateRow, stored.candidate_id)
    assert row.alert_id == stored.alert_id
    assert await count(session_factory, AlertRow) == 1
    assert notices == []


async def test_a_candidate_without_an_alert_stores_only_the_candidate(session_factory, notices):
    writer = AlertWriter(session_factory)

    stored = await writer.write(candidate(Verdict.REPEAT))

    assert stored is not None and stored.alert_id is None
    assert await count(session_factory, CandidateRow) == 1
    assert await count(session_factory, AlertRow) == 0


async def test_a_hit_already_decided_is_a_no_op_and_leaves_no_alert_behind(session_factory, notices):
    writer = AlertWriter(session_factory)
    first = await writer.write(candidate(), alert())

    second = await writer.write(candidate(), alert())

    assert isinstance(first, Stored)
    assert second is None
    assert await count(session_factory, CandidateRow) == 1
    assert await count(session_factory, AlertRow) == 1


async def test_a_transient_failure_is_retried_with_backoff(session_factory, make_flaky, sleeps, notices):
    factory = make_flaky(failures=2)
    writer = AlertWriter(factory, sleep=sleeps)

    stored = await writer.write(candidate(), alert())

    assert stored is not None
    assert await count(session_factory, AlertRow) == 1
    assert sleeps.seconds == [1.0, 2.0]
    assert notices == []


async def test_a_persistent_failure_reports_the_alert_to_ops_and_raises(
    session_factory, make_flaky, sleeps, notices
):
    factory = make_flaky(failures=99)
    writer = AlertWriter(factory, policy=RetryPolicy(attempts=4), sleep=sleeps)

    with pytest.raises(AlertWriteError, match="ABC"):
        await writer.write(candidate(), alert())

    assert factory.opened == 4
    assert sleeps.seconds == [1.0, 2.0, 4.0]
    [notice] = notices
    assert "Alert NOT stored" in notice
    assert '"note":"unit"' in notice
    assert "OperationalError" in notice
    assert await count(session_factory, AlertRow) == 0
    assert await count(session_factory, CandidateRow) == 0


async def test_a_candidate_that_cannot_be_stored_is_reported_as_a_candidate(
    session_factory, make_flaky, sleeps, notices
):
    writer = AlertWriter(make_flaky(failures=99), policy=RetryPolicy(attempts=1), sleep=sleeps)

    with pytest.raises(AlertWriteError, match="candidate"):
        await writer.write(candidate(Verdict.REPEAT))

    [notice] = notices
    assert notice.startswith("Candidate NOT stored: popularity_surge for ABC")


async def test_backoff_is_capped(session_factory, make_flaky, sleeps, notices):
    factory = make_flaky(failures=99)
    writer = AlertWriter(factory, policy=RetryPolicy(attempts=7, max_backoff_seconds=5.0), sleep=sleeps)

    with pytest.raises(AlertWriteError):
        await writer.write(candidate(), alert())

    assert sleeps.seconds == [1.0, 2.0, 4.0, 5.0, 5.0, 5.0]


async def test_a_non_retryable_error_is_reported_at_once(session_factory, make_flaky, sleeps, notices):
    factory = make_flaky(failures=99, error=DataError("INSERT", {}, Exception("invalid input")))
    writer = AlertWriter(factory, sleep=sleeps)

    with pytest.raises(AlertWriteError):
        await writer.write(candidate(), alert())

    assert factory.opened == 1
    assert sleeps.seconds == []
    assert len(notices) == 1
    assert "non-retryable" in notices[0]


async def test_a_second_alert_for_the_stock_and_day_is_reported_and_stores_nothing(session_factory, sleeps, notices):
    writer = AlertWriter(session_factory, sleep=sleeps)
    await writer.write(candidate(), alert())
    later = T0 + timedelta(minutes=30)

    with pytest.raises(AlertWriteError, match="non-retryable"):
        await writer.write(candidate(at=later), alert(later))

    assert sleeps.seconds == []
    assert len(notices) == 1
    assert await count(session_factory, AlertRow) == 1
    assert await count(session_factory, CandidateRow) == 1


async def test_the_ops_notice_fits_discords_limit_for_a_large_alert(
    session_factory, make_flaky, sleeps, notices
):
    writer = AlertWriter(make_flaky(failures=99), policy=RetryPolicy(attempts=1), sleep=sleeps)
    big = alert().model_copy(update={"payload": {"blob": "x" * 5000}})

    with pytest.raises(AlertWriteError):
        await writer.write(candidate(), big)

    assert len(notices[0]) <= 2000
    assert notices[0].startswith("Alert NOT stored: popularity_surge for ABC")
