"""Durable alert insert: transient failures retry, then ops hears the alert."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import DataError

from rs_domain import Alert
from rs_ingest.alert_writer import AlertWriteError, AlertWriter, RetryPolicy
from rs_persistence import AlertRow

T0 = datetime(2026, 10, 19, 14, 0, tzinfo=UTC)


def alert(dedup_key: str = "attention_surge:1:ABC:20261019T1400Z") -> Alert:
    return Alert(
        symbol="ABC",
        alert_type="attention_surge",
        triggered_at=T0,
        payload={"note": "unit"},
        rule_version="1",
        dedup_key=dedup_key,
    )


async def stored(session_factory) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(AlertRow))


async def test_a_healthy_insert_stores_the_alert(session_factory, notices):
    writer = AlertWriter(session_factory)

    alert_id = await writer.write(alert())

    assert alert_id is not None
    assert await stored(session_factory) == 1
    assert notices == []


async def test_the_same_dedup_key_is_stored_once(session_factory, notices):
    writer = AlertWriter(session_factory)

    first = await writer.write(alert())
    second = await writer.write(alert())

    assert first is not None
    assert second is None
    assert await stored(session_factory) == 1


async def test_a_transient_failure_is_retried_with_backoff(session_factory, make_flaky, sleeps, notices):
    factory = make_flaky(failures=2)
    writer = AlertWriter(factory, sleep=sleeps)

    alert_id = await writer.write(alert())

    assert alert_id is not None
    assert await stored(session_factory) == 1
    assert sleeps.seconds == [1.0, 2.0]
    assert notices == []


async def test_a_persistent_failure_reports_the_alert_to_ops_and_raises(
    session_factory, make_flaky, sleeps, notices
):
    factory = make_flaky(failures=99)
    writer = AlertWriter(factory, policy=RetryPolicy(attempts=4), sleep=sleeps)

    with pytest.raises(AlertWriteError, match="ABC"):
        await writer.write(alert())

    assert factory.opened == 4
    assert sleeps.seconds == [1.0, 2.0, 4.0]
    [notice] = notices
    assert "Alert NOT stored" in notice
    assert "attention_surge:1:ABC:20261019T1400Z" in notice
    assert "OperationalError" in notice
    assert await stored(session_factory) == 0


async def test_backoff_is_capped(session_factory, make_flaky, sleeps, notices):
    factory = make_flaky(failures=99)
    writer = AlertWriter(factory, policy=RetryPolicy(attempts=7, max_backoff_seconds=5.0), sleep=sleeps)

    with pytest.raises(AlertWriteError):
        await writer.write(alert())

    assert sleeps.seconds == [1.0, 2.0, 4.0, 5.0, 5.0, 5.0]


async def test_a_non_retryable_error_is_reported_at_once(session_factory, make_flaky, sleeps, notices):
    factory = make_flaky(failures=99, error=DataError("INSERT", {}, Exception("invalid input")))
    writer = AlertWriter(factory, sleep=sleeps)

    with pytest.raises(AlertWriteError):
        await writer.write(alert())

    assert factory.opened == 1
    assert sleeps.seconds == []
    assert len(notices) == 1
    assert "non-retryable" in notices[0]


async def test_the_ops_notice_fits_discords_limit_for_a_large_alert(
    session_factory, make_flaky, sleeps, notices
):
    writer = AlertWriter(make_flaky(failures=99), policy=RetryPolicy(attempts=1), sleep=sleeps)
    big = alert().model_copy(update={"payload": {"blob": "x" * 5000}})

    with pytest.raises(AlertWriteError):
        await writer.write(big)

    assert len(notices[0]) <= 2000
    assert notices[0].startswith("Alert NOT stored: attention_surge for ABC")
