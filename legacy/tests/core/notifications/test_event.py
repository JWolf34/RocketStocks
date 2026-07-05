"""Tests for rocketstocks.core.notifications.event."""
import datetime
import pytest
from rocketstocks.core.notifications.config import NotificationLevel
from rocketstocks.core.notifications.event import NotificationEvent


class TestNotificationEvent:
    def test_construction_with_required_fields(self):
        event = NotificationEvent(
            level=NotificationLevel.SUCCESS,
            source="test.module",
            job_name="my_job",
            message="all good",
        )
        assert event.level == NotificationLevel.SUCCESS
        assert event.source == "test.module"
        assert event.job_name == "my_job"
        assert event.message == "all good"

    def test_optional_fields_default_to_none(self):
        event = NotificationEvent(
            level=NotificationLevel.FAILURE,
            source="src",
            job_name="job",
            message="oops",
        )
        assert event.traceback is None
        assert event.elapsed_seconds is None

    def test_timestamp_defaults_to_now(self):
        before = datetime.datetime.now()
        event = NotificationEvent(
            level=NotificationLevel.WARNING,
            source="src",
            job_name="job",
            message="warn",
        )
        after = datetime.datetime.now()
        assert before <= event.timestamp <= after

    def test_explicit_timestamp(self):
        ts = datetime.datetime(2024, 1, 15, 10, 30, 0)
        event = NotificationEvent(
            level=NotificationLevel.SUCCESS,
            source="src",
            job_name="job",
            message="msg",
            timestamp=ts,
        )
        assert event.timestamp == ts

    def test_traceback_and_elapsed(self):
        event = NotificationEvent(
            level=NotificationLevel.FAILURE,
            source="src",
            job_name="job",
            message="error",
            traceback="Traceback...",
            elapsed_seconds=3.14,
        )
        assert event.traceback == "Traceback..."
        assert event.elapsed_seconds == pytest.approx(3.14)
