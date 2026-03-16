"""Tests for rocketstocks.core.notifications.config."""
import pytest
from unittest.mock import patch
from rocketstocks.core.notifications.config import (
    NotificationConfig,
    NotificationFilter,
    NotificationLevel,
)
from rocketstocks.core.notifications.event import NotificationEvent


def _event(level: NotificationLevel) -> NotificationEvent:
    return NotificationEvent(level=level, source="src", job_name="job", message="msg")


class TestNotificationLevel:
    def test_enum_values(self):
        assert NotificationLevel.SUCCESS.value == "success"
        assert NotificationLevel.FAILURE.value == "failure"
        assert NotificationLevel.WARNING.value == "warning"


class TestNotificationFilter:
    def test_enum_values(self):
        assert NotificationFilter.ALL.value == "all"
        assert NotificationFilter.FAILURES_ONLY.value == "failures_only"
        assert NotificationFilter.OFF.value == "off"


class TestNotificationConfigShouldNotify:
    def test_all_allows_success(self):
        cfg = NotificationConfig(filter=NotificationFilter.ALL)
        assert cfg.should_notify(_event(NotificationLevel.SUCCESS)) is True

    def test_all_allows_failure(self):
        cfg = NotificationConfig(filter=NotificationFilter.ALL)
        assert cfg.should_notify(_event(NotificationLevel.FAILURE)) is True

    def test_all_allows_warning(self):
        cfg = NotificationConfig(filter=NotificationFilter.ALL)
        assert cfg.should_notify(_event(NotificationLevel.WARNING)) is True

    def test_failures_only_blocks_success(self):
        cfg = NotificationConfig(filter=NotificationFilter.FAILURES_ONLY)
        assert cfg.should_notify(_event(NotificationLevel.SUCCESS)) is False

    def test_failures_only_allows_failure(self):
        cfg = NotificationConfig(filter=NotificationFilter.FAILURES_ONLY)
        assert cfg.should_notify(_event(NotificationLevel.FAILURE)) is True

    def test_failures_only_blocks_warning(self):
        cfg = NotificationConfig(filter=NotificationFilter.FAILURES_ONLY)
        assert cfg.should_notify(_event(NotificationLevel.WARNING)) is False

    def test_off_blocks_success(self):
        cfg = NotificationConfig(filter=NotificationFilter.OFF)
        assert cfg.should_notify(_event(NotificationLevel.SUCCESS)) is False

    def test_off_blocks_failure(self):
        cfg = NotificationConfig(filter=NotificationFilter.OFF)
        assert cfg.should_notify(_event(NotificationLevel.FAILURE)) is False

    def test_off_blocks_warning(self):
        cfg = NotificationConfig(filter=NotificationFilter.OFF)
        assert cfg.should_notify(_event(NotificationLevel.WARNING)) is False


class TestNotificationConfigFromEnv:
    def _from_env_with(self, value):
        with patch("rocketstocks.core.notifications.config.settings") as mock_settings:
            mock_settings.notification_filter = value
            return NotificationConfig.from_env()

    def test_from_env_defaults_to_all(self):
        cfg = self._from_env_with("all")
        assert cfg.filter == NotificationFilter.ALL

    def test_from_env_reads_all(self):
        cfg = self._from_env_with("all")
        assert cfg.filter == NotificationFilter.ALL

    def test_from_env_reads_failures_only(self):
        cfg = self._from_env_with("failures_only")
        assert cfg.filter == NotificationFilter.FAILURES_ONLY

    def test_from_env_reads_off(self):
        cfg = self._from_env_with("off")
        assert cfg.filter == NotificationFilter.OFF

    def test_from_env_unknown_value_defaults_to_all(self):
        cfg = self._from_env_with("garbage")
        assert cfg.filter == NotificationFilter.ALL

    def test_from_env_case_insensitive(self):
        cfg = self._from_env_with("FAILURES_ONLY")
        assert cfg.filter == NotificationFilter.FAILURES_ONLY

    def test_default_heartbeat_enabled(self):
        cfg = NotificationConfig()
        assert cfg.heartbeat_enabled is True

    def test_default_latency_threshold(self):
        cfg = NotificationConfig()
        assert cfg.latency_threshold_seconds == pytest.approx(1.0)
