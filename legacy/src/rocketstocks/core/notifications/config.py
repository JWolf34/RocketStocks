import logging
from dataclasses import dataclass
from enum import Enum

from rocketstocks.core.config.settings import settings

logger = logging.getLogger(__name__)


class NotificationLevel(Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    WARNING = "warning"


class NotificationFilter(Enum):
    ALL = "all"
    FAILURES_ONLY = "failures_only"
    OFF = "off"


@dataclass
class NotificationConfig:
    filter: NotificationFilter = NotificationFilter.ALL
    heartbeat_enabled: bool = True
    latency_threshold_seconds: float = 1.0

    def should_notify(self, event) -> bool:
        """Return True if the event passes the current filter."""
        if self.filter == NotificationFilter.OFF:
            return False
        if self.filter == NotificationFilter.FAILURES_ONLY:
            return event.level in (NotificationLevel.FAILURE, NotificationLevel.WARNING)
        return True  # ALL

    @classmethod
    def from_env(cls) -> "NotificationConfig":
        raw = settings.notification_filter.lower().strip()
        filter_map = {
            "all": NotificationFilter.ALL,
            "failures_only": NotificationFilter.FAILURES_ONLY,
            "off": NotificationFilter.OFF,
        }
        notification_filter = filter_map.get(raw, NotificationFilter.ALL)
        if raw not in filter_map:
            logger.warning(f"Unknown NOTIFICATION_FILTER value '{raw}', defaulting to 'all'")
        return cls(filter=notification_filter)
