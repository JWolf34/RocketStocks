import logging
from dataclasses import dataclass, field
from datetime import datetime

from rocketstocks.core.notifications.config import NotificationLevel

logger = logging.getLogger(__name__)


@dataclass
class NotificationEvent:
    level: NotificationLevel
    source: str
    job_name: str
    message: str
    traceback: str | None = None
    elapsed_seconds: float | None = None
    timestamp: datetime = field(default_factory=datetime.now)
