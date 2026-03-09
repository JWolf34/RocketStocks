"""Token status inspection — pure business logic, no discord/data imports."""
import datetime
import json
import logging
import os
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

EXPIRING_SOON_THRESHOLD = datetime.timedelta(days=2)


class TokenStatus(Enum):
    HEALTHY = "healthy"          # >2 days remaining
    EXPIRING_SOON = "expiring_soon"  # <=2 days remaining
    EXPIRED = "expired"          # refresh token expired by time
    INVALID = "invalid"          # refresh token rejected by Schwab (revoked, etc.)
    MISSING = "missing"          # no token file or unreadable


@dataclass
class TokenInfo:
    status: TokenStatus
    expires_at: datetime.datetime | None
    time_remaining: datetime.timedelta | None


def get_token_info(token_path: str) -> TokenInfo:
    """Read the Schwab token file and return its status."""
    if not os.path.exists(token_path):
        return TokenInfo(status=TokenStatus.MISSING, expires_at=None, time_remaining=None)

    try:
        with open(token_path, "r") as f:
            data = json.load(f)
        expires_at_ts = data["token"]["expires_at"]
        expires_at = datetime.datetime.fromtimestamp(expires_at_ts)
    except Exception as exc:
        logger.warning(f"Could not read Schwab token file at {token_path!r}: {exc}")
        return TokenInfo(status=TokenStatus.MISSING, expires_at=None, time_remaining=None)

    now = datetime.datetime.now()
    time_remaining = expires_at - now

    if time_remaining <= datetime.timedelta(0):
        return TokenInfo(
            status=TokenStatus.EXPIRED,
            expires_at=expires_at,
            time_remaining=time_remaining,
        )
    elif time_remaining <= EXPIRING_SOON_THRESHOLD:
        return TokenInfo(
            status=TokenStatus.EXPIRING_SOON,
            expires_at=expires_at,
            time_remaining=time_remaining,
        )
    else:
        return TokenInfo(
            status=TokenStatus.HEALTHY,
            expires_at=expires_at,
            time_remaining=time_remaining,
        )
