"""Token status inspection — pure business logic, no discord/data imports."""
import datetime
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

EXPIRING_SOON_THRESHOLD = datetime.timedelta(days=2)
REFRESH_TOKEN_LIFETIME = datetime.timedelta(days=7)


class TokenStatus(Enum):
    HEALTHY = "healthy"          # >2 days remaining
    EXPIRING_SOON = "expiring_soon"  # <=2 days remaining
    EXPIRED = "expired"          # refresh token expired by time
    INVALID = "invalid"          # refresh token rejected by Schwab (revoked, etc.)
    MISSING = "missing"          # no token in DB


@dataclass
class TokenInfo:
    status: TokenStatus
    expires_at: datetime.datetime | None
    time_remaining: datetime.timedelta | None


def get_token_info(token_dict: dict | None) -> TokenInfo:
    """Inspect a Schwab token dict and return its status."""
    if token_dict is None:
        return TokenInfo(status=TokenStatus.MISSING, expires_at=None, time_remaining=None)

    try:
        creation_ts = token_dict["creation_timestamp"]
        expires_at = datetime.datetime.fromtimestamp(creation_ts) + REFRESH_TOKEN_LIFETIME
    except Exception as exc:
        logger.warning(f"Could not read Schwab token data: {exc}")
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
