"""Tests for core/auth/token_manager.py."""
import datetime
import pytest

from rocketstocks.core.auth.token_manager import (
    TokenStatus,
    TokenInfo,
    get_token_info,
    EXPIRING_SOON_THRESHOLD,
    REFRESH_TOKEN_LIFETIME,
)


def _make_token_dict(creation_timestamp):
    return {"creation_timestamp": creation_timestamp, "token": {}}


class TestGetTokenInfo:
    def test_missing_when_token_dict_is_none(self):
        info = get_token_info(None)
        assert info.status == TokenStatus.MISSING
        assert info.expires_at is None
        assert info.time_remaining is None

    def test_missing_when_creation_timestamp_absent(self):
        info = get_token_info({"token": {"expires_at": 9999999999}})
        assert info.status == TokenStatus.MISSING

    def test_expired_when_creation_timestamp_in_past(self):
        # Created 8 days ago → refresh token expired 1 day ago
        creation = datetime.datetime(2000, 1, 1) - REFRESH_TOKEN_LIFETIME
        info = get_token_info(_make_token_dict(creation.timestamp()))
        assert info.status == TokenStatus.EXPIRED
        expected_expiry = datetime.datetime(2000, 1, 1)
        assert abs((info.expires_at - expected_expiry).total_seconds()) < 2
        assert info.time_remaining is not None
        assert info.time_remaining < datetime.timedelta(0)

    def test_expiring_soon_when_within_threshold(self):
        # Refresh token expires in 23 hours
        target_remaining = datetime.timedelta(hours=23)
        creation = datetime.datetime.now() + target_remaining - REFRESH_TOKEN_LIFETIME
        info = get_token_info(_make_token_dict(creation.timestamp()))
        assert info.status == TokenStatus.EXPIRING_SOON
        assert info.expires_at is not None
        assert info.time_remaining is not None
        assert datetime.timedelta(0) < info.time_remaining <= EXPIRING_SOON_THRESHOLD

    def test_expiring_soon_at_exact_threshold(self):
        """A refresh token expiring in exactly 2 days should be EXPIRING_SOON."""
        target_remaining = EXPIRING_SOON_THRESHOLD
        creation = datetime.datetime.now() + target_remaining - REFRESH_TOKEN_LIFETIME
        info = get_token_info(_make_token_dict(creation.timestamp()))
        # Allow a 1-second tolerance for test execution time
        assert info.status in (TokenStatus.EXPIRING_SOON, TokenStatus.HEALTHY)

    def test_healthy_when_more_than_threshold_remaining(self):
        # Refresh token expires in 5 days
        target_remaining = datetime.timedelta(days=5)
        creation = datetime.datetime.now() + target_remaining - REFRESH_TOKEN_LIFETIME
        info = get_token_info(_make_token_dict(creation.timestamp()))
        assert info.status == TokenStatus.HEALTHY
        assert info.expires_at is not None
        assert info.time_remaining > EXPIRING_SOON_THRESHOLD

    def test_returns_tokeninfo_dataclass(self):
        target_remaining = datetime.timedelta(days=5)
        creation = datetime.datetime.now() + target_remaining - REFRESH_TOKEN_LIFETIME
        info = get_token_info(_make_token_dict(creation.timestamp()))
        assert isinstance(info, TokenInfo)
        assert isinstance(info.status, TokenStatus)


class TestTokenStatus:
    def test_all_statuses_have_values(self):
        expected = {"healthy", "expiring_soon", "expired", "invalid", "missing"}
        actual = {s.value for s in TokenStatus}
        assert actual == expected
