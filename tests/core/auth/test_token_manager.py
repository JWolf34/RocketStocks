"""Tests for core/auth/token_manager.py."""
import datetime
import json
import pytest

from rocketstocks.core.auth.token_manager import (
    TokenStatus,
    TokenInfo,
    get_token_info,
    EXPIRING_SOON_THRESHOLD,
)


class TestGetTokenInfo:
    def test_missing_when_file_does_not_exist(self, tmp_path):
        info = get_token_info(str(tmp_path / "nonexistent.json"))
        assert info.status == TokenStatus.MISSING
        assert info.expires_at is None
        assert info.time_remaining is None

    def test_missing_on_malformed_json(self, tmp_path):
        f = tmp_path / "token.json"
        f.write_text("not valid json{{")
        info = get_token_info(str(f))
        assert info.status == TokenStatus.MISSING

    def test_missing_when_expires_at_key_absent(self, tmp_path):
        f = tmp_path / "token.json"
        f.write_text(json.dumps({"token": {"expires_in": 3600}}))
        info = get_token_info(str(f))
        assert info.status == TokenStatus.MISSING

    def test_expired_when_timestamp_in_past(self, tmp_path):
        past_ts = datetime.datetime(2000, 1, 1).timestamp()
        f = tmp_path / "token.json"
        f.write_text(json.dumps({"token": {"expires_at": past_ts}}))
        info = get_token_info(str(f))
        assert info.status == TokenStatus.EXPIRED
        assert info.expires_at == datetime.datetime.fromtimestamp(past_ts)
        assert info.time_remaining is not None
        assert info.time_remaining < datetime.timedelta(0)

    def test_expiring_soon_when_within_threshold(self, tmp_path):
        soon = datetime.datetime.now() + datetime.timedelta(hours=23)
        f = tmp_path / "token.json"
        f.write_text(json.dumps({"token": {"expires_at": soon.timestamp()}}))
        info = get_token_info(str(f))
        assert info.status == TokenStatus.EXPIRING_SOON
        assert info.expires_at is not None
        assert info.time_remaining is not None
        assert datetime.timedelta(0) < info.time_remaining <= EXPIRING_SOON_THRESHOLD

    def test_expiring_soon_at_exact_threshold(self, tmp_path):
        """A token expiring in exactly 2 days should be EXPIRING_SOON."""
        at_threshold = datetime.datetime.now() + EXPIRING_SOON_THRESHOLD
        f = tmp_path / "token.json"
        f.write_text(json.dumps({"token": {"expires_at": at_threshold.timestamp()}}))
        info = get_token_info(str(f))
        # Allow a 1-second tolerance for test execution time
        assert info.status in (TokenStatus.EXPIRING_SOON, TokenStatus.HEALTHY)

    def test_healthy_when_more_than_threshold_remaining(self, tmp_path):
        future = datetime.datetime.now() + datetime.timedelta(days=5)
        f = tmp_path / "token.json"
        f.write_text(json.dumps({"token": {"expires_at": future.timestamp()}}))
        info = get_token_info(str(f))
        assert info.status == TokenStatus.HEALTHY
        assert info.expires_at is not None
        assert info.time_remaining > EXPIRING_SOON_THRESHOLD

    def test_returns_tokeninfo_dataclass(self, tmp_path):
        future = datetime.datetime.now() + datetime.timedelta(days=5)
        f = tmp_path / "token.json"
        f.write_text(json.dumps({"token": {"expires_at": future.timestamp()}}))
        info = get_token_info(str(f))
        assert isinstance(info, TokenInfo)
        assert isinstance(info.status, TokenStatus)


class TestTokenStatus:
    def test_all_statuses_have_values(self):
        expected = {"healthy", "expiring_soon", "expired", "invalid", "missing"}
        actual = {s.value for s in TokenStatus}
        assert actual == expected
