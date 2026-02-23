"""Tests for rocketstocks.core.config.environment."""
from unittest.mock import patch


def test_get_env_returns_value_when_present():
    with patch.dict("os.environ", {"MY_VAR": "hello"}):
        from rocketstocks.core.config.environment import get_env
        assert get_env("MY_VAR") == "hello"


def test_get_env_returns_none_when_missing():
    with patch.dict("os.environ", {}, clear=True):
        # Force re-evaluation by patching os.getenv directly
        with patch("rocketstocks.core.config.environment.os.getenv", return_value=None):
            from rocketstocks.core.config.environment import get_env
            result = get_env("MISSING_VAR")
            assert result is None


def test_get_env_logs_error_when_missing(caplog):
    import logging
    with patch("rocketstocks.core.config.environment.os.getenv", return_value=None):
        from rocketstocks.core.config.environment import get_env
        with caplog.at_level(logging.ERROR, logger="rocketstocks.core.config.environment"):
            get_env("NONEXISTENT_VAR")
        assert any("NONEXISTENT_VAR" in r.message for r in caplog.records)


def test_get_env_does_not_log_when_present(caplog):
    import logging
    with patch("rocketstocks.core.config.environment.os.getenv", return_value="val"):
        from rocketstocks.core.config.environment import get_env
        with caplog.at_level(logging.ERROR, logger="rocketstocks.core.config.environment"):
            result = get_env("PRESENT_VAR")
        assert result == "val"
        assert not any("PRESENT_VAR" in r.message for r in caplog.records)
