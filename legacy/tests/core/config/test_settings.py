"""Tests for rocketstocks.core.config.settings."""
import pytest
from pydantic import ValidationError


def _make_settings(env: dict, monkeypatch):
    """Build a Settings instance from the given env dict (no .env file)."""
    from rocketstocks.core.config.settings import Settings
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    # _env_file=None prevents loading any .env file so tests are isolated
    return Settings(_env_file=None)


_REQUIRED = {
    "DISCORD_TOKEN": "tok",
    "SCHWAB_API_KEY": "sk",
    "SCHWAB_API_SECRET": "ss",
    "TIINGO_API_KEY": "tk",
    "NEWS_API_KEY": "nk",
    "POSTGRES_USER": "u",
    "POSTGRES_PASSWORD": "pw",
    "POSTGRES_DB": "db",
    "POSTGRES_HOST": "host",
    "POSTGRES_PORT": "5432",
}


class TestSettingsValid:
    def test_all_required_fields_instantiates(self, monkeypatch):
        s = _make_settings(_REQUIRED, monkeypatch)
        assert s.discord_token == "tok"
        assert s.schwab_api_key == "sk"
        assert s.postgres_host == "host"

    def test_postgres_port_parsed_as_int(self, monkeypatch):
        s = _make_settings(_REQUIRED, monkeypatch)
        assert s.postgres_port == 5432
        assert isinstance(s.postgres_port, int)

    def test_optional_fields_default_to_none(self, monkeypatch):
        monkeypatch.delenv("NASDAQ_API_KEY", raising=False)
        monkeypatch.delenv("EODHD_API_TOKEN", raising=False)
        monkeypatch.delenv("DOLTHUB_API_TOKEN", raising=False)
        s = _make_settings(_REQUIRED, monkeypatch)
        assert s.nasdaq_api_key is None
        assert s.eodhd_api_token is None
        assert s.dolthub_api_token is None

    def test_notification_filter_defaults_to_all(self, monkeypatch):
        monkeypatch.delenv("NOTIFICATION_FILTER", raising=False)
        s = _make_settings(_REQUIRED, monkeypatch)
        assert s.notification_filter == "all"

    def test_tz_defaults_to_chicago(self, monkeypatch):
        s = _make_settings(_REQUIRED, monkeypatch)
        assert s.tz == "America/Chicago"

    def test_optional_fields_set_when_provided(self, monkeypatch):
        env = {**_REQUIRED, "NASDAQ_API_KEY": "naq", "NOTIFICATION_FILTER": "failures_only"}
        s = _make_settings(env, monkeypatch)
        assert s.nasdaq_api_key == "naq"
        assert s.notification_filter == "failures_only"


class TestSettingsMissingRequired:
    @pytest.mark.parametrize("missing_key", list(_REQUIRED.keys()))
    def test_missing_required_field_raises_validation_error(self, missing_key, monkeypatch):
        env = {k: v for k, v in _REQUIRED.items() if k != missing_key}
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        monkeypatch.delenv(missing_key, raising=False)
        from rocketstocks.core.config.settings import Settings
        with pytest.raises(ValidationError):
            Settings(_env_file=None)
