"""Tests for rocketstocks.core.config.settings."""
import importlib
import json
import pytest
from unittest.mock import patch


class TestConfig:
    def test_load_config_returns_dict(self, tmp_path):
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({"key": "value"}))

        with patch("rocketstocks.core.config.settings.get_env", return_value=str(config_file)):
            from rocketstocks.core.config.settings import config
            c = config()
            result = c.load_config()
        assert result == {"key": "value"}

    def test_load_config_returns_none_on_missing_file(self, tmp_path):
        with patch("rocketstocks.core.config.settings.get_env", return_value=str(tmp_path / "nonexistent.json")):
            from rocketstocks.core.config.settings import config
            c = config()
            result = c.load_config()
        assert result is None

    def test_write_config_persists_data(self, tmp_path):
        config_file = tmp_path / "config.json"
        config_file.write_text("{}")

        with patch("rocketstocks.core.config.settings.get_env", return_value=str(config_file)):
            from rocketstocks.core.config.settings import config
            c = config()
            c.write_config({"foo": "bar"})
            result = json.loads(config_file.read_text())
        assert result == {"foo": "bar"}


class TestChannelIds:
    def test_channel_ids_are_ints_when_set(self):
        """Channel IDs should parse to int from numeric string env vars."""
        with patch("rocketstocks.core.config.environment.get_env", return_value="12345"):
            import rocketstocks.core.config.settings as settings_mod
            importlib.reload(settings_mod)
            assert isinstance(settings_mod.reports_channel_id, int)
            assert settings_mod.reports_channel_id == 12345

    def test_channel_ids_default_to_zero_on_missing(self):
        """When env vars are missing (None), IDs should default to 0."""
        with patch("rocketstocks.core.config.environment.get_env", return_value=None):
            import rocketstocks.core.config.settings as settings_mod
            importlib.reload(settings_mod)
            assert settings_mod.reports_channel_id == 0
            assert settings_mod.alerts_channel_id == 0
            assert settings_mod.screeners_channel_id == 0
            assert settings_mod.charts_channel_id == 0
