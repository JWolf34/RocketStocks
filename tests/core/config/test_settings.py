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



