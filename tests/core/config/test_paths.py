"""Tests for rocketstocks.core.config.paths."""
import os
from unittest.mock import patch, call

from rocketstocks.core.config.paths import validate_path, datapaths


class TestValidatePath:
    def test_creates_dir_when_missing(self, tmp_path):
        new_dir = str(tmp_path / "new_dir" / "subdir")
        validate_path(new_dir)
        assert os.path.isdir(new_dir)

    def test_returns_true_when_dir_exists(self, tmp_path):
        existing = tmp_path / "existing"
        existing.mkdir()
        result = validate_path(str(existing))
        assert result is True

    def test_returns_none_when_created(self, tmp_path):
        new_dir = str(tmp_path / "brand_new")
        result = validate_path(new_dir)
        assert result is None

    def test_does_not_raise_on_existing_path(self, tmp_path):
        existing = tmp_path / "safe"
        existing.mkdir()
        # Should not raise
        validate_path(str(existing))

    def test_calls_makedirs_when_missing(self):
        with patch("rocketstocks.core.config.paths.os.path.isdir", return_value=False), \
             patch("rocketstocks.core.config.paths.os.makedirs") as mock_makedirs:
            validate_path("/some/fake/path")
            mock_makedirs.assert_called_once_with("/some/fake/path")


class TestDatapaths:
    def test_attachments_path_defined(self):
        assert datapaths.attachments_path == "discord/attachments"
