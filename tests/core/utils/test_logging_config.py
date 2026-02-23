"""Tests for rocketstocks.core.utils.logging_config."""
import logging
import sys
from unittest.mock import patch

import pytest

from rocketstocks.core.utils.logging_config import (
    ModuleFilter,
    get_file_handler,
    setup_logging,
)


@pytest.fixture(autouse=True)
def reset_root_logger():
    """Remove all handlers from the root logger before and after each test."""
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    root.handlers.clear()
    yield
    root.handlers.clear()
    root.handlers.extend(original_handlers)


def test_setup_logging_creates_handlers():
    setup_logging()
    root = logging.getLogger()
    handler_names = {h.name for h in root.handlers}
    assert "stdout" in handler_names
    assert "file" in handler_names


def test_module_filter_passes_rocketstocks():
    f = ModuleFilter()
    record = logging.LogRecord(
        name="rocketstocks.core.charting.chart",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="test",
        args=(),
        exc_info=None,
    )
    assert f.filter(record) is True


def test_module_filter_passes_main():
    f = ModuleFilter()
    record = logging.LogRecord(
        name="__main__",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="test",
        args=(),
        exc_info=None,
    )
    assert f.filter(record) is True


def test_module_filter_blocks_other():
    f = ModuleFilter()
    record = logging.LogRecord(
        name="discord.gateway",
        level=logging.DEBUG,
        pathname="",
        lineno=0,
        msg="heartbeat",
        args=(),
        exc_info=None,
    )
    assert f.filter(record) is False


def test_get_file_handler():
    setup_logging()
    handler = get_file_handler()
    assert handler is not None
    assert handler.name == "file"


def test_setup_logging_fallback_on_mkdir_failure():
    """When RotatingFileHandler creation fails, only the stdout handler is added."""
    with patch(
        "rocketstocks.core.utils.logging_config.logging.handlers.RotatingFileHandler",
        side_effect=OSError("permission denied"),
    ):
        setup_logging()

    root = logging.getLogger()
    handler_names = {h.name for h in root.handlers}
    assert "stdout" in handler_names
    assert "file" not in handler_names


def test_setup_logging_idempotent():
    """Calling setup_logging twice should not add duplicate handlers."""
    setup_logging()
    setup_logging()
    root = logging.getLogger()
    names = [h.name for h in root.handlers]
    assert names.count("stdout") == 1
