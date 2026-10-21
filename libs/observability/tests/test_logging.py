"""configure_logging: stdlib front door, structlog engine.

Every test drives the pipeline the way services do — `logging.getLogger`
module loggers (the zero-churn front door) — and asserts on the rendered
stdout stream, because the stream IS the interface: one JSON line per record
carrying service/level/timestamp/logger/event plus any bound fields.
"""

import json
import logging
from datetime import UTC, datetime

import pytest
import structlog

from rs_observability import configure_logging


@pytest.fixture(autouse=True)
def _isolated_logging(monkeypatch):
    """Restore root-logger and structlog global state around every test."""
    monkeypatch.delenv("RS_LOG_FORMAT", raising=False)
    monkeypatch.delenv("RS_LOG_LEVEL", raising=False)
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    saved_lib_levels = {n: logging.getLogger(n).level for n in ("httpx", "httpcore")}
    yield
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)
    for name, lib_level in saved_lib_levels.items():
        logging.getLogger(name).setLevel(lib_level)
    structlog.contextvars.clear_contextvars()
    structlog.reset_defaults()


def json_lines(capsys) -> list[dict]:
    return [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]


def test_stdlib_record_carries_required_fields(monkeypatch, capsys):
    monkeypatch.setenv("RS_LOG_FORMAT", "json")
    configure_logging("ingest")
    logging.getLogger("some.module").info("bar closed")
    (record,) = json_lines(capsys)
    assert record["service"] == "ingest"
    assert record["level"] == "info"
    assert record["logger"] == "some.module"
    assert record["event"] == "bar closed"
    ts = datetime.fromisoformat(record["timestamp"])
    assert ts.tzinfo is not None and ts.utcoffset().total_seconds() == 0
    assert abs((datetime.now(UTC) - ts).total_seconds()) < 60


def test_percent_style_args_are_interpolated(monkeypatch, capsys):
    monkeypatch.setenv("RS_LOG_FORMAT", "json")
    configure_logging("bot")
    logging.getLogger("m").info("posted alert id=%d lag=%.1fs", 7, 2.5)
    (record,) = json_lines(capsys)
    assert record["event"] == "posted alert id=7 lag=2.5s"


def test_stdlib_record_merges_bound_contextvars(monkeypatch, capsys):
    """A foreign stdlib record emitted inside a
    bound context (discord.py, uvicorn, ...) still carries the bound fields."""
    monkeypatch.setenv("RS_LOG_FORMAT", "json")
    configure_logging("bot")
    with structlog.contextvars.bound_contextvars(correlation_id="abc", alert_id=42):
        logging.getLogger("discord.gateway").info("resumed session")
    logging.getLogger("discord.gateway").info("outside")
    inside, outside = json_lines(capsys)
    assert inside["correlation_id"] == "abc"
    assert inside["alert_id"] == 42
    assert "correlation_id" not in outside


def test_exception_renders_full_traceback(monkeypatch, capsys):
    monkeypatch.setenv("RS_LOG_FORMAT", "json")
    configure_logging("serve")
    logger = logging.getLogger("m")
    try:
        raise ValueError("boom")
    except ValueError:
        logger.exception("unhandled")
    (record,) = json_lines(capsys)
    assert record["level"] == "error"
    assert "Traceback" in record["exception"]
    assert "ValueError: boom" in record["exception"]


def test_structlog_front_door_shares_the_stream(monkeypatch, capsys):
    """New code MAY use structlog.get_logger() — same pipeline, same fields."""
    monkeypatch.setenv("RS_LOG_FORMAT", "json")
    configure_logging("ingest")
    structlog.get_logger("rs_new.module").info("native", symbol="SPY")
    (record,) = json_lines(capsys)
    assert record["service"] == "ingest"
    assert record["event"] == "native"
    assert record["symbol"] == "SPY"
    assert record["logger"] == "rs_new.module"
    assert "timestamp" in record


def test_default_level_is_info_and_rs_log_level_overrides(monkeypatch, capsys):
    monkeypatch.setenv("RS_LOG_FORMAT", "json")
    configure_logging("ingest")
    logging.getLogger("m").debug("hidden")
    assert capsys.readouterr().out == ""
    monkeypatch.setenv("RS_LOG_LEVEL", "DEBUG")
    configure_logging("ingest")
    logging.getLogger("m").debug("shown")
    (record,) = json_lines(capsys)
    assert record["event"] == "shown"


def test_http_client_request_logs_are_not_emitted_at_info(monkeypatch, capsys):
    monkeypatch.setenv("RS_LOG_FORMAT", "json")
    configure_logging("ingest")
    logging.getLogger("httpx").info('HTTP Request: POST https://discord.test/webhooks/1/token')
    logging.getLogger("httpx").warning("kept")
    (record,) = json_lines(capsys)
    assert record["event"] == "kept"


def test_reconfigure_replaces_the_root_handler(monkeypatch, capsys):
    """Idempotent: calling twice (or after basicConfig) still yields exactly
    one stream — never doubled records."""
    monkeypatch.setenv("RS_LOG_FORMAT", "json")
    logging.basicConfig(level=logging.INFO)
    configure_logging("ingest")
    configure_logging("ingest")
    logging.getLogger("m").info("once")
    assert len(json_lines(capsys)) == 1


def test_console_format_is_human_readable(monkeypatch, capsys):
    monkeypatch.setenv("RS_LOG_FORMAT", "console")
    configure_logging("ingest")
    logging.getLogger("m").info("pretty line")
    out = capsys.readouterr().out
    assert "pretty line" in out
    with pytest.raises(json.JSONDecodeError):
        json.loads(out.splitlines()[0])


def test_auto_format_defaults_to_json_when_not_a_tty(monkeypatch, capsys):
    configure_logging("ingest")  # capsys' stdout is not a tty
    logging.getLogger("m").info("auto")
    (record,) = json_lines(capsys)
    assert record["event"] == "auto"


def test_invalid_format_and_level_are_loud(monkeypatch):
    monkeypatch.setenv("RS_LOG_FORMAT", "yaml")
    with pytest.raises(ValueError, match="RS_LOG_FORMAT"):
        configure_logging("ingest")
    monkeypatch.setenv("RS_LOG_FORMAT", "json")
    monkeypatch.setenv("RS_LOG_LEVEL", "CHATTY")
    with pytest.raises(ValueError, match="RS_LOG_LEVEL"):
        configure_logging("ingest")
