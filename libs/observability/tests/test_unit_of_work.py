"""unit_of_work: fresh opaque correlation_id + domain keys as fields (S006
pin #3). Asserted through the rendered stream — binding only matters if the
records actually carry the fields. (That the stream targets stdout is
test_logging.py's subject; here the root handler is pointed at a StringIO so
the assertions don't depend on pytest's capture internals.)"""

import io
import json
import logging
import uuid

import pytest
import structlog

from rs_observability import configure_logging, unit_of_work


@pytest.fixture
def stream(monkeypatch):
    """configure_logging("test") with the root handler feeding a StringIO."""
    monkeypatch.setenv("RS_LOG_FORMAT", "json")
    monkeypatch.delenv("RS_LOG_LEVEL", raising=False)
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    configure_logging("test")
    buffer = io.StringIO()
    root.handlers[0].setStream(buffer)
    yield buffer
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)
    structlog.contextvars.clear_contextvars()
    structlog.reset_defaults()


def json_lines(stream: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in stream.getvalue().strip().splitlines()]


def test_binds_fresh_uuid_and_domain_fields(stream):
    with unit_of_work(symbol="SPY", bucket="2026-07-13T14:30:00Z") as correlation_id:
        logging.getLogger("rs_ingest.pipeline").info("bar persisted")
    (record,) = json_lines(stream)
    assert record["correlation_id"] == correlation_id
    uuid.UUID(correlation_id)  # opaque UUID, never a domain value
    assert record["symbol"] == "SPY"
    assert record["bucket"] == "2026-07-13T14:30:00Z"


def test_every_unit_of_work_gets_a_new_id(stream):
    with unit_of_work() as first:
        pass
    with unit_of_work() as second:
        pass
    assert first != second


def test_fields_do_not_leak_after_exit(stream):
    with unit_of_work(alert_id=42):
        pass
    logging.getLogger("m").info("outside")
    (record,) = json_lines(stream)
    assert "correlation_id" not in record
    assert "alert_id" not in record


def test_fields_do_not_leak_when_body_raises(stream):
    with pytest.raises(ValueError), unit_of_work(alert_id=42):
        raise ValueError("boom")
    logging.getLogger("m").info("after")
    (record,) = json_lines(stream)
    assert "correlation_id" not in record


def test_all_records_in_the_unit_share_the_id(stream):
    """The grouping property: every record inside one unit of work — from any
    logger, e.g. a third-party retry internal — joins on the one id."""
    with unit_of_work(alert_id=7):
        logging.getLogger("rs_bot.sweep").info("posting")
        logging.getLogger("discord.http").warning("rate limited, retrying")
    first, second = json_lines(stream)
    assert first["correlation_id"] == second["correlation_id"]
    assert second["alert_id"] == 7
