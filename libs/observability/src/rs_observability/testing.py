"""Shared test helper (the rs_persistence.testing pattern): capture the
rendered JSON stream configure_logging produces.

Correlation/unit-of-work fields merge at FORMAT time, so caplog's raw records
never see them — tests must assert on rendered output. And a handler created
inside a pytest fixture binds a sys.stdout object pytest may swap before the
test body runs, so capsys can't see it either. This helper sidesteps both:
the root handler is pointed at a StringIO, and all mutated global state (env,
root logger, structlog defaults + contextvars) is restored on exit.
"""

import io
import json
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager

import structlog

from rs_observability.logs import configure_logging


@contextmanager
def captured_json_stream(service: str) -> Iterator[io.StringIO]:
    """configure_logging(service) forced to json mode, root handler → StringIO."""
    saved_env = {key: os.environ.get(key) for key in ("RS_LOG_FORMAT", "RS_LOG_LEVEL")}
    os.environ["RS_LOG_FORMAT"] = "json"
    os.environ.pop("RS_LOG_LEVEL", None)
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    configure_logging(service)
    buffer = io.StringIO()
    handler = root.handlers[0]
    assert isinstance(handler, logging.StreamHandler)
    handler.setStream(buffer)
    try:
        yield buffer
    finally:
        for key, value in saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)
        structlog.contextvars.clear_contextvars()
        structlog.reset_defaults()


def json_records(stream: io.StringIO) -> list[dict]:
    """Every record on the captured stream, parsed — one dict per line."""
    return [json.loads(line) for line in stream.getvalue().strip().splitlines()]
