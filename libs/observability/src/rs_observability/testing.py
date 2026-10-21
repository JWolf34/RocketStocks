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
from typing import Any, cast

import httpx
import structlog
from pydantic import SecretStr

from rs_observability import ops as ops_module
from rs_observability.logs import configure_logging
from rs_observability.ops import OpsSettings, configure_ops


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
    cast(logging.StreamHandler[Any], root.handlers[0]).setStream(buffer)
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


@contextmanager
def captured_ops_pings() -> Iterator[list[str]]:
    """Every Ops ping `notify_ops` sends inside the block, as message text.

    De-duplication is off so every send shows; the process-wide notifier is
    restored on exit.
    """
    sent: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content)["content"])
        return httpx.Response(204)

    saved = ops_module._notifier
    configure_ops(
        OpsSettings(
            _env_file=None,
            rs_ops_webhook_url=SecretStr("https://discord.test/api/webhooks/1/secret"),
            rs_ops_dedup_window_seconds=0.0,
        ),
        transport=httpx.MockTransport(handler),
    )
    try:
        yield sent
    finally:
        ops_module._notifier = saved
