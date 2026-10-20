"""Structured logging (ADR-0015, S006 pin #2): stdlib front door, structlog
engine.

configure_logging installs a structlog ProcessorFormatter on the root stdlib
handler. Module code keeps `logging.getLogger(__name__)` — zero churn in the
accepted S003–S005 modules — and third-party stdlib records (uvicorn,
discord.py, schwab-py, asyncio, alembic) render through the same processor
chain, including the contextvars merge that carries `correlation_id` and the
unit-of-work domain keys. New code MAY use `structlog.get_logger()`; both
front doors feed the single stream on stdout.
"""

import logging
import os
import sys
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager

import structlog

_FORMATS = ("json", "console", "auto")

# httpx logs every request URL at INFO, and a Discord webhook URL is a credential.
_URL_LOGGING_LIBRARIES = ("httpx", "httpcore")


def _add_service(service: str) -> Callable:
    def processor(logger, method_name, event_dict):
        event_dict["service"] = service
        return event_dict

    return processor


def configure_logging(service: str) -> None:
    """Route ALL logging — stdlib and structlog-native — through one processor
    chain to one stream on stdout. Safe to call more than once: the root
    handler is replaced, never stacked.

    RS_LOG_FORMAT ∈ {json, console, auto} (default auto: TTY → pretty console,
    else JSON); RS_LOG_LEVEL names a stdlib level (default INFO). Invalid
    values raise — loud exit, never a silent default (settings convention).
    """
    fmt = os.environ.get("RS_LOG_FORMAT", "auto")
    if fmt not in _FORMATS:
        raise ValueError(f"RS_LOG_FORMAT must be one of {_FORMATS}, got {fmt!r}")
    if fmt == "auto":
        fmt = "console" if sys.stdout.isatty() else "json"
    level_name = os.environ.get("RS_LOG_LEVEL", "INFO").upper()
    level = logging.getLevelNamesMapping().get(level_name)
    if level is None:
        raise ValueError(f"RS_LOG_LEVEL must name a logging level, got {level_name!r}")

    # Runs on every record: in-pipeline for structlog-native entries, as the
    # foreign_pre_chain for stdlib ones — so both carry the same fields.
    shared = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.ExtraAdder(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_service(service),
    ]
    if fmt == "json":
        # ConsoleRenderer formats exc_info itself; JSONRenderer needs the
        # traceback flattened into an `exception` field first.
        renderers = [structlog.processors.format_exc_info, structlog.processors.JSONRenderer()]
    else:
        renderers = [structlog.dev.ConsoleRenderer()]
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, *renderers],
    )

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *shared,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,  # reconfiguration (tests) must take
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    for name in _URL_LOGGING_LIBRARIES:
        logging.getLogger(name).setLevel(max(level, logging.WARNING))


@contextmanager
def unit_of_work(**fields) -> Iterator[str]:
    """Bind a fresh opaque UUID as `correlation_id` plus the given domain-key
    fields onto every record logged inside the block — from any logger,
    third-party retry internals included (S006 pin #3).

    correlation_id groups one unit of work; cross-service tracing joins on the
    domain keys (`alert_id` in ingest's insert log and bot's post log), never
    on the id. Yields the id so callers can echo it (serve's
    `X-Correlation-ID` response header).
    """
    correlation_id = str(uuid.uuid4())
    with structlog.contextvars.bound_contextvars(correlation_id=correlation_id, **fields):
        yield correlation_id
