"""Native unhandled-exception capture.

Every path an exception can escape a service unnoticed ends in
`logger.error(..., exc_info=...)` — a full-traceback record on the one JSON
stream. Framework hooks are separate and already wired (discord.py logs
through stdlib loggers the stream captures).
"""

import asyncio
import logging
import sys
import threading
from types import TracebackType
from typing import Any

logger = logging.getLogger(__name__)


def install_excepthooks() -> None:
    """Wire `sys.excepthook`, `threading.excepthook`, and — when called with a
    loop running — the asyncio loop exception handler.

    The asyncio handler can only attach to a live loop, and `asyncio.run()`
    creates one per call: invoke this once at process start (sys/threading)
    and once inside the running loop (all three; re-running is idempotent).
    """
    sys.excepthook = _sys_hook
    threading.excepthook = _threading_hook
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        loop.set_exception_handler(_asyncio_handler)


def _sys_hook(
    exc_type: type[BaseException], exc_value: BaseException, exc_tb: TracebackType | None
) -> None:
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_tb)  # Ctrl-C is not an error
        return
    logger.error("unhandled exception", exc_info=(exc_type, exc_value, exc_tb))


def _threading_hook(args: threading.ExceptHookArgs) -> None:
    thread_name = args.thread.name if args.thread is not None else "<unknown>"
    # The hook contract allows exc_value None; log without a traceback then.
    exc_info = (
        (args.exc_type, args.exc_value, args.exc_traceback) if args.exc_value is not None else None
    )
    logger.error("unhandled exception in thread %s", thread_name, exc_info=exc_info)


def _asyncio_handler(loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
    # context always carries "message"; "exception" only when one escaped
    # (the handler also fires for e.g. resource leaks — still worth a record).
    logger.error(
        "unhandled exception in asyncio: %s",
        context.get("message"),
        exc_info=context.get("exception"),
    )
