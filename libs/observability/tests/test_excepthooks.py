"""install_excepthooks: every unhandled-exception path ends as a JSON record
with a full traceback (ADR-0015 native capture; S006 acceptance criterion)."""

import asyncio
import sys
import threading

import pytest

from rs_observability import install_excepthooks
from rs_observability.testing import captured_json_stream, json_records


@pytest.fixture
def stream():
    """The captured stream, plus save/restore of the process-wide hooks the
    subject mutates."""
    saved_sys_hook = sys.excepthook
    saved_threading_hook = threading.excepthook
    with captured_json_stream("test") as buffer:
        yield buffer
    sys.excepthook = saved_sys_hook
    threading.excepthook = saved_threading_hook


def planted_exc_info():
    try:
        raise ValueError("planted")
    except ValueError:
        return sys.exc_info()


def test_sys_excepthook_logs_full_traceback(stream):
    install_excepthooks()
    assert sys.excepthook is not sys.__excepthook__
    sys.excepthook(*planted_exc_info())
    (record,) = json_records(stream)
    assert record["level"] == "error"
    assert "Traceback" in record["exception"]
    assert "ValueError: planted" in record["exception"]


def test_sys_excepthook_lets_keyboardinterrupt_through(stream, capsys):
    """Ctrl-C is not an error condition — no JSON record, default handling."""
    install_excepthooks()
    try:
        raise KeyboardInterrupt
    except KeyboardInterrupt:
        sys.excepthook(*sys.exc_info())
    assert stream.getvalue() == ""
    assert "KeyboardInterrupt" in capsys.readouterr().err  # default hook ran


def test_threading_excepthook_logs_full_traceback(stream):
    install_excepthooks()

    def target():
        raise ValueError("planted in thread")

    thread = threading.Thread(target=target, name="worker-1")
    thread.start()
    thread.join()
    (record,) = json_records(stream)
    assert record["level"] == "error"
    assert "worker-1" in record["event"]
    assert "ValueError: planted in thread" in record["exception"]


def test_threading_excepthook_tolerates_a_missing_exception_value(stream):
    install_excepthooks()
    threading.excepthook(
        threading.ExceptHookArgs((ValueError, None, None, threading.current_thread()))
    )
    (record,) = json_records(stream)
    assert record["level"] == "error"
    assert "exception" not in record


async def test_asyncio_handler_logs_full_traceback(stream):
    install_excepthooks()  # inside a running loop → wires the loop handler too
    loop = asyncio.get_running_loop()
    assert loop.get_exception_handler() is not None
    try:
        raise ValueError("planted in task")
    except ValueError as exc:
        loop.call_exception_handler(
            {"message": "task exception was never retrieved", "exception": exc}
        )
    (record,) = json_records(stream)
    assert record["level"] == "error"
    assert "task exception was never retrieved" in record["event"]
    assert "ValueError: planted in task" in record["exception"]


async def test_asyncio_handler_without_exception_still_logs(stream):
    install_excepthooks()
    asyncio.get_running_loop().call_exception_handler({"message": "socket leak"})
    (record,) = json_records(stream)
    assert "socket leak" in record["event"]


def test_no_running_loop_installs_sync_hooks_only(stream):
    """Callable from a sync main() before asyncio.run — must not blow up."""
    install_excepthooks()
    assert sys.excepthook is not sys.__excepthook__
