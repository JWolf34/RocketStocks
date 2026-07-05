import asyncio
import logging
import queue
import time
import traceback as tb
from functools import wraps

from rocketstocks.core.notifications.config import NotificationLevel
from rocketstocks.core.notifications.event import NotificationEvent

logger = logging.getLogger(__name__)


class EventEmitter:
    """Thread-safe event queue bridge between scheduler/cog threads and the Discord notification cog."""

    def __init__(self):
        self._queue: queue.Queue[NotificationEvent] = queue.Queue()

    def emit(self, event: NotificationEvent) -> None:
        """Put an event onto the queue (non-blocking)."""
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            logger.warning(f"Notification queue full — dropping event: {event.job_name}")

    def drain(self, max_items: int = 50) -> list[NotificationEvent]:
        """Non-blocking drain of up to *max_items* events from the queue."""
        events: list[NotificationEvent] = []
        for _ in range(max_items):
            try:
                events.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return events

    def job_wrapper(self, job_name: str, func):
        """
        Wrap a sync or async callable with try/except.
        Sync functions are run in a thread pool via asyncio.to_thread.
        Emits SUCCESS on normal return, FAILURE on exception (re-raises).
        Records elapsed time via time.monotonic().
        """
        source = func.__module__ if hasattr(func, '__module__') else "scheduler"
        is_async = asyncio.iscoroutinefunction(func)

        @wraps(func)
        async def wrapped(*args, **kwargs):
            start = time.monotonic()
            try:
                if is_async:
                    result = await func(*args, **kwargs)
                else:
                    result = await asyncio.to_thread(func, *args, **kwargs)
                elapsed = time.monotonic() - start
                self.emit(NotificationEvent(
                    level=NotificationLevel.SUCCESS,
                    source=source,
                    job_name=job_name,
                    message=f"Job completed successfully",
                    elapsed_seconds=elapsed,
                ))
                return result
            except Exception as exc:
                elapsed = time.monotonic() - start
                trace = tb.format_exc()
                self.emit(NotificationEvent(
                    level=NotificationLevel.FAILURE,
                    source=source,
                    job_name=job_name,
                    message=str(exc),
                    traceback=trace,
                    elapsed_seconds=elapsed,
                ))
                raise

        return wrapped

    def task_wrapper(self, task_name: str):
        """
        Decorator for discord.ext.tasks loop methods (cog methods with `self` as first arg).
        Emits SUCCESS on normal return, FAILURE on exception (re-raises).
        """
        def decorator(func):
            source = func.__module__ if hasattr(func, '__module__') else "cog"

            @wraps(func)
            async def wrapped(cog_self, *args, **kwargs):
                start = time.monotonic()
                try:
                    result = await func(cog_self, *args, **kwargs)
                    elapsed = time.monotonic() - start
                    self.emit(NotificationEvent(
                        level=NotificationLevel.SUCCESS,
                        source=source,
                        job_name=task_name,
                        message="Task completed successfully",
                        elapsed_seconds=elapsed,
                    ))
                    return result
                except Exception as exc:
                    elapsed = time.monotonic() - start
                    trace = tb.format_exc()
                    self.emit(NotificationEvent(
                        level=NotificationLevel.FAILURE,
                        source=source,
                        job_name=task_name,
                        message=str(exc),
                        traceback=trace,
                        elapsed_seconds=elapsed,
                    ))
                    raise

            return wrapped
        return decorator
