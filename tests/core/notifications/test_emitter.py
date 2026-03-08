"""Tests for rocketstocks.core.notifications.emitter."""
import asyncio
import threading
import pytest
from rocketstocks.core.notifications.config import NotificationLevel
from rocketstocks.core.notifications.emitter import EventEmitter
from rocketstocks.core.notifications.event import NotificationEvent


class TestEmitAndDrain:
    def test_emit_then_drain_returns_event(self):
        emitter = EventEmitter()
        event = NotificationEvent(level=NotificationLevel.SUCCESS, source="src", job_name="job", message="ok")
        emitter.emit(event)
        result = emitter.drain()
        assert len(result) == 1
        assert result[0] is event

    def test_drain_empty_queue_returns_empty_list(self):
        emitter = EventEmitter()
        assert emitter.drain() == []

    def test_drain_respects_max_items(self):
        emitter = EventEmitter()
        for i in range(10):
            emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS, source="src", job_name=f"job{i}", message="ok"
            ))
        result = emitter.drain(max_items=3)
        assert len(result) == 3
        # Remaining 7 still in queue
        rest = emitter.drain(max_items=100)
        assert len(rest) == 7

    def test_drain_default_max_items_fifty(self):
        emitter = EventEmitter()
        for i in range(60):
            emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS, source="src", job_name=f"job{i}", message="ok"
            ))
        result = emitter.drain()
        assert len(result) == 50

    def test_multiple_emits_drained_in_fifo_order(self):
        emitter = EventEmitter()
        names = ["first", "second", "third"]
        for name in names:
            emitter.emit(NotificationEvent(
                level=NotificationLevel.SUCCESS, source="src", job_name=name, message="ok"
            ))
        result = emitter.drain()
        assert [e.job_name for e in result] == names


class TestJobWrapper:
    def test_success_emits_success_event(self):
        emitter = EventEmitter()

        async def good_job():
            return 42

        wrapped = emitter.job_wrapper("my_job", good_job)
        asyncio.get_event_loop().run_until_complete(wrapped())

        events = emitter.drain()
        assert len(events) == 1
        assert events[0].level == NotificationLevel.SUCCESS
        assert events[0].job_name == "my_job"

    def test_success_event_has_elapsed_seconds(self):
        emitter = EventEmitter()

        async def good_job():
            pass

        wrapped = emitter.job_wrapper("timed_job", good_job)
        asyncio.get_event_loop().run_until_complete(wrapped())

        events = emitter.drain()
        assert events[0].elapsed_seconds is not None
        assert events[0].elapsed_seconds >= 0.0

    def test_failure_emits_failure_event(self):
        emitter = EventEmitter()

        async def bad_job():
            raise ValueError("boom")

        wrapped = emitter.job_wrapper("fail_job", bad_job)
        with pytest.raises(ValueError, match="boom"):
            asyncio.get_event_loop().run_until_complete(wrapped())

        events = emitter.drain()
        assert len(events) == 1
        assert events[0].level == NotificationLevel.FAILURE
        assert events[0].job_name == "fail_job"
        assert "boom" in events[0].message

    def test_failure_event_includes_traceback(self):
        emitter = EventEmitter()

        async def bad_job():
            raise RuntimeError("traceback test")

        wrapped = emitter.job_wrapper("tb_job", bad_job)
        with pytest.raises(RuntimeError):
            asyncio.get_event_loop().run_until_complete(wrapped())

        events = emitter.drain()
        assert events[0].traceback is not None
        assert "RuntimeError" in events[0].traceback

    def test_failure_reraises_exception(self):
        emitter = EventEmitter()

        async def bad_job():
            raise TypeError("reraised")

        wrapped = emitter.job_wrapper("reraise_job", bad_job)
        with pytest.raises(TypeError, match="reraised"):
            asyncio.get_event_loop().run_until_complete(wrapped())

    def test_job_wrapper_returns_value_on_success(self):
        emitter = EventEmitter()

        async def value_job():
            return 99

        wrapped = emitter.job_wrapper("value_job", value_job)
        result = asyncio.get_event_loop().run_until_complete(wrapped())
        assert result == 99

    def test_job_wrapper_handles_sync_function(self):
        """Wrapping a sync def runs it via asyncio.to_thread and emits SUCCESS."""
        emitter = EventEmitter()

        def sync_job():
            return 42

        wrapped = emitter.job_wrapper("sync_job", sync_job)
        result = asyncio.get_event_loop().run_until_complete(wrapped())
        assert result == 42
        events = emitter.drain()
        assert len(events) == 1
        assert events[0].level == NotificationLevel.SUCCESS
        assert events[0].job_name == "sync_job"

    def test_job_wrapper_handles_sync_function_failure(self):
        """Sync function that raises emits FAILURE and re-raises."""
        emitter = EventEmitter()

        def sync_job():
            raise ValueError("sync boom")

        wrapped = emitter.job_wrapper("sync_fail_job", sync_job)
        with pytest.raises(ValueError, match="sync boom"):
            asyncio.get_event_loop().run_until_complete(wrapped())

        events = emitter.drain()
        assert len(events) == 1
        assert events[0].level == NotificationLevel.FAILURE
        assert events[0].job_name == "sync_fail_job"
        assert "sync boom" in events[0].message

    def test_sync_function_runs_in_thread(self):
        """Sync function wrapped by job_wrapper executes on a non-main thread."""
        import threading
        emitter = EventEmitter()
        thread_ids = []

        def sync_job():
            thread_ids.append(threading.current_thread().ident)

        wrapped = emitter.job_wrapper("thread_job", sync_job)
        asyncio.get_event_loop().run_until_complete(wrapped())
        assert thread_ids[0] != threading.main_thread().ident


class TestTaskWrapper:
    def test_success_emits_success_event(self):
        emitter = EventEmitter()

        @emitter.task_wrapper("my_task")
        async def my_method(cog_self):
            pass

        mock_self = object()
        asyncio.get_event_loop().run_until_complete(my_method(mock_self))

        events = emitter.drain()
        assert len(events) == 1
        assert events[0].level == NotificationLevel.SUCCESS
        assert events[0].job_name == "my_task"

    def test_failure_emits_failure_event_and_reraises(self):
        emitter = EventEmitter()

        @emitter.task_wrapper("fail_task")
        async def failing_method(cog_self):
            raise ValueError("task failed")

        mock_self = object()
        with pytest.raises(ValueError, match="task failed"):
            asyncio.get_event_loop().run_until_complete(failing_method(mock_self))

        events = emitter.drain()
        assert len(events) == 1
        assert events[0].level == NotificationLevel.FAILURE
        assert events[0].job_name == "fail_task"
        assert events[0].traceback is not None

    def test_task_wrapper_elapsed_seconds_set(self):
        emitter = EventEmitter()

        @emitter.task_wrapper("elapsed_task")
        async def my_method(cog_self):
            pass

        asyncio.get_event_loop().run_until_complete(my_method(object()))
        events = emitter.drain()
        assert events[0].elapsed_seconds is not None
        assert events[0].elapsed_seconds >= 0.0


class TestThreadSafety:
    def test_emit_from_one_thread_drain_from_another(self):
        emitter = EventEmitter()
        emitted = []

        def producer():
            for i in range(20):
                emitter.emit(NotificationEvent(
                    level=NotificationLevel.SUCCESS,
                    source="thread",
                    job_name=f"job{i}",
                    message="ok",
                ))

        thread = threading.Thread(target=producer)
        thread.start()
        thread.join()

        result = emitter.drain(max_items=100)
        assert len(result) == 20

    def test_concurrent_emitters(self):
        emitter = EventEmitter()

        def producer(n: int):
            for i in range(10):
                emitter.emit(NotificationEvent(
                    level=NotificationLevel.SUCCESS,
                    source=f"thread{n}",
                    job_name=f"job{n}_{i}",
                    message="ok",
                ))

        threads = [threading.Thread(target=producer, args=(n,)) for n in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        result = emitter.drain(max_items=100)
        assert len(result) == 50
