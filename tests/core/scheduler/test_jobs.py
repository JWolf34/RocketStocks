"""Tests for rocketstocks.core.scheduler.jobs."""
from unittest.mock import MagicMock, patch, AsyncMock
import datetime

import pytest


def _make_stock_data():
    sd = MagicMock(name="StockData")
    sd.tickers = MagicMock()
    sd.price_history = MagicMock()
    sd.earnings = MagicMock()
    sd.capitol_trades = MagicMock()
    return sd


def _make_emitter():
    emitter = MagicMock(name="EventEmitter")
    # job_wrapper returns a coroutine-like callable
    emitter.job_wrapper.side_effect = lambda name, func: func
    return emitter


class TestRegisterJobs:
    def test_adds_thirteen_jobs(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        emitter = _make_emitter()
        register_jobs(mock_sched, sd, emitter)
        assert mock_sched.add_job.call_count == 13

    def test_update_tickers_job_registered(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        emitter = _make_emitter()
        register_jobs(mock_sched, sd, emitter)
        job_names = [call.kwargs.get("name") for call in mock_sched.add_job.call_args_list]
        assert "Update tickers data in DB" in job_names

    def test_update_upcoming_earnings_job_registered(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        emitter = _make_emitter()
        register_jobs(mock_sched, sd, emitter)
        job_names = [call.kwargs.get("name") for call in mock_sched.add_job.call_args_list]
        assert "Update upcoming earnings" in job_names

    def test_update_politicians_job_registered(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        emitter = _make_emitter()
        register_jobs(mock_sched, sd, emitter)
        job_names = [call.kwargs.get("name") for call in mock_sched.add_job.call_args_list]
        assert "Update politicians" in job_names

    def test_check_schwab_token_expiry_job_registered(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        emitter = _make_emitter()
        register_jobs(mock_sched, sd, emitter)
        job_names = [call.kwargs.get("name") for call in mock_sched.add_job.call_args_list]
        assert "Check Schwab token expiry" in job_names

    def test_all_jobs_have_replace_existing_true(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        emitter = _make_emitter()
        register_jobs(mock_sched, sd, emitter)
        for call in mock_sched.add_job.call_args_list:
            assert call.kwargs.get("replace_existing") is True

    def test_all_jobs_use_utc_timezone(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        emitter = _make_emitter()
        register_jobs(mock_sched, sd, emitter)
        for call in mock_sched.add_job.call_args_list:
            assert call.kwargs.get("timezone") == "UTC"

    def test_job_wrapper_called_for_each_job(self):
        """Verify that emitter.job_wrapper is called once per wrapped job (12 total; token-expiry is unwrapped)."""
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        emitter = _make_emitter()
        register_jobs(mock_sched, sd, emitter)
        assert emitter.job_wrapper.call_count == 12

    def test_job_wrapper_receives_correct_names(self):
        """Verify job_wrapper is called with expected job names."""
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        emitter = _make_emitter()
        register_jobs(mock_sched, sd, emitter)
        wrapper_names = [call.args[0] for call in emitter.job_wrapper.call_args_list]
        assert "Update tickers data in DB" in wrapper_names
        assert "Update politicians" in wrapper_names
        assert "Update daily price history (daily)" in wrapper_names
        assert "Update 5m price history (daily)" in wrapper_names
        assert "Enrich tickers" in wrapper_names
        assert "Import delisted tickers" in wrapper_names
        assert "Load delisted price history" in wrapper_names


    def test_misfire_grace_times(self):
        """Data-update jobs use misfire_grace_time=600; token-expiry job uses 60."""
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        emitter = _make_emitter()
        register_jobs(mock_sched, sd, emitter)
        for call in mock_sched.add_job.call_args_list:
            name = call.kwargs.get("name")
            grace = call.kwargs.get("misfire_grace_time")
            if name == "Check Schwab token expiry":
                assert grace == 60, f"Token expiry job should use misfire_grace_time=60, got {grace}"
            else:
                assert grace == 600, f"Job '{name}' should use misfire_grace_time=600, got {grace}"

    def test_scheduler_function_removed(self):
        """The scheduler() thread-entry function must not exist (it was removed)."""
        import rocketstocks.core.scheduler.jobs as jobs_module
        assert not hasattr(jobs_module, "scheduler"), (
            "scheduler() thread-entry function should have been removed"
        )

    def test_wrapped_functions_sync_async_classification(self):
        """Verify which job functions are sync and which are genuinely async."""
        import asyncio
        from rocketstocks.data.tickers import TickerRepository
        from rocketstocks.data.earnings import Earnings
        from rocketstocks.data.clients.capitol_trades import CapitolTrades
        from rocketstocks.data.price_history import PriceHistoryRepository

        # Genuinely async (use await on Schwab httpx client)
        assert asyncio.iscoroutinefunction(PriceHistoryRepository.update_daily_price_history)
        assert asyncio.iscoroutinefunction(PriceHistoryRepository.update_5m_price_history)

        # Sync (blocking I/O — run in thread pool via asyncio.to_thread)
        assert not asyncio.iscoroutinefunction(TickerRepository.update_tickers)
        assert not asyncio.iscoroutinefunction(TickerRepository.insert_tickers)
        assert not asyncio.iscoroutinefunction(TickerRepository.enrich_unenriched_batch)
        assert not asyncio.iscoroutinefunction(TickerRepository.import_delisted_tickers)
        assert not asyncio.iscoroutinefunction(Earnings.update_upcoming_earnings)
        assert not asyncio.iscoroutinefunction(Earnings.update_historical_earnings)
        assert not asyncio.iscoroutinefunction(Earnings.remove_past_earnings)
        assert not asyncio.iscoroutinefunction(CapitolTrades.update_politicians)
        assert not asyncio.iscoroutinefunction(PriceHistoryRepository.load_delisted_price_history_batch)
        assert not asyncio.iscoroutinefunction(PriceHistoryRepository.load_delisted_price_history)


class TestCheckSchwabTokenExpiry:
    @pytest.mark.asyncio
    async def test_emits_failure_when_token_is_none(self):
        """Test that FAILURE notification is emitted when token is None."""
        from rocketstocks.core.scheduler.jobs import register_jobs
        from rocketstocks.core.notifications.config import NotificationLevel

        mock_sched = MagicMock()
        sd = _make_stock_data()
        sd.schwab.get_token_expiry.return_value = None
        emitter = _make_emitter()

        register_jobs(mock_sched, sd, emitter)

        # Get the check_schwab_token_expiry job
        check_job = None
        for call in mock_sched.add_job.call_args_list:
            if call.kwargs.get("name") == "Check Schwab token expiry":
                check_job = call[0][0]
                break

        assert check_job is not None
        await check_job()

        # Verify emit was called with FAILURE level
        emitter.emit.assert_called_once()
        event = emitter.emit.call_args[0][0]
        assert event.level == NotificationLevel.FAILURE
        assert "not initialized" in event.message

    @pytest.mark.asyncio
    async def test_emits_failure_when_token_is_expired(self):
        """Test that FAILURE notification is emitted when token is expired."""
        from rocketstocks.core.scheduler.jobs import register_jobs
        from rocketstocks.core.notifications.config import NotificationLevel

        mock_sched = MagicMock()
        sd = _make_stock_data()
        # Token expired 1 hour ago
        sd.schwab.get_token_expiry.return_value = datetime.datetime.now() - datetime.timedelta(hours=1)
        emitter = _make_emitter()

        register_jobs(mock_sched, sd, emitter)

        check_job = None
        for call in mock_sched.add_job.call_args_list:
            if call.kwargs.get("name") == "Check Schwab token expiry":
                check_job = call[0][0]
                break

        assert check_job is not None
        await check_job()

        emitter.emit.assert_called_once()
        event = emitter.emit.call_args[0][0]
        assert event.level == NotificationLevel.FAILURE
        assert "expired" in event.message

    @pytest.mark.asyncio
    async def test_emits_warning_when_token_expires_within_one_day(self):
        """Test that WARNING notification is emitted when token expires within 1 day."""
        from rocketstocks.core.scheduler.jobs import register_jobs
        from rocketstocks.core.notifications.config import NotificationLevel

        mock_sched = MagicMock()
        sd = _make_stock_data()
        # Token expires in 6 hours
        sd.schwab.get_token_expiry.return_value = datetime.datetime.now() + datetime.timedelta(hours=6)
        emitter = _make_emitter()

        register_jobs(mock_sched, sd, emitter)

        check_job = None
        for call in mock_sched.add_job.call_args_list:
            if call.kwargs.get("name") == "Check Schwab token expiry":
                check_job = call[0][0]
                break

        assert check_job is not None
        await check_job()

        emitter.emit.assert_called_once()
        event = emitter.emit.call_args[0][0]
        assert event.level == NotificationLevel.WARNING
        assert "expire" in event.message.lower()
        assert "hours" in event.message

    @pytest.mark.asyncio
    async def test_does_not_emit_when_token_expires_after_one_day(self):
        """Test that no notification is emitted when token expires after 1 day."""
        from rocketstocks.core.scheduler.jobs import register_jobs

        mock_sched = MagicMock()
        sd = _make_stock_data()
        # Token expires in 2 days
        sd.schwab.get_token_expiry.return_value = datetime.datetime.now() + datetime.timedelta(days=2)
        emitter = _make_emitter()

        register_jobs(mock_sched, sd, emitter)

        check_job = None
        for call in mock_sched.add_job.call_args_list:
            if call.kwargs.get("name") == "Check Schwab token expiry":
                check_job = call[0][0]
                break

        assert check_job is not None
        await check_job()

        # No emit should be called
        emitter.emit.assert_not_called()

    @pytest.mark.asyncio
    async def test_emits_failure_on_exception(self):
        """Test that FAILURE notification is emitted on any exception."""
        from rocketstocks.core.scheduler.jobs import register_jobs
        from rocketstocks.core.notifications.config import NotificationLevel

        mock_sched = MagicMock()
        sd = _make_stock_data()
        sd.schwab.get_token_expiry.side_effect = Exception("Test error")
        emitter = _make_emitter()

        register_jobs(mock_sched, sd, emitter)

        check_job = None
        for call in mock_sched.add_job.call_args_list:
            if call.kwargs.get("name") == "Check Schwab token expiry":
                check_job = call[0][0]
                break

        assert check_job is not None
        await check_job()

        emitter.emit.assert_called_once()
        event = emitter.emit.call_args[0][0]
        assert event.level == NotificationLevel.FAILURE
        assert "Error checking token expiry" in event.message


class TestJobWrapperIntegration:
    """Integration tests using a real EventEmitter + real job_wrapper."""

    @pytest.mark.asyncio
    async def test_real_wrapper_with_sync_job(self):
        """Real EventEmitter wrapping a sync function emits SUCCESS."""
        from rocketstocks.core.notifications.emitter import EventEmitter
        from rocketstocks.core.notifications.config import NotificationLevel

        emitter = EventEmitter()

        def my_sync_job():
            return "done"

        wrapped = emitter.job_wrapper("sync_test", my_sync_job)
        result = await wrapped()
        assert result == "done"
        events = emitter.drain()
        assert len(events) == 1
        assert events[0].level == NotificationLevel.SUCCESS

    @pytest.mark.asyncio
    async def test_real_wrapper_with_async_job(self):
        """Real EventEmitter wrapping an async function emits SUCCESS."""
        from rocketstocks.core.notifications.emitter import EventEmitter
        from rocketstocks.core.notifications.config import NotificationLevel

        emitter = EventEmitter()

        async def my_async_job():
            return "async done"

        wrapped = emitter.job_wrapper("async_test", my_async_job)
        result = await wrapped()
        assert result == "async done"
        events = emitter.drain()
        assert len(events) == 1
        assert events[0].level == NotificationLevel.SUCCESS

    def test_all_wrapped_jobs_are_coroutine_functions(self):
        """job_wrapper always returns a coroutine function regardless of sync/async input."""
        import asyncio
        from rocketstocks.core.notifications.emitter import EventEmitter

        emitter = EventEmitter()

        def sync_fn():
            pass

        async def async_fn():
            pass

        assert asyncio.iscoroutinefunction(emitter.job_wrapper("s", sync_fn))
        assert asyncio.iscoroutinefunction(emitter.job_wrapper("a", async_fn))
