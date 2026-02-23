"""Tests for rocketstocks.core.scheduler.jobs."""
from unittest.mock import MagicMock, patch

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
    def test_adds_eight_jobs(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        emitter = _make_emitter()
        register_jobs(mock_sched, sd, emitter)
        assert mock_sched.add_job.call_count == 8

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
        """Verify that emitter.job_wrapper is called once per job (8 total)."""
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        emitter = _make_emitter()
        register_jobs(mock_sched, sd, emitter)
        assert emitter.job_wrapper.call_count == 8

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
