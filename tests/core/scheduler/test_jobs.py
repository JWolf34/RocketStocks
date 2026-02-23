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


class TestRegisterJobs:
    def test_adds_eight_jobs(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        register_jobs(mock_sched, sd)
        assert mock_sched.add_job.call_count == 8

    def test_update_tickers_job_registered(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        register_jobs(mock_sched, sd)
        job_names = [call.kwargs.get("name") for call in mock_sched.add_job.call_args_list]
        assert "Update tickers data in DB" in job_names

    def test_update_upcoming_earnings_job_registered(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        register_jobs(mock_sched, sd)
        job_names = [call.kwargs.get("name") for call in mock_sched.add_job.call_args_list]
        assert "Update upcoming earnings" in job_names

    def test_update_politicians_job_registered(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        register_jobs(mock_sched, sd)
        job_names = [call.kwargs.get("name") for call in mock_sched.add_job.call_args_list]
        assert "Update politicians" in job_names

    def test_all_jobs_have_replace_existing_true(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        register_jobs(mock_sched, sd)
        for call in mock_sched.add_job.call_args_list:
            assert call.kwargs.get("replace_existing") is True

    def test_all_jobs_use_utc_timezone(self):
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        register_jobs(mock_sched, sd)
        for call in mock_sched.add_job.call_args_list:
            assert call.kwargs.get("timezone") == "UTC"

    def test_job_callables_reference_stock_data(self):
        """Verify that the registered job callables are stock_data methods (not raw functions)."""
        from rocketstocks.core.scheduler.jobs import register_jobs
        mock_sched = MagicMock()
        sd = _make_stock_data()
        register_jobs(mock_sched, sd)
        # First job: update_tickers
        first_call = mock_sched.add_job.call_args_list[0]
        callable_arg = first_call.args[0]
        assert callable_arg is sd.tickers.update_tickers
