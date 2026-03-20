"""Tests for YFinanceClient."""
import datetime
import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch


class TestGetFinancials:
    def _make_client(self):
        from rocketstocks.data.clients.yfinance_client import YFinanceClient
        return YFinanceClient()

    def test_get_financials_returns_correct_keys(self):
        """Keys should be correct and no duplicates."""
        with patch('yfinance.Ticker') as mock_ticker_cls:
            mock_ticker = MagicMock()
            mock_ticker_cls.return_value = mock_ticker

            client = self._make_client()
            result = client.get_financials('AAPL')

            assert 'income_statement' in result
            assert 'income_statment' not in result
            assert 'quarterly_balance_sheet' in result
            assert result['quarterly_balance_sheet'] is mock_ticker.quarterly_balance_sheet
            assert 'quarterly_income_statement' in result
            assert result['quarterly_income_statement'] is mock_ticker.quarterly_income_stmt

    def test_get_financials_no_overwrite(self):
        """quarterly_income_statement must not be overwritten by quarterly_balance_sheet."""
        with patch('yfinance.Ticker') as mock_ticker_cls:
            mock_income = MagicMock(name='quarterly_income')
            mock_balance = MagicMock(name='quarterly_balance')
            mock_ticker = MagicMock()
            mock_ticker.quarterly_income_stmt = mock_income
            mock_ticker.quarterly_balance_sheet = mock_balance
            mock_ticker_cls.return_value = mock_ticker

            client = self._make_client()
            result = client.get_financials('AAPL')

            assert result['quarterly_income_statement'] is not result['quarterly_balance_sheet']

    def test_get_financials_calls_yfinance_ticker(self):
        """Should call yf.Ticker with the given ticker symbol."""
        with patch('yfinance.Ticker') as mock_ticker_cls:
            mock_ticker_cls.return_value = MagicMock()
            client = self._make_client()
            client.get_financials('MSFT')
            mock_ticker_cls.assert_called_once_with('MSFT')

    def test_get_financials_maps_all_statements(self):
        """All 6 financial statement keys should be present."""
        with patch('yfinance.Ticker') as mock_ticker_cls:
            mock_ticker_cls.return_value = MagicMock()
            client = self._make_client()
            result = client.get_financials('AAPL')
            expected_keys = {
                'income_statement', 'quarterly_income_statement',
                'balance_sheet', 'quarterly_balance_sheet',
                'cash_flow', 'quarterly_cash_flow',
            }
            assert set(result.keys()) == expected_keys


class TestGetEarningsResult:
    def _make_client(self):
        from rocketstocks.data.clients.yfinance_client import YFinanceClient
        return YFinanceClient()

    def _make_earnings_df(self, today, eps_actual=1.52, eps_estimate=1.45, surprise_pct=4.83):
        """Build a minimal earnings_dates-style DataFrame with today's result."""
        index = pd.DatetimeIndex([pd.Timestamp(today)])
        df = pd.DataFrame(
            {
                'Reported EPS': [eps_actual],
                'EPS Estimate': [eps_estimate],
                'Surprise(%)': [surprise_pct],
            },
            index=index,
        )
        return df

    def test_returns_result_when_available(self):
        """Returns dict with eps_actual/estimate/surprise when today's row has Reported EPS."""
        today = datetime.date.today()
        df = self._make_earnings_df(today, eps_actual=1.52, eps_estimate=1.45, surprise_pct=4.83)

        with patch('yfinance.Ticker') as mock_ticker_cls:
            mock_ticker = MagicMock()
            mock_ticker.earnings_dates = df
            mock_ticker_cls.return_value = mock_ticker

            client = self._make_client()
            result = client.get_earnings_result('AAPL')

        assert result is not None
        assert result['eps_actual'] == pytest.approx(1.52)
        assert result['eps_estimate'] == pytest.approx(1.45)
        assert result['surprise_pct'] == pytest.approx(4.83)

    def test_returns_none_when_eps_not_yet_reported(self):
        """Returns None when Reported EPS is NaN (not yet released)."""
        today = datetime.date.today()
        index = pd.DatetimeIndex([pd.Timestamp(today)])
        df = pd.DataFrame(
            {'Reported EPS': [float('nan')], 'EPS Estimate': [1.45], 'Surprise(%)': [float('nan')]},
            index=index,
        )
        with patch('yfinance.Ticker') as mock_ticker_cls:
            mock_ticker = MagicMock()
            mock_ticker.earnings_dates = df
            mock_ticker_cls.return_value = mock_ticker

            client = self._make_client()
            result = client.get_earnings_result('AAPL')

        assert result is None

    def test_returns_none_when_no_row_for_today(self):
        """Returns None when there is no earnings row for today."""
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        df = self._make_earnings_df(yesterday)

        with patch('yfinance.Ticker') as mock_ticker_cls:
            mock_ticker = MagicMock()
            mock_ticker.earnings_dates = df
            mock_ticker_cls.return_value = mock_ticker

            client = self._make_client()
            result = client.get_earnings_result('AAPL')

        assert result is None

    def test_returns_none_when_earnings_dates_empty(self):
        """Returns None when earnings_dates DataFrame is empty."""
        with patch('yfinance.Ticker') as mock_ticker_cls:
            mock_ticker = MagicMock()
            mock_ticker.earnings_dates = pd.DataFrame()
            mock_ticker_cls.return_value = mock_ticker

            client = self._make_client()
            result = client.get_earnings_result('AAPL')

        assert result is None

    def test_returns_none_on_yfinance_exception(self):
        """Returns None (with warning logged) when yfinance raises an exception."""
        with patch('yfinance.Ticker') as mock_ticker_cls:
            mock_ticker_cls.side_effect = RuntimeError("network error")

            client = self._make_client()
            result = client.get_earnings_result('AAPL')

        assert result is None

    def test_handles_none_earnings_dates(self):
        """Returns None when earnings_dates property is None."""
        with patch('yfinance.Ticker') as mock_ticker_cls:
            mock_ticker = MagicMock()
            mock_ticker.earnings_dates = None
            mock_ticker_cls.return_value = mock_ticker

            client = self._make_client()
            result = client.get_earnings_result('AAPL')

        assert result is None
