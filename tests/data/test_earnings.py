"""Tests for rocketstocks.data.earnings.Earnings."""
import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _make_earnings(db=None, nasdaq=None):
    with patch("rocketstocks.data.earnings.market_utils"):
        from rocketstocks.data.earnings import Earnings
        mock_db = db or MagicMock()
        mock_nasdaq = nasdaq or MagicMock()
        mock_db.get_table_columns.return_value = [
            "date", "ticker", "time", "fiscal_quarter_ending",
            "eps_forecast", "no_of_ests", "last_year_eps", "last_year_rpt_dt",
        ]
        return Earnings(nasdaq=mock_nasdaq, db=mock_db), mock_db, mock_nasdaq


class TestFetchUpcomingEarnings:
    def test_returns_dataframe_with_rows(self):
        earnings, db, _ = _make_earnings()
        db.get_table_columns.return_value = ["date", "ticker"]
        db.select.return_value = [("2024-02-15", "AAPL")]
        result = earnings.fetch_upcoming_earnings()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_returns_empty_df_when_no_data(self):
        earnings, db, _ = _make_earnings()
        db.select.return_value = []
        result = earnings.fetch_upcoming_earnings()
        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestGetNextEarningsDate:
    def test_returns_date_when_found(self):
        earnings, db, _ = _make_earnings()
        db.select.return_value = ("2024-03-01",)
        result = earnings.get_next_earnings_date("AAPL")
        assert result == "2024-03-01"

    def test_returns_none_when_not_found(self):
        earnings, db, _ = _make_earnings()
        db.select.return_value = None
        assert earnings.get_next_earnings_date("FAKE") is None


class TestGetNextEarningsInfo:
    def test_returns_dict_when_found(self):
        earnings, db, _ = _make_earnings()
        cols = ["date", "ticker", "time", "fiscal_quarter_ending",
                "eps_forecast", "no_of_ests", "last_year_eps", "last_year_rpt_dt"]
        db.get_table_columns.return_value = cols
        db.select.return_value = tuple(["2024-03-01", "AAPL", "BMO", "Q1", "2.10", "30", "1.80", "2023-03-01"])
        result = earnings.get_next_earnings_info("AAPL")
        assert isinstance(result, dict)
        assert result["ticker"] == "AAPL"

    def test_returns_none_when_not_found(self):
        earnings, db, _ = _make_earnings()
        db.select.return_value = None
        assert earnings.get_next_earnings_info("FAKE") is None


class TestGetEarningsOnDate:
    def test_returns_dataframe_when_empty(self):
        earnings, db, _ = _make_earnings()
        db.select.return_value = []
        result = earnings.get_earnings_on_date(datetime.date(2024, 2, 15))
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_returns_dataframe_with_data(self):
        earnings, db, _ = _make_earnings()
        db.select.return_value = [
            ("2024-02-15", "AAPL", "BMO", "Q1", "2.10", "30", "1.80", "2023-02-15")
        ]
        result = earnings.get_earnings_on_date(datetime.date(2024, 2, 15))
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1


class TestGetHistoricalEarnings:
    def test_returns_dataframe_with_data(self):
        earnings, db, _ = _make_earnings()
        cols = ["date", "ticker", "eps", "surprise", "epsforecast", "fiscalquarterending"]
        db.get_table_columns.return_value = cols
        db.select.return_value = [("2024-01-30", "AAPL", 2.18, 3.5, 2.10, "Dec 2023")]
        result = earnings.get_historical_earnings("AAPL")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_returns_empty_df_when_no_data(self):
        earnings, db, _ = _make_earnings()
        db.select.return_value = []
        result = earnings.get_historical_earnings("FAKE")
        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestRemovePastEarnings:
    def test_calls_db_delete(self):
        earnings, db, _ = _make_earnings()
        earnings.remove_past_earnings()
        db.delete.assert_called_once()
        call_kwargs = db.delete.call_args[1]
        assert call_kwargs["table"] == "upcoming_earnings"
