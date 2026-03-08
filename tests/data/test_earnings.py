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


class TestUpdateHistoricalEarnings:
    def _make_nasdaq_df(self, eps='$2.30', eps_forecast='$2.10', surprise='0.20'):
        """Return a DataFrame shaped like the NASDAQ API response."""
        return pd.DataFrame({
            'symbol': ['AAPL'],
            'eps': [eps],
            'surprise': [surprise],
            'epsForecast': [eps_forecast],
            'fiscalQuarterEnding': ['Dec 2024'],
        })

    def test_column_names_are_lowercase(self):
        """db.insert must be called with lowercase 'epsforecast' and 'fiscalquarterending'."""
        earnings, db, nasdaq = _make_earnings()
        # Return a date 2 days ago so the loop runs once
        start_date = datetime.date.today() - datetime.timedelta(days=2)
        db.select.return_value = (start_date,)
        earnings.mutils.market_open_on_date.return_value = True
        nasdaq.get_earnings_by_date.return_value = self._make_nasdaq_df()

        earnings.update_historical_earnings()

        assert db.insert.called
        call_kwargs = db.insert.call_args[1]
        fields = call_kwargs['fields']
        assert 'epsforecast' in fields, f"Expected 'epsforecast' in fields, got: {fields}"
        assert 'fiscalquarterending' in fields, f"Expected 'fiscalquarterending' in fields, got: {fields}"
        assert 'epsForecast' not in fields
        assert 'fiscalQuarterEnding' not in fields

    def test_eps_parsing_handles_parentheses_and_dollar_signs(self):
        """EPS values like '($1.50)' → -1.5, '$2.30' → 2.3, 'N/A' → None."""
        earnings, db, nasdaq = _make_earnings()
        start_date = datetime.date.today() - datetime.timedelta(days=2)
        db.select.return_value = (start_date,)
        earnings.mutils.market_open_on_date.return_value = True

        nasdaq.get_earnings_by_date.return_value = pd.DataFrame({
            'symbol': ['AAPL', 'MSFT', 'GOOG'],
            'eps': ['($1.50)', '$2.30', 'N/A'],
            'surprise': ['N/A', 'N/A', 'N/A'],
            'epsForecast': ['$2.10', '$2.00', 'N/A'],
            'fiscalQuarterEnding': ['Dec 2024', 'Dec 2024', 'Dec 2024'],
        })

        earnings.update_historical_earnings()

        assert db.insert.called
        values = db.insert.call_args[1]['values']
        fields = db.insert.call_args[1]['fields']
        eps_idx = fields.index('eps')
        assert values[0][eps_idx] == -1.5
        assert values[1][eps_idx] == 2.3
        assert pd.isna(values[2][eps_idx])  # None → NaN when stored in a float DataFrame column

    def test_skips_empty_earnings(self):
        """When NASDAQ returns empty DataFrame, db.insert must not be called."""
        earnings, db, nasdaq = _make_earnings()
        start_date = datetime.date.today() - datetime.timedelta(days=2)
        db.select.return_value = (start_date,)
        earnings.mutils.market_open_on_date.return_value = True
        nasdaq.get_earnings_by_date.return_value = pd.DataFrame()

        earnings.update_historical_earnings()

        db.insert.assert_not_called()


class TestUpdateUpcomingEarnings:
    def test_column_names_match_schema(self):
        """db.insert must be called with snake_case 'eps_forecast' and 'fiscal_quarter_ending'."""
        earnings, db, nasdaq = _make_earnings()
        nasdaq.get_earnings_by_date.return_value = pd.DataFrame({
            'symbol': ['AAPL'],
            'date': ['2024-03-15'],
            'time': ['BMO'],
            'fiscalQuarterEnding': ['Q1 2024'],
            'epsForecast': ['2.10'],
            'noOfEsts': ['30'],
            'lastYearEPS': ['1.80'],
            'lastYearRptDt': ['2023-03-15'],
        })

        earnings.update_upcoming_earnings()

        assert db.insert.called
        call_kwargs = db.insert.call_args[1]
        fields = call_kwargs['fields']
        assert 'eps_forecast' in fields, f"Expected 'eps_forecast' in fields, got: {fields}"
        assert 'fiscal_quarter_ending' in fields, f"Expected 'fiscal_quarter_ending' in fields, got: {fields}"

    def test_skips_weekends(self):
        """get_earnings_by_date must never be called for a Saturday or Sunday."""
        earnings, db, nasdaq = _make_earnings()
        nasdaq.get_earnings_by_date.return_value = pd.DataFrame()

        earnings.update_upcoming_earnings()

        for call in nasdaq.get_earnings_by_date.call_args_list:
            date_str = call[0][0]
            date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            assert date.weekday() < 5, f"API called for weekend date: {date_str}"
