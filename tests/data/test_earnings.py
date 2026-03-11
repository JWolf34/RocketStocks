"""Tests for rocketstocks.data.earnings.Earnings."""
import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest


def _make_earnings(db=None, nasdaq=None):
    with patch("rocketstocks.data.earnings.market_utils"):
        from rocketstocks.data.earnings import Earnings
        mock_db = db or MagicMock()
        if not hasattr(mock_db, 'execute') or not isinstance(mock_db.execute, AsyncMock):
            mock_db.execute = AsyncMock(return_value=None)
        mock_nasdaq = nasdaq or MagicMock()
        return Earnings(nasdaq=mock_nasdaq, db=mock_db), mock_db, mock_nasdaq


class TestFetchUpcomingEarnings:
    async def test_returns_dataframe_with_rows(self):
        earnings, db, _ = _make_earnings()
        db.execute.return_value = [("2024-02-15", "AAPL", None, None, None, None, None, None)]
        result = await earnings.fetch_upcoming_earnings()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    async def test_returns_empty_df_when_no_data(self):
        earnings, db, _ = _make_earnings()
        db.execute.return_value = []
        result = await earnings.fetch_upcoming_earnings()
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    async def test_returns_empty_df_when_none(self):
        earnings, db, _ = _make_earnings()
        db.execute.return_value = None
        result = await earnings.fetch_upcoming_earnings()
        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestGetNextEarningsDate:
    async def test_returns_date_when_found(self):
        earnings, db, _ = _make_earnings()
        db.execute.return_value = ("2024-03-01",)
        result = await earnings.get_next_earnings_date("AAPL")
        assert result == "2024-03-01"

    async def test_returns_none_when_not_found(self):
        earnings, db, _ = _make_earnings()
        db.execute.return_value = None
        assert await earnings.get_next_earnings_date("FAKE") is None


class TestGetNextEarningsInfo:
    async def test_returns_dict_when_found(self):
        earnings, db, _ = _make_earnings()
        db.execute.return_value = ("2024-03-01", "AAPL", "bmo", "Q4", "1.50", "5", "1.20", "2023-03-01")
        result = await earnings.get_next_earnings_info("AAPL")
        assert isinstance(result, dict)
        assert result['ticker'] == "AAPL"

    async def test_returns_none_when_not_found(self):
        earnings, db, _ = _make_earnings()
        db.execute.return_value = None
        result = await earnings.get_next_earnings_info("FAKE")
        assert result is None


class TestRemovePastEarnings:
    async def test_calls_execute_with_delete(self):
        earnings, db, _ = _make_earnings()
        await earnings.remove_past_earnings()
        db.execute.assert_called_once()
        sql = db.execute.call_args[0][0]
        assert 'DELETE FROM upcoming_earnings' in sql


class TestGetHistoricalEarnings:
    async def test_returns_dataframe_with_data(self):
        earnings, db, _ = _make_earnings()
        db.execute.return_value = [
            (datetime.date(2024, 1, 15), "AAPL", 2.18, 0.05, 2.10, "Q1 2024"),
        ]
        result = await earnings.get_historical_earnings("AAPL")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    async def test_returns_empty_df_when_no_data(self):
        earnings, db, _ = _make_earnings()
        db.execute.return_value = []
        result = await earnings.get_historical_earnings("FAKE")
        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestUpdateHistoricalEarnings:
    def _make_nasdaq_rows(self, **overrides):
        """Return a DataFrame simulating NASDAQ API response."""
        row = {
            'symbol': 'AAPL',
            'eps': '$1.50',
            'surprise': '5',
            'epsForecast': '$1.40',
            'fiscalQuarterEnding': 'Q1 2024',
        }
        row.update(overrides)
        return pd.DataFrame([row])

    async def test_normal_case_inserts_rows(self):
        earnings, db, nasdaq = _make_earnings()
        db.execute.return_value = None  # no prior date → use default
        nasdaq.get_earnings_by_date.return_value = self._make_nasdaq_rows()
        db.execute_batch = AsyncMock()
        with patch("rocketstocks.data.earnings.market_utils") as mu:
            mu.return_value.market_open_on_date.return_value = True
            with patch("rocketstocks.data.earnings.datetime") as mock_dt:
                mock_dt.date.today.return_value = datetime.date(2008, 1, 5)
                mock_dt.date.side_effect = lambda **kw: datetime.date(**kw)
                mock_dt.timedelta = datetime.timedelta
                earnings.mutils = mu.return_value
                await earnings.update_historical_earnings()
        db.execute_batch.assert_called()

    async def test_missing_eps_and_surprise_columns(self):
        """API response missing eps/surprise should fill with None, not raise KeyError."""
        earnings, db, nasdaq = _make_earnings()
        db.execute.return_value = None
        nasdaq.get_earnings_by_date.return_value = pd.DataFrame([{
            'symbol': 'AAPL',
            'epsForecast': '$1.40',
            'fiscalQuarterEnding': 'Q1 2024',
            # 'eps' and 'surprise' intentionally absent
        }])
        db.execute_batch = AsyncMock()
        with patch("rocketstocks.data.earnings.market_utils") as mu:
            mu.return_value.market_open_on_date.return_value = True
            with patch("rocketstocks.data.earnings.datetime") as mock_dt:
                mock_dt.date.today.return_value = datetime.date(2008, 1, 5)
                mock_dt.date.side_effect = lambda **kw: datetime.date(**kw)
                mock_dt.timedelta = datetime.timedelta
                earnings.mutils = mu.return_value
                await earnings.update_historical_earnings()  # must not raise
        db.execute_batch.assert_called()

    async def test_empty_api_response_skips_insert(self):
        earnings, db, nasdaq = _make_earnings()
        db.execute.return_value = None
        nasdaq.get_earnings_by_date.return_value = pd.DataFrame()
        db.execute_batch = AsyncMock()
        with patch("rocketstocks.data.earnings.market_utils") as mu:
            mu.return_value.market_open_on_date.return_value = True
            with patch("rocketstocks.data.earnings.datetime") as mock_dt:
                mock_dt.date.today.return_value = datetime.date(2008, 1, 5)
                mock_dt.date.side_effect = lambda **kw: datetime.date(**kw)
                mock_dt.timedelta = datetime.timedelta
                earnings.mutils = mu.return_value
                await earnings.update_historical_earnings()
        db.execute_batch.assert_not_called()


class TestGetEarningsOnDate:
    async def test_returns_dataframe_for_date(self):
        earnings, db, _ = _make_earnings()
        db.execute.return_value = [
            ("2024-03-01", "AAPL", "bmo", "Q4", "1.50", "5", "1.20", "2023-03-01"),
        ]
        result = await earnings.get_earnings_on_date(datetime.date(2024, 3, 1))
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    async def test_returns_empty_df_when_no_earnings(self):
        earnings, db, _ = _make_earnings()
        db.execute.return_value = []
        result = await earnings.get_earnings_on_date(datetime.date(2024, 3, 1))
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    async def test_calls_execute_with_date_param(self):
        earnings, db, _ = _make_earnings()
        db.execute.return_value = []
        target = datetime.date(2024, 3, 1)
        await earnings.get_earnings_on_date(target)
        _, params = db.execute.call_args[0]
        assert target in params
