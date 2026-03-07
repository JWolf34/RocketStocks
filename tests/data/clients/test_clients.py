"""Tests for client bug fixes."""
import asyncio
import datetime
import pytest
import pandas as pd
from unittest.mock import MagicMock, AsyncMock, patch


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Schwab
# ---------------------------------------------------------------------------

class TestSchwab:
    def _make(self):
        """Build a Schwab instance with a fully mocked underlying client."""
        mock_inner_client = AsyncMock()
        with patch('rocketstocks.data.clients.schwab.schwab') as mock_schwab_pkg, \
             patch('rocketstocks.data.clients.schwab.secrets'):
            mock_schwab_pkg.auth.easy_client.return_value = mock_inner_client
            from rocketstocks.data.clients.schwab import Schwab
            obj = Schwab()
        # Replace the client attr so we can configure returns
        obj.client = mock_inner_client
        return obj

    def test_no_import_time_datetime_default(self):
        """B3: end_datetime default must NOT be evaluated at import/class time."""
        import inspect
        from rocketstocks.data.clients.schwab import Schwab
        sig = inspect.signature(Schwab.get_daily_price_history)
        default = sig.parameters['end_datetime'].default
        assert default is None

    def test_get_daily_price_history_raises_for_status(self):
        """B7: should call raise_for_status(), not assert."""
        schwab_obj = self._make()
        mock_resp = MagicMock()
        # raise_for_status raises an httpx error → method catches it and returns empty df
        import httpx
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "401", request=MagicMock(), response=MagicMock()
        )
        schwab_obj.client.get_price_history_every_day = AsyncMock(return_value=mock_resp)

        result = _run(schwab_obj.get_daily_price_history('AAPL'))
        mock_resp.raise_for_status.assert_called_once()
        # Should return empty DataFrame on error
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def _make_with_token_path(self, token_path):
        """Build a Schwab instance with a custom token path."""
        mock_inner_client = AsyncMock()
        with patch('rocketstocks.data.clients.schwab.schwab') as mock_schwab_pkg, \
             patch('rocketstocks.data.clients.schwab.secrets'):
            mock_schwab_pkg.auth.easy_client.return_value = mock_inner_client
            from rocketstocks.data.clients.schwab import Schwab
            obj = Schwab(token_path=token_path)
        obj.client = mock_inner_client
        return obj

    def test_get_token_expiry_returns_none_when_file_missing(self, tmp_path):
        obj = self._make_with_token_path(str(tmp_path / "nonexistent.json"))
        assert obj.get_token_expiry() is None

    def test_get_token_expiry_returns_correct_datetime(self, tmp_path):
        import json
        token_file = tmp_path / "schwab-token.json"
        expires_at = 1800000000  # arbitrary future Unix timestamp
        token_file.write_text(json.dumps({"token": {"expires_at": expires_at}}))
        obj = self._make_with_token_path(str(token_file))
        result = obj.get_token_expiry()
        assert result == datetime.datetime.fromtimestamp(expires_at)

    def test_get_token_expiry_returns_none_on_malformed_json(self, tmp_path):
        token_file = tmp_path / "schwab-token.json"
        token_file.write_text("not valid json{{{")
        obj = self._make_with_token_path(str(token_file))
        assert obj.get_token_expiry() is None

    def test_get_token_expiry_returns_none_when_expires_at_missing(self, tmp_path):
        import json
        token_file = tmp_path / "schwab-token.json"
        token_file.write_text(json.dumps({"token": {"expires_in": 1800}}))
        obj = self._make_with_token_path(str(token_file))
        assert obj.get_token_expiry() is None

    def test_get_daily_price_history_resolves_end_datetime_in_method(self):
        """B3: end_datetime should be resolved to now() inside the method."""
        schwab_obj = self._make()
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {'candles': []}
        schwab_obj.client.get_price_history_every_day = AsyncMock(return_value=mock_resp)

        before = datetime.datetime.now(datetime.timezone.utc)
        _run(schwab_obj.get_daily_price_history('AAPL'))
        after = datetime.datetime.now(datetime.timezone.utc)

        _, kwargs = schwab_obj.client.get_price_history_every_day.call_args
        end_dt = kwargs['end_datetime']
        assert before <= end_dt <= after, "end_datetime should be resolved at call time"


# ---------------------------------------------------------------------------
# CapitolTrades
# ---------------------------------------------------------------------------

class TestCapitolTrades:
    def _make(self, db=None):
        from rocketstocks.data.clients.capitol_trades import CapitolTrades
        return CapitolTrades(db=db or MagicMock())

    def test_politician_returns_none_on_empty_db_result(self):
        """B9: should not crash when DB returns None."""
        db = MagicMock()
        db.get_table_columns.return_value = ['politician_id', 'name', 'party', 'state']
        db.select.return_value = None  # DB returned no row
        ct = self._make(db=db)
        result = ct.politician(politician_id='abc1234')
        assert result is None

    def test_politician_returns_dict_when_found(self):
        db = MagicMock()
        db.get_table_columns.return_value = ['politician_id', 'name', 'party', 'state']
        db.select.return_value = ('abc1234', 'John Doe', 'D', 'CA')
        ct = self._make(db=db)
        result = ct.politician(politician_id='abc1234')
        assert result == {'politician_id': 'abc1234', 'name': 'John Doe', 'party': 'D', 'state': 'CA'}

    def test_politician_returns_none_with_no_args(self):
        ct = self._make()
        result = ct.politician()
        assert result is None

    def test_trades_handles_missing_table(self):
        """B8: should return empty DataFrame if tbody not found instead of AttributeError."""
        from rocketstocks.data.clients.capitol_trades import CapitolTrades
        with patch('requests.get') as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.raise_for_status.return_value = None
            # Return HTML with no tbody
            mock_resp.content = b'<html><body><p>No trades</p></body></html>'
            mock_get.return_value = mock_resp

            result = CapitolTrades.trades('abc1234')
            assert isinstance(result, pd.DataFrame)

    def test_update_politicians_calls_db_insert(self):
        """update_politicians must call db.insert with politician data after parsing cards."""
        import asyncio
        db = MagicMock()
        db.get_table_columns.return_value = ['politician_id', 'name', 'party', 'state']
        ct = self._make(db=db)

        with patch('requests.get') as mock_get, \
             patch('rocketstocks.data.clients.capitol_trades.BeautifulSoup') as mock_bs_cls:
            mock_resp = MagicMock()
            mock_resp.raise_for_status.return_value = None
            mock_get.return_value = mock_resp

            # First page: one politician card
            mock_card = MagicMock()
            mock_card.__getitem__ = MagicMock(return_value='/politicians/abc1234')
            mock_card.find.return_value = MagicMock(text='TestValue')

            page1_soup = MagicMock()
            page1_soup.find_all.return_value = [mock_card]

            # Second page: no cards — triggers db.insert
            page2_soup = MagicMock()
            page2_soup.find_all.return_value = []

            mock_bs_cls.side_effect = [page1_soup, page2_soup]

            asyncio.get_event_loop().run_until_complete(ct.update_politicians())

        db.insert.assert_called_once()
        call_kwargs = db.insert.call_args[1]
        assert call_kwargs['table'] == 'ct_politicians'
        assert len(call_kwargs['values']) == 1


# ---------------------------------------------------------------------------
# Nasdaq
# ---------------------------------------------------------------------------

class TestNasdaq:
    def _make(self):
        from rocketstocks.data.clients.nasdaq import Nasdaq
        return Nasdaq()

    def test_get_all_tickers_returns_empty_df_on_none_data(self):
        """B10: should not crash when data['data'] is None."""
        nasdaq = self._make()
        with patch('requests.get') as mock_get:
            mock_resp = MagicMock()
            mock_resp.raise_for_status.return_value = None
            mock_resp.json.return_value = {'data': None}
            mock_get.return_value = mock_resp

            result = nasdaq.get_all_tickers()
            assert isinstance(result, pd.DataFrame)
            assert result.empty

    def test_get_earnings_by_date_returns_empty_df_on_none_data(self):
        nasdaq = self._make()
        with patch('requests.get') as mock_get:
            mock_resp = MagicMock()
            mock_resp.raise_for_status.return_value = None
            mock_resp.json.return_value = {'data': None}
            mock_get.return_value = mock_resp

            result = nasdaq.get_earnings_by_date('2024-01-01')
            assert isinstance(result, pd.DataFrame)
            assert result.empty


# ---------------------------------------------------------------------------
# News — typo fix
# ---------------------------------------------------------------------------

class TestNews:
    def test_general_category_is_correct(self):
        """B15: 'eeneral' should be 'general'."""
        with patch('rocketstocks.data.clients.news.NewsApiClient'), \
             patch('rocketstocks.data.clients.news.secrets'):
            from rocketstocks.data.clients.news import News
            news = News()
            assert news.categories['General'] == 'general'
            assert 'eeneral' not in news.categories.values()


# ---------------------------------------------------------------------------
# Financials — bug fixes
# ---------------------------------------------------------------------------

class TestFinancials:
    def test_fetch_financials_returns_correct_keys(self):
        """B4 + B17 fix: keys should be correct, no duplicates."""
        with patch('yfinance.Ticker') as mock_ticker_cls:
            mock_ticker = MagicMock()
            mock_ticker_cls.return_value = mock_ticker

            from rocketstocks.data.financials import fetch_financials
            result = fetch_financials('AAPL')

            # B17 fix: 'income_statement' not 'income_statment'
            assert 'income_statement' in result
            assert 'income_statment' not in result

            # B4 fix: 'quarterly_balance_sheet' should be present and map to balance_sheet
            assert 'quarterly_balance_sheet' in result
            assert result['quarterly_balance_sheet'] is mock_ticker.quarterly_balance_sheet

            # Both quarterly_income_statement and quarterly_balance_sheet should exist
            assert 'quarterly_income_statement' in result
            assert result['quarterly_income_statement'] is mock_ticker.quarterly_income_stmt

    def test_fetch_financials_no_overwrite(self):
        """B4: quarterly_income_statement must not be overwritten by quarterly_balance_sheet."""
        with patch('yfinance.Ticker') as mock_ticker_cls:
            mock_income = MagicMock(name='quarterly_income')
            mock_balance = MagicMock(name='quarterly_balance')
            mock_ticker = MagicMock()
            mock_ticker.quarterly_income_stmt = mock_income
            mock_ticker.quarterly_balance_sheet = mock_balance
            mock_ticker_cls.return_value = mock_ticker

            from rocketstocks.data.financials import fetch_financials
            result = fetch_financials('AAPL')

            # Must not be the same object (overwrite check)
            assert result['quarterly_income_statement'] is not result['quarterly_balance_sheet']


# ---------------------------------------------------------------------------
# Earnings — consistent return type (B14)
# ---------------------------------------------------------------------------

class TestEarningsReturnType:
    def _make_earnings(self):
        with patch('rocketstocks.data.earnings.market_utils'):
            from rocketstocks.data.earnings import Earnings
            mock_nasdaq = MagicMock()
            mock_db = MagicMock()
            mock_db.get_table_columns.return_value = ['date', 'ticker', 'time',
                                                       'fiscal_quarter_ending', 'eps_forecast',
                                                       'no_of_ests', 'last_year_eps',
                                                       'last_year_rpt_dt']
            return Earnings(nasdaq=mock_nasdaq, db=mock_db), mock_db

    def test_get_earnings_on_date_returns_dataframe_when_empty(self):
        """B14: should always return DataFrame, not raw list."""
        earnings, db = self._make_earnings()
        db.select.return_value = []

        result = earnings.get_earnings_on_date(datetime.date(2024, 1, 2))
        assert isinstance(result, pd.DataFrame)

    def test_get_earnings_on_date_returns_dataframe_when_has_data(self):
        earnings, db = self._make_earnings()
        db.select.return_value = [
            (datetime.date(2024, 1, 2), 'AAPL', 'AMC', '2024Q1', '2.00', '10', '1.50', '2023-01-01')
        ]
        result = earnings.get_earnings_on_date(datetime.date(2024, 1, 2))
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
