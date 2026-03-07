"""Tests for TickerRepository, PriceHistoryRepository, PopularityRepository."""
import asyncio
import datetime
import pandas as pd
from unittest.mock import MagicMock, AsyncMock


def _run(coro):
    """Run a coroutine synchronously (no pytest-asyncio needed)."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# TickerRepository
# ---------------------------------------------------------------------------

class TestTickerRepository:
    def _make(self, db=None, nasdaq=None, sec=None):
        from rocketstocks.data.tickers import TickerRepository
        return TickerRepository(
            db=db or MagicMock(),
            nasdaq=nasdaq or MagicMock(),
            sec=sec or MagicMock(),
        )

    def test_get_all_tickers_returns_list(self):
        db = MagicMock()
        db.select.return_value = [('AAPL',), ('MSFT',), ('GOOG',)]
        repo = self._make(db=db)
        result = repo.get_all_tickers()
        assert result == ['AAPL', 'MSFT', 'GOOG']

    def test_get_all_tickers_empty(self):
        db = MagicMock()
        db.select.return_value = []
        repo = self._make(db=db)
        assert repo.get_all_tickers() == []

    def test_get_ticker_info_returns_dict(self):
        db = MagicMock()
        db.get_table_columns.return_value = ['ticker', 'name', 'sector']
        db.select.return_value = ('AAPL', 'Apple Inc.', 'Technology')
        repo = self._make(db=db)
        result = repo.get_ticker_info('AAPL')
        assert result == {'ticker': 'AAPL', 'name': 'Apple Inc.', 'sector': 'Technology'}

    def test_get_ticker_info_none_result_returns_none(self):
        """B5 fix: should not crash when DB returns None."""
        db = MagicMock()
        db.get_table_columns.return_value = ['ticker', 'name']
        db.select.return_value = None
        repo = self._make(db=db)
        result = repo.get_ticker_info('FAKE')
        assert result is None

    def test_get_cik_returns_value(self):
        db = MagicMock()
        db.select.return_value = ('0000320193',)
        repo = self._make(db=db)
        assert repo.get_cik('AAPL') == '0000320193'

    def test_get_cik_returns_none_when_not_found(self):
        db = MagicMock()
        db.select.return_value = None
        repo = self._make(db=db)
        assert repo.get_cik('FAKE') is None

    def test_get_all_tickers_by_sector_uses_correct_field(self):
        """B6 fix: should select 'ticker', not 'tickers'."""
        db = MagicMock()
        db.select.return_value = [('AAPL',), ('MSFT',)]
        repo = self._make(db=db)
        result = repo.get_all_tickers_by_sector('Technology')
        call_kwargs = db.select.call_args[1]
        assert call_kwargs['fields'] == ['ticker']
        assert result == ['AAPL', 'MSFT']

    def test_get_all_tickers_by_sector_returns_none_when_empty(self):
        db = MagicMock()
        db.select.return_value = []
        repo = self._make(db=db)
        result = repo.get_all_tickers_by_sector('XYZ')
        assert result is None

    def test_get_all_ticker_info_returns_dataframe(self):
        db = MagicMock()
        db.get_table_columns.return_value = ['ticker', 'name']
        db.select.return_value = [('AAPL', 'Apple'), ('MSFT', 'Microsoft')]
        repo = self._make(db=db)
        result = repo.get_all_ticker_info()
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ['ticker', 'name']
        assert len(result) == 2

    def test_validate_ticker_true_when_found(self):
        db = MagicMock()
        db.select.return_value = ('AAPL',)
        repo = self._make(db=db)
        assert _run(repo.validate_ticker('AAPL')) is True

    def test_validate_ticker_false_when_not_found(self):
        db = MagicMock()
        db.select.return_value = None
        repo = self._make(db=db)
        assert _run(repo.validate_ticker('FAKE')) is False

    def test_parse_valid_tickers(self):
        db = MagicMock()
        # AAPL found, FAKE not
        db.select.side_effect = [('AAPL',), None]
        repo = self._make(db=db)
        valid, invalid = _run(repo.parse_valid_tickers('AAPL FAKE'))
        assert valid == ['AAPL']
        assert invalid == ['FAKE']

    def test_update_tickers_calls_db_update(self):
        """update_tickers must call db.update for each ticker that exists in DB."""
        db = MagicMock()
        nasdaq = MagicMock()
        nasdaq.get_all_tickers.return_value = pd.DataFrame({
            'symbol': ['AAPL'],
            'name': ['Apple Inc.'],
            'country': ['US'],
            'ipoyear': ['1980'],
            'industry': ['Technology Hardware'],
            'sector': ['Technology'],
            'lastsale': ['$150.00'],
            'netchange': ['1.50'],
            'pctchange': ['1.01%'],
            'volume': ['1000000'],
        })
        db.select.return_value = [('AAPL',)]  # AAPL is already in DB
        repo = self._make(db=db, nasdaq=nasdaq)
        _run(repo.update_tickers())
        db.update.assert_called_once()
        call_kwargs = db.update.call_args[1]
        assert call_kwargs['table'] == 'tickers'
        assert call_kwargs['where_conditions'] == [('ticker', 'AAPL')]

    def test_insert_tickers_merges_sec_and_nasdaq(self):
        """insert_tickers must merge SEC + NASDAQ data and zero-pad CIK to 10 digits."""
        db = MagicMock()
        nasdaq = MagicMock()
        sec = MagicMock()
        sec.get_company_tickers.return_value = pd.DataFrame({
            'ticker': ['AAPL'],
            'cik_str': [320193],
        })
        nasdaq.get_all_tickers.return_value = pd.DataFrame({
            'symbol': ['AAPL'],
            'name': ['Apple Inc.'],
            'country': ['US'],
            'ipoyear': ['1980'],
            'industry': ['Technology Hardware'],
            'sector': ['Technology'],
            'url': ['apple.com'],
        })
        repo = self._make(db=db, nasdaq=nasdaq, sec=sec)
        _run(repo.insert_tickers())
        db.insert.assert_called_once()
        call_kwargs = db.insert.call_args[1]
        assert call_kwargs['table'] == 'tickers'
        assert 'cik' in call_kwargs['fields']
        cik_idx = call_kwargs['fields'].index('cik')
        assert call_kwargs['values'][0][cik_idx] == '0000320193'


# ---------------------------------------------------------------------------
# PriceHistoryRepository
# ---------------------------------------------------------------------------

class TestPriceHistoryRepository:
    def _make(self, db=None, schwab=None):
        from rocketstocks.data.price_history import PriceHistoryRepository
        return PriceHistoryRepository(
            db=db or MagicMock(),
            schwab=schwab or AsyncMock(),
        )

    def test_fetch_daily_returns_dataframe(self):
        db = MagicMock()
        db.select.return_value = [
            ('AAPL', 100.0, 110.0, 99.0, 105.0, 1000000, datetime.date(2024, 1, 2))
        ]
        db.get_table_columns.return_value = ['ticker', 'open', 'high', 'low', 'close', 'volume', 'date']
        repo = self._make(db=db)
        result = repo.fetch_daily_price_history('AAPL')
        assert isinstance(result, pd.DataFrame)
        assert 'ticker' in result.columns
        assert len(result) == 1

    def test_fetch_daily_returns_empty_dataframe_when_no_data(self):
        db = MagicMock()
        db.select.return_value = []
        repo = self._make(db=db)
        result = repo.fetch_daily_price_history('FAKE')
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_fetch_5m_returns_dataframe(self):
        db = MagicMock()
        dt = datetime.datetime(2024, 1, 2, 10, 0)
        db.select.return_value = [('AAPL', 100.0, 110.0, 99.0, 105.0, 50000, dt)]
        db.get_table_columns.return_value = ['ticker', 'open', 'high', 'low', 'close', 'volume', 'datetime']
        repo = self._make(db=db)
        result = repo.fetch_5m_price_history('AAPL')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_fetch_5m_returns_empty_dataframe_when_no_data(self):
        db = MagicMock()
        db.select.return_value = []
        repo = self._make(db=db)
        result = repo.fetch_5m_price_history('FAKE')
        assert result.empty

    def test_update_daily_inserts_when_data_found(self):
        db = MagicMock()
        db.select.return_value = None  # No existing record
        schwab = AsyncMock()
        df = pd.DataFrame({
            'ticker': ['AAPL'], 'open': [100.0], 'high': [110.0],
            'low': [99.0], 'close': [105.0], 'volume': [1000000],
            'date': [datetime.date(2024, 1, 2)],
        })
        schwab.get_daily_price_history.return_value = df
        repo = self._make(db=db, schwab=schwab)
        _run(repo.update_daily_price_history_by_ticker('AAPL'))
        db.insert.assert_called_once()

    def test_update_daily_skips_insert_when_no_data(self):
        db = MagicMock()
        db.select.return_value = None
        schwab = AsyncMock()
        schwab.get_daily_price_history.return_value = pd.DataFrame()
        repo = self._make(db=db, schwab=schwab)
        _run(repo.update_daily_price_history_by_ticker('AAPL'))
        db.insert.assert_not_called()

    def test_update_5m_uses_existing_latest_datetime(self):
        """When a record exists, start_datetime should be that record's datetime."""
        db = MagicMock()
        existing_dt = datetime.datetime(2024, 6, 1, 9, 30)
        db.select.return_value = (existing_dt,)
        schwab = AsyncMock()
        schwab.get_5m_price_history.return_value = pd.DataFrame()
        repo = self._make(db=db, schwab=schwab)
        _run(repo.update_5m_price_history_by_ticker('AAPL'))
        schwab.get_5m_price_history.assert_called_once()
        _, kwargs = schwab.get_5m_price_history.call_args
        assert kwargs['start_datetime'] == existing_dt


# ---------------------------------------------------------------------------
# PopularityRepository
# ---------------------------------------------------------------------------

class TestPopularityRepository:
    def _make(self, db=None):
        from rocketstocks.data.popularity_store import PopularityRepository
        return PopularityRepository(db=db or MagicMock())

    def test_fetch_popularity_returns_dataframe(self):
        db = MagicMock()
        db.get_table_columns.return_value = [
            'datetime', 'rank', 'ticker', 'name', 'mentions', 'upvotes',
            'rank_24h_ago', 'mentions_24h_ago',
        ]
        db.select.return_value = [
            (datetime.datetime(2024, 1, 1), 1, 'AAPL', 'Apple', 100, 50, 2, 90),
        ]
        repo = self._make(db=db)
        result = repo.fetch_popularity('AAPL')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    def test_fetch_popularity_empty_returns_empty_df(self):
        db = MagicMock()
        db.get_table_columns.return_value = ['datetime', 'rank', 'ticker']
        db.select.return_value = []
        repo = self._make(db=db)
        result = repo.fetch_popularity()
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_fetch_popularity_with_ticker_passes_where_condition(self):
        db = MagicMock()
        db.get_table_columns.return_value = ['datetime', 'rank', 'ticker']
        db.select.return_value = []
        repo = self._make(db=db)
        repo.fetch_popularity('AAPL')
        call_kwargs = db.select.call_args[1]
        assert ('ticker', 'AAPL') in call_kwargs['where_conditions']

    def test_fetch_popularity_no_ticker_passes_no_where(self):
        db = MagicMock()
        db.get_table_columns.return_value = ['datetime', 'rank', 'ticker']
        db.select.return_value = []
        repo = self._make(db=db)
        repo.fetch_popularity()
        call_kwargs = db.select.call_args[1]
        assert call_kwargs['where_conditions'] == []

    def test_insert_popularity_calls_db_insert(self):
        db = MagicMock()
        repo = self._make(db=db)
        df = pd.DataFrame({
            'datetime': [datetime.datetime(2024, 1, 1)], 'rank': [1],
            'ticker': ['AAPL'], 'name': ['Apple'],
            'mentions': [100], 'upvotes': [50],
            'rank_24h_ago': [2], 'mentions_24h_ago': [90],
        })
        repo.insert_popularity(df)
        db.insert.assert_called_once()
        call_kwargs = db.insert.call_args[1]
        assert call_kwargs['table'] == 'popularity'
