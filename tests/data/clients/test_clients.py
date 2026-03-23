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
        from rocketstocks.data.clients.schwab import Schwab
        mock_inner_client = AsyncMock()
        token_store = AsyncMock()
        obj = Schwab(token_store=token_store)
        obj.client = mock_inner_client
        return obj

    def test_init_client_uses_client_from_access_functions(self):
        """init_client must use client_from_access_functions (not token file)."""
        mock_inner_client = AsyncMock()
        with patch('rocketstocks.data.clients.schwab.schwab') as mock_schwab_pkg, \
             patch('rocketstocks.data.clients.schwab.settings'):
            mock_schwab_pkg.auth.client_from_access_functions.return_value = mock_inner_client
            from rocketstocks.data.clients.schwab import Schwab
            token_store = AsyncMock()
            token_store.load_token.return_value = {"creation_timestamp": 1234567890, "token": {}}
            obj = Schwab(token_store=token_store)
            asyncio.get_event_loop().run_until_complete(obj.init_client())
            mock_schwab_pkg.auth.client_from_access_functions.assert_called_once()
            mock_schwab_pkg.auth.easy_client.assert_not_called()

    def test_client_is_none_when_no_token_in_db(self):
        """Schwab must set client=None gracefully when no token exists in DB."""
        from rocketstocks.data.clients.schwab import Schwab
        token_store = AsyncMock()
        token_store.load_token.return_value = None
        obj = Schwab(token_store=token_store)
        asyncio.get_event_loop().run_until_complete(obj.init_client())
        assert obj.client is None

    @pytest.mark.asyncio
    async def test_reload_client_resets_invalid_flag(self):
        """reload_client must clear _token_invalid and attempt to reload."""
        with patch('rocketstocks.data.clients.schwab.schwab') as mock_schwab_pkg, \
             patch('rocketstocks.data.clients.schwab.settings'):
            mock_schwab_pkg.auth.client_from_access_functions.return_value = AsyncMock()
            from rocketstocks.data.clients.schwab import Schwab
            token_store = AsyncMock()
            token_store.load_token.return_value = {"creation_timestamp": 1234567890, "token": {}}
            obj = Schwab(token_store=token_store)
            obj._token_invalid = True
            await obj.reload_client()
        assert obj._token_invalid is False

    def test_api_method_raises_when_client_is_none(self):
        """All API methods should raise SchwabTokenError when client is None."""
        with patch('rocketstocks.data.clients.schwab.schwab') as mock_schwab_pkg, \
             patch('rocketstocks.data.clients.schwab.settings'):
            mock_schwab_pkg.auth.client_from_token_file.side_effect = FileNotFoundError
            from rocketstocks.data.clients.schwab import Schwab, SchwabTokenError
            obj = Schwab()
        assert obj.client is None
        import asyncio
        with pytest.raises(SchwabTokenError):
            asyncio.get_event_loop().run_until_complete(obj.get_quote('AAPL'))

    def test_oauth_error_disables_client(self):
        """OAuthError during an API call should set client=None and raise SchwabTokenError."""
        from authlib.integrations.base_client.errors import OAuthError
        from rocketstocks.data.clients.schwab import SchwabTokenError
        obj = self._make()
        obj.client.get_quote = AsyncMock(side_effect=OAuthError(error='invalid_token'))
        import asyncio
        with pytest.raises(SchwabTokenError):
            asyncio.get_event_loop().run_until_complete(obj.get_quote('AAPL'))
        assert obj.client is None
        assert obj._token_invalid is True

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

    def _make_with_token_dict(self, token_dict):
        """Build a Schwab instance backed by a token_store returning token_dict."""
        from rocketstocks.data.clients.schwab import Schwab
        token_store = AsyncMock()
        token_store.load_token.return_value = token_dict
        obj = Schwab(token_store=token_store)
        return obj

    @pytest.mark.asyncio
    async def test_get_token_expiry_returns_none_when_no_token(self):
        obj = self._make_with_token_dict(None)
        assert await obj.get_token_expiry() is None

    @pytest.mark.asyncio
    async def test_get_token_expiry_returns_correct_datetime(self):
        from rocketstocks.core.auth.token_manager import REFRESH_TOKEN_LIFETIME
        creation_ts = 1800000000  # arbitrary Unix timestamp
        token_dict = {"creation_timestamp": creation_ts, "token": {}}
        obj = self._make_with_token_dict(token_dict)
        result = await obj.get_token_expiry()
        expected = datetime.datetime.fromtimestamp(creation_ts) + REFRESH_TOKEN_LIFETIME
        assert result == expected

    @pytest.mark.asyncio
    async def test_get_token_expiry_returns_none_when_creation_timestamp_missing(self):
        obj = self._make_with_token_dict({"token": {"expires_in": 1800}})
        assert await obj.get_token_expiry() is None

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

    @pytest.mark.asyncio
    async def test_politician_returns_none_on_empty_db_result(self):
        """B9: should not crash when DB returns None."""
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        ct = self._make(db=db)
        result = await ct.politician(politician_id='abc1234')
        assert result is None

    @pytest.mark.asyncio
    async def test_politician_returns_dict_when_found(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=('abc1234', 'John Doe', 'D', 'CA'))
        ct = self._make(db=db)
        result = await ct.politician(politician_id='abc1234')
        assert result == {'politician_id': 'abc1234', 'name': 'John Doe', 'party': 'D', 'state': 'CA'}

    @pytest.mark.asyncio
    async def test_politician_returns_none_with_no_args(self):
        ct = self._make()
        result = await ct.politician()
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

    @pytest.mark.asyncio
    async def test_update_politicians_calls_db_insert(self):
        """update_politicians must call db.execute_batch with politician data after parsing cards."""
        db = MagicMock()
        db.execute_batch = AsyncMock()
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

            # Second page: no cards — triggers db.execute_batch
            page2_soup = MagicMock()
            page2_soup.find_all.return_value = []

            mock_bs_cls.side_effect = [page1_soup, page2_soup]

            await ct.update_politicians()

        db.execute_batch.assert_called_once()
        call_args = db.execute_batch.call_args[0]
        assert len(call_args[1]) == 1


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
             patch('rocketstocks.data.clients.news.settings'):
            from rocketstocks.data.clients.news import News
            news = News()
            assert news.categories['General'] == 'general'
            assert 'eeneral' not in news.categories.values()


# ---------------------------------------------------------------------------
# Earnings — consistent return type (B14)
# ---------------------------------------------------------------------------

class TestEarningsReturnType:
    def _make_earnings(self):
        with patch('rocketstocks.data.earnings.MarketUtils'):
            from rocketstocks.data.earnings import Earnings
            mock_nasdaq = MagicMock()
            mock_db = MagicMock()
            return Earnings(nasdaq=mock_nasdaq, db=mock_db), mock_db

    @pytest.mark.asyncio
    async def test_get_earnings_on_date_returns_dataframe_when_empty(self):
        """B14: should always return DataFrame, not raw list."""
        earnings, db = self._make_earnings()
        db.execute = AsyncMock(return_value=None)

        result = await earnings.get_earnings_on_date(datetime.date(2024, 1, 2))
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_get_earnings_on_date_returns_dataframe_when_has_data(self):
        earnings, db = self._make_earnings()
        db.execute = AsyncMock(return_value=[
            (datetime.date(2024, 1, 2), 'AAPL', 'AMC', '2024Q1', '2.00', '10', '1.50', '2023-01-01')
        ])
        result = await earnings.get_earnings_on_date(datetime.date(2024, 1, 2))
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Tiingo client
# ---------------------------------------------------------------------------

class TestTiingoClient:
    def _make(self):
        mock_client_instance = MagicMock()
        with patch('rocketstocks.data.clients.tiingo.TiingoClient') as mock_cls, \
             patch('rocketstocks.data.clients.tiingo.settings'):
            mock_cls.return_value = mock_client_instance
            from rocketstocks.data.clients.tiingo import Tiingo
            obj = Tiingo(api_key='test-key')
        obj._client = mock_client_instance
        return obj

    def test_list_all_tickers_returns_dataframe(self):
        tiingo = self._make()
        tiingo._client.list_tickers.return_value = [
            {'ticker': 'aapl', 'name': 'Apple Inc.', 'exchangeCode': 'NASDAQ',
             'assetType': 'Stock', 'startDate': '1980-12-12', 'endDate': None},
            {'ticker': 'spy', 'name': 'SPDR ETF', 'exchangeCode': 'NYSE',
             'assetType': 'ETF', 'startDate': '1993-01-22', 'endDate': None},
        ]
        result = tiingo.list_all_tickers()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'ticker' in result.columns
        assert 'exchange' in result.columns

    def test_list_all_tickers_maps_asset_type_correctly(self):
        tiingo = self._make()
        tiingo._client.list_tickers.return_value = [
            {'ticker': 'aapl', 'name': 'Apple', 'exchangeCode': 'NASDAQ',
             'assetType': 'Stock', 'startDate': '1980-01-01', 'endDate': None},
            {'ticker': 'spy', 'name': 'SPDR', 'exchangeCode': 'NYSE',
             'assetType': 'ETF', 'startDate': '1993-01-01', 'endDate': None},
        ]
        result = tiingo.list_all_tickers()
        assert result.loc[result['ticker'] == 'AAPL', 'security_type'].iloc[0] == 'CS'
        assert result.loc[result['ticker'] == 'SPY', 'security_type'].iloc[0] == 'ETF'

    def test_get_ticker_metadata_returns_dict(self):
        tiingo = self._make()
        tiingo._client.get_ticker_metadata.return_value = {
            'ticker': 'AAPL', 'name': 'Apple Inc.', 'exchangeCode': 'NASDAQ',
            'assetType': 'Stock', 'endDate': None,
        }
        result = tiingo.get_ticker_metadata('AAPL')
        assert result is not None
        assert result['exchange'] == 'NASDAQ'
        assert result['security_type'] == 'CS'
        assert result['delist_date'] is None

    def test_get_ticker_metadata_returns_none_on_error(self):
        tiingo = self._make()
        tiingo._client.get_ticker_metadata.side_effect = Exception("Not found")
        result = tiingo.get_ticker_metadata('FAKE')
        assert result is None

    def test_list_all_tickers_active_ticker_no_delist_date(self):
        """endDate within 30 days → delist_date is None (active ticker)."""
        import datetime
        tiingo = self._make()
        recent = (datetime.date.today() - datetime.timedelta(days=5)).isoformat()
        tiingo._client.list_tickers.return_value = [
            {'ticker': 'AMZN', 'name': 'Amazon', 'exchangeCode': 'NASDAQ',
             'assetType': 'Stock', 'startDate': '1997-05-15', 'endDate': recent},
        ]
        result = tiingo.list_all_tickers()
        assert result.loc[result['ticker'] == 'AMZN', 'delist_date'].iloc[0] is None

    def test_list_all_tickers_delisted_ticker_has_delist_date(self):
        """endDate > 30 days ago → delist_date is set."""
        import datetime
        tiingo = self._make()
        old_date = datetime.date(2008, 9, 15)
        tiingo._client.list_tickers.return_value = [
            {'ticker': 'LEH', 'name': 'Lehman Brothers', 'exchangeCode': 'NYSE',
             'assetType': 'Stock', 'startDate': '1994-01-01', 'endDate': old_date.isoformat()},
        ]
        result = tiingo.list_all_tickers()
        assert result.loc[result['ticker'] == 'LEH', 'delist_date'].iloc[0] == old_date

    def test_get_ticker_metadata_active_ignores_end_date(self):
        """Recent endDate → delist_date is None."""
        import datetime
        tiingo = self._make()
        recent = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        tiingo._client.get_ticker_metadata.return_value = {
            'ticker': 'AMZN', 'name': 'Amazon', 'exchangeCode': 'NASDAQ',
            'assetType': 'Stock', 'endDate': recent,
        }
        result = tiingo.get_ticker_metadata('AMZN')
        assert result['delist_date'] is None

    def test_get_ticker_metadata_delisted_sets_delist_date(self):
        """Old endDate → correct delist_date is returned."""
        import datetime
        tiingo = self._make()
        old_date = datetime.date(2001, 11, 28)
        tiingo._client.get_ticker_metadata.return_value = {
            'ticker': 'ENRN', 'name': 'Enron Corp', 'exchangeCode': 'NYSE',
            'assetType': 'Stock', 'endDate': old_date.isoformat(),
        }
        result = tiingo.get_ticker_metadata('ENRN')
        assert result['delist_date'] == old_date

    def test_get_daily_price_history_returns_dataframe(self):
        tiingo = self._make()
        import pandas as pd
        df = pd.DataFrame({
            'adjOpen': [60.0], 'adjHigh': [65.0], 'adjLow': [59.0],
            'adjClose': [62.0], 'adjVolume': [5000000.0],
        }, index=pd.to_datetime(['2008-09-12']))
        df.index.name = 'date'
        tiingo._client.get_dataframe.return_value = df
        result = tiingo.get_daily_price_history('LEH', '2008-01-01', '2008-09-15')
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert 'ticker' in result.columns
        assert result['ticker'].iloc[0] == 'LEH'

    def test_get_daily_price_history_returns_empty_on_error(self):
        tiingo = self._make()
        tiingo._client.get_dataframe.side_effect = Exception("ticker not found")
        result = tiingo.get_daily_price_history('FAKE', '2000-01-01', '2000-12-31')
        assert isinstance(result, pd.DataFrame)
        assert result.empty


# ---------------------------------------------------------------------------
# Stooq client
# ---------------------------------------------------------------------------

class TestStooqClient:
    def _make_pdr_mock(self):
        """Return a mock pandas_datareader module injected into sys.modules."""
        import sys
        mock_pdr = MagicMock()
        sys.modules.setdefault('pandas_datareader', mock_pdr)
        sys.modules.setdefault('pandas_datareader.data', mock_pdr.data)
        return mock_pdr

    def _make(self):
        self._make_pdr_mock()  # ensure importable before importing Stooq
        from rocketstocks.data.clients.stooq import Stooq
        return Stooq()

    def test_get_daily_price_history_returns_dataframe(self):
        import sys
        mock_pdr = MagicMock()
        mock_df = pd.DataFrame({
            'Open': [60.0], 'High': [65.0], 'Low': [59.0],
            'Close': [62.0], 'Volume': [5000000],
        }, index=pd.to_datetime(['2008-09-12']))
        mock_df.index.name = 'Date'
        mock_pdr.data.DataReader.return_value = mock_df
        sys.modules['pandas_datareader'] = mock_pdr
        sys.modules['pandas_datareader.data'] = mock_pdr.data

        stooq = self._make()
        # Override _client's import inside get_daily_price_history by pre-seeding sys.modules
        with patch.dict(sys.modules, {'pandas_datareader.data': mock_pdr.data}):
            result = stooq.get_daily_price_history('LEH', '2008-01-01', '2008-09-15')
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert 'ticker' in result.columns
        assert result['ticker'].iloc[0] == 'LEH'

    def test_get_daily_price_history_returns_empty_on_exception(self):
        import sys
        mock_pdr = MagicMock()
        mock_pdr.data.DataReader.side_effect = Exception("no data")
        with patch.dict(sys.modules, {
            'pandas_datareader': mock_pdr,
            'pandas_datareader.data': mock_pdr.data,
        }):
            stooq = self._make()
            result = stooq.get_daily_price_history('FAKE', '2000-01-01', '2000-12-31')
        assert isinstance(result, pd.DataFrame)
        assert result.empty


# ---------------------------------------------------------------------------
# Schwab — rate limiting
# ---------------------------------------------------------------------------

class _MockLimiter:
    """Async context manager that records how many times it was acquired."""
    def __init__(self):
        self.acquired = 0

    async def __aenter__(self):
        self.acquired += 1

    async def __aexit__(self, *args):
        pass


class TestSchwabRateLimiting:
    def _make(self, limiter=None):
        from rocketstocks.data.clients.schwab import Schwab
        mock_inner = AsyncMock()
        mock_inner.get_quote.return_value = MagicMock(status_code=200, json=lambda: {'AAPL': {}})
        mock_inner.get_quotes.return_value = MagicMock(status_code=200, json=lambda: {})
        mock_inner.get_price_history_every_day.return_value = MagicMock(
            status_code=200,
            raise_for_status=MagicMock(),
            json=lambda: {'candles': []},
        )
        mock_inner.get_price_history_every_five_minutes.return_value = MagicMock(
            status_code=200,
            raise_for_status=MagicMock(),
            json=lambda: {'candles': []},
        )
        mock_inner.get_instruments.return_value = MagicMock(status_code=200, json=lambda: {})
        mock_inner.get_option_chain.return_value = MagicMock(status_code=200, json=lambda: {})
        mock_inner.get_movers.return_value = MagicMock(status_code=200, json=lambda: {})
        obj = Schwab(token_store=AsyncMock(), limiter=limiter or _MockLimiter())
        obj.client = mock_inner
        return obj

    def test_default_limiter_is_async_limiter(self):
        from aiolimiter import AsyncLimiter
        from rocketstocks.data.clients.schwab import Schwab
        obj = Schwab(token_store=AsyncMock())
        assert isinstance(obj._limiter, AsyncLimiter)

    def test_custom_limiter_is_stored(self):
        limiter = _MockLimiter()
        obj = self._make(limiter=limiter)
        assert obj._limiter is limiter

    @pytest.mark.asyncio
    async def test_get_quote_acquires_limiter(self):
        limiter = _MockLimiter()
        obj = self._make(limiter=limiter)
        await obj.get_quote('AAPL')
        assert limiter.acquired == 1

    @pytest.mark.asyncio
    async def test_get_quotes_acquires_limiter(self):
        limiter = _MockLimiter()
        obj = self._make(limiter=limiter)
        await obj.get_quotes(['AAPL', 'MSFT'])
        assert limiter.acquired == 1

    @pytest.mark.asyncio
    async def test_get_daily_price_history_acquires_limiter(self):
        limiter = _MockLimiter()
        obj = self._make(limiter=limiter)
        await obj.get_daily_price_history('AAPL')
        assert limiter.acquired == 1

    @pytest.mark.asyncio
    async def test_get_5m_price_history_acquires_limiter(self):
        limiter = _MockLimiter()
        obj = self._make(limiter=limiter)
        await obj.get_5m_price_history('AAPL')
        assert limiter.acquired == 1

    @pytest.mark.asyncio
    async def test_get_fundamentals_acquires_limiter(self):
        limiter = _MockLimiter()
        obj = self._make(limiter=limiter)
        await obj.get_fundamentals(['AAPL'])
        assert limiter.acquired == 1

    @pytest.mark.asyncio
    async def test_get_options_chain_acquires_limiter(self):
        limiter = _MockLimiter()
        obj = self._make(limiter=limiter)
        await obj.get_options_chain('AAPL')
        assert limiter.acquired == 1

    @pytest.mark.asyncio
    async def test_get_movers_acquires_limiter(self):
        limiter = _MockLimiter()
        obj = self._make(limiter=limiter)
        await obj.get_movers()
        assert limiter.acquired == 1

    @pytest.mark.asyncio
    async def test_multiple_calls_each_acquire_limiter(self):
        limiter = _MockLimiter()
        obj = self._make(limiter=limiter)
        await obj.get_quote('AAPL')
        await obj.get_quote('AAPL')
        assert limiter.acquired == 2


# ---------------------------------------------------------------------------
# Schwab — HTTP 429 rate limit error
# ---------------------------------------------------------------------------

class TestSchwabRateLimitError:
    def _make(self):
        from rocketstocks.data.clients.schwab import Schwab
        obj = Schwab(token_store=AsyncMock(), limiter=_MockLimiter())
        obj.client = AsyncMock()
        return obj

    def _mock_429(self):
        resp = MagicMock()
        resp.status_code = 429
        return resp

    @pytest.mark.asyncio
    async def test_get_quote_raises_on_429(self):
        from rocketstocks.data.clients.schwab import SchwabRateLimitError
        obj = self._make()
        obj.client.get_quote = AsyncMock(return_value=self._mock_429())
        with pytest.raises(SchwabRateLimitError):
            await obj.get_quote('AAPL')

    @pytest.mark.asyncio
    async def test_get_quotes_raises_on_429(self):
        from rocketstocks.data.clients.schwab import SchwabRateLimitError
        obj = self._make()
        obj.client.get_quotes = AsyncMock(return_value=self._mock_429())
        with pytest.raises(SchwabRateLimitError):
            await obj.get_quotes(['AAPL', 'MSFT'])

    @pytest.mark.asyncio
    async def test_get_daily_price_history_raises_on_429(self):
        """429 must NOT be swallowed by the HTTPStatusError catch block."""
        from rocketstocks.data.clients.schwab import SchwabRateLimitError
        obj = self._make()
        obj.client.get_price_history_every_day = AsyncMock(return_value=self._mock_429())
        with pytest.raises(SchwabRateLimitError):
            await obj.get_daily_price_history('AAPL')

    @pytest.mark.asyncio
    async def test_get_5m_price_history_raises_on_429(self):
        """429 must NOT be swallowed by the HTTPStatusError catch block."""
        from rocketstocks.data.clients.schwab import SchwabRateLimitError
        obj = self._make()
        obj.client.get_price_history_every_five_minutes = AsyncMock(return_value=self._mock_429())
        with pytest.raises(SchwabRateLimitError):
            await obj.get_5m_price_history('AAPL')

    @pytest.mark.asyncio
    async def test_get_fundamentals_raises_on_429(self):
        from rocketstocks.data.clients.schwab import SchwabRateLimitError
        obj = self._make()
        obj.client.get_instruments = AsyncMock(return_value=self._mock_429())
        with pytest.raises(SchwabRateLimitError):
            await obj.get_fundamentals(['AAPL'])

    @pytest.mark.asyncio
    async def test_get_options_chain_raises_on_429(self):
        from rocketstocks.data.clients.schwab import SchwabRateLimitError
        obj = self._make()
        obj.client.get_option_chain = AsyncMock(return_value=self._mock_429())
        with pytest.raises(SchwabRateLimitError):
            await obj.get_options_chain('AAPL')

    @pytest.mark.asyncio
    async def test_get_movers_raises_on_429(self):
        from rocketstocks.data.clients.schwab import SchwabRateLimitError
        obj = self._make()
        obj.client.get_movers = AsyncMock(return_value=self._mock_429())
        with pytest.raises(SchwabRateLimitError):
            await obj.get_movers()

    @pytest.mark.asyncio
    async def test_get_movers_passes_sort_order_to_client(self):
        """sort_order parameter must be forwarded to the underlying client call."""
        obj = self._make()
        sentinel = MagicMock(name='sort_order_sentinel')
        obj.client.get_movers = AsyncMock(
            return_value=MagicMock(status_code=200, json=lambda: {})
        )
        await obj.get_movers(sort_order=sentinel)
        _, kwargs = obj.client.get_movers.call_args
        assert kwargs.get('sort_order') == sentinel

    @pytest.mark.asyncio
    async def test_get_movers_uses_default_sort_when_none(self):
        """When sort_order is omitted, the client receives PERCENT_CHANGE_UP."""
        obj = self._make()
        obj.client.Movers = MagicMock()
        obj.client.get_movers = AsyncMock(
            return_value=MagicMock(status_code=200, json=lambda: {})
        )
        await obj.get_movers()
        _, kwargs = obj.client.get_movers.call_args
        assert kwargs.get('sort_order') == obj.client.Movers.SortOrder.PERCENT_CHANGE_UP

    @pytest.mark.asyncio
    async def test_non_429_errors_not_raised_as_rate_limit(self):
        """A 401 in get_daily_price_history should still return empty DataFrame."""
        import httpx
        from rocketstocks.data.clients.schwab import SchwabRateLimitError
        obj = self._make()
        resp = MagicMock()
        resp.status_code = 401
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "401", request=MagicMock(), response=MagicMock()
        )
        obj.client.get_price_history_every_day = AsyncMock(return_value=resp)
        result = await obj.get_daily_price_history('AAPL')
        assert isinstance(result, pd.DataFrame)
        assert result.empty


# ---------------------------------------------------------------------------
# Tiingo — rate limiting decorators
# ---------------------------------------------------------------------------

class TestTiingoRateLimiting:
    def test_get_ticker_metadata_has_rate_limit_decorator(self):
        """get_ticker_metadata must be wrapped by ratelimit decorators."""
        from rocketstocks.data.clients.tiingo import Tiingo
        assert hasattr(Tiingo.get_ticker_metadata, '__wrapped__')

    def test_get_daily_price_history_has_rate_limit_decorator(self):
        """get_daily_price_history must be wrapped by ratelimit decorators."""
        from rocketstocks.data.clients.tiingo import Tiingo
        assert hasattr(Tiingo.get_daily_price_history, '__wrapped__')

    def test_list_all_tickers_has_no_rate_limit_decorator(self):
        """list_all_tickers is a one-shot call and should not be rate-limited."""
        from rocketstocks.data.clients.tiingo import Tiingo
        assert not hasattr(Tiingo.list_all_tickers, '__wrapped__')


# ---------------------------------------------------------------------------
# SEC — asyncio.to_thread for blocking HTTP calls
# ---------------------------------------------------------------------------

class TestSECAsyncHTTP:
    def _make(self, cik='0000320193'):
        from rocketstocks.data.clients.sec import SEC
        db = MagicMock()
        db.execute = AsyncMock(return_value=(cik,))
        return SEC(db=db)

    @pytest.mark.asyncio
    async def test_get_submissions_data_uses_to_thread(self):
        """get_submissions_data must delegate requests.get to asyncio.to_thread."""
        sec = self._make()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'sic': '7372', 'filings': {'recent': {}}}
        with patch('rocketstocks.data.clients.sec.asyncio.to_thread', new_callable=AsyncMock, return_value=mock_resp) as mock_thread:
            result = await sec.get_submissions_data('AAPL')
        mock_thread.assert_called_once()
        assert result['sic'] == '7372'

    @pytest.mark.asyncio
    async def test_get_accounts_payable_uses_to_thread(self):
        """get_accounts_payable must delegate requests.get to asyncio.to_thread."""
        sec = self._make()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'entityName': ['Apple Inc.'], 'cik': ['0000320193']}
        with patch('rocketstocks.data.clients.sec.asyncio.to_thread', new_callable=AsyncMock, return_value=mock_resp):
            result = await sec.get_accounts_payable('AAPL')
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_get_company_facts_uses_to_thread(self):
        """get_company_facts must delegate requests.get to asyncio.to_thread."""
        sec = self._make()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'entityName': 'Apple Inc.', 'facts': {}}
        with patch('rocketstocks.data.clients.sec.asyncio.to_thread', new_callable=AsyncMock, return_value=mock_resp):
            result = await sec.get_company_facts('AAPL')
        assert result['entityName'] == 'Apple Inc.'

    def test_get_submissions_data_is_async(self):
        import inspect
        from rocketstocks.data.clients.sec import SEC
        assert inspect.iscoroutinefunction(SEC.get_submissions_data)

    def test_get_accounts_payable_is_async(self):
        import inspect
        from rocketstocks.data.clients.sec import SEC
        assert inspect.iscoroutinefunction(SEC.get_accounts_payable)

    def test_get_company_facts_is_async(self):
        import inspect
        from rocketstocks.data.clients.sec import SEC
        assert inspect.iscoroutinefunction(SEC.get_company_facts)
