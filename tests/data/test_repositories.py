"""Tests for TickerRepository, PriceHistoryRepository, PopularityRepository."""
import datetime
import pandas as pd
import pytest
from unittest.mock import MagicMock, AsyncMock, patch, call


# ---------------------------------------------------------------------------
# TickerRepository
# ---------------------------------------------------------------------------

class TestTickerRepository:
    def _make(self, db=None, nasdaq=None, sec=None, tiingo=None):
        from rocketstocks.data.tickers import TickerRepository
        return TickerRepository(
            db=db or MagicMock(),
            nasdaq=nasdaq or MagicMock(),
            sec=sec or MagicMock(),
            tiingo=tiingo,
        )

    def _make_db(self, return_value=None):
        db = MagicMock()
        db.execute = AsyncMock(return_value=return_value)
        db.execute_batch = AsyncMock()
        return db

    @pytest.mark.asyncio
    async def test_get_all_tickers_returns_list(self):
        db = self._make_db(return_value=[('AAPL',), ('MSFT',), ('GOOG',)])
        repo = self._make(db=db)
        result = await repo.get_all_tickers()
        assert result == ['AAPL', 'MSFT', 'GOOG']

    @pytest.mark.asyncio
    async def test_get_all_tickers_empty(self):
        db = self._make_db(return_value=[])
        repo = self._make(db=db)
        assert await repo.get_all_tickers() == []

    @pytest.mark.asyncio
    async def test_get_ticker_info_returns_dict(self):
        row = ('AAPL', None, 'Apple', 'US', '1980', 'Technology Hardware',
               'Technology', 'NASDAQ', 'CS', '7372', None)
        db = self._make_db(return_value=row)
        repo = self._make(db=db)
        result = await repo.get_ticker_info('AAPL')
        assert result == {
            'ticker': 'AAPL', 'cik': None, 'name': 'Apple', 'country': 'US',
            'ipoyear': '1980', 'industry': 'Technology Hardware', 'sector': 'Technology',
            'exchange': 'NASDAQ', 'security_type': 'CS', 'sic_code': '7372',
            'delist_date': None,
        }
        db.execute.assert_awaited_once()
        _, kwargs = db.execute.call_args
        assert kwargs.get('fetchone') is True

    @pytest.mark.asyncio
    async def test_get_ticker_info_none_result_returns_none(self):
        db = self._make_db(return_value=None)
        repo = self._make(db=db)
        result = await repo.get_ticker_info('FAKE')
        assert result is None

    @pytest.mark.asyncio
    async def test_get_cik_returns_value(self):
        db = self._make_db(return_value=('0000320193',))
        repo = self._make(db=db)
        result = await repo.get_cik('AAPL')
        assert result == '0000320193'
        db.execute.assert_awaited_once()
        _, kwargs = db.execute.call_args
        assert kwargs.get('fetchone') is True

    @pytest.mark.asyncio
    async def test_get_cik_returns_none_when_not_found(self):
        db = self._make_db(return_value=None)
        repo = self._make(db=db)
        assert await repo.get_cik('FAKE') is None

    @pytest.mark.asyncio
    async def test_get_all_tickers_by_sector_returns_list(self):
        db = self._make_db(return_value=[('AAPL',), ('MSFT',)])
        repo = self._make(db=db)
        result = await repo.get_all_tickers_by_sector('Technology')
        assert result == ['AAPL', 'MSFT']

    @pytest.mark.asyncio
    async def test_get_all_tickers_by_sector_returns_none_when_empty(self):
        db = self._make_db(return_value=[])
        repo = self._make(db=db)
        result = await repo.get_all_tickers_by_sector('XYZ')
        assert result is None

    @pytest.mark.asyncio
    async def test_get_all_ticker_info_returns_dataframe(self):
        rows = [
            ('AAPL', None, 'Apple', 'US', '1980', 'Tech', 'Technology',
             'NASDAQ', 'CS', '7372', None),
            ('MSFT', None, 'Microsoft', 'US', '1986', 'Tech', 'Technology',
             'NASDAQ', 'CS', '7372', None),
        ]
        db = self._make_db(return_value=rows)
        repo = self._make(db=db)
        result = await repo.get_all_ticker_info()
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == [
            'ticker', 'cik', 'name', 'country', 'ipoyear',
            'industry', 'sector', 'exchange', 'security_type', 'sic_code', 'delist_date',
        ]
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_validate_ticker_true_when_found(self):
        db = self._make_db(return_value=('AAPL',))
        repo = self._make(db=db)
        assert await repo.validate_ticker('AAPL') is True
        db.execute.assert_awaited_once()
        _, kwargs = db.execute.call_args
        assert kwargs.get('fetchone') is True

    @pytest.mark.asyncio
    async def test_validate_ticker_false_when_not_found(self):
        db = self._make_db(return_value=None)
        repo = self._make(db=db)
        assert await repo.validate_ticker('FAKE') is False

    @pytest.mark.asyncio
    async def test_parse_valid_tickers(self):
        db = MagicMock()
        # validate_ticker calls db.execute with fetchone=True — AAPL found, FAKE not
        db.execute = AsyncMock(side_effect=[('AAPL',), None])
        repo = self._make(db=db)
        valid, invalid = await repo.parse_valid_tickers('AAPL FAKE')
        assert valid == ['AAPL']
        assert invalid == ['FAKE']

    @pytest.mark.asyncio
    async def test_update_tickers_calls_db_execute_for_each_ticker(self):
        """update_tickers must call db.execute (UPDATE) for each ticker that exists in DB."""
        db = MagicMock()
        # First call is get_all_tickers(); subsequent calls are per-ticker UPDATEs
        db.execute = AsyncMock(return_value=[('AAPL',)])
        db.execute_batch = AsyncMock()
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
        repo = self._make(db=db, nasdaq=nasdaq)
        await repo.update_tickers()
        # execute should have been called at least twice: once for get_all_tickers,
        # once for the UPDATE of AAPL
        assert db.execute.await_count >= 2

    @pytest.mark.asyncio
    async def test_insert_tickers_merges_sec_and_nasdaq(self):
        """insert_tickers must merge SEC + NASDAQ data and zero-pad CIK to 10 digits."""
        db = MagicMock()
        db.execute = AsyncMock()
        db.execute_batch = AsyncMock()
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
        })
        repo = self._make(db=db, nasdaq=nasdaq, sec=sec)
        await repo.insert_tickers()
        db.execute_batch.assert_awaited_once()
        args, _ = db.execute_batch.call_args
        # args[1] is the values list; each row is a tuple
        values = args[1]
        # Find the row with AAPL and verify CIK is zero-padded
        aapl_row = next(row for row in values if row[0] == 'AAPL')
        # cik is at index matching the merged DataFrame column order
        cik_idx = list(pd.DataFrame({'ticker': [], 'name': [], 'country': [],
                                      'ipoyear': [], 'industry': [], 'sector': [],
                                      'cik': []}).columns).index('cik')
        assert aapl_row[cik_idx] == '0000320193'

    @pytest.mark.asyncio
    async def test_insert_tickers_uses_nasdaq_as_primary(self):
        """NASDAQ is the left table; tickers without SEC CIK are still inserted."""
        db = MagicMock()
        db.execute = AsyncMock()
        db.execute_batch = AsyncMock()
        nasdaq = MagicMock()
        nasdaq.get_all_tickers.return_value = pd.DataFrame([
            {'symbol': 'AAPL', 'name': 'Apple Inc.', 'country': 'US',
             'ipoyear': '1980', 'industry': 'Tech', 'sector': 'Technology'},
            {'symbol': 'SPY', 'name': 'SPDR ETF', 'country': 'US',
             'ipoyear': '1993', 'industry': '', 'sector': ''},
        ])
        sec = MagicMock()
        sec.get_company_tickers.return_value = pd.DataFrame([
            {'ticker': 'AAPL', 'cik_str': 320193},
            # SPY not in SEC
        ])
        repo = self._make(db=db, nasdaq=nasdaq, sec=sec)
        await repo.insert_tickers()

        db.execute_batch.assert_awaited_once()
        args, _ = db.execute_batch.call_args
        values = args[1]
        tickers_inserted = [row[0] for row in values]
        assert 'AAPL' in tickers_inserted
        assert 'SPY' in tickers_inserted

    @pytest.mark.asyncio
    async def test_enrich_ticker_updates_exchange_and_security_type(self):
        tiingo = MagicMock()
        tiingo.get_ticker_metadata.return_value = {
            'ticker': 'AAPL', 'exchange': 'NASDAQ', 'security_type': 'CS',
            'delist_date': None,
        }
        db = MagicMock()
        # First call: UPDATE exchange/security_type (returns None for DML)
        # Second call: get_cik SELECT (returns CIK row)
        # Third call: UPDATE sic_code (returns None for DML)
        db.execute = AsyncMock(side_effect=[None, ('0000320193',), None])
        db.execute_batch = AsyncMock()
        sec = MagicMock()
        sec.get_submissions_data.return_value = {'sic': '7372'}
        repo = self._make(db=db, sec=sec, tiingo=tiingo)
        result = await repo.enrich_ticker('AAPL')
        assert result is True
        assert db.execute.await_count >= 2

    @pytest.mark.asyncio
    async def test_enrich_ticker_updates_sic_code_via_sec(self):
        tiingo = MagicMock()
        tiingo.get_ticker_metadata.return_value = {
            'ticker': 'AAPL', 'exchange': 'NASDAQ', 'security_type': 'CS',
            'delist_date': None,
        }
        db = MagicMock()
        # Call 1: UPDATE exchange/security_type
        # Call 2: get_cik SELECT → returns CIK
        # Call 3: UPDATE sic_code
        db.execute = AsyncMock(side_effect=[None, ('0000320193',), None])
        db.execute_batch = AsyncMock()
        sec = MagicMock()
        sec.get_submissions_data.return_value = {'sic': '7372'}
        repo = self._make(db=db, sec=sec, tiingo=tiingo)
        await repo.enrich_ticker('AAPL')
        # The last execute call should be the sic_code UPDATE
        last_call_args = db.execute.call_args_list[-1]
        query = last_call_args[0][0]
        assert 'sic_code' in query
        params = last_call_args[0][1]
        assert '7372' in params

    @pytest.mark.asyncio
    async def test_enrich_ticker_returns_false_on_missing_metadata(self):
        tiingo = MagicMock()
        tiingo.get_ticker_metadata.return_value = None
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        db.execute_batch = AsyncMock()
        repo = self._make(db=db, tiingo=tiingo)
        result = await repo.enrich_ticker('FAKE')
        assert result is False

    @pytest.mark.asyncio
    async def test_enrich_ticker_updates_name_from_tiingo(self):
        """enrich_ticker must include name in the UPDATE when Tiingo returns a name."""
        tiingo = MagicMock()
        tiingo.get_ticker_metadata.return_value = {
            'ticker': 'AAPL', 'exchange': 'NASDAQ', 'security_type': 'CS',
            'delist_date': None, 'name': 'Apple Inc. Common Stock',
        }
        db = MagicMock()
        # Call 1: UPDATE (with name), Call 2: get_cik → None (no CIK)
        db.execute = AsyncMock(side_effect=[None, None])
        db.execute_batch = AsyncMock()
        repo = self._make(db=db, tiingo=tiingo)
        await repo.enrich_ticker('AAPL')
        # The first UPDATE query should contain 'name'
        first_call_query = db.execute.call_args_list[0][0][0]
        assert 'name' in first_call_query
        # The cleaned name 'Apple' should appear in the params
        first_call_params = db.execute.call_args_list[0][0][1]
        assert 'Apple' in first_call_params

    @pytest.mark.asyncio
    async def test_enrich_unenriched_batch_returns_count(self):
        tiingo = MagicMock()
        tiingo.get_ticker_metadata.return_value = {
            'ticker': 'X', 'exchange': 'NYSE', 'security_type': 'CS', 'delist_date': None,
        }
        db = MagicMock()
        # First call: SELECT unenriched tickers → 3 rows
        # Then for each ticker: enrich_ticker calls execute twice (UPDATE + get_cik)
        # get_cik returns None → no SEC enrichment (so only 2 calls per ticker)
        unenriched_rows = [('AAPL',), ('MSFT',), ('GOOG',)]
        # Pattern per ticker: None (UPDATE), None (get_cik)
        per_ticker = [None, None]
        db.execute = AsyncMock(side_effect=[unenriched_rows] + per_ticker * 3)
        db.execute_batch = AsyncMock()
        repo = self._make(db=db, tiingo=tiingo)
        count = await repo.enrich_unenriched_batch(limit=3)
        assert count == 3

    @pytest.mark.asyncio
    async def test_import_delisted_tickers_inserts_new_rows(self):
        tiingo = MagicMock()
        tiingo.list_all_tickers.return_value = pd.DataFrame([
            {'ticker': 'LEH', 'name': 'Lehman Brothers', 'exchange': 'NYSE',
             'security_type': 'CS', 'delist_date': datetime.date(2008, 9, 15)},
            {'ticker': 'ENRN', 'name': 'Enron Corp', 'exchange': 'NYSE',
             'security_type': 'CS', 'delist_date': datetime.date(2001, 11, 28)},
        ])
        db = MagicMock()
        # SELECT COUNT(*) before → 10; SELECT COUNT(*) after → 12
        db.execute = AsyncMock(side_effect=[(10,), (12,)])
        db.execute_batch = AsyncMock()
        repo = self._make(db=db, tiingo=tiingo)
        result = await repo.import_delisted_tickers()
        db.execute_batch.assert_awaited_once()
        assert result == 2

    @pytest.mark.asyncio
    async def test_import_delisted_tickers_cleans_names(self):
        """import_delisted_tickers must apply clean_company_name to ticker names."""
        tiingo = MagicMock()
        tiingo.list_all_tickers.return_value = pd.DataFrame([
            {'ticker': 'LEH', 'name': 'Lehman Brothers Holdings Inc. Common Stock',
             'exchange': 'NYSE', 'security_type': 'CS',
             'delist_date': datetime.date(2008, 9, 15)},
        ])
        db = MagicMock()
        db.execute = AsyncMock(side_effect=[(5,), (6,)])
        captured = {}

        async def fake_execute_batch(query, values):
            captured['values'] = values

        db.execute_batch = fake_execute_batch
        repo = self._make(db=db, tiingo=tiingo)
        await repo.import_delisted_tickers()
        # Each row is (ticker, name, exchange, security_type, delist_date)
        inserted_name = captured['values'][0][1]
        assert inserted_name == 'Lehman Brothers Holdings'


    # --- clean_company_name (pure function — sync tests) ---

    def test_clean_company_name_removes_common_stock(self):
        from rocketstocks.data.tickers import clean_company_name
        assert clean_company_name('Apple Inc. Common Stock') == 'Apple'

    def test_clean_company_name_removes_class_a(self):
        from rocketstocks.data.tickers import clean_company_name
        assert clean_company_name('Alphabet Inc. Class A Common Stock') == 'Alphabet'

    def test_clean_company_name_leaves_etf_name_unchanged(self):
        from rocketstocks.data.tickers import clean_company_name
        assert clean_company_name('SPDR S&P 500 ETF Trust') == 'SPDR S&P 500 ETF Trust'

    def test_clean_company_name_adr_clause(self):
        from rocketstocks.data.tickers import clean_company_name
        result = clean_company_name(
            'Alibaba Group Holding Limited American Depositary Shares'
            ' each representing eight Ordinary shares'
        )
        assert result == 'Alibaba Group Holding'

    def test_clean_company_name_dash_class(self):
        from rocketstocks.data.tickers import clean_company_name
        assert clean_company_name('Rivian Automotive - Class A') == 'Rivian Automotive'

    def test_clean_company_name_registered_shares(self):
        from rocketstocks.data.tickers import clean_company_name
        assert clean_company_name('SAP SE, Registered Shares') == 'SAP'

    def test_clean_company_name_warrants(self):
        from rocketstocks.data.tickers import clean_company_name
        assert clean_company_name('ACME Corp. Warrants') == 'ACME'

    def test_clean_company_name_legal_suffix_only(self):
        from rocketstocks.data.tickers import clean_company_name
        assert clean_company_name('Microsoft Corp.') == 'Microsoft'

    def test_clean_company_name_novabay(self):
        from rocketstocks.data.tickers import clean_company_name
        assert clean_company_name('NovaBay Pharmaceuticals, Inc. Common Stock') == 'NovaBay Pharmaceuticals'

    def test_clean_company_name_empty_string(self):
        from rocketstocks.data.tickers import clean_company_name
        assert clean_company_name('') == ''


# ---------------------------------------------------------------------------
# PriceHistoryRepository
# ---------------------------------------------------------------------------

class TestPriceHistoryRepository:
    def _make(self, db=None, schwab=None, tiingo=None, stooq=None):
        from rocketstocks.data.price_history import PriceHistoryRepository
        return PriceHistoryRepository(
            db=db or MagicMock(),
            schwab=schwab or AsyncMock(),
            tiingo=tiingo,
            stooq=stooq,
        )

    def _make_db(self, return_value=None):
        db = MagicMock()
        db.execute = AsyncMock(return_value=return_value)
        db.execute_batch = AsyncMock()
        return db

    def _make_ohlcv_df(self, ticker='LEH'):
        return pd.DataFrame([{
            'ticker': ticker, 'open': 60.0, 'high': 65.0,
            'low': 59.0, 'close': 62.0, 'volume': 5000000,
            'date': datetime.date(2008, 9, 12),
        }])

    @pytest.mark.asyncio
    async def test_fetch_daily_returns_dataframe(self):
        rows = [('AAPL', 100.0, 110.0, 99.0, 105.0, 1000000, datetime.date(2024, 1, 2))]
        db = self._make_db(return_value=rows)
        repo = self._make(db=db)
        result = await repo.fetch_daily_price_history('AAPL')
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ['ticker', 'open', 'high', 'low', 'close', 'volume', 'date']
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_fetch_daily_returns_empty_dataframe_when_no_data(self):
        db = self._make_db(return_value=None)
        repo = self._make(db=db)
        result = await repo.fetch_daily_price_history('FAKE')
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @pytest.mark.asyncio
    async def test_fetch_5m_returns_dataframe(self):
        dt = datetime.datetime(2024, 1, 2, 10, 0)
        rows = [('AAPL', 100.0, 110.0, 99.0, 105.0, 50000, dt)]
        db = self._make_db(return_value=rows)
        repo = self._make(db=db)
        result = await repo.fetch_5m_price_history('AAPL')
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ['ticker', 'open', 'high', 'low', 'close', 'volume', 'datetime']
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_fetch_5m_returns_empty_dataframe_when_no_data(self):
        db = self._make_db(return_value=None)
        repo = self._make(db=db)
        result = await repo.fetch_5m_price_history('FAKE')
        assert result.empty

    @pytest.mark.asyncio
    async def test_update_daily_inserts_when_data_found(self):
        db = self._make_db(return_value=None)  # no existing row
        schwab = AsyncMock()
        df = pd.DataFrame({
            'ticker': ['AAPL'], 'open': [100.0], 'high': [110.0],
            'low': [99.0], 'close': [105.0], 'volume': [1000000],
            'date': [datetime.date(2024, 1, 2)],
        })
        schwab.get_daily_price_history = AsyncMock(return_value=df)
        repo = self._make(db=db, schwab=schwab)
        await repo.update_daily_price_history_by_ticker('AAPL')
        db.execute_batch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_update_daily_skips_insert_when_no_data(self):
        db = self._make_db(return_value=None)
        schwab = AsyncMock()
        schwab.get_daily_price_history = AsyncMock(return_value=pd.DataFrame())
        repo = self._make(db=db, schwab=schwab)
        await repo.update_daily_price_history_by_ticker('AAPL')
        db.execute_batch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_update_5m_uses_existing_latest_datetime(self):
        """When a record exists, start_datetime should be that record's datetime."""
        existing_dt = datetime.datetime(2024, 6, 1, 9, 30)
        db = self._make_db(return_value=(existing_dt,))
        schwab = AsyncMock()
        schwab.get_5m_price_history = AsyncMock(return_value=pd.DataFrame())
        repo = self._make(db=db, schwab=schwab)
        await repo.update_5m_price_history_by_ticker('AAPL')
        schwab.get_5m_price_history.assert_awaited_once()
        _, kwargs = schwab.get_5m_price_history.call_args
        assert kwargs['start_datetime'] == existing_dt

    @pytest.mark.asyncio
    async def test_load_delisted_price_history_uses_tiingo_first(self):
        """Tiingo returns data → Stooq should NOT be called."""
        db = self._make_db(return_value=None)  # no existing data
        tiingo = MagicMock()
        tiingo.get_daily_price_history.return_value = self._make_ohlcv_df()
        stooq = MagicMock()
        repo = self._make(db=db, tiingo=tiingo, stooq=stooq)
        result = await repo.load_delisted_price_history('LEH')
        assert result == 1
        tiingo.get_daily_price_history.assert_called_once()
        stooq.get_daily_price_history.assert_not_called()
        db.execute_batch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_load_delisted_price_history_falls_back_to_stooq(self):
        """Tiingo empty → Stooq called as fallback."""
        db = self._make_db(return_value=None)
        tiingo = MagicMock()
        tiingo.get_daily_price_history.return_value = pd.DataFrame()
        stooq = MagicMock()
        stooq.get_daily_price_history.return_value = self._make_ohlcv_df()
        repo = self._make(db=db, tiingo=tiingo, stooq=stooq)
        result = await repo.load_delisted_price_history('LEH')
        assert result == 1
        stooq.get_daily_price_history.assert_called_once()
        db.execute_batch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_load_delisted_price_history_skips_if_data_exists(self):
        """If data already exists, neither Tiingo nor Stooq should be called."""
        db = self._make_db(return_value=('LEH',))  # existing row found
        tiingo = MagicMock()
        stooq = MagicMock()
        repo = self._make(db=db, tiingo=tiingo, stooq=stooq)
        result = await repo.load_delisted_price_history('LEH')
        assert result == 0
        tiingo.get_daily_price_history.assert_not_called()
        stooq.get_daily_price_history.assert_not_called()
        db.execute_batch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_load_delisted_price_history_batch_returns_count(self):
        """Batch method queues delisted tickers and returns total rows inserted."""
        db = MagicMock()
        # First call: SELECT delisted tickers with no price history
        # Then per ticker (LEH, ENRN): check existing (None) → insert
        db.execute = AsyncMock(side_effect=[
            [('LEH',), ('ENRN',)],  # batch SELECT
            None,                   # LEH: no existing data
            None,                   # ENRN: no existing data
        ])
        db.execute_batch = AsyncMock()
        tiingo = MagicMock()
        tiingo.get_daily_price_history.return_value = self._make_ohlcv_df()
        repo = self._make(db=db, tiingo=tiingo)
        result = await repo.load_delisted_price_history_batch(limit=2)
        assert result == 2


# ---------------------------------------------------------------------------
# PopularityRepository
# ---------------------------------------------------------------------------

class TestPopularityRepository:
    def _make(self, db=None, ape_wisdom=None):
        from rocketstocks.data.popularity_store import PopularityRepository
        mock_ape = MagicMock() if ape_wisdom is None else ape_wisdom
        with patch('rocketstocks.data.popularity_store.ApeWisdom', return_value=mock_ape):
            return PopularityRepository(db=db or MagicMock())

    def _make_db(self, return_value=None):
        db = MagicMock()
        db.execute = AsyncMock(return_value=return_value)
        db.execute_batch = AsyncMock()
        return db

    @pytest.mark.asyncio
    async def test_fetch_popularity_returns_dataframe(self):
        rows = [
            (datetime.datetime(2024, 1, 1), 1, 'AAPL', 'Apple', 100, 50, 2, 90),
        ]
        db = self._make_db(return_value=rows)
        repo = self._make(db=db)
        result = await repo.fetch_popularity('AAPL')
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == [
            'datetime', 'rank', 'ticker', 'name',
            'mentions', 'upvotes', 'rank_24h_ago', 'mentions_24h_ago',
        ]
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_fetch_popularity_empty_returns_empty_df(self):
        db = self._make_db(return_value=[])
        repo = self._make(db=db)
        result = await repo.fetch_popularity()
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    @pytest.mark.asyncio
    async def test_fetch_popularity_with_ticker_uses_where_clause(self):
        db = self._make_db(return_value=[])
        repo = self._make(db=db)
        await repo.fetch_popularity('AAPL')
        call_args = db.execute.call_args[0]
        query = call_args[0]
        params = call_args[1]
        assert 'WHERE ticker' in query
        assert 'AAPL' in params

    @pytest.mark.asyncio
    async def test_fetch_popularity_no_ticker_uses_no_where(self):
        db = self._make_db(return_value=[])
        repo = self._make(db=db)
        await repo.fetch_popularity()
        call_args = db.execute.call_args[0]
        query = call_args[0]
        assert 'WHERE' not in query

    @pytest.mark.asyncio
    async def test_insert_popularity_calls_execute_batch(self):
        db = self._make_db()
        repo = self._make(db=db)
        df = pd.DataFrame({
            'datetime': [datetime.datetime(2024, 1, 1)], 'rank': [1],
            'ticker': ['AAPL'], 'name': ['Apple'],
            'mentions': [100], 'upvotes': [50],
            'rank_24h_ago': [2], 'mentions_24h_ago': [90],
        })
        await repo.insert_popularity(df)
        db.execute_batch.assert_awaited_once()
        args, _ = db.execute_batch.call_args
        query = args[0]
        values = args[1]
        assert 'INSERT INTO popularity' in query
        assert len(values) == 1

    def test_get_popular_stocks_calls_ape_wisdom(self):
        ape_wisdom = MagicMock()
        ape_wisdom.get_popular_stocks.return_value = pd.DataFrame()
        db = MagicMock()
        with patch('rocketstocks.data.popularity_store.ApeWisdom', return_value=ape_wisdom):
            from rocketstocks.data.popularity_store import PopularityRepository
            repo = PopularityRepository(db=db)
        repo.get_popular_stocks()
        ape_wisdom.get_popular_stocks.assert_called_once()
