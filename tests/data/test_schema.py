"""Tests for data/schema.py DDL functions."""
from unittest.mock import AsyncMock, MagicMock


class TestCreateTables:
    async def test_create_tables_calls_execute_twice(self):
        """create_tables calls execute for CREATE script + migration script."""
        from rocketstocks.data.schema import create_tables
        mock_db = MagicMock()
        mock_db.execute = AsyncMock(return_value=None)

        await create_tables(mock_db)

        assert mock_db.execute.call_count == 2

    async def test_create_tables_executes_create_script(self):
        from rocketstocks.data.schema import create_tables
        mock_db = MagicMock()
        mock_db.execute = AsyncMock(return_value=None)

        await create_tables(mock_db)

        create_script = mock_db.execute.call_args_list[0][0][0]
        assert 'CREATE TABLE IF NOT EXISTS tickers' in create_script
        assert 'CREATE TABLE IF NOT EXISTS popularity' in create_script
        assert 'CREATE TABLE IF NOT EXISTS alerts' in create_script

    def test_create_tables_creates_all_expected_tables(self):
        from rocketstocks.data.schema import _CREATE_SCRIPT
        expected_tables = [
            'tickers', 'upcoming_earnings', 'watchlists', 'popularity',
            'historical_earnings', 'reports', 'alerts', 'daily_price_history',
            'five_minute_price_history', 'ct_politicians',
        ]
        for table in expected_tables:
            assert table in _CREATE_SCRIPT, f"Missing table: {table}"

    def test_tickers_schema_has_new_columns(self):
        from rocketstocks.data.schema import _CREATE_SCRIPT
        for col in ('exchange', 'security_type', 'sic_code', 'delist_date'):
            assert col in _CREATE_SCRIPT, f"Missing column in tickers schema: {col}"

    def test_tickers_schema_no_url_column(self):
        from rocketstocks.data.schema import _CREATE_SCRIPT
        import re
        match = re.search(r'CREATE TABLE IF NOT EXISTS tickers \((.+?)\);', _CREATE_SCRIPT, re.DOTALL)
        assert match, "Tickers table not found in CREATE script"
        assert 'url' not in match.group(1)

    def test_migration_script_drops_url_and_adds_columns(self):
        from rocketstocks.data.schema import _MIGRATION_SCRIPT
        assert 'DROP COLUMN IF EXISTS url' in _MIGRATION_SCRIPT
        for col in ('exchange', 'security_type', 'sic_code', 'delist_date'):
            assert col in _MIGRATION_SCRIPT

    def test_alerts_table_uses_jsonb(self):
        from rocketstocks.data.schema import _CREATE_SCRIPT
        import re
        match = re.search(r'CREATE TABLE IF NOT EXISTS alerts \((.+?)\);', _CREATE_SCRIPT, re.DOTALL)
        assert match
        assert 'jsonb' in match.group(1).lower()

    def test_market_signals_table_uses_jsonb(self):
        from rocketstocks.data.schema import _CREATE_SCRIPT
        import re
        match = re.search(r'CREATE TABLE IF NOT EXISTS market_signals \((.+?)\);', _CREATE_SCRIPT, re.DOTALL)
        assert match
        assert 'jsonb' in match.group(1).lower()

    def test_migration_script_alters_jsonb_columns(self):
        from rocketstocks.data.schema import _MIGRATION_SCRIPT
        assert 'alert_data' in _MIGRATION_SCRIPT
        assert 'signal_data' in _MIGRATION_SCRIPT
        assert 'jsonb' in _MIGRATION_SCRIPT.lower()


class TestDropAllTables:
    async def test_drop_all_tables_calls_execute(self):
        from rocketstocks.data.schema import drop_all_tables
        mock_db = MagicMock()
        mock_db.execute = AsyncMock(return_value=None)

        await drop_all_tables(mock_db)

        mock_db.execute.assert_called_once()
        script = mock_db.execute.call_args[0][0]
        assert 'DROP TABLE' in script


class TestDropTable:
    async def test_drop_table_calls_execute(self):
        from rocketstocks.data.schema import drop_table
        mock_db = MagicMock()
        mock_db.execute = AsyncMock(return_value=None)

        await drop_table(mock_db, 'alerts')

        mock_db.execute.assert_called_once()
