"""Tests for data/schema.py DDL functions."""
from unittest.mock import MagicMock


class TestCreateTables:
    def test_create_tables_executes_create_script(self):
        from rocketstocks.data.schema import create_tables
        mock_db = MagicMock()
        mock_cur = MagicMock()
        mock_db._cursor.return_value.__enter__ = lambda s: mock_cur
        mock_db._cursor.return_value.__exit__ = MagicMock(return_value=False)

        create_tables(mock_db)

        # create_tables calls execute twice: CREATE script + MIGRATION script
        assert mock_cur.execute.call_count == 2
        create_script = mock_cur.execute.call_args_list[0][0][0]
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
        # url column should NOT be in the CREATE TABLE (dropped via migration)
        import re
        # extract tickers table block
        match = re.search(r'CREATE TABLE IF NOT EXISTS tickers \((.+?)\);', _CREATE_SCRIPT, re.DOTALL)
        assert match, "Tickers table not found in CREATE script"
        assert 'url' not in match.group(1)

    def test_migration_script_drops_url_and_adds_columns(self):
        from rocketstocks.data.schema import _MIGRATION_SCRIPT
        assert 'DROP COLUMN IF EXISTS url' in _MIGRATION_SCRIPT
        for col in ('exchange', 'security_type', 'sic_code', 'delist_date'):
            assert col in _MIGRATION_SCRIPT


class TestDropAllTables:
    def test_drop_all_tables_executes_drop_script(self):
        from rocketstocks.data.schema import drop_all_tables
        mock_db = MagicMock()
        mock_cur = MagicMock()
        mock_db._cursor.return_value.__enter__ = lambda s: mock_cur
        mock_db._cursor.return_value.__exit__ = MagicMock(return_value=False)

        drop_all_tables(mock_db)

        mock_cur.execute.assert_called_once()
        script = mock_cur.execute.call_args[0][0]
        assert 'DROP TABLE' in script


class TestDropTable:
    def test_drop_table_uses_identifier(self):
        from rocketstocks.data.schema import drop_table
        mock_db = MagicMock()
        mock_cur = MagicMock()
        mock_db._cursor.return_value.__enter__ = lambda s: mock_cur
        mock_db._cursor.return_value.__exit__ = MagicMock(return_value=False)

        drop_table(mock_db, 'alerts')

        mock_cur.execute.assert_called_once()
