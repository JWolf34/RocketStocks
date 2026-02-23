"""Tests for data/schema.py DDL functions."""
from unittest.mock import MagicMock, patch, call


class TestCreateTables:
    def test_create_tables_executes_create_script(self):
        from rocketstocks.data.schema import create_tables
        mock_db = MagicMock()
        mock_cur = MagicMock()
        mock_db._cursor.return_value.__enter__ = lambda s: mock_cur
        mock_db._cursor.return_value.__exit__ = MagicMock(return_value=False)

        create_tables(mock_db)

        mock_cur.execute.assert_called_once()
        script = mock_cur.execute.call_args[0][0]
        assert 'CREATE TABLE IF NOT EXISTS tickers' in script
        assert 'CREATE TABLE IF NOT EXISTS popularity' in script
        assert 'CREATE TABLE IF NOT EXISTS alerts' in script

    def test_create_tables_creates_all_expected_tables(self):
        from rocketstocks.data.schema import _CREATE_SCRIPT
        expected_tables = [
            'tickers', 'upcoming_earnings', 'watchlists', 'popularity',
            'historical_earnings', 'reports', 'alerts', 'daily_price_history',
            'five_minute_price_history', 'ct_politicians',
        ]
        for table in expected_tables:
            assert table in _CREATE_SCRIPT, f"Missing table: {table}"


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
