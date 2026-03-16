"""Tests for data/db.py — async Postgres class."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


def _make_postgres():
    """Return a Postgres instance with a mocked pool."""
    with patch('rocketstocks.data.db.AsyncConnectionPool') as mock_pool_cls, \
         patch('rocketstocks.data.db.settings') as mock_settings:
        mock_settings.postgres_host = 'h'
        mock_settings.postgres_db = 'db'
        mock_settings.postgres_user = 'u'
        mock_settings.postgres_password = 'pw'
        mock_settings.postgres_port = 5432

        from rocketstocks.data.db import Postgres
        pg = Postgres(minconn=1, maxconn=2)
        pg._pool = mock_pool_cls.return_value
        return pg


# ---------------------------------------------------------------------------
# Pool initialisation
# ---------------------------------------------------------------------------

class TestPostgresInit:
    def test_pool_created_with_open_false(self):
        with patch('rocketstocks.data.db.AsyncConnectionPool') as mock_cls, \
             patch('rocketstocks.data.db.settings') as mock_settings:
            mock_settings.postgres_host = 'h'
            mock_settings.postgres_db = 'db'
            mock_settings.postgres_user = 'u'
            mock_settings.postgres_password = 'pw'
            mock_settings.postgres_port = 5432

            from rocketstocks.data.db import Postgres
            Postgres(minconn=3, maxconn=8)

            mock_cls.assert_called_once()
            _, kwargs = mock_cls.call_args
            assert kwargs['min_size'] == 3
            assert kwargs['max_size'] == 8
            assert kwargs['open'] is False


# ---------------------------------------------------------------------------
# open / close
# ---------------------------------------------------------------------------

class TestOpenClose:
    async def test_open_calls_pool_open(self):
        pg = _make_postgres()
        pg._pool.open = AsyncMock()
        await pg.open()
        pg._pool.open.assert_called_once()

    async def test_close_calls_pool_close(self):
        pg = _make_postgres()
        pg._pool.close = AsyncMock()
        await pg.close()
        pg._pool.close.assert_called_once()


# ---------------------------------------------------------------------------
# execute — SELECT returns rows
# ---------------------------------------------------------------------------

class TestExecuteSelect:
    async def test_returns_all_rows_by_default(self):
        pg = _make_postgres()
        mock_cur = MagicMock()
        mock_cur.description = [('col',)]
        mock_cur.fetchall = AsyncMock(return_value=[('AAPL',), ('TSLA',)])
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock(return_value=mock_cur)
        pg._pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pg._pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

        result = await pg.execute("SELECT ticker FROM tickers")
        assert result == [('AAPL',), ('TSLA',)]

    async def test_returns_one_row_when_fetchone(self):
        pg = _make_postgres()
        mock_cur = MagicMock()
        mock_cur.description = [('col',)]
        mock_cur.fetchone = AsyncMock(return_value=('AAPL',))
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock(return_value=mock_cur)
        pg._pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pg._pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

        result = await pg.execute("SELECT ticker FROM tickers LIMIT 1", fetchone=True)
        assert result == ('AAPL',)

    async def test_returns_none_for_dml(self):
        pg = _make_postgres()
        mock_cur = MagicMock()
        mock_cur.description = None  # DML has no description
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock(return_value=mock_cur)
        pg._pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pg._pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

        result = await pg.execute("INSERT INTO tickers VALUES (%s)", ['AAPL'])
        assert result is None

    async def test_passes_params_to_connection_execute(self):
        pg = _make_postgres()
        mock_cur = MagicMock()
        mock_cur.description = None
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock(return_value=mock_cur)
        pg._pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pg._pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

        await pg.execute("DELETE FROM tickers WHERE ticker = %s", ['AAPL'])
        mock_conn.execute.assert_called_once_with(
            "DELETE FROM tickers WHERE ticker = %s", ['AAPL']
        )


# ---------------------------------------------------------------------------
# execute_batch
# ---------------------------------------------------------------------------

class TestExecuteBatch:
    async def test_calls_executemany(self):
        pg = _make_postgres()
        mock_cur = AsyncMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__aenter__ = AsyncMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__aexit__ = AsyncMock(return_value=False)
        pg._pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pg._pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

        values = [('AAPL', 'Apple'), ('TSLA', 'Tesla')]
        await pg.execute_batch("INSERT INTO tickers (ticker, name) VALUES (%s, %s)", values)

        mock_cur.executemany.assert_called_once_with(
            "INSERT INTO tickers (ticker, name) VALUES (%s, %s)", values
        )


# ---------------------------------------------------------------------------
# transaction
# ---------------------------------------------------------------------------

class TestTransaction:
    async def test_yields_connection(self):
        pg = _make_postgres()
        mock_conn = MagicMock()
        mock_conn.transaction.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=False)
        pg._pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        pg._pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

        async with pg.transaction() as conn:
            assert conn is mock_conn
