"""Tests for data/db.py — Postgres class with connection pooling."""
import pytest
from unittest.mock import MagicMock, patch, call
from contextlib import contextmanager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_postgres():
    """Return a Postgres instance with a mocked pool."""
    with patch('rocketstocks.data.db.ThreadedConnectionPool') as mock_pool_cls:
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool

        from rocketstocks.data.db import Postgres
        pg = Postgres(minconn=1, maxconn=2)
        pg._pool = mock_pool
        return pg, mock_pool


def _cursor_ctx(pg, mock_pool, rows=None, rowcount=None):
    """Configure mock pool so _cursor() yields a usable cursor."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = rows or []
    mock_cur.fetchone.return_value = rows[0] if rows else None
    mock_conn.cursor.return_value.__enter__ = lambda s: mock_cur
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_pool.getconn.return_value = mock_conn
    return mock_cur, mock_conn


# ---------------------------------------------------------------------------
# Pool initialisation
# ---------------------------------------------------------------------------

class TestPostgresInit:
    def test_pool_created_with_correct_params(self):
        with patch('rocketstocks.data.db.ThreadedConnectionPool') as mock_cls, \
             patch('rocketstocks.data.db.secrets') as mock_secrets:
            mock_secrets.db_host = 'h'
            mock_secrets.db_name = 'db'
            mock_secrets.db_user = 'u'
            mock_secrets.db_password = 'pw'
            mock_secrets.db_port = 5432

            from rocketstocks.data.db import Postgres
            Postgres(minconn=3, maxconn=8)

            mock_cls.assert_called_once_with(
                3, 8, host='h', dbname='db', user='u', password='pw', port=5432
            )

    def test_default_port_fallback(self):
        with patch('rocketstocks.data.db.ThreadedConnectionPool') as mock_cls, \
             patch('rocketstocks.data.db.secrets') as mock_secrets:
            mock_secrets.db_host = 'h'
            mock_secrets.db_name = 'db'
            mock_secrets.db_user = 'u'
            mock_secrets.db_password = 'pw'
            mock_secrets.db_port = None  # falsy → should default to 5432

            from rocketstocks.data.db import Postgres
            Postgres()

            _, kwargs = mock_cls.call_args
            assert kwargs['port'] == 5432


# ---------------------------------------------------------------------------
# _cursor context manager
# ---------------------------------------------------------------------------

class TestCursorContextManager:
    def test_commits_on_success(self):
        pg, mock_pool = _make_postgres()
        mock_cur, mock_conn = _cursor_ctx(pg, mock_pool)

        with pg._cursor():
            pass

        mock_conn.commit.assert_called_once()
        mock_pool.putconn.assert_called_once_with(mock_conn)

    def test_rolls_back_and_reraises_on_exception(self):
        pg, mock_pool = _make_postgres()
        mock_cur, mock_conn = _cursor_ctx(pg, mock_pool)

        with pytest.raises(ValueError):
            with pg._cursor():
                raise ValueError("boom")

        mock_conn.rollback.assert_called_once()
        mock_pool.putconn.assert_called_once_with(mock_conn)

    def test_connection_always_returned_to_pool(self):
        pg, mock_pool = _make_postgres()
        mock_cur, mock_conn = _cursor_ctx(pg, mock_pool)

        try:
            with pg._cursor():
                raise RuntimeError("oops")
        except RuntimeError:
            pass

        mock_pool.putconn.assert_called_once()


# ---------------------------------------------------------------------------
# where_clauses
# ---------------------------------------------------------------------------

class TestWhereClauses:
    def _pg(self):
        pg, _ = _make_postgres()
        return pg

    def test_two_element_condition_uses_equals(self):
        from psycopg2 import sql
        pg = self._pg()
        script, vals = pg.where_clauses([('ticker', 'AAPL')])
        assert vals == ('AAPL',)

    def test_three_element_condition_uses_operator(self):
        pg = self._pg()
        script, vals = pg.where_clauses([('date', '>', '2024-01-01')])
        assert vals == ('2024-01-01',)

    def test_disallowed_operator_raises(self):
        pg = self._pg()
        with pytest.raises(ValueError, match="Disallowed WHERE operator"):
            pg.where_clauses([('ticker', 'DROP TABLE--', 'x')])

    def test_allowed_operators_pass(self):
        pg = self._pg()
        for op in ('=', '!=', '<>', '<', '>', '<=', '>=', 'LIKE', 'ILIKE'):
            pg.where_clauses([('col', op, 'val')])  # should not raise

    def test_in_operator_uses_any_syntax(self):
        pg = self._pg()
        script, vals = pg.where_clauses([('ticker', 'IN', ['AAPL', 'TSLA'])])
        # repr() works without a DB connection; confirms the SQL template used
        assert '= ANY' in repr(script)
        assert vals == (['AAPL', 'TSLA'],)

    def test_not_in_operator_uses_all_syntax(self):
        pg = self._pg()
        script, vals = pg.where_clauses([('ticker', 'NOT IN', ['AAPL', 'TSLA'])])
        assert '<> ALL' in repr(script)
        assert vals == (['AAPL', 'TSLA'],)

    def test_in_empty_collection_raises(self):
        pg = self._pg()
        with pytest.raises(ValueError, match="non-empty"):
            pg.where_clauses([('ticker', 'IN', [])])

    def test_not_in_empty_collection_raises(self):
        pg = self._pg()
        with pytest.raises(ValueError, match="non-empty"):
            pg.where_clauses([('ticker', 'NOT IN', [])])


# ---------------------------------------------------------------------------
# select — ORDER BY validation
# ---------------------------------------------------------------------------

class TestSelectOrderBy:
    def test_invalid_order_direction_raises(self):
        pg, mock_pool = _make_postgres()
        _cursor_ctx(pg, mock_pool)

        with pytest.raises(ValueError, match="Invalid ORDER BY direction"):
            pg.select('tickers', ['ticker'], order_by=('ticker', 'INVALID'))

    def test_valid_asc_does_not_raise(self):
        pg, mock_pool = _make_postgres()
        _cursor_ctx(pg, mock_pool, rows=[('AAPL',)])
        # Should not raise
        pg.select('tickers', ['ticker'], order_by=('ticker', 'ASC'))

    def test_valid_desc_does_not_raise(self):
        pg, mock_pool = _make_postgres()
        _cursor_ctx(pg, mock_pool, rows=[('AAPL',)])
        pg.select('tickers', ['ticker'], order_by=('ticker', 'DESC'))


# ---------------------------------------------------------------------------
# get_table_columns — parameterized (B2 fix)
# ---------------------------------------------------------------------------

class TestGetTableColumns:
    def test_uses_parameterized_query(self):
        pg, mock_pool = _make_postgres()
        mock_cur, mock_conn = _cursor_ctx(pg, mock_pool, rows=[('col1',), ('col2',)])

        result = pg.get_table_columns('tickers')

        # The execute call should use %s placeholder, not f-string interpolation
        execute_args = mock_cur.execute.call_args
        sql_str, params = execute_args[0]
        assert '%s' in sql_str
        assert params == ('tickers',)
        assert result == ['col1', 'col2']


# ---------------------------------------------------------------------------
# Mutable default arg fix (B11)
# ---------------------------------------------------------------------------

class TestMutableDefaultArgs:
    def test_select_does_not_share_where_conditions_list(self):
        """Two consecutive calls should not share the same list object."""
        pg, mock_pool = _make_postgres()
        _cursor_ctx(pg, mock_pool, rows=[])

        import inspect
        from rocketstocks.data.db import Postgres
        sig = inspect.signature(Postgres.select)
        default = sig.parameters['where_conditions'].default
        # Default should be None (not a mutable list)
        assert default is None

    def test_update_does_not_share_where_conditions_list(self):
        from rocketstocks.data.db import Postgres
        import inspect
        sig = inspect.signature(Postgres.update)
        default = sig.parameters['where_conditions'].default
        assert default is None


# ---------------------------------------------------------------------------
# Bulk guard — update and delete without WHERE
# ---------------------------------------------------------------------------

class TestBulkGuard:
    def _pg(self):
        pg, _ = _make_postgres()
        return pg

    def test_update_without_where_raises(self):
        pg, mock_pool = _make_postgres()
        _cursor_ctx(pg, mock_pool)
        with pytest.raises(ValueError, match="force_bulk"):
            pg.update('tickers', [('name', 'Foo')])

    def test_update_none_where_raises(self):
        pg, mock_pool = _make_postgres()
        _cursor_ctx(pg, mock_pool)
        with pytest.raises(ValueError, match="force_bulk"):
            pg.update('tickers', [('name', 'Foo')], where_conditions=None)

    def test_update_force_bulk_allowed(self):
        pg, mock_pool = _make_postgres()
        _cursor_ctx(pg, mock_pool)
        # Should not raise
        pg.update('tickers', [('name', 'Foo')], force_bulk=True)

    def test_delete_empty_where_raises(self):
        pg, mock_pool = _make_postgres()
        _cursor_ctx(pg, mock_pool)
        with pytest.raises(ValueError, match="force_bulk"):
            pg.delete('tickers', [])

    def test_delete_force_bulk_allowed(self):
        pg, mock_pool = _make_postgres()
        _cursor_ctx(pg, mock_pool)
        # Should not raise
        pg.delete('tickers', [], force_bulk=True)


# ---------------------------------------------------------------------------
# select — LIMIT support
# ---------------------------------------------------------------------------

class TestSelectLimit:
    def test_limit_appended_when_provided(self):
        pg, mock_pool = _make_postgres()
        mock_cur, mock_conn = _cursor_ctx(pg, mock_pool, rows=[('AAPL',)])

        pg.select('tickers', ['ticker'], limit=10)

        execute_args = mock_cur.execute.call_args
        sql_obj, params = execute_args[0]
        # LIMIT value should be passed as a parameter
        assert 10 in params

    def test_no_limit_by_default(self):
        pg, mock_pool = _make_postgres()
        mock_cur, mock_conn = _cursor_ctx(pg, mock_pool, rows=[('AAPL',)])

        pg.select('tickers', ['ticker'])

        execute_args = mock_cur.execute.call_args
        _, params = execute_args[0]
        # No extra numeric params when limit is not used
        assert params == tuple()


# ---------------------------------------------------------------------------
# update — empty set_fields guard
# ---------------------------------------------------------------------------

class TestUpdateValidation:
    def test_empty_set_fields_raises(self):
        pg, mock_pool = _make_postgres()
        _cursor_ctx(pg, mock_pool)
        with pytest.raises(ValueError, match="at least one set_field"):
            pg.update('tickers', [], where_conditions=[('ticker', 'AAPL')])
