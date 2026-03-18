"""Tests for rocketstocks.data.watchlists.Watchlists."""
from unittest.mock import AsyncMock, MagicMock

import pytest

from rocketstocks.data.watchlists import Watchlists


def _make(db=None):
    if db is None:
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
    return Watchlists(db)


class TestGetWatchlistTickers:
    async def test_returns_sorted_tickers(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=("TSLA AAPL MSFT",))
        wl = _make(db)
        result = await wl.get_watchlist_tickers("my-list")
        assert result == ["AAPL", "MSFT", "TSLA"]

    async def test_returns_empty_list_when_not_found(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        assert await wl.get_watchlist_tickers("nonexistent") == []

    async def test_calls_execute_with_watchlist_id(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=("AAPL",))
        wl = _make(db)
        await wl.get_watchlist_tickers("my-list")
        db.execute.assert_called_once()
        params = db.execute.call_args[0][1]
        assert "my-list" in params


class TestGetAllWatchlistTickers:
    async def test_excludes_personal_watchlists(self):
        db = MagicMock()
        # "123" is numeric → personal; "global" is not
        db.execute = AsyncMock(return_value=[
            ("123", "AAPL MSFT", False),
            ("global", "GOOG TSLA", False),
        ])
        wl = _make(db)
        result = await wl.get_all_watchlist_tickers(no_personal=True, no_systemGenerated=False)
        assert "GOOG" in result
        assert "TSLA" in result
        assert "AAPL" not in result

    async def test_excludes_system_generated(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            ("alpha", "AAPL", True),    # system-generated
            ("beta", "MSFT", False),    # normal
        ])
        wl = _make(db)
        result = await wl.get_all_watchlist_tickers(no_personal=False, no_systemGenerated=True)
        assert "MSFT" in result
        assert "AAPL" not in result

    async def test_returns_sorted_unique_tickers(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            ("alpha", "AAPL MSFT", False),
            ("beta", "AAPL GOOG", False),
        ])
        wl = _make(db)
        result = await wl.get_all_watchlist_tickers(no_personal=False, no_systemGenerated=False)
        assert result == sorted(set(result))
        assert result.count("AAPL") == 1

    async def test_returns_empty_list_when_db_returns_none(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        result = await wl.get_all_watchlist_tickers()
        assert result == []


class TestGetWatchlists:
    async def test_appends_personal_when_no_personal_true(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[("alpha", "AAPL", False)])
        wl = _make(db)
        result = await wl.get_watchlists(no_personal=True, no_systemGenerated=False)
        assert "personal" in result

    async def test_returns_sorted_list(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            ("zebra", "TSLA", False),
            ("alpha", "AAPL", False),
        ])
        wl = _make(db)
        result = await wl.get_watchlists(no_personal=False, no_systemGenerated=False)
        assert result == sorted(result)

    async def test_excludes_system_generated_when_flag_set(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            ("syslist", "AAPL", True),
            ("userlist", "MSFT", False),
        ])
        wl = _make(db)
        result = await wl.get_watchlists(no_personal=False, no_systemGenerated=True)
        assert "userlist" in result
        assert "syslist" not in result

    async def test_returns_empty_list_when_db_returns_none(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        result = await wl.get_watchlists(no_personal=False, no_systemGenerated=False)
        assert result == []


class TestUpdateWatchlist:
    async def test_calls_db_execute(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.update_watchlist("my-list", ["AAPL", "MSFT"])
        db.execute.assert_called_once()

    async def test_joins_tickers_with_space(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.update_watchlist("my-list", ["AAPL", "MSFT"])
        params = db.execute.call_args[0][1]
        assert "AAPL MSFT" in params

    async def test_passes_watchlist_id_in_params(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.update_watchlist("my-list", ["AAPL", "MSFT"])
        params = db.execute.call_args[0][1]
        assert "my-list" in params

    async def test_sql_targets_watchlists_table(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.update_watchlist("my-list", ["AAPL"])
        sql = db.execute.call_args[0][0]
        assert "watchlists" in sql.lower()


class TestCreateWatchlist:
    async def test_calls_db_execute(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.create_watchlist("my-list", ["AAPL", "MSFT"], False)
        db.execute.assert_called_once()

    async def test_sql_contains_insert_into_watchlists(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.create_watchlist("my-list", ["AAPL", "MSFT"], False)
        sql = db.execute.call_args[0][0]
        assert "INSERT INTO watchlists" in sql

    async def test_passes_id_tickers_and_systemgenerated(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.create_watchlist("my-list", ["AAPL", "MSFT"], False)
        params = db.execute.call_args[0][1]
        assert "my-list" in params
        assert "AAPL MSFT" in params
        assert False in params


class TestDeleteWatchlist:
    async def test_calls_db_execute(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.delete_watchlist("my-list")
        db.execute.assert_called_once()

    async def test_passes_watchlist_id_in_params(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.delete_watchlist("my-list")
        params = db.execute.call_args[0][1]
        assert "my-list" in params

    async def test_sql_is_delete_from_watchlists(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.delete_watchlist("my-list")
        sql = db.execute.call_args[0][0]
        assert "DELETE" in sql.upper()
        assert "watchlists" in sql.lower()


class TestValidateWatchlist:
    async def test_returns_true_when_found(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=("my-list",))
        wl = _make(db)
        assert await wl.validate_watchlist("my-list") is True

    async def test_returns_false_when_not_found(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        assert await wl.validate_watchlist("nonexistent") is False


class TestRenameWatchlist:
    def _make_rename_db(self, old_exists: bool, new_exists: bool):
        """
        Build a db mock for rename_watchlist.

        validate_watchlist calls db.execute(SELECT id..., fetchone=True):
          - first call: old_id check → ("old-list",) if old_exists else None
          - second call: new_id check → ("new-list",) if new_exists else None

        The transaction context manager yields a conn mock whose .execute()
        returns a cursor mock with .fetchone() supplying the row data.
        """
        db = MagicMock()

        validate_side_effects = [
            ("old-list",) if old_exists else None,
            ("new-list",) if new_exists else None,
        ]
        mock_cur = MagicMock()
        mock_cur.fetchone = AsyncMock(return_value=("AAPL MSFT", False))
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock(return_value=mock_cur)

        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        db.transaction.return_value = mock_ctx

        # validate_watchlist uses fetchone=True; the transaction SELECT/INSERT/DELETE
        # use conn.execute. We wire db.execute for the validate calls only.
        db.execute = AsyncMock(side_effect=validate_side_effects)

        return db, mock_conn

    async def test_returns_true_on_success(self):
        db, _ = self._make_rename_db(old_exists=True, new_exists=False)
        wl = _make(db)
        result = await wl.rename_watchlist("old-list", "new-list")
        assert result is True

    async def test_inserts_new_and_deletes_old_via_transaction(self):
        db, mock_conn = self._make_rename_db(old_exists=True, new_exists=False)
        wl = _make(db)
        await wl.rename_watchlist("old-list", "new-list")
        # conn.execute should have been called 3 times: SELECT, INSERT, DELETE
        assert mock_conn.execute.call_count == 3

    async def test_insert_uses_new_id(self):
        db, mock_conn = self._make_rename_db(old_exists=True, new_exists=False)
        wl = _make(db)
        await wl.rename_watchlist("old-list", "new-list")
        calls = mock_conn.execute.call_args_list
        # Second call is the INSERT
        insert_params = calls[1][0][1]
        assert "new-list" in insert_params

    async def test_delete_uses_old_id(self):
        db, mock_conn = self._make_rename_db(old_exists=True, new_exists=False)
        wl = _make(db)
        await wl.rename_watchlist("old-list", "new-list")
        calls = mock_conn.execute.call_args_list
        # Third call is the DELETE
        delete_params = calls[2][0][1]
        assert "old-list" in delete_params

    async def test_preserves_tickers_and_systemgenerated(self):
        db, mock_conn = self._make_rename_db(old_exists=True, new_exists=False)
        # Override fetchone to return custom data
        mock_conn.execute.return_value.fetchone = AsyncMock(return_value=("AAPL MSFT", True))
        wl = _make(db)
        await wl.rename_watchlist("old-list", "new-list")
        calls = mock_conn.execute.call_args_list
        insert_params = calls[1][0][1]
        assert "AAPL MSFT" in insert_params
        assert True in insert_params

    async def test_returns_false_when_old_does_not_exist(self):
        db, mock_conn = self._make_rename_db(old_exists=False, new_exists=False)
        wl = _make(db)
        result = await wl.rename_watchlist("ghost", "new-list")
        assert result is False
        mock_conn.execute.assert_not_called()

    async def test_returns_false_when_new_already_exists(self):
        db, mock_conn = self._make_rename_db(old_exists=True, new_exists=True)
        wl = _make(db)
        result = await wl.rename_watchlist("old-list", "existing")
        assert result is False
        mock_conn.execute.assert_not_called()


class TestGetClassificationOverrides:
    async def test_returns_overrides_from_class_prefixed_watchlists(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            ("class:volatile", "GME AMC"),
            ("other", "AAPL"),
        ])
        wl = _make(db)
        result = await wl.get_classification_overrides()
        assert result["GME"] == "volatile"
        assert result["AMC"] == "volatile"
        assert "AAPL" not in result

    async def test_ignores_non_class_watchlists(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            ("regular", "TSLA MSFT"),
            ("class:growth", "NVDA"),
        ])
        wl = _make(db)
        result = await wl.get_classification_overrides()
        assert "TSLA" not in result
        assert "MSFT" not in result
        assert result["NVDA"] == "growth"

    async def test_returns_empty_dict_when_no_rows(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        result = await wl.get_classification_overrides()
        assert result == {}

    async def test_returns_empty_dict_when_no_class_watchlists(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            ("alpha", "AAPL MSFT"),
            ("beta", "GOOG"),
        ])
        wl = _make(db)
        result = await wl.get_classification_overrides()
        assert result == {}
