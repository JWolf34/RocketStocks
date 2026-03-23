"""Tests for rocketstocks.data.watchlists.Watchlists."""
from unittest.mock import AsyncMock, MagicMock

import pytest

from rocketstocks.data.watchlists import Watchlists, NAMED, PERSONAL, SYSTEM, CLASSIFICATION


def _make(db=None):
    if db is None:
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
    return Watchlists(db)


# ---------------------------------------------------------------------------
# resolve_personal_id
# ---------------------------------------------------------------------------

class TestResolvePersonalId:
    def test_returns_prefixed_string(self):
        assert Watchlists.resolve_personal_id(12345) == "personal:12345"

    def test_large_discord_id(self):
        assert Watchlists.resolve_personal_id(123456789012345678) == "personal:123456789012345678"

    def test_different_users_produce_different_ids(self):
        assert Watchlists.resolve_personal_id(1) != Watchlists.resolve_personal_id(2)


# ---------------------------------------------------------------------------
# get_watchlist_tickers
# ---------------------------------------------------------------------------

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

    async def test_returns_empty_list_for_empty_tickers_string(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=("",))
        wl = _make(db)
        assert await wl.get_watchlist_tickers("empty-list") == []

    async def test_calls_execute_with_watchlist_id(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=("AAPL",))
        wl = _make(db)
        await wl.get_watchlist_tickers("my-list")
        db.execute.assert_called_once()
        params = db.execute.call_args[0][1]
        assert "my-list" in params


# ---------------------------------------------------------------------------
# get_all_watchlist_tickers
# ---------------------------------------------------------------------------

class TestGetAllWatchlistTickers:
    async def test_returns_tickers_for_requested_types(self):
        db = MagicMock()
        # Mock returns whatever the SQL WHERE would return — just named rows here
        db.execute = AsyncMock(return_value=[
            ("GOOG TSLA",),
        ])
        wl = _make(db)
        result = await wl.get_all_watchlist_tickers(watchlist_types=[NAMED])
        assert "GOOG" in result
        assert "TSLA" in result

    async def test_passes_types_as_sql_params(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[])
        wl = _make(db)
        await wl.get_all_watchlist_tickers(watchlist_types=[NAMED, PERSONAL])
        params = db.execute.call_args[0][1]
        assert NAMED in params
        assert PERSONAL in params

    async def test_defaults_to_named_only(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[])
        wl = _make(db)
        await wl.get_all_watchlist_tickers()
        params = db.execute.call_args[0][1]
        assert params == [NAMED]

    async def test_returns_sorted_unique_tickers(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            ("AAPL MSFT",),
            ("AAPL GOOG",),
        ])
        wl = _make(db)
        result = await wl.get_all_watchlist_tickers(watchlist_types=[NAMED])
        assert result == sorted(set(result))
        assert result.count("AAPL") == 1

    async def test_returns_empty_list_when_db_returns_none(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        result = await wl.get_all_watchlist_tickers()
        assert result == []


# ---------------------------------------------------------------------------
# get_watchlists
# ---------------------------------------------------------------------------

class TestGetWatchlists:
    async def test_includes_personal_entry_when_personal_in_types(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[("alpha", NAMED, None)])
        wl = _make(db)
        result = await wl.get_watchlists(watchlist_types=[NAMED, PERSONAL])
        assert "personal" in result

    async def test_does_not_include_personal_when_named_only(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[("alpha", NAMED, None)])
        wl = _make(db)
        result = await wl.get_watchlists(watchlist_types=[NAMED])
        # No personal rows returned, personal not in types
        assert "personal" not in result

    async def test_returns_sorted_list(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            ("zebra", NAMED, None),
            ("alpha", NAMED, None),
        ])
        wl = _make(db)
        result = await wl.get_watchlists(watchlist_types=[NAMED])
        assert result == sorted(result)

    async def test_deduplicates_personal_entries(self):
        db = MagicMock()
        # Two different users' personal watchlists — both translate to "personal"
        db.execute = AsyncMock(return_value=[
            ("personal:111", PERSONAL, None),
            ("personal:222", PERSONAL, None),
        ])
        wl = _make(db)
        result = await wl.get_watchlists(watchlist_types=[PERSONAL])
        assert result.count("personal") == 1

    async def test_returns_empty_list_when_db_returns_none(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        result = await wl.get_watchlists(watchlist_types=[NAMED])
        # NAMED in types but no rows; PERSONAL not in types → no "personal" appended
        assert result == []

    async def test_defaults_to_named_only(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[])
        wl = _make(db)
        await wl.get_watchlists()
        params = db.execute.call_args[0][1]
        assert params == [NAMED]


# ---------------------------------------------------------------------------
# get_ticker_to_watchlist_map
# ---------------------------------------------------------------------------

class TestGetTickerToWatchlistMap:
    async def test_returns_ticker_to_watchlist_name(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            ("mag7", "AAPL MSFT NVDA", NAMED, "mag7"),
            ("semiconductors", "NVDA AMD", NAMED, "semiconductors"),
        ])
        wl = _make(db)
        result = await wl.get_ticker_to_watchlist_map(watchlist_types=[NAMED])
        assert result["AAPL"] == "mag7"
        assert result["AMD"] == "semiconductors"
        # Last watchlist wins if ticker appears in multiple
        assert result["NVDA"] in {"mag7", "semiconductors"}

    async def test_personal_watchlist_maps_to_Personal(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            ("personal:12345", "TSLA GOOG", PERSONAL, None),
        ])
        wl = _make(db)
        result = await wl.get_ticker_to_watchlist_map(watchlist_types=[PERSONAL])
        assert result["TSLA"] == "Personal"
        assert result["GOOG"] == "Personal"

    async def test_returns_empty_dict_when_no_rows(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        result = await wl.get_ticker_to_watchlist_map()
        assert result == {}

    async def test_defaults_to_named_and_personal(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[])
        wl = _make(db)
        await wl.get_ticker_to_watchlist_map()
        params = db.execute.call_args[0][1]
        assert NAMED in params
        assert PERSONAL in params


# ---------------------------------------------------------------------------
# update_watchlist
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# create_watchlist
# ---------------------------------------------------------------------------

class TestCreateWatchlist:
    async def test_calls_db_execute(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.create_watchlist("my-list", ["AAPL", "MSFT"], watchlist_type=NAMED)
        db.execute.assert_called_once()

    async def test_sql_contains_insert_into_watchlists(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.create_watchlist("my-list", ["AAPL", "MSFT"], watchlist_type=NAMED)
        sql = db.execute.call_args[0][0]
        assert "INSERT INTO watchlists" in sql

    async def test_passes_id_and_tickers(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.create_watchlist("my-list", ["AAPL", "MSFT"], watchlist_type=NAMED)
        params = db.execute.call_args[0][1]
        assert "my-list" in params
        assert "AAPL MSFT" in params

    async def test_system_type_sets_systemgenerated_true(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.create_watchlist("__sentinel__", [], watchlist_type=SYSTEM)
        params = db.execute.call_args[0][1]
        assert True in params   # systemgenerated

    async def test_named_type_sets_systemgenerated_false(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.create_watchlist("my-list", ["AAPL"], watchlist_type=NAMED)
        params = db.execute.call_args[0][1]
        assert False in params   # systemgenerated

    async def test_personal_type_includes_owner_id(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.create_watchlist("personal:12345", [], watchlist_type=PERSONAL, owner_id=12345)
        params = db.execute.call_args[0][1]
        assert 12345 in params

    async def test_legacy_systemGenerated_false(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.create_watchlist("my-list", ["AAPL"], systemGenerated=False)
        params = db.execute.call_args[0][1]
        assert False in params  # systemgenerated computed from type

    async def test_legacy_systemGenerated_true(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        await wl.create_watchlist("__seed__", [], systemGenerated=True)
        params = db.execute.call_args[0][1]
        assert True in params  # systemgenerated


# ---------------------------------------------------------------------------
# delete_watchlist
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# validate_watchlist
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# rename_watchlist
# ---------------------------------------------------------------------------

class TestRenameWatchlist:
    def _make_rename_db(self, old_exists: bool, new_exists: bool):
        """Build a db mock for rename_watchlist."""
        db = MagicMock()

        validate_side_effects = [
            ("old-list",) if old_exists else None,
            ("new-list",) if new_exists else None,
        ]
        mock_cur = MagicMock()
        mock_cur.fetchone = AsyncMock(return_value=("AAPL MSFT", False, NAMED, None, "old-list"))
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock(return_value=mock_cur)

        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        db.transaction.return_value = mock_ctx

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
        insert_params = calls[1][0][1]
        assert "new-list" in insert_params

    async def test_delete_uses_old_id(self):
        db, mock_conn = self._make_rename_db(old_exists=True, new_exists=False)
        wl = _make(db)
        await wl.rename_watchlist("old-list", "new-list")
        calls = mock_conn.execute.call_args_list
        delete_params = calls[2][0][1]
        assert "old-list" in delete_params

    async def test_preserves_tickers_and_systemgenerated(self):
        db, mock_conn = self._make_rename_db(old_exists=True, new_exists=False)
        mock_conn.execute.return_value.fetchone = AsyncMock(
            return_value=("AAPL MSFT", True, SYSTEM, None, "old-list")
        )
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


# ---------------------------------------------------------------------------
# get_classification_overrides
# ---------------------------------------------------------------------------

class TestGetClassificationOverrides:
    async def test_returns_overrides_from_classification_type_watchlists(self):
        db = MagicMock()
        # New query returns (id, tickers, display_name) for watchlist_type='classification'
        db.execute = AsyncMock(return_value=[
            ("class:volatile", "GME AMC", "volatile"),
        ])
        wl = _make(db)
        result = await wl.get_classification_overrides()
        assert result["GME"] == "volatile"
        assert result["AMC"] == "volatile"

    async def test_uses_display_name_as_category(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            ("class:growth", "NVDA", "growth"),
        ])
        wl = _make(db)
        result = await wl.get_classification_overrides()
        assert result["NVDA"] == "growth"

    async def test_falls_back_to_id_prefix_when_no_display_name(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[
            ("class:meme", "GME", None),
        ])
        wl = _make(db)
        result = await wl.get_classification_overrides()
        assert result["GME"] == "meme"

    async def test_returns_empty_dict_when_no_rows(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=None)
        wl = _make(db)
        result = await wl.get_classification_overrides()
        assert result == {}

    async def test_sql_filters_by_classification_type(self):
        db = MagicMock()
        db.execute = AsyncMock(return_value=[])
        wl = _make(db)
        await wl.get_classification_overrides()
        sql = db.execute.call_args[0][0]
        assert "classification" in sql
