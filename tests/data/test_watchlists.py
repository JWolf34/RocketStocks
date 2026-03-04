"""Tests for rocketstocks.data.watchlists.Watchlists."""
from unittest.mock import MagicMock
import pytest

from rocketstocks.data.watchlists import Watchlists


def _make(db=None):
    return Watchlists(db or MagicMock())


class TestGetWatchlistTickers:
    def test_returns_sorted_tickers(self):
        db = MagicMock()
        db.select.return_value = ("TSLA AAPL MSFT",)
        wl = _make(db)
        result = wl.get_watchlist_tickers("my-list")
        assert result == ["AAPL", "MSFT", "TSLA"]

    def test_returns_none_when_not_found(self):
        db = MagicMock()
        db.select.return_value = None
        wl = _make(db)
        assert wl.get_watchlist_tickers("nonexistent") is None

    def test_passes_correct_where_condition(self):
        db = MagicMock()
        db.select.return_value = ("AAPL",)
        wl = _make(db)
        wl.get_watchlist_tickers("my-list")
        call_kwargs = db.select.call_args[1]
        assert ("id", "my-list") in call_kwargs["where_conditions"]


class TestGetAllWatchlistTickers:
    def test_excludes_personal_watchlists(self):
        db = MagicMock()
        # "123" is numeric → personal; "global" is not
        db.select.return_value = [
            ("123", "AAPL MSFT", False),
            ("global", "GOOG TSLA", False),
        ]
        wl = _make(db)
        result = wl.get_all_watchlist_tickers(no_personal=True, no_systemGenerated=False)
        assert "GOOG" in result
        assert "TSLA" in result
        assert "AAPL" not in result

    def test_excludes_system_generated(self):
        db = MagicMock()
        db.select.return_value = [
            ("alpha", "AAPL", True),    # system-generated
            ("beta", "MSFT", False),    # normal
        ]
        wl = _make(db)
        result = wl.get_all_watchlist_tickers(no_personal=False, no_systemGenerated=True)
        assert "MSFT" in result
        assert "AAPL" not in result

    def test_returns_sorted_unique_tickers(self):
        db = MagicMock()
        db.select.return_value = [
            ("alpha", "AAPL MSFT", False),
            ("beta", "AAPL GOOG", False),
        ]
        wl = _make(db)
        result = wl.get_all_watchlist_tickers(no_personal=False, no_systemGenerated=False)
        assert result == sorted(set(result))
        assert result.count("AAPL") == 1


class TestGetWatchlists:
    def test_appends_personal_when_no_personal_true(self):
        db = MagicMock()
        db.select.return_value = [("alpha", "AAPL", False)]
        wl = _make(db)
        result = wl.get_watchlists(no_personal=True, no_systemGenerated=False)
        assert "personal" in result

    def test_returns_sorted_list(self):
        db = MagicMock()
        db.select.return_value = [
            ("zebra", "TSLA", False),
            ("alpha", "AAPL", False),
        ]
        wl = _make(db)
        result = wl.get_watchlists(no_personal=False, no_systemGenerated=False)
        assert result == sorted(result)


class TestUpdateWatchlist:
    def test_calls_db_update_with_joined_tickers(self):
        db = MagicMock()
        wl = _make(db)
        wl.update_watchlist("my-list", ["AAPL", "MSFT"])
        db.update.assert_called_once()
        call_kwargs = db.update.call_args[1]
        assert call_kwargs["table"] == "watchlists"
        set_fields = call_kwargs["set_fields"]
        assert any("AAPL MSFT" == v for _, v in set_fields)


class TestCreateWatchlist:
    def test_calls_db_insert(self):
        db = MagicMock()
        wl = _make(db)
        wl.create_watchlist("my-list", ["AAPL", "MSFT"], False)
        db.insert.assert_called_once()
        call_kwargs = db.insert.call_args[1]
        assert call_kwargs["table"] == "watchlists"


class TestDeleteWatchlist:
    def test_calls_db_delete_with_id(self):
        db = MagicMock()
        wl = _make(db)
        wl.delete_watchlist("my-list")
        db.delete.assert_called_once()
        call_kwargs = db.delete.call_args[1]
        assert ("id", "my-list") in call_kwargs["where_conditions"]


class TestValidateWatchlist:
    def test_returns_true_when_found(self):
        db = MagicMock()
        db.select.return_value = ("my-list",)
        wl = _make(db)
        assert wl.validate_watchlist("my-list") is True

    def test_returns_false_when_not_found(self):
        db = MagicMock()
        db.select.return_value = None
        wl = _make(db)
        assert wl.validate_watchlist("nonexistent") is False


class TestRenameWatchlist:
    def _make_with_validate(self, old_exists: bool, new_exists: bool):
        db = MagicMock()
        wl = _make(db)
        # validate_watchlist is called twice: once for old_id, once for new_id
        wl.validate_watchlist = MagicMock(side_effect=[old_exists, new_exists])
        # Provide a row for the select call when old exists
        db.select.return_value = ("AAPL MSFT", False)
        return wl, db

    def test_returns_true_on_success(self):
        wl, db = self._make_with_validate(old_exists=True, new_exists=False)
        result = wl.rename_watchlist("old-list", "new-list")
        assert result is True

    def test_inserts_new_row_and_deletes_old(self):
        wl, db = self._make_with_validate(old_exists=True, new_exists=False)
        wl.rename_watchlist("old-list", "new-list")
        db.insert.assert_called_once()
        db.delete.assert_called_once()
        delete_kwargs = db.delete.call_args[1]
        assert ("id", "old-list") in delete_kwargs["where_conditions"]

    def test_new_id_used_in_insert(self):
        wl, db = self._make_with_validate(old_exists=True, new_exists=False)
        wl.rename_watchlist("old-list", "new-list")
        insert_kwargs = db.insert.call_args[1]
        values = insert_kwargs["values"]
        assert values[0][0] == "new-list"

    def test_returns_false_when_old_does_not_exist(self):
        wl, db = self._make_with_validate(old_exists=False, new_exists=False)
        result = wl.rename_watchlist("ghost", "new-list")
        assert result is False
        db.insert.assert_not_called()

    def test_returns_false_when_new_already_exists(self):
        wl, db = self._make_with_validate(old_exists=True, new_exists=True)
        result = wl.rename_watchlist("old-list", "existing")
        assert result is False
        db.insert.assert_not_called()

    def test_preserves_tickers_and_systemgenerated(self):
        wl, db = self._make_with_validate(old_exists=True, new_exists=False)
        db.select.return_value = ("AAPL MSFT", True)
        wl.rename_watchlist("old-list", "new-list")
        insert_kwargs = db.insert.call_args[1]
        values = insert_kwargs["values"]
        # values[0] = (new_id, tickers_str, system_generated)
        assert values[0][1] == "AAPL MSFT"
        assert values[0][2] is True
