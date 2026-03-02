"""Tests for TickerStatsRepository."""
import pytest
from unittest.mock import MagicMock, call

from rocketstocks.data.ticker_stats import TickerStatsRepository


def _make_repo():
    db = MagicMock()
    return TickerStatsRepository(db=db), db


class TestUpsertStats:
    def test_executes_upsert_sql(self):
        repo, db = _make_repo()
        ctx = MagicMock()
        db._cursor.return_value.__enter__ = MagicMock(return_value=ctx)
        db._cursor.return_value.__exit__ = MagicMock(return_value=False)

        repo.upsert_stats('AAPL', {'classification': 'blue_chip', 'volatility_20d': 1.2})

        ctx.execute.assert_called_once()
        sql_text, vals = ctx.execute.call_args[0]
        assert 'INSERT INTO' in sql_text
        assert 'ticker_stats' in sql_text
        assert 'AAPL' in vals

    def test_does_not_include_ticker_in_stats_dict(self):
        """ticker key in stats_dict should be stripped to avoid duplicate column."""
        repo, db = _make_repo()
        ctx = MagicMock()
        db._cursor.return_value.__enter__ = MagicMock(return_value=ctx)
        db._cursor.return_value.__exit__ = MagicMock(return_value=False)

        repo.upsert_stats('AAPL', {'ticker': 'AAPL', 'classification': 'standard'})

        sql_text, vals = ctx.execute.call_args[0]
        # The ticker should appear only once (the explicit one), not twice via stats_dict
        assert vals.count('AAPL') == 1


class TestGetStats:
    def test_returns_none_when_not_found(self):
        repo, db = _make_repo()
        db.select.return_value = None
        result = repo.get_stats('ZZZZ')
        assert result is None

    def test_returns_dict_when_found(self):
        repo, db = _make_repo()
        # Mock a return row matching the _FIELDS length
        from rocketstocks.data.ticker_stats import _FIELDS
        row = tuple([None] * len(_FIELDS))
        db.select.return_value = row
        result = repo.get_stats('AAPL')
        assert isinstance(result, dict)
        assert 'ticker' in result


class TestGetClassification:
    def test_returns_standard_when_not_found(self):
        repo, db = _make_repo()
        db.select.return_value = None
        cls = repo.get_classification('UNKNOWN')
        assert cls == 'standard'

    def test_returns_stored_classification(self):
        repo, db = _make_repo()
        db.select.return_value = ('blue_chip',)
        cls = repo.get_classification('AAPL')
        assert cls == 'blue_chip'


class TestGetAllClassifications:
    def test_returns_empty_dict_when_no_rows(self):
        repo, db = _make_repo()
        db.select.return_value = []
        result = repo.get_all_classifications()
        assert result == {}

    def test_returns_ticker_classification_mapping(self):
        repo, db = _make_repo()
        db.select.return_value = [('AAPL', 'blue_chip'), ('GME', 'meme')]
        result = repo.get_all_classifications()
        assert result['AAPL'] == 'blue_chip'
        assert result['GME'] == 'meme'

    def test_returns_none_as_empty_dict(self):
        repo, db = _make_repo()
        db.select.return_value = None
        result = repo.get_all_classifications()
        assert result == {}


class TestGetAllStats:
    def test_returns_empty_list_when_no_rows(self):
        repo, db = _make_repo()
        db.select.return_value = []
        result = repo.get_all_stats()
        assert result == []

    def test_returns_list_of_dicts(self):
        repo, db = _make_repo()
        from rocketstocks.data.ticker_stats import _FIELDS
        row = tuple([None] * len(_FIELDS))
        db.select.return_value = [row, row]
        result = repo.get_all_stats()
        assert len(result) == 2
        assert all(isinstance(r, dict) for r in result)
