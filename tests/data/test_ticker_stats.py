"""Tests for TickerStatsRepository."""
import pytest
from unittest.mock import AsyncMock, MagicMock

from rocketstocks.data.ticker_stats import TickerStatsRepository, _FIELDS


def _make_repo():
    db = MagicMock()
    db.execute = AsyncMock()
    return TickerStatsRepository(db=db), db


class TestUpsertStats:
    async def test_executes_upsert_sql(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        await repo.upsert_stats('AAPL', {'classification': 'blue_chip', 'volatility_20d': 1.2})

        db.execute.assert_called_once()
        sql_text, params = db.execute.call_args[0]
        assert 'INSERT INTO ticker_stats' in sql_text
        assert 'ON CONFLICT' in sql_text
        assert params[0] == 'AAPL'

    async def test_does_not_include_ticker_in_stats_dict(self):
        """ticker key in stats_dict should be stripped to avoid duplicate column."""
        repo, db = _make_repo()
        db.execute.return_value = None

        await repo.upsert_stats('AAPL', {'ticker': 'AAPL', 'classification': 'standard'})

        sql_text, params = db.execute.call_args[0]
        # The ticker should appear only once (the explicit first param), not twice via stats_dict
        assert params.count('AAPL') == 1


class TestGetStats:
    async def test_returns_none_when_not_found(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        result = await repo.get_stats('ZZZZ')

        assert result is None

    async def test_returns_dict_when_found(self):
        repo, db = _make_repo()
        row = tuple([None] * len(_FIELDS))
        db.execute.return_value = row

        result = await repo.get_stats('AAPL')

        assert isinstance(result, dict)
        assert 'ticker' in result

    async def test_calls_execute_with_fetchone(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        await repo.get_stats('AAPL')

        _, kwargs = db.execute.call_args
        assert kwargs.get('fetchone') is True


class TestGetClassification:
    async def test_returns_standard_when_not_found(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        cls = await repo.get_classification('UNKNOWN')

        assert cls == 'standard'

    async def test_returns_stored_classification(self):
        repo, db = _make_repo()
        db.execute.return_value = ('blue_chip',)

        cls = await repo.get_classification('AAPL')

        assert cls == 'blue_chip'

    async def test_calls_execute_with_fetchone(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        await repo.get_classification('AAPL')

        _, kwargs = db.execute.call_args
        assert kwargs.get('fetchone') is True


class TestGetAllClassifications:
    async def test_returns_empty_dict_when_no_rows(self):
        repo, db = _make_repo()
        db.execute.return_value = []

        result = await repo.get_all_classifications()

        assert result == {}

    async def test_returns_ticker_classification_mapping(self):
        repo, db = _make_repo()
        db.execute.return_value = [('AAPL', 'blue_chip'), ('GME', 'meme')]

        result = await repo.get_all_classifications()

        assert result['AAPL'] == 'blue_chip'
        assert result['GME'] == 'meme'

    async def test_returns_none_as_empty_dict(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        result = await repo.get_all_classifications()

        assert result == {}


class TestGetAllMarketCaps:
    async def test_returns_empty_dict_when_no_rows(self):
        repo, db = _make_repo()
        db.execute.return_value = []

        result = await repo.get_all_market_caps()

        assert result == {}

    async def test_returns_ticker_market_cap_mapping(self):
        repo, db = _make_repo()
        db.execute.return_value = [('AAPL', 3_000_000_000), ('GME', 500_000_000)]

        result = await repo.get_all_market_caps()

        assert result['AAPL'] == 3_000_000_000
        assert result['GME'] == 500_000_000

    async def test_returns_none_market_cap(self):
        repo, db = _make_repo()
        db.execute.return_value = [('AAPL', None)]

        result = await repo.get_all_market_caps()

        assert result['AAPL'] is None

    async def test_returns_empty_dict_when_none(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        result = await repo.get_all_market_caps()

        assert result == {}


class TestGetAllStats:
    async def test_returns_empty_list_when_no_rows(self):
        repo, db = _make_repo()
        db.execute.return_value = []

        result = await repo.get_all_stats()

        assert result == []

    async def test_returns_list_of_dicts(self):
        repo, db = _make_repo()
        row = tuple([None] * len(_FIELDS))
        db.execute.return_value = [row, row]

        result = await repo.get_all_stats()

        assert len(result) == 2
        assert all(isinstance(r, dict) for r in result)

    async def test_returns_empty_list_when_none(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        result = await repo.get_all_stats()

        assert result == []
