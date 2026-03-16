"""Tests for data/schwab_token_store.py."""
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from rocketstocks.data.schwab_token_store import SchwabTokenRepository


@pytest.fixture
def mock_db():
    db = AsyncMock(name="Postgres")
    return db


class TestLoadToken:
    @pytest.mark.asyncio
    async def test_returns_none_when_empty(self, mock_db):
        mock_db.execute.return_value = []
        repo = SchwabTokenRepository(db=mock_db)
        result = await repo.load_token()
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_dict_when_row_exists(self, mock_db):
        token_dict = {"creation_timestamp": 1234567890, "token": {"access_token": "abc"}}
        mock_db.execute.return_value = [[token_dict]]
        repo = SchwabTokenRepository(db=mock_db)
        result = await repo.load_token()
        assert result == token_dict


class TestSaveToken:
    @pytest.mark.asyncio
    async def test_calls_execute_with_upsert_sql(self, mock_db):
        token_dict = {"creation_timestamp": 1234567890, "token": {}}
        repo = SchwabTokenRepository(db=mock_db)
        await repo.save_token(token_dict)
        mock_db.execute.assert_awaited_once()
        call_args = mock_db.execute.call_args
        sql = call_args.args[0]
        params = call_args.args[1]
        assert "INSERT INTO schwab_tokens" in sql
        assert "ON CONFLICT" in sql
        # Params should be a tuple with the JSON-serialized token
        assert json.loads(params[0]) == token_dict


class TestScheduleSave:
    def test_creates_task_via_event_loop(self, mock_db):
        token_dict = {"creation_timestamp": 1234567890, "token": {}}
        repo = SchwabTokenRepository(db=mock_db)
        mock_loop = MagicMock()
        with patch("rocketstocks.data.schwab_token_store.asyncio.get_running_loop", return_value=mock_loop):
            repo.schedule_save(token_dict)
        mock_loop.create_task.assert_called_once()
        # Close the unawaited coroutine to suppress RuntimeWarning
        mock_loop.create_task.call_args[0][0].close()
