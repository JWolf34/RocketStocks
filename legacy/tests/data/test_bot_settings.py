"""Tests for rocketstocks.data.bot_settings.BotSettingsRepository."""
import pytest
from unittest.mock import AsyncMock, MagicMock

from rocketstocks.data.bot_settings import BotSettingsRepository


def _make_repo():
    db = MagicMock()
    db.execute = AsyncMock(return_value=None)
    return BotSettingsRepository(db=db), db


class TestGet:
    @pytest.mark.asyncio
    async def test_returns_value_when_row_exists(self):
        repo, db = _make_repo()
        db.execute = AsyncMock(return_value=("America/Chicago",))

        result = await repo.get("tz")

        assert result == "America/Chicago"
        db.execute.assert_awaited_once()
        call_args = db.execute.call_args
        assert "tz" in call_args.args[1]

    @pytest.mark.asyncio
    async def test_returns_none_when_no_row(self):
        repo, db = _make_repo()
        db.execute = AsyncMock(return_value=None)

        result = await repo.get("tz")

        assert result is None

    @pytest.mark.asyncio
    async def test_passes_key_to_query(self):
        repo, db = _make_repo()
        db.execute = AsyncMock(return_value=None)

        await repo.get("notification_filter")

        db.execute.assert_awaited_once()
        assert db.execute.call_args.kwargs.get("fetchone") is True


class TestSet:
    @pytest.mark.asyncio
    async def test_executes_upsert(self):
        repo, db = _make_repo()

        await repo.set("tz", "UTC")

        db.execute.assert_awaited_once()
        sql = db.execute.call_args.args[0]
        assert "ON CONFLICT" in sql
        assert "EXCLUDED.value" in sql

    @pytest.mark.asyncio
    async def test_passes_key_and_value(self):
        repo, db = _make_repo()

        await repo.set("notification_filter", "failures_only")

        params = db.execute.call_args.args[1]
        assert "notification_filter" in params
        assert "failures_only" in params


class TestDelete:
    @pytest.mark.asyncio
    async def test_executes_delete(self):
        repo, db = _make_repo()

        await repo.delete("tz")

        db.execute.assert_awaited_once()
        sql = db.execute.call_args.args[0]
        assert "DELETE" in sql.upper()
        params = db.execute.call_args.args[1]
        assert "tz" in params

    @pytest.mark.asyncio
    async def test_no_error_when_key_absent(self):
        repo, db = _make_repo()
        db.execute = AsyncMock(return_value=None)

        # Should not raise
        await repo.delete("nonexistent_key")
        db.execute.assert_awaited_once()


class TestGetAll:
    @pytest.mark.asyncio
    async def test_returns_dict_of_all_rows(self):
        repo, db = _make_repo()
        db.execute = AsyncMock(return_value=[
            ("tz", "America/New_York"),
            ("notification_filter", "all"),
        ])

        result = await repo.get_all()

        assert result == {"tz": "America/New_York", "notification_filter": "all"}

    @pytest.mark.asyncio
    async def test_returns_empty_dict_when_no_rows(self):
        repo, db = _make_repo()
        db.execute = AsyncMock(return_value=[])

        result = await repo.get_all()

        assert result == {}

    @pytest.mark.asyncio
    async def test_returns_empty_dict_when_none_returned(self):
        repo, db = _make_repo()
        db.execute = AsyncMock(return_value=None)

        result = await repo.get_all()

        assert result == {}
