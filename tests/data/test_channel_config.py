"""Tests for ChannelConfigRepository."""
import pytest
from unittest.mock import AsyncMock, MagicMock

from rocketstocks.data.channel_config import ChannelConfigRepository, ALL_CONFIG_TYPES


def _make_repo(db=None):
    if db is None:
        db = MagicMock()
        db.execute = AsyncMock()
    return ChannelConfigRepository(db=db), db


class TestGetChannelId:
    async def test_returns_channel_id_when_found(self):
        repo, db = _make_repo()
        db.execute.return_value = (111,)

        result = await repo.get_channel_id(guild_id=1, config_type="reports")

        assert result == 111

    async def test_returns_none_when_not_found(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        result = await repo.get_channel_id(guild_id=1, config_type="reports")

        assert result is None

    async def test_calls_execute_with_fetchone(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        await repo.get_channel_id(guild_id=42, config_type="alerts")

        _, kwargs = db.execute.call_args
        assert kwargs.get('fetchone') is True

    async def test_passes_guild_and_type_as_params(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        await repo.get_channel_id(guild_id=42, config_type="alerts")

        sql_text, params, *_ = db.execute.call_args[0]
        assert 42 in params
        assert "alerts" in params


class TestGetAllForGuild:
    async def test_returns_dict_of_configured_types(self):
        repo, db = _make_repo()
        db.execute.return_value = [("reports", 111), ("alerts", 222)]

        result = await repo.get_all_for_guild(guild_id=1)

        assert result == {"reports": 111, "alerts": 222}

    async def test_returns_empty_dict_when_none(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        result = await repo.get_all_for_guild(guild_id=1)

        assert result == {}

    async def test_returns_empty_dict_when_empty_list(self):
        repo, db = _make_repo()
        db.execute.return_value = []

        result = await repo.get_all_for_guild(guild_id=1)

        assert result == {}


class TestGetAllGuildsForType:
    async def test_returns_list_of_tuples(self):
        repo, db = _make_repo()
        db.execute.return_value = [(100, 555), (200, 666)]

        result = await repo.get_all_guilds_for_type("reports")

        assert result == [(100, 555), (200, 666)]

    async def test_returns_empty_list_when_none(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        result = await repo.get_all_guilds_for_type("reports")

        assert result == []

    async def test_returns_empty_list_when_empty(self):
        repo, db = _make_repo()
        db.execute.return_value = []

        result = await repo.get_all_guilds_for_type("reports")

        assert result == []


class TestUpsertChannel:
    async def test_executes_insert_on_conflict_update(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        await repo.upsert_channel(guild_id=1, config_type="reports", channel_id=999)

        db.execute.assert_called_once()
        sql_text, params = db.execute.call_args[0]
        assert "ON CONFLICT" in sql_text
        assert "DO UPDATE" in sql_text
        assert list(params) == [1, "reports", 999]

    async def test_sql_contains_insert_into_channel_config(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        await repo.upsert_channel(guild_id=2, config_type="alerts", channel_id=777)

        sql_text, _ = db.execute.call_args[0]
        assert "INSERT INTO channel_config" in sql_text


class TestDeleteChannel:
    async def test_calls_execute_with_delete_sql(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        await repo.delete_channel(guild_id=1, config_type="alerts")

        db.execute.assert_called_once()
        sql_text, params = db.execute.call_args[0]
        assert "DELETE" in sql_text
        assert "channel_config" in sql_text
        assert 1 in params
        assert "alerts" in params


class TestDeleteGuild:
    async def test_calls_execute_with_delete_sql(self):
        repo, db = _make_repo()
        db.execute.return_value = None

        await repo.delete_guild(guild_id=99)

        db.execute.assert_called_once()
        sql_text, params = db.execute.call_args[0]
        assert "DELETE" in sql_text
        assert "channel_config" in sql_text
        assert 99 in params


class TestIsFullyConfigured:
    async def test_returns_true_when_all_5_types_set(self):
        repo, db = _make_repo()
        db.execute.return_value = [(ct, i) for i, ct in enumerate(ALL_CONFIG_TYPES)]

        result = await repo.is_fully_configured(guild_id=1)

        assert result is True

    async def test_returns_false_when_missing_types(self):
        repo, db = _make_repo()
        db.execute.return_value = [("reports", 1), ("alerts", 2)]

        result = await repo.is_fully_configured(guild_id=1)

        assert result is False

    async def test_returns_false_when_empty(self):
        repo, db = _make_repo()
        db.execute.return_value = []

        result = await repo.is_fully_configured(guild_id=1)

        assert result is False


class TestGetUnconfiguredGuilds:
    async def test_returns_guilds_not_fully_configured(self):
        repo, db = _make_repo()

        # Guild 10: fully configured; Guild 20: missing types
        async def fake_execute(sql, params=None, **kwargs):
            guild_id = params[0] if params else None
            if guild_id == 10:
                return [(ct, 100 + i) for i, ct in enumerate(ALL_CONFIG_TYPES)]
            return [("reports", 111)]

        db.execute.side_effect = fake_execute

        result = await repo.get_unconfigured_guilds([10, 20])

        assert result == [20]

    async def test_all_guilds_configured_returns_empty(self):
        repo, db = _make_repo()
        db.execute.return_value = [(ct, i) for i, ct in enumerate(ALL_CONFIG_TYPES)]

        result = await repo.get_unconfigured_guilds([1, 2])

        assert result == []

    async def test_empty_input_returns_empty(self):
        repo, db = _make_repo()

        result = await repo.get_unconfigured_guilds([])

        assert result == []
        db.execute.assert_not_called()
