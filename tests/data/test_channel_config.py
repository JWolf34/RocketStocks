"""Tests for ChannelConfigRepository."""
from contextlib import contextmanager
from unittest.mock import MagicMock, call

import pytest


def _make_repo(db=None):
    from rocketstocks.data.channel_config import ChannelConfigRepository
    return ChannelConfigRepository(db=db or MagicMock())


def _make_db_with_cursor():
    """Return a MagicMock db with a working _cursor context manager."""
    db = MagicMock(name="Postgres")
    mock_cur = MagicMock(name="cursor")

    @contextmanager
    def _cursor():
        yield mock_cur

    db._cursor = _cursor
    return db, mock_cur


class TestGetChannelId:
    def test_returns_channel_id_when_found(self):
        db = MagicMock()
        db.select.return_value = (111,)
        repo = _make_repo(db)
        result = repo.get_channel_id(guild_id=1, config_type="reports")
        assert result == 111

    def test_returns_none_when_not_found(self):
        db = MagicMock()
        db.select.return_value = None
        repo = _make_repo(db)
        result = repo.get_channel_id(guild_id=1, config_type="reports")
        assert result is None

    def test_calls_select_with_correct_args(self):
        db = MagicMock()
        db.select.return_value = None
        repo = _make_repo(db)
        repo.get_channel_id(guild_id=42, config_type="alerts")
        db.select.assert_called_once_with(
            table="channel_config",
            fields=["channel_id"],
            where_conditions=[("guild_id", "=", 42), ("config_type", "=", "alerts")],
            fetchall=False,
        )


class TestGetAllForGuild:
    def test_returns_dict_of_configured_types(self):
        db = MagicMock()
        db.select.return_value = [("reports", 111), ("alerts", 222)]
        repo = _make_repo(db)
        result = repo.get_all_for_guild(guild_id=1)
        assert result == {"reports": 111, "alerts": 222}

    def test_returns_empty_dict_when_none(self):
        db = MagicMock()
        db.select.return_value = None
        repo = _make_repo(db)
        assert repo.get_all_for_guild(guild_id=1) == {}

    def test_returns_empty_dict_when_empty_list(self):
        db = MagicMock()
        db.select.return_value = []
        repo = _make_repo(db)
        assert repo.get_all_for_guild(guild_id=1) == {}


class TestGetAllGuildsForType:
    def test_returns_list_of_tuples(self):
        db = MagicMock()
        db.select.return_value = [(100, 555), (200, 666)]
        repo = _make_repo(db)
        result = repo.get_all_guilds_for_type("reports")
        assert result == [(100, 555), (200, 666)]

    def test_returns_empty_list_when_none(self):
        db = MagicMock()
        db.select.return_value = None
        repo = _make_repo(db)
        assert repo.get_all_guilds_for_type("reports") == []


class TestUpsertChannel:
    def test_executes_insert_on_conflict_update(self):
        db, mock_cur = _make_db_with_cursor()
        repo = _make_repo(db)
        repo.upsert_channel(guild_id=1, config_type="reports", channel_id=999)
        assert mock_cur.execute.called
        sql_text = mock_cur.execute.call_args[0][0]
        assert "ON CONFLICT" in sql_text
        assert "DO UPDATE" in sql_text
        assert mock_cur.execute.call_args[0][1] == (1, "reports", 999)

    def test_upsert_uses_raw_cursor_not_insert(self):
        """upsert_channel must not use db.insert() (which uses ON CONFLICT DO NOTHING)."""
        db, mock_cur = _make_db_with_cursor()
        repo = _make_repo(db)
        repo.upsert_channel(guild_id=1, config_type="reports", channel_id=999)
        db.insert.assert_not_called()


class TestDeleteChannel:
    def test_calls_db_delete_with_conditions(self):
        db = MagicMock()
        repo = _make_repo(db)
        repo.delete_channel(guild_id=1, config_type="alerts")
        db.delete.assert_called_once_with(
            table="channel_config",
            where_conditions=[("guild_id", "=", 1), ("config_type", "=", "alerts")],
        )


class TestDeleteGuild:
    def test_calls_db_delete_for_guild(self):
        db = MagicMock()
        repo = _make_repo(db)
        repo.delete_guild(guild_id=99)
        db.delete.assert_called_once_with(
            table="channel_config",
            where_conditions=[("guild_id", "=", 99)],
        )


class TestIsFullyConfigured:
    def test_returns_true_when_all_5_types_set(self):
        db = MagicMock()
        db.select.return_value = [
            ("reports", 1), ("alerts", 2), ("screeners", 3),
            ("charts", 4), ("notifications", 5),
        ]
        repo = _make_repo(db)
        assert repo.is_fully_configured(guild_id=1) is True

    def test_returns_false_when_missing_types(self):
        db = MagicMock()
        db.select.return_value = [("reports", 1), ("alerts", 2)]
        repo = _make_repo(db)
        assert repo.is_fully_configured(guild_id=1) is False

    def test_returns_false_when_empty(self):
        db = MagicMock()
        db.select.return_value = []
        repo = _make_repo(db)
        assert repo.is_fully_configured(guild_id=1) is False


class TestGetUnconfiguredGuilds:
    def test_returns_guilds_not_fully_configured(self):
        from rocketstocks.data.channel_config import ChannelConfigRepository, ALL_CONFIG_TYPES

        db = MagicMock()
        repo = ChannelConfigRepository(db=db)

        # Guild 10: fully configured; Guild 20: missing types
        def fake_select(table, fields, where_conditions=None, **kwargs):
            guild_id = where_conditions[0][2] if where_conditions else None
            if guild_id == 10:
                return [(ct, 100 + i) for i, ct in enumerate(ALL_CONFIG_TYPES)]
            return [("reports", 111)]

        db.select.side_effect = fake_select

        result = repo.get_unconfigured_guilds([10, 20])
        assert result == [20]

    def test_all_guilds_configured_returns_empty(self):
        from rocketstocks.data.channel_config import ChannelConfigRepository, ALL_CONFIG_TYPES

        db = MagicMock()
        repo = ChannelConfigRepository(db=db)

        db.select.return_value = [(ct, i) for i, ct in enumerate(ALL_CONFIG_TYPES)]

        result = repo.get_unconfigured_guilds([1, 2])
        assert result == []
