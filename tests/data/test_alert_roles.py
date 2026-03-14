"""Tests for rocketstocks.data.alert_roles.AlertRolesRepository."""
from unittest.mock import AsyncMock, MagicMock

import pytest

from rocketstocks.data.alert_roles import AlertRolesRepository


@pytest.fixture
def mock_db():
    db = MagicMock(name='Postgres')
    db.execute = AsyncMock(return_value=None)
    return db


@pytest.fixture
def repo(mock_db):
    return AlertRolesRepository(db=mock_db)


# ---------------------------------------------------------------------------
# upsert
# ---------------------------------------------------------------------------

async def test_upsert_executes_insert_on_conflict(repo, mock_db):
    await repo.upsert(guild_id=111, role_key='earnings_mover', role_id=999)
    mock_db.execute.assert_called_once()
    sql, params = mock_db.execute.call_args[0]
    assert 'INSERT INTO alert_roles' in sql
    assert 'ON CONFLICT' in sql
    assert params == [111, 'earnings_mover', 999]


# ---------------------------------------------------------------------------
# get_role_id
# ---------------------------------------------------------------------------

async def test_get_role_id_returns_value_when_row_exists(repo, mock_db):
    mock_db.execute = AsyncMock(return_value=(42,))
    result = await repo.get_role_id(guild_id=111, role_key='earnings_mover')
    assert result == 42


async def test_get_role_id_returns_none_when_no_row(repo, mock_db):
    mock_db.execute = AsyncMock(return_value=None)
    result = await repo.get_role_id(guild_id=111, role_key='earnings_mover')
    assert result is None


# ---------------------------------------------------------------------------
# get_role_ids
# ---------------------------------------------------------------------------

async def test_get_role_ids_returns_empty_for_empty_keys(repo, mock_db):
    result = await repo.get_role_ids(guild_id=111, keys=[])
    assert result == []
    mock_db.execute.assert_not_called()


async def test_get_role_ids_returns_list_of_ids(repo, mock_db):
    mock_db.execute = AsyncMock(return_value=[(10,), (20,)])
    result = await repo.get_role_ids(guild_id=111, keys=['earnings_mover', 'all_alerts'])
    assert result == [10, 20]


async def test_get_role_ids_returns_empty_when_no_matches(repo, mock_db):
    mock_db.execute = AsyncMock(return_value=[])
    result = await repo.get_role_ids(guild_id=111, keys=['earnings_mover'])
    assert result == []


async def test_get_role_ids_includes_guild_id_in_params(repo, mock_db):
    mock_db.execute = AsyncMock(return_value=[])
    await repo.get_role_ids(guild_id=555, keys=['popularity_surge', 'all_alerts'])
    _, params = mock_db.execute.call_args[0]
    assert params[0] == 555
    assert 'popularity_surge' in params
    assert 'all_alerts' in params


# ---------------------------------------------------------------------------
# get_all_for_guild
# ---------------------------------------------------------------------------

async def test_get_all_for_guild_returns_dict(repo, mock_db):
    mock_db.execute = AsyncMock(return_value=[
        ('earnings_mover', 100),
        ('all_alerts', 200),
    ])
    result = await repo.get_all_for_guild(guild_id=111)
    assert result == {'earnings_mover': 100, 'all_alerts': 200}


async def test_get_all_for_guild_returns_empty_dict_when_none(repo, mock_db):
    mock_db.execute = AsyncMock(return_value=None)
    result = await repo.get_all_for_guild(guild_id=111)
    assert result == {}


# ---------------------------------------------------------------------------
# delete_guild
# ---------------------------------------------------------------------------

async def test_delete_guild_executes_delete(repo, mock_db):
    await repo.delete_guild(guild_id=777)
    mock_db.execute.assert_called_once()
    sql, params = mock_db.execute.call_args[0]
    assert 'DELETE FROM alert_roles' in sql
    assert params == [777]
