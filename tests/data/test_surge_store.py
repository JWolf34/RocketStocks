"""Tests for rocketstocks.data.surge_store.SurgeRepository."""
import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rocketstocks.data.surge_store import SurgeRepository, _ACTIVE_CUTOFF_HOURS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    db = MagicMock(name='Postgres')
    db.execute = AsyncMock(return_value=None)
    return db


@pytest.fixture
def repo(mock_db):
    return SurgeRepository(db=mock_db)


# ---------------------------------------------------------------------------
# insert_surge
# ---------------------------------------------------------------------------

async def test_insert_surge_executes_sql(repo, mock_db):
    """insert_surge calls db.execute with correct values."""
    ts = datetime.datetime(2026, 3, 1, 10, 0)
    await repo.insert_surge(
        ticker='GME',
        flagged_at=ts,
        surge_types='mention_surge,rank_jump',
        current_rank=50,
        mention_ratio=4.5,
        rank_change=150,
        price_at_flag=25.0,
        alert_message_id=999,
    )
    mock_db.execute.assert_called_once()
    sql, params = mock_db.execute.call_args[0]
    assert 'INSERT INTO popularity_surges' in sql
    assert 'ON CONFLICT' in sql
    assert params == ['GME', ts, 'mention_surge,rank_jump', 50, 4.5, 150, 25.0, 999]


async def test_insert_surge_without_message_id(repo, mock_db):
    """insert_surge works with alert_message_id=None."""
    ts = datetime.datetime(2026, 3, 1, 10, 0)
    await repo.insert_surge(
        ticker='AAPL',
        flagged_at=ts,
        surge_types='rank_jump',
        current_rank=100,
        mention_ratio=None,
        rank_change=120,
        price_at_flag=150.0,
    )
    _, params = mock_db.execute.call_args[0]
    assert params[-1] is None  # alert_message_id defaults to None


# ---------------------------------------------------------------------------
# get_active_surges
# ---------------------------------------------------------------------------

async def test_get_active_surges_returns_list_of_dicts(repo, mock_db):
    """get_active_surges maps rows to dicts using _FIELDS."""
    ts = datetime.datetime(2026, 3, 1, 10, 0)
    mock_db.execute.return_value = [
        ('GME', ts, 'mention_surge', 50, 4.5, 150, 25.0, 999, False, None, False),
    ]
    results = await repo.get_active_surges()
    assert len(results) == 1
    row = results[0]
    assert row['ticker'] == 'GME'
    assert row['flagged_at'] == ts
    assert row['surge_types'] == 'mention_surge'
    assert row['current_rank'] == 50
    assert row['confirmed'] is False
    assert row['expired'] is False


async def test_get_active_surges_empty_result(repo, mock_db):
    """get_active_surges returns empty list when no rows."""
    mock_db.execute.return_value = []
    results = await repo.get_active_surges()
    assert results == []


async def test_get_active_surges_filters_by_cutoff(repo, mock_db):
    """get_active_surges passes a cutoff datetime to the query."""
    mock_db.execute.return_value = []
    with patch('rocketstocks.data.surge_store.datetime') as mock_dt:
        now = datetime.datetime(2026, 3, 2, 12, 0)
        mock_dt.datetime.utcnow.return_value = now
        mock_dt.timedelta.side_effect = datetime.timedelta
        await repo.get_active_surges()
    sql, params = mock_db.execute.call_args[0]
    assert 'WHERE confirmed = FALSE AND expired = FALSE AND flagged_at >=' in sql
    cutoff = now - datetime.timedelta(hours=_ACTIVE_CUTOFF_HOURS)
    assert params[0] == cutoff


async def test_get_active_surges_multiple_rows(repo, mock_db):
    """get_active_surges correctly maps multiple rows."""
    ts1 = datetime.datetime(2026, 3, 1, 10, 0)
    ts2 = datetime.datetime(2026, 3, 1, 11, 0)
    mock_db.execute.return_value = [
        ('GME', ts1, 'mention_surge', 50, 4.5, 150, 25.0, 111, False, None, False),
        ('AMC', ts2, 'rank_jump', 100, None, 120, 8.0, 222, False, None, False),
    ]
    results = await repo.get_active_surges()
    assert len(results) == 2
    assert results[0]['ticker'] == 'GME'
    assert results[1]['ticker'] == 'AMC'


# ---------------------------------------------------------------------------
# mark_confirmed
# ---------------------------------------------------------------------------

async def test_mark_confirmed_executes_update(repo, mock_db):
    """mark_confirmed updates confirmed=TRUE and sets confirmed_at."""
    ts = datetime.datetime(2026, 3, 1, 10, 0)
    await repo.mark_confirmed('GME', ts)
    mock_db.execute.assert_called_once()
    sql, params = mock_db.execute.call_args[0]
    assert 'UPDATE popularity_surges' in sql
    assert 'confirmed = TRUE' in sql
    assert 'confirmed_at = CURRENT_TIMESTAMP' in sql
    assert params == ['GME', ts]


# ---------------------------------------------------------------------------
# expire_old_surges
# ---------------------------------------------------------------------------

async def test_expire_old_surges_executes_update(repo, mock_db):
    """expire_old_surges marks unconfirmed surges beyond cutoff as expired."""
    await repo.expire_old_surges()
    mock_db.execute.assert_called_once()
    sql, params = mock_db.execute.call_args[0]
    assert 'UPDATE popularity_surges' in sql
    assert 'expired = TRUE' in sql
    assert 'confirmed = FALSE' in sql
    assert len(params) == 1  # just the cutoff datetime
    assert isinstance(params[0], datetime.datetime)


async def test_expire_old_surges_cutoff_is_24h_ago(repo, mock_db):
    """Cutoff passed to expire_old_surges is exactly ACTIVE_CUTOFF_HOURS ago."""
    with patch('rocketstocks.data.surge_store.datetime') as mock_dt:
        now = datetime.datetime(2026, 3, 2, 12, 0)
        mock_dt.datetime.utcnow.return_value = now
        mock_dt.timedelta.side_effect = datetime.timedelta
        await repo.expire_old_surges()
    _, params = mock_db.execute.call_args[0]
    expected_cutoff = now - datetime.timedelta(hours=_ACTIVE_CUTOFF_HOURS)
    assert params[0] == expected_cutoff


# ---------------------------------------------------------------------------
# is_already_flagged
# ---------------------------------------------------------------------------

async def test_is_already_flagged_true_when_count_nonzero(repo, mock_db):
    """Returns True when DB count > 0."""
    mock_db.execute.return_value = (1,)
    assert await repo.is_already_flagged('GME') is True


async def test_is_already_flagged_false_when_count_zero(repo, mock_db):
    """Returns False when DB count == 0."""
    mock_db.execute.return_value = (0,)
    assert await repo.is_already_flagged('AAPL') is False


async def test_is_already_flagged_queries_correct_ticker(repo, mock_db):
    """SQL params include the ticker and a cutoff."""
    mock_db.execute.return_value = (0,)
    await repo.is_already_flagged('TSLA')
    sql, params = mock_db.execute.call_args[0]
    assert 'WHERE ticker = %s' in sql
    assert 'confirmed = FALSE' in sql
    assert 'expired = FALSE' in sql
    assert params[0] == 'TSLA'
    assert isinstance(params[1], datetime.datetime)


# ---------------------------------------------------------------------------
# update_alert_message_id
# ---------------------------------------------------------------------------

async def test_update_alert_message_id_executes_update(repo, mock_db):
    """update_alert_message_id sets alert_message_id in the DB."""
    ts = datetime.datetime(2026, 3, 1, 10, 0)
    await repo.update_alert_message_id('GME', ts, 123456789)
    mock_db.execute.assert_called_once()
    sql, params = mock_db.execute.call_args[0]
    assert 'UPDATE popularity_surges' in sql
    assert 'alert_message_id = %s' in sql
    assert params == [123456789, 'GME', ts]


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

def test_repo_stores_db_reference():
    db = MagicMock()
    repo = SurgeRepository(db=db)
    assert repo._db is db


def test_repo_none_db():
    """SurgeRepository can be constructed without a DB (late binding)."""
    repo = SurgeRepository()
    assert repo._db is None
