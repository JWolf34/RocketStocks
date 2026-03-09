"""Tests for rocketstocks.data.market_signal_store.MarketSignalRepository."""
import datetime
import json
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from rocketstocks.data.market_signal_store import MarketSignalRepository, _ACTIVE_CUTOFF_HOURS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_cursor():
    return MagicMock(name='cursor')


@pytest.fixture
def mock_db(mock_cursor):
    db = MagicMock(name='Postgres')

    @contextmanager
    def _cursor():
        yield mock_cursor

    db._cursor = _cursor
    return db


@pytest.fixture
def repo(mock_db):
    return MarketSignalRepository(db=mock_db)


# ---------------------------------------------------------------------------
# insert_signal
# ---------------------------------------------------------------------------

def test_insert_signal_executes_sql(repo, mock_cursor):
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    repo.insert_signal(
        ticker='GME',
        detected_at=ts,
        composite_score=3.2,
        price_z=2.1,
        vol_z=3.5,
        pct_change=4.5,
        dominant_signal='volume',
        rvol=2.5,
        signal_data=[{'ts': 'x', 'pct_change': 4.5}],
    )
    mock_cursor.execute.assert_called_once()
    sql, params = mock_cursor.execute.call_args[0]
    assert 'INSERT INTO market_signals' in sql
    assert 'ON CONFLICT' in sql
    assert params[0] == 'GME'
    assert params[1] == ts
    assert params[2] == pytest.approx(3.2)


def test_insert_signal_default_empty_signal_data(repo, mock_cursor):
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    repo.insert_signal(
        ticker='AAPL',
        detected_at=ts,
        composite_score=2.8,
        price_z=1.8,
        vol_z=2.5,
        pct_change=3.0,
        dominant_signal='mixed',
        rvol=None,
    )
    _, params = mock_cursor.execute.call_args[0]
    # signal_data as JSON
    assert json.loads(params[-1]) == []


# ---------------------------------------------------------------------------
# get_active_signals
# ---------------------------------------------------------------------------

def test_get_active_signals_returns_list_of_dicts(repo, mock_cursor):
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    signal_data_json = json.dumps([{'ts': 'x', 'pct_change': 3.0}])
    mock_cursor.fetchall.return_value = [
        ('GME', ts, 3.2, 2.1, 3.5, 4.5, 'volume', 2.5, 'pending', None, None, signal_data_json),
    ]
    results = repo.get_active_signals()
    assert len(results) == 1
    row = results[0]
    assert row['ticker'] == 'GME'
    assert row['status'] == 'pending'
    assert isinstance(row['signal_data'], list)
    assert row['signal_data'][0]['pct_change'] == pytest.approx(3.0)


def test_get_active_signals_empty(repo, mock_cursor):
    mock_cursor.fetchall.return_value = []
    assert repo.get_active_signals() == []


def test_get_active_signals_queries_today(repo, mock_cursor):
    mock_cursor.fetchall.return_value = []
    repo.get_active_signals()
    sql, params = mock_cursor.execute.call_args[0]
    assert "status = 'pending'" in sql
    assert 'DATE(detected_at) = %s' in sql
    import datetime as _dt
    assert isinstance(params[0], _dt.date)


# ---------------------------------------------------------------------------
# get_signal_history
# ---------------------------------------------------------------------------

def test_get_signal_history_merges_observations(repo, mock_cursor):
    obs1 = json.dumps([{'ts': 'a', 'pct_change': 2.0}])
    obs2 = json.dumps([{'ts': 'b', 'pct_change': 2.5}])
    mock_cursor.fetchall.return_value = [(obs1,), (obs2,)]
    history = repo.get_signal_history('GME')
    assert len(history) == 2
    assert history[0]['pct_change'] == pytest.approx(2.0)
    assert history[1]['pct_change'] == pytest.approx(2.5)


def test_get_signal_history_empty(repo, mock_cursor):
    mock_cursor.fetchall.return_value = []
    assert repo.get_signal_history('AAPL') == []


# ---------------------------------------------------------------------------
# mark_confirmed
# ---------------------------------------------------------------------------

def test_mark_confirmed_executes_update(repo, mock_cursor):
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    repo.mark_confirmed('GME', ts)
    mock_cursor.execute.assert_called_once()
    sql, params = mock_cursor.execute.call_args[0]
    assert 'UPDATE market_signals' in sql
    assert "status = 'confirmed'" in sql
    assert 'confirmed_at = CURRENT_TIMESTAMP' in sql
    assert params == ['GME', ts]


# ---------------------------------------------------------------------------
# expire_old_signals
# ---------------------------------------------------------------------------

def test_expire_old_signals_executes_update(repo, mock_cursor):
    repo.expire_old_signals()
    mock_cursor.execute.assert_called_once()
    sql, params = mock_cursor.execute.call_args[0]
    assert 'UPDATE market_signals' in sql
    assert "status = 'expired'" in sql
    assert "status = 'pending'" in sql
    assert len(params) == 1
    assert isinstance(params[0], datetime.datetime)


def test_expire_old_signals_cutoff_is_active_cutoff_hours_ago(repo, mock_cursor):
    with patch('rocketstocks.data.market_signal_store.datetime') as mock_dt:
        now = datetime.datetime(2026, 3, 8, 12, 0)
        mock_dt.datetime.utcnow.return_value = now
        mock_dt.timedelta.side_effect = datetime.timedelta
        repo.expire_old_signals()
    _, params = mock_cursor.execute.call_args[0]
    expected = now - datetime.timedelta(hours=_ACTIVE_CUTOFF_HOURS)
    assert params[0] == expected


# ---------------------------------------------------------------------------
# is_already_signaled
# ---------------------------------------------------------------------------

def test_is_already_signaled_true(repo, mock_cursor):
    mock_cursor.fetchone.return_value = (1,)
    assert repo.is_already_signaled('GME') is True


def test_is_already_signaled_false(repo, mock_cursor):
    mock_cursor.fetchone.return_value = (0,)
    assert repo.is_already_signaled('AAPL') is False


def test_is_already_signaled_queries_ticker(repo, mock_cursor):
    mock_cursor.fetchone.return_value = (0,)
    repo.is_already_signaled('TSLA')
    sql, params = mock_cursor.execute.call_args[0]
    assert 'WHERE ticker = %s' in sql
    assert "status = 'pending'" in sql
    assert params[0] == 'TSLA'


# ---------------------------------------------------------------------------
# update_observation
# ---------------------------------------------------------------------------

def test_update_observation_appends_to_signal_data(repo, mock_cursor):
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    existing = [{'ts': 'first', 'pct_change': 2.0}]
    mock_cursor.fetchone.return_value = (json.dumps(existing),)

    repo.update_observation(
        ticker='GME',
        detected_at=ts,
        pct_change=3.0,
        composite_score=3.5,
        vol_z=2.8,
        price_z=2.0,
    )

    # Two execute calls: SELECT then UPDATE
    assert mock_cursor.execute.call_count == 2
    update_call = mock_cursor.execute.call_args_list[1]
    sql, params = update_call[0]
    assert 'UPDATE market_signals' in sql
    updated_data = json.loads(params[0])
    assert len(updated_data) == 2
    assert updated_data[-1]['pct_change'] == pytest.approx(3.0)


def test_update_observation_no_op_when_no_row(repo, mock_cursor):
    """If signal row not found, no update executed."""
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    mock_cursor.fetchone.return_value = None

    repo.update_observation('GME', ts, 3.0, 3.5, 2.8, 2.0)

    assert mock_cursor.execute.call_count == 1  # only SELECT


# ---------------------------------------------------------------------------
# get_latest_signal
# ---------------------------------------------------------------------------

def test_get_latest_signal_returns_dict(repo, mock_cursor):
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    signal_data_json = json.dumps([{'ts': 'x', 'pct_change': 3.0}])
    mock_cursor.fetchone.return_value = (
        'GME', ts, 3.2, 2.1, 3.5, 4.5, 'volume', 2.5, 'pending', None, None, signal_data_json
    )
    result = repo.get_latest_signal('GME')
    assert result is not None
    assert result['ticker'] == 'GME'
    assert isinstance(result['signal_data'], list)


def test_get_latest_signal_returns_none_when_empty(repo, mock_cursor):
    mock_cursor.fetchone.return_value = None
    assert repo.get_latest_signal('AAPL') is None


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

def test_repo_stores_db_reference():
    db = MagicMock()
    repo = MarketSignalRepository(db=db)
    assert repo._db is db


def test_repo_none_db():
    repo = MarketSignalRepository()
    assert repo._db is None
