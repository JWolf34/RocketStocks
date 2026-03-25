"""Tests for rocketstocks.data.market_signal_store.MarketSignalRepository."""
import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from psycopg.types.json import Json

import pytest

from rocketstocks.data.market_signal_store import MarketSignalRepository, _ACTIVE_CUTOFF_HOURS


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
    return MarketSignalRepository(db=mock_db)


# ---------------------------------------------------------------------------
# insert_signal
# ---------------------------------------------------------------------------

async def test_insert_signal_executes_sql(repo, mock_db):
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    await repo.insert_signal(
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
    mock_db.execute.assert_called_once()
    sql, params = mock_db.execute.call_args[0]
    assert 'INSERT INTO market_signals' in sql
    assert 'ON CONFLICT' in sql
    assert params[0] == 'GME'
    assert params[1] == ts
    assert params[2] == pytest.approx(3.2)


async def test_insert_signal_default_empty_signal_data(repo, mock_db):
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    await repo.insert_signal(
        ticker='AAPL',
        detected_at=ts,
        composite_score=2.8,
        price_z=1.8,
        vol_z=2.5,
        pct_change=3.0,
        dominant_signal='mixed',
        rvol=None,
    )
    _, params = mock_db.execute.call_args[0]
    # signal_data is at index 8 (after ticker, detected_at, composite_score, price_z, vol_z,
    # pct_change, dominant_signal, rvol); followed by signal_source and price_at_flag
    assert isinstance(params[8], Json) and params[8].obj == []


# ---------------------------------------------------------------------------
# get_active_signals
# ---------------------------------------------------------------------------

async def test_get_active_signals_returns_list_of_dicts(repo, mock_db):
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    signal_data = [{'ts': 'x', 'pct_change': 3.0}]
    mock_db.execute.return_value = [
        ('GME', ts, 3.2, 2.1, 3.5, 4.5, 'volume', 2.5, 'pending', None, None, signal_data),
    ]
    results = await repo.get_active_signals()
    assert len(results) == 1
    row = results[0]
    assert row['ticker'] == 'GME'
    assert row['status'] == 'pending'
    assert isinstance(row['signal_data'], list)
    assert row['signal_data'][0]['pct_change'] == pytest.approx(3.0)


async def test_get_active_signals_empty(repo, mock_db):
    mock_db.execute.return_value = []
    assert await repo.get_active_signals() == []


async def test_get_active_signals_queries_today(repo, mock_db):
    mock_db.execute.return_value = []
    await repo.get_active_signals()
    sql, params = mock_db.execute.call_args[0]
    assert "status = 'pending'" in sql
    assert 'detected_at >= %s AND detected_at < %s' in sql
    assert isinstance(params[0], datetime.datetime)
    assert isinstance(params[1], datetime.datetime)


# ---------------------------------------------------------------------------
# get_signal_history
# ---------------------------------------------------------------------------

async def test_get_signal_history_merges_observations(repo, mock_db):
    obs1 = [{'ts': 'a', 'pct_change': 2.0}]
    obs2 = [{'ts': 'b', 'pct_change': 2.5}]
    mock_db.execute.return_value = [(obs1,), (obs2,)]
    history = await repo.get_signal_history('GME')
    assert len(history) == 2
    assert history[0]['pct_change'] == pytest.approx(2.0)
    assert history[1]['pct_change'] == pytest.approx(2.5)


async def test_get_signal_history_empty(repo, mock_db):
    mock_db.execute.return_value = []
    assert await repo.get_signal_history('AAPL') == []


# ---------------------------------------------------------------------------
# mark_confirmed
# ---------------------------------------------------------------------------

async def test_mark_confirmed_executes_update(repo, mock_db):
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    await repo.mark_confirmed('GME', ts)
    mock_db.execute.assert_called_once()
    sql, params = mock_db.execute.call_args[0]
    assert 'UPDATE market_signals' in sql
    assert "status = 'confirmed'" in sql
    assert 'confirmed_at = CURRENT_TIMESTAMP' in sql
    assert params == ['GME', ts]


# ---------------------------------------------------------------------------
# expire_old_signals
# ---------------------------------------------------------------------------

async def test_expire_old_signals_executes_update(repo, mock_db):
    await repo.expire_old_signals()
    mock_db.execute.assert_called_once()
    sql, params = mock_db.execute.call_args[0]
    assert 'UPDATE market_signals' in sql
    assert "status = 'expired'" in sql
    assert "status = 'pending'" in sql
    assert len(params) == 1
    assert isinstance(params[0], datetime.datetime)


async def test_expire_old_signals_cutoff_is_active_cutoff_hours_ago(repo, mock_db):
    with patch('rocketstocks.data.market_signal_store.datetime') as mock_dt:
        now = datetime.datetime(2026, 3, 8, 12, 0)
        mock_dt.datetime.utcnow.return_value = now
        mock_dt.timedelta.side_effect = datetime.timedelta
        await repo.expire_old_signals()
    _, params = mock_db.execute.call_args[0]
    expected = now - datetime.timedelta(hours=_ACTIVE_CUTOFF_HOURS)
    assert params[0] == expected


# ---------------------------------------------------------------------------
# is_already_signaled
# ---------------------------------------------------------------------------

async def test_is_already_signaled_true(repo, mock_db):
    mock_db.execute.return_value = (1,)
    assert await repo.is_already_signaled('GME') is True


async def test_is_already_signaled_false(repo, mock_db):
    mock_db.execute.return_value = (0,)
    assert await repo.is_already_signaled('AAPL') is False


async def test_is_already_signaled_queries_ticker(repo, mock_db):
    mock_db.execute.return_value = (0,)
    await repo.is_already_signaled('TSLA')
    sql, params = mock_db.execute.call_args[0]
    assert 'WHERE ticker = %s' in sql
    assert "status = 'pending'" in sql
    assert params[0] == 'TSLA'


# ---------------------------------------------------------------------------
# update_observation
# ---------------------------------------------------------------------------

async def test_update_observation_appends_to_signal_data(repo, mock_db):
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    existing = [{'ts': 'first', 'pct_change': 2.0}]

    mock_cur = MagicMock()
    mock_cur.fetchone = AsyncMock(return_value=(existing,))  # JSONB — list directly
    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock(return_value=mock_cur)
    mock_db.transaction.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db.transaction.return_value.__aexit__ = AsyncMock(return_value=False)

    await repo.update_observation(
        ticker='GME',
        detected_at=ts,
        pct_change=3.0,
        composite_score=3.5,
        vol_z=2.8,
        price_z=2.0,
    )

    # Two execute calls on conn: SELECT then UPDATE
    assert mock_conn.execute.call_count == 2
    update_call = mock_conn.execute.call_args_list[1]
    sql, params = update_call[0]
    assert 'UPDATE market_signals' in sql
    updated_data = params[0]
    assert isinstance(updated_data, Json)
    assert len(updated_data.obj) == 2
    assert updated_data.obj[-1]['pct_change'] == pytest.approx(3.0)


async def test_update_observation_no_op_when_no_row(repo, mock_db):
    """If signal row not found, no update executed."""
    ts = datetime.datetime(2026, 3, 8, 10, 0)

    mock_cur = MagicMock()
    mock_cur.fetchone = AsyncMock(return_value=None)
    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock(return_value=mock_cur)
    mock_db.transaction.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db.transaction.return_value.__aexit__ = AsyncMock(return_value=False)

    await repo.update_observation('GME', ts, 3.0, 3.5, 2.8, 2.0)

    assert mock_conn.execute.call_count == 1  # only SELECT


# ---------------------------------------------------------------------------
# get_latest_signal
# ---------------------------------------------------------------------------

async def test_get_latest_signal_returns_dict(repo, mock_db):
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    signal_data = [{'ts': 'x', 'pct_change': 3.0}]
    mock_db.execute.return_value = (
        'GME', ts, 3.2, 2.1, 3.5, 4.5, 'volume', 2.5, 'pending', None, None, signal_data
    )
    result = await repo.get_latest_signal('GME')
    assert result is not None
    assert result['ticker'] == 'GME'
    assert isinstance(result['signal_data'], list)


async def test_get_latest_signal_returns_none_when_empty(repo, mock_db):
    mock_db.execute.return_value = None
    assert await repo.get_latest_signal('AAPL') is None


# ---------------------------------------------------------------------------
# get_signaled_tickers_today
# ---------------------------------------------------------------------------

async def test_get_signaled_tickers_today_returns_dict(repo, mock_db):
    """get_signaled_tickers_today returns {ticker: signal_dict} for pending signals."""
    ts = datetime.datetime(2026, 3, 8, 10, 0)
    signal_data = [{'ts': 'x', 'pct_change': 3.0}]
    mock_db.execute.return_value = [
        ('GME', ts, 3.2, 2.1, 3.5, 4.5, 'volume', 2.5, 'pending', None, None, signal_data),
        ('AAPL', ts, 2.8, 1.5, 2.0, 2.1, 'price', 1.8, 'pending', None, None, []),
    ]
    result = await repo.get_signaled_tickers_today()
    assert set(result.keys()) == {'GME', 'AAPL'}
    assert result['GME']['ticker'] == 'GME'
    assert result['GME']['composite_score'] == pytest.approx(3.2)
    assert result['AAPL']['ticker'] == 'AAPL'


async def test_get_signaled_tickers_today_empty(repo, mock_db):
    """get_signaled_tickers_today returns empty dict when no pending signals."""
    mock_db.execute.return_value = []
    result = await repo.get_signaled_tickers_today()
    assert result == {}


async def test_get_signaled_tickers_today_none_result(repo, mock_db):
    """get_signaled_tickers_today handles None DB result gracefully."""
    mock_db.execute.return_value = None
    result = await repo.get_signaled_tickers_today()
    assert result == {}


async def test_get_signaled_tickers_today_query_structure(repo, mock_db):
    """get_signaled_tickers_today uses DISTINCT ON and today's day bounds."""
    mock_db.execute.return_value = []
    with patch('rocketstocks.data.market_signal_store.datetime') as mock_dt:
        now = datetime.datetime(2026, 3, 8, 15, 0)
        mock_dt.datetime.utcnow.return_value = now
        mock_dt.timedelta.side_effect = datetime.timedelta
        await repo.get_signaled_tickers_today()
    sql, params = mock_db.execute.call_args[0]
    assert 'DISTINCT ON (ticker)' in sql
    assert "status = 'pending'" in sql
    assert 'ORDER BY ticker' in sql
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + datetime.timedelta(days=1)
    assert params == [day_start, day_end]


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
