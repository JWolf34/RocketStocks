"""Tests for rocketstocks.backtest.repository.BacktestRepository."""
import datetime
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from rocketstocks.backtest.repository import BacktestRepository, _RUN_FIELDS, _STAT_FIELDS


@pytest.fixture
def mock_db():
    db = MagicMock(name='Postgres')
    db.execute = AsyncMock(return_value=None)
    db.execute_batch = AsyncMock(return_value=None)
    return db


@pytest.fixture
def repo(mock_db):
    return BacktestRepository(db=mock_db)


# ---------------------------------------------------------------------------
# insert_run
# ---------------------------------------------------------------------------

async def test_insert_run_returns_run_id(repo, mock_db):
    mock_db.execute.return_value = (42,)
    run_id = await repo.insert_run(
        strategy_name='alert_signal',
        timeframe='daily',
        parameters={'hold_bars': 5},
        filters={'classifications': ['volatile']},
        ticker_count=10,
        start_date=datetime.date(2024, 1, 1),
        end_date=datetime.date(2025, 1, 1),
    )
    assert run_id == 42


async def test_insert_run_sql_contains_table_and_returning(repo, mock_db):
    mock_db.execute.return_value = (1,)
    await repo.insert_run(
        strategy_name='test', timeframe='daily',
        parameters={}, filters={}, ticker_count=1,
        start_date=None, end_date=None,
    )
    sql, params = mock_db.execute.call_args[0]
    assert 'INSERT INTO backtest_runs' in sql
    assert 'RETURNING run_id' in sql


async def test_insert_run_passes_strategy_name(repo, mock_db):
    mock_db.execute.return_value = (1,)
    await repo.insert_run(
        strategy_name='my_strategy', timeframe='5m',
        parameters={}, filters={}, ticker_count=5,
        start_date=None, end_date=None,
    )
    _, params = mock_db.execute.call_args[0]
    assert 'my_strategy' in params
    assert '5m' in params


# ---------------------------------------------------------------------------
# get_run
# ---------------------------------------------------------------------------

async def test_get_run_returns_dict_on_hit(repo, mock_db):
    mock_row = tuple(range(len(_RUN_FIELDS)))
    mock_db.execute.return_value = mock_row
    result = await repo.get_run(1)
    assert isinstance(result, dict)
    assert set(result.keys()) == set(_RUN_FIELDS)


async def test_get_run_returns_none_on_miss(repo, mock_db):
    mock_db.execute.return_value = None
    result = await repo.get_run(999)
    assert result is None


# ---------------------------------------------------------------------------
# insert_results_batch
# ---------------------------------------------------------------------------

async def test_insert_results_batch_calls_execute_batch(repo, mock_db):
    results = [
        {'ticker': 'AAPL', 'classification': 'blue_chip', 'sector': 'Tech',
         'return_pct': 5.0, 'sharpe_ratio': 0.8, 'max_drawdown': -3.0,
         'win_rate': 60.0, 'num_trades': 10, 'avg_trade_pct': 0.5,
         'profit_factor': 1.5, 'exposure_pct': 30.0, 'equity_final': 10500.0,
         'buy_hold_pct': 4.0, 'error': None},
    ]
    await repo.insert_results_batch(run_id=1, results=results)
    mock_db.execute_batch.assert_called_once()


async def test_insert_results_batch_includes_run_id_in_values(repo, mock_db):
    results = [{'ticker': 'GME', 'classification': 'meme', 'sector': None,
                'return_pct': None, 'sharpe_ratio': None, 'max_drawdown': None,
                'win_rate': None, 'num_trades': None, 'avg_trade_pct': None,
                'profit_factor': None, 'exposure_pct': None, 'equity_final': None,
                'buy_hold_pct': None, 'error': 'insufficient_data'}]
    await repo.insert_results_batch(run_id=7, results=results)
    sql, values = mock_db.execute_batch.call_args[0]
    assert values[0][0] == 7  # run_id is first field


# ---------------------------------------------------------------------------
# get_results_by_run
# ---------------------------------------------------------------------------

async def test_get_results_by_run_returns_empty_list_when_no_rows(repo, mock_db):
    mock_db.execute.return_value = None
    result = await repo.get_results_by_run(1)
    assert result == []


# ---------------------------------------------------------------------------
# get_successful_results_by_run
# ---------------------------------------------------------------------------

async def test_get_successful_results_sql_filters_error_is_null(repo, mock_db):
    mock_db.execute.return_value = []
    await repo.get_successful_results_by_run(1)
    sql, _ = mock_db.execute.call_args[0]
    assert 'error IS NULL' in sql


# ---------------------------------------------------------------------------
# insert_stats_batch
# ---------------------------------------------------------------------------

async def test_insert_stats_batch_calls_execute_batch(repo, mock_db):
    stats = [
        {'group_key': 'all', 'group_value': 'All Tickers', 'ticker_count': 3,
         'mean_return': 5.0, 'median_return': 5.0, 'std_return': 1.0,
         'mean_sharpe': 0.8, 'mean_win_rate': 60.0, 'total_trades': 27,
         'mean_max_dd': -3.0, 'mean_profit_factor': 1.5,
         't_stat': 2.5, 'p_value': 0.03, 'significant': True},
    ]
    await repo.insert_stats_batch(run_id=1, stats_list=stats)
    mock_db.execute_batch.assert_called_once()
    sql, values = mock_db.execute_batch.call_args[0]
    assert 'INSERT INTO strategy_stats' in sql


# ---------------------------------------------------------------------------
# get_stats_by_run
# ---------------------------------------------------------------------------

async def test_get_stats_by_run_returns_empty_on_no_rows(repo, mock_db):
    mock_db.execute.return_value = []
    result = await repo.get_stats_by_run(1)
    assert result == []


# ---------------------------------------------------------------------------
# get_stats_across_runs
# ---------------------------------------------------------------------------

async def test_get_stats_across_runs_joins_backtest_runs(repo, mock_db):
    mock_db.execute.return_value = []
    await repo.get_stats_across_runs('alert_signal', 'all')
    sql, params = mock_db.execute.call_args[0]
    assert 'JOIN backtest_runs' in sql
    assert 'strategy_name' in sql
    assert params == ['alert_signal', 'all']
