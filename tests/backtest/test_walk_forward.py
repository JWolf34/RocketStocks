"""Tests for rocketstocks.backtest.walk_forward."""
import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rocketstocks.backtest.walk_forward import (
    WalkForwardRunner,
    _extract_chosen_params,
    _split_date_range,
)

START = datetime.date(2022, 1, 1)
END = datetime.date(2023, 12, 31)


# ---------------------------------------------------------------------------
# _split_date_range
# ---------------------------------------------------------------------------

def test_split_date_range_returns_correct_fold_count():
    splits = _split_date_range(START, END, folds=5, train_pct=0.7)
    assert len(splits) == 5


def test_split_date_range_each_fold_has_four_dates():
    splits = _split_date_range(START, END, folds=3, train_pct=0.7)
    for fold in splits:
        assert len(fold) == 4


def test_split_date_range_train_start_always_global_start():
    """Anchored window: every fold's train_start is the global start."""
    splits = _split_date_range(START, END, folds=4, train_pct=0.7)
    for train_start, *_ in splits:
        assert train_start == START


def test_split_date_range_test_start_after_train_end():
    splits = _split_date_range(START, END, folds=4, train_pct=0.7)
    for train_start, train_end, test_start, test_end in splits:
        assert test_start > train_end


def test_split_date_range_test_end_after_test_start():
    splits = _split_date_range(START, END, folds=4, train_pct=0.7)
    for _, _, test_start, test_end in splits:
        assert test_end > test_start


def test_split_date_range_expanding_windows():
    """Later folds should have a later train_end (expanding window)."""
    splits = _split_date_range(START, END, folds=5, train_pct=0.7)
    train_ends = [s[1] for s in splits]
    assert train_ends == sorted(train_ends)


def test_split_date_range_zero_folds_returns_empty():
    assert _split_date_range(START, END, folds=0, train_pct=0.7) == []


def test_split_date_range_invalid_range_returns_empty():
    assert _split_date_range(END, START, folds=3, train_pct=0.7) == []


def test_split_date_range_single_fold():
    splits = _split_date_range(START, END, folds=1, train_pct=0.7)
    assert len(splits) == 1
    train_start, train_end, test_start, test_end = splits[0]
    assert train_start == START
    assert test_end == END


def test_split_date_range_train_pct_applied():
    """train_end should be roughly train_pct into the fold."""
    splits = _split_date_range(START, END, folds=1, train_pct=0.7)
    train_start, train_end, _, _ = splits[0]
    total = (END - START).days
    train_days = (train_end - train_start).days
    assert abs(train_days / total - 0.7) < 0.05


# ---------------------------------------------------------------------------
# _extract_chosen_params
# ---------------------------------------------------------------------------

def test_extract_chosen_params_uses_best_stats():
    param_grid = {'hold_bars': [5, 10, 20]}
    opt_result = {'best_stats': {'hold_bars': 10.0}}
    chosen = _extract_chosen_params(param_grid, opt_result)
    assert chosen['hold_bars'] == 10


def test_extract_chosen_params_int_coercion():
    param_grid = {'hold_bars': [5, 10]}
    opt_result = {'best_stats': {'hold_bars': 7.9}}
    chosen = _extract_chosen_params(param_grid, opt_result)
    assert isinstance(chosen['hold_bars'], int)
    assert chosen['hold_bars'] == 8


def test_extract_chosen_params_float_coercion():
    param_grid = {'vol_threshold': [1.5, 2.0, 2.5]}
    opt_result = {'best_stats': {'vol_threshold': '2.0'}}
    chosen = _extract_chosen_params(param_grid, opt_result)
    assert isinstance(chosen['vol_threshold'], float)
    assert chosen['vol_threshold'] == pytest.approx(2.0)


def test_extract_chosen_params_fallback_to_first_candidate():
    """When param not in best_stats, fall back to first candidate."""
    param_grid = {'hold_bars': [5, 10, 20]}
    opt_result = {'best_stats': {}}
    chosen = _extract_chosen_params(param_grid, opt_result)
    assert chosen['hold_bars'] == 5


def test_extract_chosen_params_multiple_params():
    param_grid = {'hold_bars': [5, 10], 'vol_threshold': [1.5, 2.0]}
    opt_result = {'best_stats': {'hold_bars': 10.0, 'vol_threshold': 2.0}}
    chosen = _extract_chosen_params(param_grid, opt_result)
    assert chosen['hold_bars'] == 10
    assert chosen['vol_threshold'] == pytest.approx(2.0)


def test_extract_chosen_params_empty_grid():
    chosen = _extract_chosen_params({}, {'best_stats': {'hold_bars': 5}})
    assert chosen == {}


def test_extract_chosen_params_none_value_falls_back():
    param_grid = {'hold_bars': [5, 10]}
    opt_result = {'best_stats': {'hold_bars': None}}
    chosen = _extract_chosen_params(param_grid, opt_result)
    assert chosen['hold_bars'] == 5


# ---------------------------------------------------------------------------
# WalkForwardRunner.run — guard clauses
# ---------------------------------------------------------------------------

def _make_runner(tickers: list[str] | None = None) -> MagicMock:
    """Build a minimal mock BacktestRunner for WalkForwardRunner tests."""
    runner = MagicMock(name='BacktestRunner')
    runner._stock_data = MagicMock(name='StockData')
    runner._repo = MagicMock(name='Repo')
    tickers = tickers if tickers is not None else ['AAPL', 'MSFT']
    runner.optimize = AsyncMock(return_value={
        'best_stats': {'hold_bars': 5},
        'best_return': 10.0,
    })
    runner.run = AsyncMock(return_value=1)
    runner._repo.get_successful_results_by_run = AsyncMock(return_value=[])
    return runner, tickers


async def test_walk_forward_no_start_date_returns_error():
    """start_date=None guard fires before any network or DB call."""
    runner, _ = _make_runner()
    wf = WalkForwardRunner(runner)
    ticker_filter = MagicMock()
    ticker_filter.apply = AsyncMock(return_value=['AAPL'])
    result = await wf.run(
        strategy_name='alert_signal',
        ticker_filter=ticker_filter,
        param_grid={'hold_bars': [5, 10]},
        start_date=None,
        end_date=END,
    )
    assert 'error' in result


async def test_walk_forward_no_end_date_returns_error():
    runner, _ = _make_runner()
    wf = WalkForwardRunner(runner)
    ticker_filter = MagicMock()
    ticker_filter.apply = AsyncMock(return_value=['AAPL'])
    result = await wf.run(
        strategy_name='alert_signal',
        ticker_filter=ticker_filter,
        param_grid={'hold_bars': [5]},
        start_date=START,
        end_date=None,
    )
    assert 'error' in result


async def test_walk_forward_no_tickers_returns_error():
    runner, _ = _make_runner(tickers=[])
    wf = WalkForwardRunner(runner)
    ticker_filter = MagicMock()
    ticker_filter.apply = AsyncMock(return_value=[])
    result = await wf.run(
        strategy_name='alert_signal',
        ticker_filter=ticker_filter,
        param_grid={'hold_bars': [5]},
        start_date=START,
        end_date=END,
    )
    assert 'error' in result


async def test_walk_forward_invalid_strategy_propagates_error():
    runner, tickers = _make_runner()
    wf = WalkForwardRunner(runner)
    ticker_filter = MagicMock()
    ticker_filter.apply = AsyncMock(return_value=tickers)
    with patch('rocketstocks.backtest.walk_forward.get_strategy', side_effect=KeyError('no such strategy')):
        with pytest.raises(KeyError):
            await wf.run(
                strategy_name='nonexistent',
                ticker_filter=ticker_filter,
                param_grid={'hold_bars': [5]},
                start_date=START,
                end_date=END,
            )


# ---------------------------------------------------------------------------
# WalkForwardRunner.run — happy path structure
# ---------------------------------------------------------------------------

async def test_walk_forward_returns_expected_keys():
    runner, tickers = _make_runner()
    wf = WalkForwardRunner(runner)
    ticker_filter = MagicMock()
    ticker_filter.apply = AsyncMock(return_value=tickers)
    with patch('rocketstocks.backtest.walk_forward.get_strategy', return_value=MagicMock()):
        result = await wf.run(
            strategy_name='alert_signal',
            ticker_filter=ticker_filter,
            param_grid={'hold_bars': [5, 10]},
            folds=2,
            start_date=START,
            end_date=END,
        )
    assert 'folds' in result
    assert 'param_stability' in result
    assert 'n_folds' in result


async def test_walk_forward_fold_list_length():
    runner, tickers = _make_runner()
    wf = WalkForwardRunner(runner)
    ticker_filter = MagicMock()
    ticker_filter.apply = AsyncMock(return_value=tickers)
    with patch('rocketstocks.backtest.walk_forward.get_strategy', return_value=MagicMock()):
        result = await wf.run(
            strategy_name='alert_signal',
            ticker_filter=ticker_filter,
            param_grid={'hold_bars': [5, 10]},
            folds=3,
            start_date=START,
            end_date=END,
        )
    assert len(result['folds']) == 3


async def test_walk_forward_param_stability_has_fold_values():
    runner, tickers = _make_runner()
    wf = WalkForwardRunner(runner)
    ticker_filter = MagicMock()
    ticker_filter.apply = AsyncMock(return_value=tickers)
    with patch('rocketstocks.backtest.walk_forward.get_strategy', return_value=MagicMock()):
        result = await wf.run(
            strategy_name='alert_signal',
            ticker_filter=ticker_filter,
            param_grid={'hold_bars': [5, 10]},
            folds=2,
            start_date=START,
            end_date=END,
        )
    assert 'hold_bars' in result['param_stability']
    assert len(result['param_stability']['hold_bars']) == 2
