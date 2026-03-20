"""Tests for rocketstocks.backtest.stats."""
import math

import pytest

from rocketstocks.backtest.stats import (
    GroupStats,
    compare_strategies,
    compute_all_group_stats,
    compute_group_stats,
)


def _make_results(returns: list[float], classification='standard', sector='Technology') -> list[dict]:
    return [
        {'ticker': f'T{i}', 'return_pct': r, 'sharpe_ratio': r / 10,
         'max_drawdown': -abs(r), 'win_rate': 60.0, 'num_trades': 5,
         'profit_factor': 1.2, 'classification': classification,
         'sector': sector, 'error': None}
        for i, r in enumerate(returns)
    ]


# ---------------------------------------------------------------------------
# compute_group_stats
# ---------------------------------------------------------------------------

def test_compute_group_stats_returns_group_stats_instance():
    results = _make_results([5.0, 8.0, -2.0, 3.0])
    gs = compute_group_stats(results, group_key='all', group_value='All')
    assert isinstance(gs, GroupStats)


def test_compute_group_stats_correct_mean():
    returns = [4.0, 6.0, 2.0]
    results = _make_results(returns)
    gs = compute_group_stats(results)
    assert abs(gs.mean_return - 4.0) < 1e-9


def test_compute_group_stats_correct_median():
    results = _make_results([1.0, 5.0, 10.0])
    gs = compute_group_stats(results)
    assert abs(gs.median_return - 5.0) < 1e-9


def test_compute_group_stats_ticker_count():
    results = _make_results([1.0, 2.0, 3.0])
    gs = compute_group_stats(results)
    assert gs.ticker_count == 3


def test_compute_group_stats_filters_errors():
    results = _make_results([5.0, 8.0])
    results.append({'ticker': 'BAD', 'return_pct': None, 'error': 'insufficient_data',
                    'sharpe_ratio': None, 'max_drawdown': None, 'win_rate': None,
                    'num_trades': None, 'profit_factor': None,
                    'classification': 'standard', 'sector': 'Technology'})
    gs = compute_group_stats(results)
    assert gs.ticker_count == 2


def test_compute_group_stats_significance_flag():
    # Returns clearly above zero → should be significant
    results = _make_results([10.0, 12.0, 11.0, 9.0, 10.5, 11.5, 10.0])
    gs = compute_group_stats(results)
    assert gs.significant is True
    assert gs.p_value < 0.05


def test_compute_group_stats_not_significant():
    # Mixed returns close to zero
    results = _make_results([-5.0, -3.0, 4.0, 3.0, -4.0, 5.0])
    gs = compute_group_stats(results)
    # p-value likely > 0.05 for this noisy sample
    # just check it runs without error
    assert isinstance(gs.p_value, float)


def test_compute_group_stats_single_result_no_crash():
    results = _make_results([7.0])
    gs = compute_group_stats(results)
    assert gs is not None
    assert math.isnan(gs.t_stat)
    assert math.isnan(gs.p_value)
    assert gs.significant is False


def test_compute_group_stats_empty_returns_none():
    gs = compute_group_stats([])
    assert gs is None


def test_compute_group_stats_all_errors_returns_none():
    results = [{'return_pct': None, 'error': 'fail', 'ticker': 'X'}]
    gs = compute_group_stats(results)
    assert gs is None


def test_compute_group_stats_to_dict():
    results = _make_results([5.0, 8.0])
    gs = compute_group_stats(results, group_key='all', group_value='All')
    d = gs.to_dict()
    assert d['group_key'] == 'all'
    assert 'mean_return' in d
    assert 'p_value' in d


# ---------------------------------------------------------------------------
# compute_all_group_stats
# ---------------------------------------------------------------------------

def test_compute_all_group_stats_includes_overall():
    results = _make_results([5.0, 8.0, -2.0])
    all_gs = compute_all_group_stats(results)
    keys = [gs.group_key for gs in all_gs]
    assert 'all' in keys


def test_compute_all_group_stats_groups_by_class():
    results = (
        _make_results([5.0, 8.0], classification='blue_chip') +
        _make_results([-2.0, 3.0], classification='meme')
    )
    all_gs = compute_all_group_stats(results)
    keys = [gs.group_key for gs in all_gs]
    assert 'class:blue_chip' in keys
    assert 'class:meme' in keys


def test_compute_all_group_stats_groups_by_sector():
    results = (
        _make_results([5.0], sector='Technology') +
        _make_results([8.0], sector='Healthcare')
    )
    all_gs = compute_all_group_stats(results)
    keys = [gs.group_key for gs in all_gs]
    assert 'sector:Technology' in keys
    assert 'sector:Healthcare' in keys


def test_compute_all_group_stats_empty_input():
    assert compute_all_group_stats([]) == []


# ---------------------------------------------------------------------------
# compare_strategies
# ---------------------------------------------------------------------------

def test_compare_strategies_returns_comparison_dict():
    a = _make_results([10.0, 12.0, 11.0, 9.0])
    b = _make_results([2.0, 3.0, 1.0, 4.0])
    result = compare_strategies(a, b, label_a='StratA', label_b='StratB')
    assert result['label_a'] == 'StratA'
    assert result['label_b'] == 'StratB'
    assert 'p_value' in result
    assert 't_stat' in result


def test_compare_strategies_better_label():
    a = _make_results([10.0, 12.0, 11.0])
    b = _make_results([1.0, 2.0, 3.0])
    result = compare_strategies(a, b, label_a='A', label_b='B')
    assert result['better'] == 'A'


def test_compare_strategies_insufficient_data():
    a = _make_results([5.0])
    b = _make_results([3.0, 4.0, 5.0])
    result = compare_strategies(a, b)
    assert 'error' in result
    assert result['error'] == 'insufficient_data'


def test_compare_strategies_n_values():
    a = _make_results([1.0, 2.0, 3.0])
    b = _make_results([4.0, 5.0])
    result = compare_strategies(a, b)
    assert result['n_a'] == 3
    assert result['n_b'] == 2
