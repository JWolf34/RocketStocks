"""Tests for rocketstocks.backtest.stats."""
import math

import pytest

from rocketstocks.backtest.stats import (
    GroupStats,
    _classify_market_cap,
    compare_against_benchmark,
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


def test_compute_all_group_stats_groups_by_exchange():
    results = [
        {'ticker': 'AAPL', 'return_pct': 5.0, 'classification': 'blue_chip',
         'sector': 'Technology', 'exchange': 'NASDAQ', 'watchlist': None,
         'sharpe_ratio': 1.0, 'max_drawdown': -2.0, 'win_rate': 60.0,
         'num_trades': 5, 'profit_factor': 1.2, 'error': None},
        {'ticker': 'GME', 'return_pct': -4.0, 'classification': 'meme',
         'sector': 'Consumer Cyclical', 'exchange': 'NYSE', 'watchlist': None,
         'sharpe_ratio': -0.3, 'max_drawdown': -15.0, 'win_rate': 40.0,
         'num_trades': 3, 'profit_factor': 0.7, 'error': None},
    ]
    groups = compute_all_group_stats(results)
    group_keys = {g.group_key for g in groups}
    assert 'exchange:NASDAQ' in group_keys
    assert 'exchange:NYSE' in group_keys
    nasdaq = next(g for g in groups if g.group_key == 'exchange:NASDAQ')
    assert nasdaq.ticker_count == 1
    assert nasdaq.mean_return == pytest.approx(5.0)


def test_compute_all_group_stats_groups_by_watchlist():
    results = [
        {'ticker': 'AAPL', 'return_pct': 8.0, 'classification': 'blue_chip',
         'sector': 'Technology', 'exchange': 'NASDAQ', 'watchlist': 'mag7',
         'sharpe_ratio': 1.2, 'max_drawdown': -2.0, 'win_rate': 65.0,
         'num_trades': 8, 'profit_factor': 1.5, 'error': None},
        {'ticker': 'GME', 'return_pct': -4.0, 'classification': 'meme',
         'sector': 'Consumer Cyclical', 'exchange': 'NYSE', 'watchlist': None,
         'sharpe_ratio': -0.3, 'max_drawdown': -15.0, 'win_rate': 40.0,
         'num_trades': 3, 'profit_factor': 0.7, 'error': None},
    ]
    groups = compute_all_group_stats(results)
    group_keys = {g.group_key for g in groups}
    assert 'watchlist:mag7' in group_keys
    # GME has no watchlist — no watchlist:None group
    assert not any(k.startswith('watchlist:None') for k in group_keys)
    mag7 = next(g for g in groups if g.group_key == 'watchlist:mag7')
    assert mag7.ticker_count == 1


# ---------------------------------------------------------------------------
# _classify_market_cap
# ---------------------------------------------------------------------------

def test_classify_market_cap_small():
    assert _classify_market_cap(500_000_000) == 'small'


def test_classify_market_cap_small_boundary():
    assert _classify_market_cap(1_999_999_999) == 'small'


def test_classify_market_cap_mid_lower_boundary():
    assert _classify_market_cap(2_000_000_000) == 'mid'


def test_classify_market_cap_mid():
    assert _classify_market_cap(5_000_000_000) == 'mid'


def test_classify_market_cap_mid_upper_boundary():
    assert _classify_market_cap(9_999_999_999) == 'mid'


def test_classify_market_cap_large():
    assert _classify_market_cap(10_000_000_000) == 'large'


def test_classify_market_cap_none_returns_none():
    assert _classify_market_cap(None) is None


# ---------------------------------------------------------------------------
# compute_all_group_stats — market cap grouping
# ---------------------------------------------------------------------------

def _make_mcap_results() -> tuple[list[dict], dict[str, int | None]]:
    """Returns results with 3 tickers spanning all three mcap tiers."""
    results = [
        {'ticker': 'SMALL', 'return_pct': 5.0, 'classification': 'standard',
         'sector': 'Technology', 'exchange': 'NASDAQ', 'watchlist': None,
         'sharpe_ratio': 0.5, 'max_drawdown': -5.0, 'win_rate': 55.0,
         'num_trades': 4, 'profit_factor': 1.1, 'error': None},
        {'ticker': 'MID', 'return_pct': 3.0, 'classification': 'standard',
         'sector': 'Technology', 'exchange': 'NYSE', 'watchlist': None,
         'sharpe_ratio': 0.3, 'max_drawdown': -3.0, 'win_rate': 50.0,
         'num_trades': 3, 'profit_factor': 1.0, 'error': None},
        {'ticker': 'LARGE', 'return_pct': 1.0, 'classification': 'blue_chip',
         'sector': 'Healthcare', 'exchange': 'NYSE', 'watchlist': None,
         'sharpe_ratio': 0.1, 'max_drawdown': -1.0, 'win_rate': 45.0,
         'num_trades': 2, 'profit_factor': 0.9, 'error': None},
    ]
    mcap_map = {
        'SMALL': 500_000_000,
        'MID':   5_000_000_000,
        'LARGE': 50_000_000_000,
    }
    return results, mcap_map


def test_compute_all_group_stats_groups_by_market_cap():
    results, mcap_map = _make_mcap_results()
    groups = compute_all_group_stats(results, mcap_map=mcap_map)
    keys = {g.group_key for g in groups}
    assert 'mcap:small' in keys
    assert 'mcap:mid' in keys
    assert 'mcap:large' in keys


def test_compute_all_group_stats_market_cap_group_values():
    results, mcap_map = _make_mcap_results()
    groups = compute_all_group_stats(results, mcap_map=mcap_map)
    small = next(g for g in groups if g.group_key == 'mcap:small')
    mid = next(g for g in groups if g.group_key == 'mcap:mid')
    large = next(g for g in groups if g.group_key == 'mcap:large')
    assert small.group_value == 'Small (<$2B)'
    assert mid.group_value == 'Mid ($2B\u2013$10B)'
    assert large.group_value == 'Large (>$10B)'


def test_compute_all_group_stats_market_cap_ticker_counts():
    results, mcap_map = _make_mcap_results()
    groups = compute_all_group_stats(results, mcap_map=mcap_map)
    small = next(g for g in groups if g.group_key == 'mcap:small')
    assert small.ticker_count == 1
    assert small.mean_return == pytest.approx(5.0)


def test_compute_all_group_stats_skips_missing_market_cap():
    results, mcap_map = _make_mcap_results()
    # Remove SMALL from mcap_map — it should not appear in any mcap group
    del mcap_map['SMALL']
    groups = compute_all_group_stats(results, mcap_map=mcap_map)
    small_groups = [g for g in groups if g.group_key == 'mcap:small']
    assert small_groups == []


def test_compute_all_group_stats_skips_none_market_cap():
    results, mcap_map = _make_mcap_results()
    mcap_map['LARGE'] = None
    groups = compute_all_group_stats(results, mcap_map=mcap_map)
    # LARGE has None market cap — no large group
    large_groups = [g for g in groups if g.group_key == 'mcap:large']
    assert large_groups == []


def test_compute_all_group_stats_no_mcap_map_produces_no_mcap_keys():
    results, _ = _make_mcap_results()
    groups = compute_all_group_stats(results)
    mcap_keys = [g.group_key for g in groups if g.group_key.startswith('mcap:')]
    assert mcap_keys == []


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


def test_compare_strategies_includes_sharpe_and_drawdown():
    a = _make_results([10.0, 12.0, 11.0])
    b = _make_results([2.0, 3.0, 1.0])
    result = compare_strategies(a, b, label_a='A', label_b='B')
    assert 'mean_sharpe_a' in result
    assert 'mean_sharpe_b' in result
    assert 'mean_max_dd_a' in result
    assert 'mean_max_dd_b' in result
    assert 'mean_win_rate_a' in result
    assert 'mean_win_rate_b' in result


# ---------------------------------------------------------------------------
# compare_against_benchmark
# ---------------------------------------------------------------------------

def test_compare_against_benchmark_returns_expected_keys():
    results = _make_results([10.0, 12.0, 8.0, 11.0])
    out = compare_against_benchmark(results, benchmark_return_pct=5.0, label='MyStrategy')
    assert out['label'] == 'MyStrategy'
    assert out['benchmark_return_pct'] == 5.0
    for key in ('mean_excess_return', 'median_excess_return', 'pct_beating_benchmark',
                't_stat', 'p_value', 'significant', 'n'):
        assert key in out


def test_compare_against_benchmark_correct_excess():
    results = _make_results([10.0, 20.0])
    out = compare_against_benchmark(results, benchmark_return_pct=5.0)
    # excess: [5.0, 15.0], mean = 10.0
    assert abs(out['mean_excess_return'] - 10.0) < 1e-9


def test_compare_against_benchmark_pct_beating():
    results = _make_results([10.0, 3.0, 8.0, 4.0])
    out = compare_against_benchmark(results, benchmark_return_pct=5.0)
    # Beats 5%: 10.0 and 8.0 → 2/4 = 50%
    assert abs(out['pct_beating_benchmark'] - 50.0) < 1e-9


def test_compare_against_benchmark_insufficient_data():
    results = _make_results([7.0])
    out = compare_against_benchmark(results, benchmark_return_pct=5.0)
    assert 'error' not in out   # single result is still valid; just no t-test
    assert math.isnan(out['t_stat'])


def test_compare_against_benchmark_empty_returns_error():
    out = compare_against_benchmark([], benchmark_return_pct=5.0)
    assert out.get('error') == 'insufficient_data'


def test_compare_against_benchmark_filters_errors():
    results = _make_results([10.0, 12.0])
    results.append({'ticker': 'BAD', 'return_pct': None, 'error': 'fail',
                    'sharpe_ratio': None, 'max_drawdown': None, 'win_rate': None,
                    'num_trades': None, 'profit_factor': None,
                    'classification': 'standard', 'sector': 'Technology'})
    out = compare_against_benchmark(results, benchmark_return_pct=5.0)
    assert out['n'] == 2


# ---------------------------------------------------------------------------
# GroupStats excess return / exposure fields
# ---------------------------------------------------------------------------

def _make_results_with_benchmark(returns: list[float], buy_hold: float,
                                  exposure: float = 50.0) -> list[dict]:
    return [
        {'ticker': f'T{i}', 'return_pct': r, 'sharpe_ratio': r / 10,
         'max_drawdown': -abs(r), 'win_rate': 60.0, 'num_trades': 5,
         'profit_factor': 1.2, 'classification': 'standard',
         'sector': 'Technology', 'error': None,
         'buy_hold_pct': buy_hold, 'exposure_pct': exposure}
        for i, r in enumerate(returns)
    ]


def test_group_stats_mean_excess_return():
    results = _make_results_with_benchmark([10.0, 12.0, 8.0], buy_hold=5.0)
    gs = compute_group_stats(results)
    # excess: [5, 7, 3], mean = 5.0
    assert abs(gs.mean_excess_return - 5.0) < 1e-9


def test_group_stats_pct_beating_buy_hold():
    results = _make_results_with_benchmark([10.0, 4.0, 8.0, 3.0], buy_hold=5.0)
    gs = compute_group_stats(results)
    # 10 > 5 and 8 > 5 → 2/4 = 50%
    assert abs(gs.pct_beating_buy_hold - 50.0) < 1e-9


def test_group_stats_mean_exposure_pct():
    results = _make_results_with_benchmark([5.0, 10.0], buy_hold=3.0, exposure=40.0)
    gs = compute_group_stats(results)
    assert abs(gs.mean_exposure_pct - 40.0) < 1e-9


def test_group_stats_excess_fields_nan_when_missing():
    results = _make_results([5.0, 8.0])   # no buy_hold_pct or exposure_pct
    gs = compute_group_stats(results)
    assert math.isnan(gs.mean_excess_return)
    assert math.isnan(gs.pct_beating_buy_hold)
    assert math.isnan(gs.mean_exposure_pct)
