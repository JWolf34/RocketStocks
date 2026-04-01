"""Tests for rocketstocks.backtest.monte_carlo."""
import math

import pytest

from rocketstocks.backtest.monte_carlo import MonteCarloResult, _compute_equity_curve, run_monte_carlo

_RETURNS = [1.5, -0.5, 2.0, -1.0, 0.8, 1.2, -0.3, 3.0, -1.5, 0.9]  # 10 trades


# ---------------------------------------------------------------------------
# _compute_equity_curve
# ---------------------------------------------------------------------------

def test_equity_curve_starts_at_starting_cash():
    curve, _ = _compute_equity_curve([1.0, 2.0], starting_cash=10_000)
    assert curve[0] == 10_000


def test_equity_curve_length_is_n_trades_plus_one():
    trades = [1.0, -1.0, 2.0]
    curve, _ = _compute_equity_curve(trades, 10_000)
    assert len(curve) == len(trades) + 1


def test_equity_curve_positive_return_grows_equity():
    curve, _ = _compute_equity_curve([10.0], 10_000)
    assert curve[-1] > 10_000


def test_equity_curve_negative_return_shrinks_equity():
    curve, _ = _compute_equity_curve([-10.0], 10_000)
    assert curve[-1] < 10_000


def test_equity_curve_max_drawdown_is_non_positive():
    _, max_dd = _compute_equity_curve([10.0, -20.0, 5.0], 10_000)
    assert max_dd <= 0.0


def test_equity_curve_max_drawdown_zero_when_monotone_up():
    _, max_dd = _compute_equity_curve([1.0, 2.0, 3.0], 10_000)
    assert max_dd == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# run_monte_carlo — basic contract
# ---------------------------------------------------------------------------

def test_run_monte_carlo_returns_result_type():
    result = run_monte_carlo(_RETURNS, seed=42)
    assert isinstance(result, MonteCarloResult)


def test_run_monte_carlo_n_simulations_recorded():
    result = run_monte_carlo(_RETURNS, n_simulations=200, seed=0)
    assert result.n_simulations == 200


def test_run_monte_carlo_n_trades_recorded():
    result = run_monte_carlo(_RETURNS, seed=0)
    assert result.n_trades == len(_RETURNS)


def test_run_monte_carlo_starting_cash_recorded():
    result = run_monte_carlo(_RETURNS, starting_cash=5_000, seed=0)
    assert result.starting_cash == 5_000


def test_run_monte_carlo_ruin_threshold_recorded():
    result = run_monte_carlo(_RETURNS, ruin_threshold=0.4, seed=0)
    assert result.ruin_threshold == pytest.approx(0.4)


# ---------------------------------------------------------------------------
# run_monte_carlo — drawdown percentile ordering
# ---------------------------------------------------------------------------

def test_drawdown_percentiles_ordered():
    result = run_monte_carlo(_RETURNS, n_simulations=500, seed=1)
    # p5 ≤ p25 ≤ p50 ≤ p75 ≤ p95 for max_drawdown (all ≤ 0)
    assert result.p5_max_dd <= result.p25_max_dd
    assert result.p25_max_dd <= result.p50_max_dd
    assert result.p50_max_dd <= result.p75_max_dd
    assert result.p75_max_dd <= result.p95_max_dd


def test_equity_percentiles_ordered():
    result = run_monte_carlo(_RETURNS, n_simulations=500, seed=2)
    assert result.p5_final_equity <= result.p25_final_equity
    assert result.p25_final_equity <= result.p50_final_equity
    assert result.p50_final_equity <= result.p75_final_equity
    assert result.p75_final_equity <= result.p95_final_equity


def test_max_drawdown_all_non_positive():
    result = run_monte_carlo(_RETURNS, n_simulations=200, seed=3)
    assert result.p5_max_dd <= 0.0
    assert result.p95_max_dd <= 0.0


# ---------------------------------------------------------------------------
# run_monte_carlo — ruin probability
# ---------------------------------------------------------------------------

def test_ruin_probability_between_0_and_100():
    result = run_monte_carlo(_RETURNS, n_simulations=200, seed=4)
    assert 0.0 <= result.ruin_probability <= 100.0


def test_ruin_probability_zero_with_all_winning_trades():
    all_wins = [5.0] * 20
    # Ruin threshold 50% — equity always grows, no ruin possible
    result = run_monte_carlo(all_wins, n_simulations=100, seed=5, ruin_threshold=0.5)
    assert result.ruin_probability == 0.0


def test_ruin_probability_high_with_all_losing_trades():
    all_losses = [-5.0] * 20
    result = run_monte_carlo(all_losses, n_simulations=100, seed=6, ruin_threshold=0.99)
    assert result.ruin_probability == 100.0


# ---------------------------------------------------------------------------
# run_monte_carlo — reproducibility
# ---------------------------------------------------------------------------

def test_same_seed_produces_same_result():
    r1 = run_monte_carlo(_RETURNS, n_simulations=100, seed=99)
    r2 = run_monte_carlo(_RETURNS, n_simulations=100, seed=99)
    assert r1.mean_max_dd == pytest.approx(r2.mean_max_dd)
    assert r1.ruin_probability == pytest.approx(r2.ruin_probability)


def test_different_seeds_may_differ():
    r1 = run_monte_carlo(_RETURNS, n_simulations=500, seed=1)
    r2 = run_monte_carlo(_RETURNS, n_simulations=500, seed=9999)
    # Not guaranteed to differ for every metric, but mean_max_dd should vary
    # (soft assertion — just verify neither crashes)
    assert isinstance(r1.mean_max_dd, float)
    assert isinstance(r2.mean_max_dd, float)


# ---------------------------------------------------------------------------
# run_monte_carlo — historical_max_dd override
# ---------------------------------------------------------------------------

def test_historical_max_dd_passed_through():
    result = run_monte_carlo(_RETURNS, historical_max_dd=-12.5, seed=0)
    assert result.historical_max_dd == pytest.approx(-12.5)


def test_historical_max_dd_computed_when_not_provided():
    result = run_monte_carlo(_RETURNS, seed=0)
    assert result.historical_max_dd <= 0.0


# ---------------------------------------------------------------------------
# run_monte_carlo — error on empty input
# ---------------------------------------------------------------------------

def test_empty_trade_returns_raises_value_error():
    with pytest.raises(ValueError, match='trade_returns'):
        run_monte_carlo([])
