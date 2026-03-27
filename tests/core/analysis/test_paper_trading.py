"""Tests for rocketstocks.core.analysis.paper_trading pure functions."""
import pytest

from rocketstocks.core.analysis.paper_trading import (
    calculate_gain_loss,
    calculate_new_avg_cost_basis,
    calculate_portfolio_total,
    calculate_position_value,
    calculate_total_gain_loss,
)


# ---------------------------------------------------------------------------
# calculate_position_value
# ---------------------------------------------------------------------------

def test_position_value_basic():
    assert calculate_position_value(10, 150.0) == pytest.approx(1500.0)


def test_position_value_zero_shares():
    assert calculate_position_value(0, 150.0) == pytest.approx(0.0)


def test_position_value_zero_price():
    assert calculate_position_value(10, 0.0) == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# calculate_gain_loss
# ---------------------------------------------------------------------------

def test_gain_loss_profit():
    gain, pct = calculate_gain_loss(10, 100.0, 150.0)
    assert gain == pytest.approx(500.0)
    assert pct == pytest.approx(50.0)


def test_gain_loss_loss():
    gain, pct = calculate_gain_loss(10, 150.0, 100.0)
    assert gain == pytest.approx(-500.0)
    assert pct == pytest.approx(-33.333, rel=1e-3)


def test_gain_loss_zero_cost_basis():
    gain, pct = calculate_gain_loss(10, 0.0, 100.0)
    assert gain == pytest.approx(1000.0)
    assert pct == pytest.approx(0.0)  # avoids division by zero


def test_gain_loss_breakeven():
    gain, pct = calculate_gain_loss(5, 200.0, 200.0)
    assert gain == pytest.approx(0.0)
    assert pct == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# calculate_portfolio_total
# ---------------------------------------------------------------------------

def test_portfolio_total():
    assert calculate_portfolio_total(1000.0, 9000.0) == pytest.approx(10000.0)


def test_portfolio_total_no_positions():
    assert calculate_portfolio_total(10000.0, 0.0) == pytest.approx(10000.0)


# ---------------------------------------------------------------------------
# calculate_total_gain_loss
# ---------------------------------------------------------------------------

def test_total_gain_loss_profit():
    gain, pct = calculate_total_gain_loss(11000.0)
    assert gain == pytest.approx(1000.0)
    assert pct == pytest.approx(10.0)


def test_total_gain_loss_loss():
    gain, pct = calculate_total_gain_loss(9000.0)
    assert gain == pytest.approx(-1000.0)
    assert pct == pytest.approx(-10.0)


def test_total_gain_loss_breakeven():
    gain, pct = calculate_total_gain_loss(10000.0)
    assert gain == pytest.approx(0.0)
    assert pct == pytest.approx(0.0)


def test_total_gain_loss_custom_starting_capital():
    gain, pct = calculate_total_gain_loss(11000.0, starting_capital=5000.0)
    assert gain == pytest.approx(6000.0)
    assert pct == pytest.approx(120.0)


def test_total_gain_loss_zero_starting_capital():
    gain, pct = calculate_total_gain_loss(10000.0, starting_capital=0.0)
    assert gain == pytest.approx(10000.0)
    assert pct == pytest.approx(0.0)  # avoids division by zero


# ---------------------------------------------------------------------------
# calculate_new_avg_cost_basis
# ---------------------------------------------------------------------------

def test_new_avg_cost_basis_equal_lots():
    avg = calculate_new_avg_cost_basis(10, 100.0, 10, 200.0)
    assert avg == pytest.approx(150.0)


def test_new_avg_cost_basis_unequal_lots():
    # 10 shares at $100, 20 more at $160 → (1000 + 3200) / 30 = 140.0
    avg = calculate_new_avg_cost_basis(10, 100.0, 20, 160.0)
    assert avg == pytest.approx(140.0)


def test_new_avg_cost_basis_first_buy():
    avg = calculate_new_avg_cost_basis(0, 0.0, 10, 150.0)
    assert avg == pytest.approx(150.0)


def test_new_avg_cost_basis_zero_total_shares():
    avg = calculate_new_avg_cost_basis(0, 0.0, 0, 150.0)
    assert avg == pytest.approx(0.0)
