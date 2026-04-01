"""Tests for rocketstocks.backtest.correlation."""
import datetime

import pytest

from rocketstocks.backtest.correlation import SignalCorrelation, compute_signal_correlation


def _trade(ticker: str, date: str, return_pct: float = 1.0) -> dict:
    return {
        'ticker': ticker,
        'entry_time': datetime.datetime.fromisoformat(date),
        'return_pct': return_pct,
    }


# ---------------------------------------------------------------------------
# Basic contract
# ---------------------------------------------------------------------------

def test_compute_signal_correlation_returns_dataclass():
    a = [_trade('AAPL', '2024-01-10')]
    b = [_trade('AAPL', '2024-01-10')]
    result = compute_signal_correlation(a, b)
    assert isinstance(result, SignalCorrelation)


def test_compute_signal_correlation_labels_stored():
    a = [_trade('AAPL', '2024-01-10')]
    b = [_trade('AAPL', '2024-01-10')]
    result = compute_signal_correlation(a, b, label_a='VolStrat', label_b='PopStrat')
    assert result.label_a == 'VolStrat'
    assert result.label_b == 'PopStrat'


def test_compute_signal_correlation_trade_counts():
    a = [_trade('AAPL', '2024-01-10'), _trade('MSFT', '2024-01-15')]
    b = [_trade('AAPL', '2024-01-10')]
    result = compute_signal_correlation(a, b)
    assert result.n_trades_a == 2
    assert result.n_trades_b == 1


# ---------------------------------------------------------------------------
# Perfect overlap
# ---------------------------------------------------------------------------

def test_perfect_overlap_jaccard_is_one():
    """Identical trades on same tickers and same dates → full overlap."""
    trades = [_trade('AAPL', '2024-01-10', 2.0), _trade('MSFT', '2024-02-01', 1.5)]
    result = compute_signal_correlation(trades, trades, overlap_window=3)
    assert result.jaccard_index == pytest.approx(1.0)


def test_perfect_overlap_n_overlap_equals_total():
    trades = [_trade('AAPL', '2024-01-10'), _trade('MSFT', '2024-02-01')]
    result = compute_signal_correlation(trades, trades)
    assert result.n_overlap == len(trades)


def test_perfect_overlap_pct_100():
    trades = [_trade('AAPL', '2024-01-10')]
    result = compute_signal_correlation(trades, trades)
    assert result.overlap_pct_a == pytest.approx(100.0)
    assert result.overlap_pct_b == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# No overlap
# ---------------------------------------------------------------------------

def test_no_overlap_jaccard_is_zero():
    """Different tickers → no overlap."""
    a = [_trade('AAPL', '2024-01-10')]
    b = [_trade('GME', '2024-01-10')]
    result = compute_signal_correlation(a, b)
    assert result.jaccard_index == pytest.approx(0.0)


def test_no_overlap_n_overlap_zero():
    a = [_trade('AAPL', '2024-01-10')]
    b = [_trade('GME', '2024-01-11')]
    result = compute_signal_correlation(a, b)
    assert result.n_overlap == 0


def test_no_overlap_independent_flag_true():
    a = [_trade('AAPL', '2024-01-10')]
    b = [_trade('GME', '2024-01-10')]
    result = compute_signal_correlation(a, b)
    assert result.independent is True


# ---------------------------------------------------------------------------
# Partial overlap
# ---------------------------------------------------------------------------

def test_partial_overlap_correct_n_overlap():
    """A fires on AAPL+MSFT, B fires on AAPL only → 1 overlap."""
    a = [_trade('AAPL', '2024-01-10'), _trade('MSFT', '2024-01-10')]
    b = [_trade('AAPL', '2024-01-11')]  # within 3-day window
    result = compute_signal_correlation(a, b, overlap_window=3)
    assert result.n_overlap == 1


def test_partial_overlap_n_a_only():
    a = [_trade('AAPL', '2024-01-10'), _trade('MSFT', '2024-01-10')]
    b = [_trade('AAPL', '2024-01-10')]
    result = compute_signal_correlation(a, b)
    assert result.n_a_only == 1  # MSFT is only in A


def test_partial_overlap_n_b_only():
    a = [_trade('AAPL', '2024-01-10')]
    b = [_trade('AAPL', '2024-01-10'), _trade('GME', '2024-03-01')]
    result = compute_signal_correlation(a, b)
    assert result.n_b_only == 1  # GME is only in B


# ---------------------------------------------------------------------------
# Overlap window
# ---------------------------------------------------------------------------

def test_overlap_window_zero_requires_exact_date():
    """With window=0, trades must match the same calendar date."""
    a = [_trade('AAPL', '2024-01-10')]
    b = [_trade('AAPL', '2024-01-11')]  # one day off
    result = compute_signal_correlation(a, b, overlap_window=0)
    assert result.n_overlap == 0


def test_overlap_window_includes_nearby_dates():
    """With window=5, a 4-day gap should count as overlap."""
    a = [_trade('AAPL', '2024-01-10')]
    b = [_trade('AAPL', '2024-01-14')]
    result = compute_signal_correlation(a, b, overlap_window=5)
    assert result.n_overlap == 1


# ---------------------------------------------------------------------------
# Statistical fields
# ---------------------------------------------------------------------------

def test_overlap_stats_none_when_insufficient():
    """With a single overlapping trade (n<2), mean_return_overlap should be None."""
    a = [_trade('AAPL', '2024-01-10', 5.0)]
    b = [_trade('AAPL', '2024-01-10', 3.0)]
    result = compute_signal_correlation(a, b)
    # n_overlap == 1, so stats are None
    assert result.mean_return_overlap is None
    assert result.p_value_overlap is None
    assert result.significant_overlap is False


def test_a_only_returns_none_when_no_a_only():
    """When every A trade overlaps with B, a_only stats are None."""
    a = [_trade('AAPL', '2024-01-10')]
    b = [_trade('AAPL', '2024-01-10')]
    result = compute_signal_correlation(a, b)
    assert result.mean_return_a_only is None


# ---------------------------------------------------------------------------
# Independent flag
# ---------------------------------------------------------------------------

def test_independent_true_when_jaccard_below_025():
    a = [_trade('AAPL', '2024-01-10')]
    b = [_trade('GME', '2024-06-01')]  # no overlap
    result = compute_signal_correlation(a, b)
    assert result.independent is True


def test_independent_false_when_high_jaccard():
    trades = [
        _trade('AAPL', '2024-01-10'), _trade('MSFT', '2024-01-10'),
        _trade('GOOG', '2024-01-10'), _trade('AMZN', '2024-01-10'),
    ]
    result = compute_signal_correlation(trades, trades)
    assert result.independent is False


# ---------------------------------------------------------------------------
# Conclusion
# ---------------------------------------------------------------------------

def test_conclusion_is_non_empty_string():
    a = [_trade('AAPL', '2024-01-10')]
    b = [_trade('MSFT', '2024-01-10')]
    result = compute_signal_correlation(a, b)
    assert isinstance(result.conclusion, str)
    assert len(result.conclusion) > 0


def test_conclusion_mentions_jaccard():
    a = [_trade('AAPL', '2024-01-10')]
    b = [_trade('AAPL', '2024-01-10')]
    result = compute_signal_correlation(a, b)
    assert 'Jaccard' in result.conclusion


# ---------------------------------------------------------------------------
# Empty inputs
# ---------------------------------------------------------------------------

def test_empty_a_produces_zero_overlap():
    b = [_trade('AAPL', '2024-01-10')]
    result = compute_signal_correlation([], b)
    assert result.n_overlap == 0
    assert result.n_trades_a == 0
    assert result.jaccard_index == 0.0


def test_empty_b_produces_zero_overlap():
    a = [_trade('AAPL', '2024-01-10')]
    result = compute_signal_correlation(a, [])
    assert result.n_overlap == 0
    assert result.n_trades_b == 0
