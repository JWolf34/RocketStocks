"""Monte Carlo drawdown simulation for backtest results.

Takes a list of per-trade return percentages from a completed backtest run,
randomly shuffles the trade order N times, and computes the equity curve and
maximum drawdown for each permutation. The resulting distribution shows
realistic worst-case drawdown estimates that go beyond the single historical
path recorded during the backtest.

Key insight: the historical max drawdown is just ONE possible ordering of
the same trades. Monte Carlo reveals the probability distribution — e.g.,
there is a 5% chance you would have experienced a -30% drawdown even with
the same trades in a different sequence.
"""
import logging
import math
import random
from dataclasses import dataclass

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class MonteCarloResult:
    """Statistics from a Monte Carlo drawdown simulation."""
    n_simulations: int
    n_trades: int
    starting_cash: float

    # Max drawdown percentiles
    historical_max_dd: float    # actual historical max drawdown from the run
    mean_max_dd: float
    p5_max_dd: float            # 5th percentile — worst 5% of paths
    p25_max_dd: float
    p50_max_dd: float           # median
    p75_max_dd: float
    p95_max_dd: float           # 95th percentile — best 5% of paths

    # Final equity percentiles
    mean_final_equity: float
    p5_final_equity: float
    p25_final_equity: float
    p50_final_equity: float
    p75_final_equity: float
    p95_final_equity: float

    # Probability of ruin (equity drops below ruin_threshold * starting_cash)
    ruin_probability: float
    ruin_threshold: float       # fraction of starting_cash (e.g. 0.5 = 50%)


def _compute_equity_curve(
    trade_returns: list[float],
    starting_cash: float,
) -> tuple[list[float], float]:
    """Simulate a single equity curve from a sequence of trade returns.

    Args:
        trade_returns: List of per-trade return percentages (not fractions).
        starting_cash: Starting portfolio value.

    Returns:
        Tuple of (equity_curve, max_drawdown_pct). max_drawdown_pct is negative.
    """
    equity = starting_cash
    peak = equity
    max_dd = 0.0
    curve = [equity]

    for ret_pct in trade_returns:
        equity *= (1 + ret_pct / 100)
        curve.append(equity)
        if equity > peak:
            peak = equity
        dd = (equity / peak - 1) * 100
        if dd < max_dd:
            max_dd = dd

    return curve, max_dd


def run_monte_carlo(
    trade_returns: list[float],
    starting_cash: float = 10_000,
    n_simulations: int = 1_000,
    ruin_threshold: float = 0.5,
    historical_max_dd: float | None = None,
    seed: int | None = None,
) -> MonteCarloResult:
    """Run Monte Carlo simulation by shuffling trade return order.

    Args:
        trade_returns: List of per-trade return percentages from backtest_trades
            (return_pct column, expressed as a percentage, e.g. 2.5 for +2.5%).
        starting_cash: Starting portfolio value for equity curve simulation (default 10_000).
        n_simulations: Number of random shuffles to perform (default 1_000).
        ruin_threshold: Equity level below which the simulation counts as "ruin",
            expressed as a fraction of starting_cash (default 0.5 = 50%).
        historical_max_dd: The actual max drawdown from the backtest run's result
            (for comparison). If None, computed from the original order.
        seed: Optional random seed for reproducibility.

    Returns:
        MonteCarloResult with drawdown and equity percentile distributions.
    """
    if not trade_returns:
        raise ValueError('trade_returns must not be empty')

    rng = random.Random(seed)

    # Compute historical max drawdown from the original trade order if not provided
    if historical_max_dd is None:
        _, historical_max_dd = _compute_equity_curve(trade_returns, starting_cash)

    ruin_equity = starting_cash * ruin_threshold
    max_drawdowns: list[float] = []
    final_equities: list[float] = []
    ruin_count = 0

    shuffled = list(trade_returns)
    for _ in range(n_simulations):
        rng.shuffle(shuffled)
        curve, max_dd = _compute_equity_curve(shuffled, starting_cash)
        max_drawdowns.append(max_dd)
        final_equity = curve[-1]
        final_equities.append(final_equity)
        if min(curve) < ruin_equity:
            ruin_count += 1

    dd_arr = np.array(max_drawdowns)
    eq_arr = np.array(final_equities)

    return MonteCarloResult(
        n_simulations=n_simulations,
        n_trades=len(trade_returns),
        starting_cash=starting_cash,
        historical_max_dd=historical_max_dd,
        mean_max_dd=float(dd_arr.mean()),
        p5_max_dd=float(np.percentile(dd_arr, 5)),
        p25_max_dd=float(np.percentile(dd_arr, 25)),
        p50_max_dd=float(np.percentile(dd_arr, 50)),
        p75_max_dd=float(np.percentile(dd_arr, 75)),
        p95_max_dd=float(np.percentile(dd_arr, 95)),
        mean_final_equity=float(eq_arr.mean()),
        p5_final_equity=float(np.percentile(eq_arr, 5)),
        p25_final_equity=float(np.percentile(eq_arr, 25)),
        p50_final_equity=float(np.percentile(eq_arr, 50)),
        p75_final_equity=float(np.percentile(eq_arr, 75)),
        p95_final_equity=float(np.percentile(eq_arr, 95)),
        ruin_probability=ruin_count / n_simulations * 100,
        ruin_threshold=ruin_threshold,
    )
