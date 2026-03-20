"""Statistical aggregation and significance testing for backtest results."""
import logging
import math
from dataclasses import asdict, dataclass

import numpy as np
from scipy import stats as scipy_stats

logger = logging.getLogger(__name__)


@dataclass
class GroupStats:
    """Pre-computed aggregate statistics for a group of backtest results."""

    group_key: str          # e.g. 'all', 'sector:Technology', 'class:volatile'
    group_value: str | None
    ticker_count: int
    mean_return: float
    median_return: float
    std_return: float
    mean_sharpe: float
    mean_win_rate: float
    total_trades: int
    mean_max_dd: float
    mean_profit_factor: float
    t_stat: float
    p_value: float
    significant: bool       # p < 0.05

    def to_dict(self) -> dict:
        return asdict(self)


def _safe_mean(values: list) -> float:
    cleaned = [v for v in values if v is not None and not math.isnan(float(v))]
    return float(np.mean(cleaned)) if cleaned else float('nan')


def compute_group_stats(
    results: list[dict],
    group_key: str = 'all',
    group_value: str | None = None,
) -> GroupStats | None:
    """Compute aggregate statistics for a group of backtest results.

    Runs a one-sample t-test against zero to determine whether the mean
    return is statistically significantly positive.

    Args:
        results: List of result dicts from backtest_results table. Each
            must contain 'return_pct' (and optionally other metric fields).
        group_key: Identifier for the group (e.g. 'all', 'class:volatile').
        group_value: Human-readable group label for display.

    Returns:
        GroupStats dataclass, or None if there are no valid results.
    """
    valid = [r for r in results if r.get('return_pct') is not None and r.get('error') is None]
    if not valid:
        return None

    returns = [float(r['return_pct']) for r in valid]
    n = len(returns)

    if n >= 2:
        t_stat_val, p_val = scipy_stats.ttest_1samp(returns, 0)
        t_stat_val = float(t_stat_val)
        p_val = float(p_val)
    else:
        t_stat_val = float('nan')
        p_val = float('nan')

    return GroupStats(
        group_key=group_key,
        group_value=group_value,
        ticker_count=n,
        mean_return=float(np.mean(returns)),
        median_return=float(np.median(returns)),
        std_return=float(np.std(returns, ddof=1)) if n > 1 else 0.0,
        mean_sharpe=_safe_mean([r.get('sharpe_ratio') for r in valid]),
        mean_win_rate=_safe_mean([r.get('win_rate') for r in valid]),
        total_trades=int(sum(r.get('num_trades') or 0 for r in valid)),
        mean_max_dd=_safe_mean([r.get('max_drawdown') for r in valid]),
        mean_profit_factor=_safe_mean([r.get('profit_factor') for r in valid]),
        t_stat=t_stat_val,
        p_value=p_val,
        significant=p_val < 0.05 if not math.isnan(p_val) else False,
    )


def compute_all_group_stats(results: list[dict]) -> list[GroupStats]:
    """Compute aggregate stats across all tickers, by classification, and by sector.

    Args:
        results: Full list of result dicts from a backtest run.

    Returns:
        List of GroupStats: one for 'all', one per classification found in
        results, and one per sector found in results.
    """
    out: list[GroupStats] = []

    overall = compute_group_stats(results, group_key='all', group_value='All Tickers')
    if overall:
        out.append(overall)

    by_class: dict[str, list[dict]] = {}
    for r in results:
        cls = r.get('classification') or 'unknown'
        by_class.setdefault(cls, []).append(r)
    for cls, group in by_class.items():
        gs = compute_group_stats(group, group_key=f'class:{cls}', group_value=cls)
        if gs:
            out.append(gs)

    by_sector: dict[str, list[dict]] = {}
    for r in results:
        sector = r.get('sector') or 'unknown'
        by_sector.setdefault(sector, []).append(r)
    for sector, group in by_sector.items():
        gs = compute_group_stats(group, group_key=f'sector:{sector}', group_value=sector)
        if gs:
            out.append(gs)

    return out


def compare_strategies(
    results_a: list[dict],
    results_b: list[dict],
    label_a: str = 'A',
    label_b: str = 'B',
) -> dict:
    """Compare two sets of backtest results using an independent two-sample t-test.

    Tests whether Strategy A's mean return is significantly different from
    Strategy B's mean return (two-tailed).

    Args:
        results_a: Results from the first strategy.
        results_b: Results from the second strategy.
        label_a: Display label for strategy A.
        label_b: Display label for strategy B.

    Returns:
        Dict with keys: label_a, label_b, mean_a, mean_b, n_a, n_b,
        t_stat, p_value, significant, better.
        If either group has fewer than 2 results, returns an error dict.
    """
    returns_a = [float(r['return_pct']) for r in results_a if r.get('return_pct') is not None]
    returns_b = [float(r['return_pct']) for r in results_b if r.get('return_pct') is not None]

    if len(returns_a) < 2 or len(returns_b) < 2:
        return {
            'label_a': label_a,
            'label_b': label_b,
            'mean_a': float(np.mean(returns_a)) if returns_a else None,
            'mean_b': float(np.mean(returns_b)) if returns_b else None,
            'error': 'insufficient_data',
        }

    t_stat, p_value = scipy_stats.ttest_ind(returns_a, returns_b)
    mean_a = float(np.mean(returns_a))
    mean_b = float(np.mean(returns_b))

    return {
        'label_a': label_a,
        'label_b': label_b,
        'mean_a': mean_a,
        'mean_b': mean_b,
        'n_a': len(returns_a),
        'n_b': len(returns_b),
        't_stat': float(t_stat),
        'p_value': float(p_value),
        'significant': float(p_value) < 0.05,
        'better': label_a if mean_a > mean_b else label_b,
    }
