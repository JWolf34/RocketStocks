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
    # Optional fields — populated when buy_hold_pct / exposure_pct are stored in results
    mean_excess_return: float = float('nan')   # mean(return_pct - buy_hold_pct) per ticker
    pct_beating_buy_hold: float = float('nan') # % of tickers where return_pct > buy_hold_pct
    mean_exposure_pct: float = float('nan')    # mean time the strategy held a position

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

    # Per-ticker excess return over buy-and-hold
    excess_returns = [
        float(r['return_pct']) - float(r['buy_hold_pct'])
        for r in valid
        if r.get('buy_hold_pct') is not None
    ]
    mean_excess = float(np.mean(excess_returns)) if excess_returns else float('nan')
    pct_beating = (
        sum(e > 0 for e in excess_returns) / len(excess_returns) * 100
        if excess_returns else float('nan')
    )

    # Mean exposure time
    exposure_vals = [float(r['exposure_pct']) for r in valid if r.get('exposure_pct') is not None]
    mean_exposure = float(np.mean(exposure_vals)) if exposure_vals else float('nan')

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
        mean_excess_return=mean_excess,
        pct_beating_buy_hold=pct_beating,
        mean_exposure_pct=mean_exposure,
    )


_MCAP_LABELS = {
    'small': 'Small (<$2B)',
    'mid':   'Mid ($2B–$10B)',
    'large': 'Large (>$10B)',
}


def _classify_market_cap(market_cap: int | None) -> str | None:
    if market_cap is None:
        return None
    if market_cap < 2_000_000_000:
        return 'small'
    if market_cap < 10_000_000_000:
        return 'mid'
    return 'large'


def compute_all_group_stats(
    results: list[dict],
    mcap_map: dict[str, int | None] | None = None,
) -> list[GroupStats]:
    """Compute aggregate stats across all tickers, by classification, sector, exchange,
    watchlist, and optionally by market cap tier.

    Args:
        results: Full list of result dicts from a backtest run.
        mcap_map: Optional {ticker: market_cap} mapping. When provided, results
            are also grouped into 'mcap:small' (<$2B), 'mcap:mid' ($2B–$10B),
            and 'mcap:large' (>$10B) tiers. Tickers absent from the map or with
            None market_cap are excluded from mcap groups.

    Returns:
        List of GroupStats: one for 'all', one per classification, one per sector,
        one per exchange, one per watchlist, and (if mcap_map given) one per tier.
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

    by_exchange: dict[str, list[dict]] = {}
    for r in results:
        exchange = r.get('exchange') or 'unknown'
        by_exchange.setdefault(exchange, []).append(r)
    for exchange, group in by_exchange.items():
        gs = compute_group_stats(group, group_key=f'exchange:{exchange}', group_value=exchange)
        if gs:
            out.append(gs)

    by_watchlist: dict[str, list[dict]] = {}
    for r in results:
        wl = r.get('watchlist')
        if wl:
            by_watchlist.setdefault(wl, []).append(r)
    for wl, group in by_watchlist.items():
        gs = compute_group_stats(group, group_key=f'watchlist:{wl}', group_value=wl)
        if gs:
            out.append(gs)

    if mcap_map:
        by_mcap: dict[str, list[dict]] = {}
        for r in results:
            tier = _classify_market_cap(mcap_map.get(r['ticker']))
            if tier:
                by_mcap.setdefault(tier, []).append(r)
        for tier in ('small', 'mid', 'large'):
            group = by_mcap.get(tier, [])
            gs = compute_group_stats(
                group, group_key=f'mcap:{tier}', group_value=_MCAP_LABELS[tier]
            )
            if gs:
                out.append(gs)

    return out


def compute_regime_stats(trades: list[dict]) -> list[dict]:
    """Compute per-regime trade statistics from trade-level records.

    Unlike compute_group_stats (which groups per-ticker results), this function
    groups individual trades by the market regime at entry time and computes
    aggregate statistics for each regime.

    Args:
        trades: List of trade dicts from backtest_trades table. Each must have
            'return_pct' and 'regime' fields.

    Returns:
        List of regime stat dicts, one per regime found. Each dict has:
            group_key, regime, n_trades, mean_return, median_return,
            std_return, win_rate, t_stat, p_value, significant.
    """
    from collections import defaultdict

    by_regime: dict[str, list[float]] = defaultdict(list)
    for t in trades:
        regime = t.get('regime') or 'unknown'
        ret = t.get('return_pct')
        if ret is not None:
            try:
                by_regime[regime].append(float(ret))
            except (TypeError, ValueError):
                pass

    out = []
    for regime, returns in sorted(by_regime.items()):
        n = len(returns)
        if n < 2:
            continue

        t_stat_val, p_val = scipy_stats.ttest_1samp(returns, 0)
        arr = np.array(returns)
        out.append({
            'group_key': f'regime:{regime}',
            'regime': regime,
            'n_trades': n,
            'mean_return': float(arr.mean()),
            'median_return': float(np.median(arr)),
            'std_return': float(arr.std(ddof=1)),
            'win_rate': float((arr > 0).mean() * 100),
            't_stat': float(t_stat_val),
            'p_value': float(p_val),
            'significant': float(p_val) < 0.05,
        })

    return out


def compare_against_benchmark(
    strategy_results: list[dict],
    benchmark_return_pct: float,
    label: str = 'Strategy',
) -> dict:
    """Compare strategy per-ticker returns against a single benchmark return.

    Uses a one-sample t-test on per-ticker excess returns (strategy_return -
    benchmark_return) vs 0 to test whether the strategy meaningfully beats
    the benchmark.

    Args:
        strategy_results: List of result dicts with 'return_pct'.
        benchmark_return_pct: Return of the benchmark over the same period.
        label: Display label for the strategy.

    Returns:
        Dict with: label, benchmark_return_pct, mean_excess_return,
        median_excess_return, pct_beating_benchmark, t_stat, p_value,
        significant, n. Returns error dict if no valid results.
    """
    valid_returns = [
        float(r['return_pct'])
        for r in strategy_results
        if r.get('return_pct') is not None and r.get('error') is None
    ]
    if not valid_returns:
        return {'label': label, 'error': 'insufficient_data'}

    excess = [r - benchmark_return_pct for r in valid_returns]
    n = len(excess)

    if n >= 2:
        t_stat, p_value = scipy_stats.ttest_1samp(excess, 0)
        t_stat = float(t_stat)
        p_value = float(p_value)
    else:
        t_stat = float('nan')
        p_value = float('nan')

    return {
        'label': label,
        'benchmark_return_pct': benchmark_return_pct,
        'mean_excess_return': float(np.mean(excess)),
        'median_excess_return': float(np.median(excess)),
        'pct_beating_benchmark': sum(e > 0 for e in excess) / n * 100,
        't_stat': t_stat,
        'p_value': p_value,
        'significant': p_value < 0.05 if not math.isnan(p_value) else False,
        'n': n,
    }


def compare_strategies(
    results_a: list[dict],
    results_b: list[dict],
    label_a: str = 'A',
    label_b: str = 'B',
) -> dict:
    """Compare two sets of backtest results using an independent two-sample t-test.

    Tests whether Strategy A's mean return is significantly different from
    Strategy B's mean return (two-tailed). Also compares mean Sharpe ratio,
    max drawdown, and win rate.

    Args:
        results_a: Results from the first strategy.
        results_b: Results from the second strategy.
        label_a: Display label for strategy A.
        label_b: Display label for strategy B.

    Returns:
        Dict with keys: label_a, label_b, mean_a, mean_b, n_a, n_b,
        t_stat, p_value, significant, better, plus mean_sharpe_a/b,
        mean_max_dd_a/b, mean_win_rate_a/b.
        If either group has fewer than 2 results, returns an error dict.
    """
    valid_a = [r for r in results_a if r.get('return_pct') is not None]
    valid_b = [r for r in results_b if r.get('return_pct') is not None]
    returns_a = [float(r['return_pct']) for r in valid_a]
    returns_b = [float(r['return_pct']) for r in valid_b]

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
        'mean_sharpe_a': _safe_mean([r.get('sharpe_ratio') for r in valid_a]),
        'mean_sharpe_b': _safe_mean([r.get('sharpe_ratio') for r in valid_b]),
        'mean_max_dd_a': _safe_mean([r.get('max_drawdown') for r in valid_a]),
        'mean_max_dd_b': _safe_mean([r.get('max_drawdown') for r in valid_b]),
        'mean_win_rate_a': _safe_mean([r.get('win_rate') for r in valid_a]),
        'mean_win_rate_b': _safe_mean([r.get('win_rate') for r in valid_b]),
    }
