"""Signal overlap and redundancy analysis for backtest runs.

Compares two backtest runs to determine whether their signals are firing
independently (complementary) or overlapping (redundant).

A high Jaccard overlap means both strategies are finding the same opportunities
at the same time — combining them doesn't add new information. A low overlap
with both strategies independently significant means they are genuinely
complementary, and the `leading_indicator_combo` strategy should outperform either alone.

Usage:
    From the CLI: python -m rocketstocks.backtest correlation <run_id_a> <run_id_b>
"""
import logging
import math
from dataclasses import dataclass

import numpy as np
from scipy import stats as scipy_stats

logger = logging.getLogger(__name__)


@dataclass
class SignalCorrelation:
    """Result of comparing two backtest runs' trade overlap."""
    label_a: str
    label_b: str

    n_trades_a: int
    n_trades_b: int

    # Overlap metrics
    n_overlap: int              # trades within ±overlap_window bars on the same ticker
    overlap_pct_a: float        # % of A's trades that overlap with B
    overlap_pct_b: float        # % of B's trades that overlap with A
    jaccard_index: float        # |A ∩ B| / |A ∪ B|

    # Return statistics by group
    mean_return_overlap: float | None   # when both fire
    mean_return_a_only: float | None    # when only A fires
    mean_return_b_only: float | None    # when only B fires

    # Statistical significance per group
    p_value_overlap: float | None
    p_value_a_only: float | None
    p_value_b_only: float | None
    significant_overlap: bool
    significant_a_only: bool
    significant_b_only: bool

    n_a_only: int
    n_b_only: int

    # Interpretation
    independent: bool   # True if signals are largely non-overlapping (Jaccard < 0.25)
    conclusion: str


def compute_signal_correlation(
    trades_a: list[dict],
    trades_b: list[dict],
    label_a: str = 'Run A',
    label_b: str = 'Run B',
    overlap_window: int = 3,
) -> SignalCorrelation:
    """Compare two sets of trades to measure signal overlap and independence.

    Two trades are considered to "overlap" if they share the same ticker AND
    their entry times fall within ``±overlap_window`` calendar days of each other.

    Args:
        trades_a: Trades from the first run (from backtest_trades).
        trades_b: Trades from the second run (from backtest_trades).
        label_a: Display label for run A (e.g. 'volume_accumulation').
        label_b: Display label for run B (e.g. 'popularity_surge').
        overlap_window: Number of calendar days within which two entries on the
            same ticker are considered the same signal (default 3).

    Returns:
        SignalCorrelation dataclass with overlap stats and interpretation.
    """
    import datetime

    def _to_date(entry_time) -> datetime.date | None:
        if entry_time is None:
            return None
        if hasattr(entry_time, 'date'):
            return entry_time.date()
        if isinstance(entry_time, datetime.date):
            return entry_time
        try:
            import pandas as pd
            return pd.Timestamp(entry_time).date()
        except Exception:
            return None

    # Build lookup: ticker → sorted list of (date, return_pct) for each set
    def _build_index(trades: list[dict]) -> dict[str, list[tuple]]:
        idx: dict[str, list[tuple]] = {}
        for t in trades:
            ticker = t.get('ticker')
            entry_date = _to_date(t.get('entry_time'))
            ret = t.get('return_pct')
            if ticker and entry_date and ret is not None:
                idx.setdefault(ticker, []).append((entry_date, float(ret)))
        for ticker in idx:
            idx[ticker].sort(key=lambda x: x[0])
        return idx

    idx_a = _build_index(trades_a)
    idx_b = _build_index(trades_b)

    # Find overlapping trades
    overlap_returns_a: list[float] = []
    overlap_returns_b: list[float] = []
    a_only_returns: list[float] = []
    b_only_returns: list[float] = []
    matched_a: set[int] = set()   # indices in trades_a that matched
    matched_b_keys: set[tuple] = set()   # (ticker, date) keys in b that matched

    window_delta = __import__('datetime').timedelta(days=overlap_window)

    for i, t_a in enumerate(trades_a):
        ticker = t_a.get('ticker')
        entry_date = _to_date(t_a.get('entry_time'))
        ret_a = t_a.get('return_pct')
        if not ticker or not entry_date or ret_a is None:
            continue

        b_entries = idx_b.get(ticker, [])
        found_match = False
        for b_date, b_ret in b_entries:
            if abs((entry_date - b_date).days) <= overlap_window:
                overlap_returns_a.append(float(ret_a))
                overlap_returns_b.append(b_ret)
                matched_a.add(i)
                matched_b_keys.add((ticker, b_date))
                found_match = True
                break  # count each A trade as overlapping at most once

        if not found_match:
            a_only_returns.append(float(ret_a))

    # B-only: trades in B that didn't match any A trade
    for t_b in trades_b:
        ticker = t_b.get('ticker')
        entry_date = _to_date(t_b.get('entry_time'))
        ret_b = t_b.get('return_pct')
        if not ticker or not entry_date or ret_b is None:
            continue
        if (ticker, entry_date) not in matched_b_keys:
            # Double-check: scan within window
            a_entries = idx_a.get(ticker, [])
            matched = any(abs((entry_date - a_date).days) <= overlap_window
                          for a_date, _ in a_entries)
            if not matched:
                b_only_returns.append(float(ret_b))

    n_a = len(trades_a)
    n_b = len(trades_b)
    n_overlap = len(overlap_returns_a)
    n_a_only = len(a_only_returns)
    n_b_only = len(b_only_returns)

    overlap_pct_a = (n_overlap / n_a * 100) if n_a > 0 else 0.0
    overlap_pct_b = (n_overlap / n_b * 100) if n_b > 0 else 0.0

    # Jaccard: |intersection| / |union|
    union = n_a + n_b - n_overlap
    jaccard = n_overlap / union if union > 0 else 0.0

    def _stats(returns: list[float]) -> tuple[float | None, float | None, bool]:
        if len(returns) < 2:
            return None, None, False
        arr = np.array(returns)
        _, p_val = scipy_stats.ttest_1samp(arr, 0)
        return float(arr.mean()), float(p_val), float(p_val) < 0.05

    mean_ov, p_ov, sig_ov = _stats(overlap_returns_a)
    mean_ao, p_ao, sig_ao = _stats(a_only_returns)
    mean_bo, p_bo, sig_bo = _stats(b_only_returns)

    independent = jaccard < 0.25

    # Build interpretation
    parts = []
    if jaccard < 0.15:
        parts.append(f'Signals are highly independent (Jaccard={jaccard:.2f}).')
    elif jaccard < 0.35:
        parts.append(f'Signals have low overlap (Jaccard={jaccard:.2f}).')
    else:
        parts.append(f'Signals overlap substantially (Jaccard={jaccard:.2f}) — may be redundant.')

    if sig_ao:
        parts.append(f'{label_a} alone is significant (p={p_ao:.3f}).')
    else:
        p_ao_str = 'N/A' if p_ao is None else f'{p_ao:.3f}'
        parts.append(f'{label_a} alone is NOT significant (p={p_ao_str}).')

    if sig_bo:
        parts.append(f'{label_b} alone is significant (p={p_bo:.3f}).')
    else:
        p_bo_str = 'N/A' if p_bo is None else f'{p_bo:.3f}'
        parts.append(f'{label_b} alone is NOT significant (p={p_bo_str}).')

    if sig_ov and mean_ov is not None and mean_ao is not None and mean_ov > mean_ao:
        parts.append('Confluence produces a stronger edge than either signal alone — combination is valuable.')
    elif sig_ov:
        parts.append('Confluence is significant but does not clearly outperform individual signals.')

    return SignalCorrelation(
        label_a=label_a,
        label_b=label_b,
        n_trades_a=n_a,
        n_trades_b=n_b,
        n_overlap=n_overlap,
        overlap_pct_a=overlap_pct_a,
        overlap_pct_b=overlap_pct_b,
        jaccard_index=jaccard,
        mean_return_overlap=mean_ov,
        mean_return_a_only=mean_ao,
        mean_return_b_only=mean_bo,
        p_value_overlap=p_ov,
        p_value_a_only=p_ao,
        p_value_b_only=p_bo,
        significant_overlap=sig_ov,
        significant_a_only=sig_ao,
        significant_b_only=sig_bo,
        n_a_only=n_a_only,
        n_b_only=n_b_only,
        independent=independent,
        conclusion=' '.join(parts),
    )
