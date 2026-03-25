"""Alert performance metrics — computed on-demand from existing tables, no new infrastructure.

All functions are pure analysis: they accept DataFrames / dicts and return dicts with stats.
No discord or data imports.
"""
from __future__ import annotations

import datetime
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def compute_surge_confidence(surges_df: pd.DataFrame) -> dict:
    """Compute confirmation rate for popularity surges from the popularity_surges table.

    Only counts settled surges (confirmed or expired) in the rate denominator, so
    still-pending surges don't dilute the stat.

    Args:
        surges_df: DataFrame from the popularity_surges table with 'confirmed' and
            'expired' columns (bool).

    Returns:
        dict with keys: total, confirmed, expired, pending, rate (float | None).
    """
    if surges_df.empty:
        return {'total': 0, 'confirmed': 0, 'expired': 0, 'pending': 0, 'rate': None}

    total = len(surges_df)
    confirmed = int((surges_df['confirmed'] == True).sum())
    expired = int((surges_df['expired'] == True).sum())
    pending = total - confirmed - expired
    settled = confirmed + expired
    rate = round(confirmed / settled * 100, 1) if settled > 0 else None

    return {
        'total': total,
        'confirmed': confirmed,
        'expired': expired,
        'pending': pending,
        'rate': rate,
    }


def compute_signal_confidence(signals_df: pd.DataFrame) -> dict:
    """Compute confirmation rate for market signals from the market_signals table.

    Args:
        signals_df: DataFrame from the market_signals table with a 'status' column
            containing values 'pending', 'confirmed', or 'expired'.

    Returns:
        dict with keys: total, confirmed, expired, pending, rate (float | None).
    """
    if signals_df.empty:
        return {'total': 0, 'confirmed': 0, 'expired': 0, 'pending': 0, 'rate': None}

    total = len(signals_df)
    if 'status' in signals_df.columns:
        confirmed = int((signals_df['status'] == 'confirmed').sum())
        expired = int((signals_df['status'] == 'expired').sum())
    else:
        confirmed = 0
        expired = 0
    pending = total - confirmed - expired
    settled = confirmed + expired
    rate = round(confirmed / settled * 100, 1) if settled > 0 else None

    return {
        'total': total,
        'confirmed': confirmed,
        'expired': expired,
        'pending': pending,
        'rate': rate,
    }


def compute_price_outcome(
    alerts: list[dict],
    price_history: dict[str, pd.DataFrame],
    horizons: list[int] | None = None,
) -> dict:
    """Compute price outcome at T+1d, T+4d horizons after alert time.

    Uses the daily_price_history table (available via price_history cache).
    Skips alerts missing price data or created_at timestamps.

    Args:
        alerts: List of alert dicts, each with 'ticker', 'date' (date object),
            and optionally 'created_at' (datetime).
        price_history: Mapping of ticker → daily OHLCV DataFrame with a 'date' column.
        horizons: List of day horizons to evaluate (default [1, 4]).

    Returns:
        dict with 'per_alert' (list of outcome dicts) and 'aggregate' (summary stats).
    """
    if horizons is None:
        horizons = [1, 4]

    outcomes = []
    for alert in alerts:
        ticker = alert.get('ticker')
        alert_date = alert.get('date')
        if not ticker or alert_date is None or ticker not in price_history:
            continue

        df = price_history[ticker]
        if df.empty or 'date' not in df.columns or 'close' not in df.columns:
            continue

        df_sorted = df.sort_values('date')

        # Find alert-day close as reference price
        alert_row = df_sorted[df_sorted['date'] == alert_date]
        if alert_row.empty:
            continue
        alert_price = float(alert_row['close'].iloc[0])
        if alert_price == 0:
            continue

        outcome = {'ticker': ticker, 'alert_date': alert_date, 'alert_price': alert_price}
        for h in horizons:
            target_date = alert_date + datetime.timedelta(days=h)
            future = df_sorted[df_sorted['date'] >= target_date]
            if not future.empty:
                future_price = float(future['close'].iloc[0])
                outcome[f'pct_{h}d'] = round((future_price - alert_price) / alert_price * 100, 2)
            else:
                outcome[f'pct_{h}d'] = None
        outcomes.append(outcome)

    # Aggregate stats per horizon
    agg: dict = {}
    for h in horizons:
        col = f'pct_{h}d'
        vals = [o[col] for o in outcomes if o.get(col) is not None]
        if vals:
            mean_val = round(sum(vals) / len(vals), 2)
            positive_rate = round(sum(1 for v in vals if v > 0) / len(vals) * 100, 1)
            agg[col] = {'mean': mean_val, 'positive_rate': positive_rate, 'count': len(vals)}
        else:
            agg[col] = {'mean': None, 'positive_rate': None, 'count': 0}

    return {'per_alert': outcomes, 'aggregate': agg}
