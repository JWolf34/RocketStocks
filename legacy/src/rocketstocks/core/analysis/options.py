"""Pure functions for options analysis — max pain, IV skew, unusual activity, HV, IV rank."""
from __future__ import annotations

import logging

import pandas as pd
import pandas_ta_classic as ta

logger = logging.getLogger(__name__)


def _nearest_exp(exp_map: dict) -> str | None:
    """Return the key of the nearest expiration in a Schwab expDateMap."""
    if not exp_map:
        return None
    return sorted(exp_map.keys())[0]


def compute_max_pain(options_chain: dict) -> float | None:
    """Find the strike at which total options pain is minimised (writers' max profit).

    Uses only the nearest expiration.  Returns None if there is insufficient data.
    """
    call_map = options_chain.get('callExpDateMap', {})
    put_map = options_chain.get('putExpDateMap', {})
    nearest = _nearest_exp(call_map)
    if not nearest:
        return None

    call_oi: dict[float, float] = {}
    put_oi: dict[float, float] = {}

    for s_str, contracts in call_map.get(nearest, {}).items():
        try:
            s = float(s_str)
            call_oi[s] = sum(c.get('openInterest', 0) or 0 for c in contracts)
        except (ValueError, TypeError):
            pass

    for s_str, contracts in put_map.get(nearest, {}).items():
        try:
            s = float(s_str)
            put_oi[s] = sum(c.get('openInterest', 0) or 0 for c in contracts)
        except (ValueError, TypeError):
            pass

    all_strikes = sorted(set(call_oi) | set(put_oi))
    if not all_strikes:
        return None

    min_pain = float('inf')
    max_pain_strike = None
    for test in all_strikes:
        call_pain = sum(oi * (test - s) for s, oi in call_oi.items() if s < test and oi > 0)
        put_pain = sum(oi * (s - test) for s, oi in put_oi.items() if s > test and oi > 0)
        total = call_pain + put_pain
        if total < min_pain:
            min_pain = total
            max_pain_strike = test

    return max_pain_strike


def compute_iv_skew(
    options_chain: dict,
    underlying_price: float,
    distance_pct: float = 0.05,
) -> dict | None:
    """Compare OTM put IV vs OTM call IV at ~distance_pct from ATM.

    Returns a dict with keys: otm_put_strike, otm_put_iv, otm_call_strike,
    otm_call_iv, skew (put_iv - call_iv), direction.
    Returns None if insufficient data.
    """
    call_map = options_chain.get('callExpDateMap', {})
    put_map = options_chain.get('putExpDateMap', {})
    nearest = _nearest_exp(call_map)
    if not nearest or underlying_price <= 0:
        return None

    target_call = underlying_price * (1 + distance_pct)
    target_put = underlying_price * (1 - distance_pct)

    call_ivs: dict[float, float] = {}
    for s_str, contracts in call_map.get(nearest, {}).items():
        try:
            s = float(s_str)
            if s > underlying_price and contracts:
                iv = contracts[0].get('volatility')
                if iv and iv > 0:
                    call_ivs[s] = float(iv)
        except (ValueError, TypeError):
            pass

    put_ivs: dict[float, float] = {}
    for s_str, contracts in put_map.get(nearest, {}).items():
        try:
            s = float(s_str)
            if s < underlying_price and contracts:
                iv = contracts[0].get('volatility')
                if iv and iv > 0:
                    put_ivs[s] = float(iv)
        except (ValueError, TypeError):
            pass

    if not call_ivs or not put_ivs:
        return None

    best_call = min(call_ivs, key=lambda s: abs(s - target_call))
    best_put = min(put_ivs, key=lambda s: abs(s - target_put))
    otm_call_iv = call_ivs[best_call]
    otm_put_iv = put_ivs[best_put]
    skew = otm_put_iv - otm_call_iv

    if skew > 3:
        direction = 'put_skew'
    elif skew < -3:
        direction = 'call_skew'
    else:
        direction = 'neutral'

    return {
        'otm_call_strike': best_call,
        'otm_call_iv': otm_call_iv,
        'otm_put_strike': best_put,
        'otm_put_iv': otm_put_iv,
        'skew': skew,
        'direction': direction,
    }


def detect_unusual_activity(
    options_chain: dict,
    vol_oi_threshold: float = 3.0,
    max_results: int = 5,
) -> list[dict]:
    """Find contracts where volume/OI ratio ≥ vol_oi_threshold.

    Returns a list of dicts sorted by ratio (descending), capped at max_results.
    """
    results = []
    for opt_type, exp_map in [
        ('call', options_chain.get('callExpDateMap', {})),
        ('put', options_chain.get('putExpDateMap', {})),
    ]:
        for exp_key, strikes_data in exp_map.items():
            exp_date = exp_key.split(':')[0]
            for s_str, contracts in strikes_data.items():
                for c in contracts:
                    vol = c.get('totalVolume', 0) or 0
                    oi = c.get('openInterest', 0) or 0
                    if oi > 0 and vol > 0:
                        ratio = vol / oi
                        if ratio >= vol_oi_threshold:
                            results.append({
                                'strike': float(s_str),
                                'expiry': exp_date,
                                'type': opt_type,
                                'volume': vol,
                                'oi': oi,
                                'ratio': ratio,
                                'iv': c.get('volatility'),
                            })
    results.sort(key=lambda x: x['ratio'], reverse=True)
    return results[:max_results]


def compute_put_call_stats(options_chain: dict) -> dict:
    """Aggregate total call/put volume and OI across all expirations."""
    call_vol = call_oi = put_vol = put_oi = 0
    for _, strikes in options_chain.get('callExpDateMap', {}).items():
        for _, contracts in strikes.items():
            for c in contracts:
                call_vol += c.get('totalVolume', 0) or 0
                call_oi += c.get('openInterest', 0) or 0
    for _, strikes in options_chain.get('putExpDateMap', {}).items():
        for _, contracts in strikes.items():
            for c in contracts:
                put_vol += c.get('totalVolume', 0) or 0
                put_oi += c.get('openInterest', 0) or 0
    return {'call_volume': call_vol, 'call_oi': call_oi, 'put_volume': put_vol, 'put_oi': put_oi}


def compute_historical_volatility(daily_price_history: pd.DataFrame, period: int) -> float | None:
    """Return NATR(period) as a percentage — used as a proxy for historical volatility."""
    if daily_price_history is None or daily_price_history.empty:
        return None
    if len(daily_price_history) < period + 1:
        return None
    if not all(c in daily_price_history.columns for c in ('high', 'low', 'close')):
        return None
    natr_s = ta.natr(
        high=daily_price_history['high'],
        low=daily_price_history['low'],
        close=daily_price_history['close'],
        length=period,
    )
    if natr_s is None or natr_s.empty:
        return None
    val = float(natr_s.iloc[-1])
    return val if not pd.isna(val) else None


def compute_iv_rank(
    current_iv: float,
    iv_history: pd.DataFrame,
    window: int = 252,
) -> float | None:
    """IV Rank = (current - min) / (max - min) over the window, as a percentage.

    Returns None when there are fewer than 20 data points.
    """
    if iv_history is None or iv_history.empty or 'iv' not in iv_history.columns:
        return None
    recent = iv_history['iv'].dropna().tail(window)
    if len(recent) < 20:
        return None
    lo, hi = recent.min(), recent.max()
    if hi == lo:
        return None
    return ((current_iv - lo) / (hi - lo)) * 100.0


def compute_iv_percentile(
    current_iv: float,
    iv_history: pd.DataFrame,
    window: int = 252,
) -> float | None:
    """IV Percentile = % of days where IV was below current_iv.

    Returns None when there are fewer than 20 data points.
    """
    if iv_history is None or iv_history.empty or 'iv' not in iv_history.columns:
        return None
    recent = iv_history['iv'].dropna().tail(window)
    if len(recent) < 20:
        return None
    return float((recent < current_iv).sum()) / len(recent) * 100.0
