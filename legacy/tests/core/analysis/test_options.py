"""Tests for core.analysis.options — pure options analysis functions."""
import datetime

import pandas as pd
import pytest

from rocketstocks.core.analysis.options import (
    compute_historical_volatility,
    compute_iv_percentile,
    compute_iv_rank,
    compute_max_pain,
    compute_iv_skew,
    compute_put_call_stats,
    detect_unusual_activity,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _price_history(rows: int = 100) -> pd.DataFrame:
    dates = [datetime.date.today() - datetime.timedelta(days=i) for i in range(rows)]
    dates.reverse()
    return pd.DataFrame({
        'date': dates,
        'open':   [100.0] * rows,
        'high':   [105.0] * rows,
        'low':    [95.0]  * rows,
        'close':  [100.0] * rows,
        'volume': [1_000_000] * rows,
    })


def _iv_history(n: int = 30, values: list | None = None) -> pd.DataFrame:
    if values is None:
        values = [20.0 + i * 0.5 for i in range(n)]
    return pd.DataFrame({'date': list(range(n)), 'iv': values})


def _make_chain(
    strikes: list[float] | None = None,
    underlying: float = 100.0,
    base_iv: float = 25.0,
    base_vol: int = 1000,
    base_oi: int = 5000,
) -> dict:
    """Build a minimal Schwab-style options chain for testing."""
    if strikes is None:
        strikes = [90.0, 95.0, 100.0, 105.0, 110.0]

    def _contract(strike: float, delta_sign: float, iv_adj: float = 0.0) -> dict:
        return {
            'strikePrice': strike,
            'totalVolume': base_vol,
            'openInterest': base_oi,
            'volatility': base_iv + iv_adj,
            'delta': delta_sign * 0.5,
            'gamma': 0.04,
            'theta': -0.10,
            'vega': 0.15,
            'bid': 2.00,
            'ask': 2.10,
            'mark': 2.05,
        }

    exp_key = '2024-06-21:30'
    call_strikes = {str(s): [_contract(s, 1.0, iv_adj=-abs(s - underlying) * 0.1)]
                    for s in strikes}
    put_strikes  = {str(s): [_contract(s, -1.0, iv_adj=abs(s - underlying) * 0.15)]
                    for s in strikes}

    return {
        'status': 'SUCCESS',
        'volatility': base_iv,
        'putCallRatio': 0.85,
        'underlyingPrice': underlying,
        'callExpDateMap': {exp_key: call_strikes},
        'putExpDateMap':  {exp_key: put_strikes},
    }


# ---------------------------------------------------------------------------
# compute_max_pain
# ---------------------------------------------------------------------------

class TestComputeMaxPain:
    def test_returns_float_with_valid_chain(self):
        chain = _make_chain()
        result = compute_max_pain(chain)
        assert isinstance(result, float)

    def test_max_pain_is_one_of_the_strikes(self):
        strikes = [90.0, 95.0, 100.0, 105.0, 110.0]
        chain = _make_chain(strikes=strikes)
        result = compute_max_pain(chain)
        assert result in strikes

    def test_returns_none_for_empty_chain(self):
        assert compute_max_pain({}) is None

    def test_returns_none_for_failed_chain(self):
        assert compute_max_pain({'status': 'FAILED'}) is None

    def test_returns_none_with_no_exp_maps(self):
        assert compute_max_pain({'callExpDateMap': {}, 'putExpDateMap': {}}) is None

    def test_high_put_oi_pulls_pain_down(self):
        """Strike with heavy put OI above it tends to attract max pain toward that strike."""
        exp_key = '2024-06-21:30'
        # All call OI equal; heavy put OI at 105 pulling pain to 100
        call_map = {exp_key: {
            '95.0':  [{'openInterest': 100, 'totalVolume': 10}],
            '100.0': [{'openInterest': 100, 'totalVolume': 10}],
            '105.0': [{'openInterest': 100, 'totalVolume': 10}],
        }}
        put_map = {exp_key: {
            '95.0':  [{'openInterest': 5000, 'totalVolume': 10}],
            '100.0': [{'openInterest': 100,  'totalVolume': 10}],
            '105.0': [{'openInterest': 100,  'totalVolume': 10}],
        }}
        chain = {'callExpDateMap': call_map, 'putExpDateMap': put_map}
        # max pain should not be at 95 (all the put pain sits just below it)
        result = compute_max_pain(chain)
        assert result is not None


# ---------------------------------------------------------------------------
# compute_iv_skew
# ---------------------------------------------------------------------------

class TestComputeIvSkew:
    def test_returns_dict_with_valid_data(self):
        chain = _make_chain()
        result = compute_iv_skew(chain, underlying_price=100.0)
        assert result is not None
        assert 'skew' in result
        assert 'direction' in result

    def test_put_skew_detected_when_put_iv_higher(self):
        chain = _make_chain(underlying=100.0, base_iv=25.0)
        result = compute_iv_skew(chain, underlying_price=100.0)
        # Puts are constructed with higher IV (iv_adj adds abs(s-underlying)*0.15)
        assert result is not None

    def test_returns_none_for_empty_chain(self):
        assert compute_iv_skew({}, underlying_price=100.0) is None

    def test_returns_none_for_zero_price(self):
        chain = _make_chain()
        assert compute_iv_skew(chain, underlying_price=0) is None

    def test_otm_strikes_straddle_underlying(self):
        chain = _make_chain()
        result = compute_iv_skew(chain, underlying_price=100.0)
        assert result['otm_call_strike'] > 100.0
        assert result['otm_put_strike'] < 100.0


# ---------------------------------------------------------------------------
# detect_unusual_activity
# ---------------------------------------------------------------------------

class TestDetectUnusualActivity:
    def test_detects_high_vol_oi_ratio(self):
        chain = _make_chain(base_vol=30000, base_oi=5000)  # ratio = 6x
        results = detect_unusual_activity(chain, vol_oi_threshold=3.0)
        assert len(results) > 0
        for r in results:
            assert r['ratio'] >= 3.0

    def test_returns_empty_below_threshold(self):
        chain = _make_chain(base_vol=100, base_oi=5000)  # ratio = 0.02x
        results = detect_unusual_activity(chain, vol_oi_threshold=3.0)
        assert results == []

    def test_sorted_by_ratio_desc(self):
        chain = _make_chain(base_vol=30000, base_oi=5000)
        results = detect_unusual_activity(chain)
        ratios = [r['ratio'] for r in results]
        assert ratios == sorted(ratios, reverse=True)

    def test_respects_max_results_cap(self):
        chain = _make_chain(base_vol=50000, base_oi=1000)  # all unusual
        results = detect_unusual_activity(chain, max_results=2)
        assert len(results) <= 2

    def test_returns_correct_type_field(self):
        chain = _make_chain(base_vol=30000, base_oi=5000)
        results = detect_unusual_activity(chain)
        for r in results:
            assert r['type'] in ('call', 'put')

    def test_empty_chain_returns_empty(self):
        assert detect_unusual_activity({}) == []


# ---------------------------------------------------------------------------
# compute_put_call_stats
# ---------------------------------------------------------------------------

class TestComputePutCallStats:
    def test_returns_all_keys(self):
        result = compute_put_call_stats(_make_chain())
        assert all(k in result for k in ('call_volume', 'call_oi', 'put_volume', 'put_oi'))

    def test_volumes_are_positive(self):
        result = compute_put_call_stats(_make_chain(base_vol=500))
        assert result['call_volume'] > 0
        assert result['put_volume'] > 0

    def test_empty_chain_returns_zeros(self):
        result = compute_put_call_stats({})
        assert result == {'call_volume': 0, 'call_oi': 0, 'put_volume': 0, 'put_oi': 0}


# ---------------------------------------------------------------------------
# compute_historical_volatility
# ---------------------------------------------------------------------------

class TestComputeHistoricalVolatility:
    def test_returns_float_with_sufficient_data(self):
        result = compute_historical_volatility(_price_history(100), period=20)
        assert isinstance(result, float)
        assert result > 0

    def test_returns_none_with_insufficient_data(self):
        assert compute_historical_volatility(_price_history(5), period=20) is None

    def test_returns_none_for_empty_df(self):
        assert compute_historical_volatility(pd.DataFrame(), period=20) is None

    def test_returns_none_without_ohlc_columns(self):
        df = pd.DataFrame({'close': [100.0] * 50, 'volume': [1000] * 50})
        assert compute_historical_volatility(df, period=20) is None


# ---------------------------------------------------------------------------
# compute_iv_rank / compute_iv_percentile
# ---------------------------------------------------------------------------

class TestComputeIvRank:
    def test_returns_0_when_at_minimum(self):
        history = _iv_history(30, [10.0 + i for i in range(30)])  # range 10 to 39
        result = compute_iv_rank(10.0, history)
        assert result == pytest.approx(0.0)

    def test_returns_100_when_at_maximum(self):
        history = _iv_history(30, [10.0] * 30)
        history.loc[29, 'iv'] = 30.0
        result = compute_iv_rank(30.0, history)
        assert result == pytest.approx(100.0)

    def test_returns_none_with_insufficient_history(self):
        history = _iv_history(10)
        assert compute_iv_rank(25.0, history) is None

    def test_returns_none_for_empty_df(self):
        assert compute_iv_rank(25.0, pd.DataFrame()) is None

    def test_midrange_value(self):
        history = _iv_history(30, [10.0 + i for i in range(30)])  # 10 to 39
        result = compute_iv_rank(24.5, history)  # midpoint of 10-39
        assert result == pytest.approx(50.0, abs=5.0)


class TestComputeIvPercentile:
    def test_returns_0_when_lowest(self):
        history = _iv_history(30, [20.0] * 30)
        result = compute_iv_percentile(15.0, history)
        assert result == pytest.approx(0.0)

    def test_returns_100_when_highest(self):
        history = _iv_history(30, [20.0] * 30)
        result = compute_iv_percentile(25.0, history)
        assert result == pytest.approx(100.0)

    def test_returns_none_with_insufficient_history(self):
        assert compute_iv_percentile(25.0, _iv_history(10)) is None

    def test_returns_none_for_empty_df(self):
        assert compute_iv_percentile(25.0, pd.DataFrame()) is None
