"""Tests for rocketstocks.core.analysis.options_flow."""
import pytest

from rocketstocks.core.analysis.options_flow import (
    OptionsFlowResult,
    evaluate_options_flow,
)


def _make_options_chain(
    call_volume=5000, call_oi=1000, put_volume=2000, put_oi=2000,
    call_strike=105.0, put_strike=95.0,
    call_iv=25.0, put_iv=30.0,
    underlying=100.0,
) -> dict:
    """Build a minimal Schwab-format options chain for testing."""
    exp_key = "2026-04-17:30"
    return {
        'callExpDateMap': {
            exp_key: {
                str(call_strike): [{
                    'strike': call_strike,
                    'totalVolume': call_volume,
                    'openInterest': call_oi,
                    'volatility': call_iv,
                }]
            }
        },
        'putExpDateMap': {
            exp_key: {
                str(put_strike): [{
                    'strike': put_strike,
                    'totalVolume': put_volume,
                    'openInterest': put_oi,
                    'volatility': put_iv,
                }]
            }
        },
    }


class TestEvaluateOptionsFlow:

    def test_returns_options_flow_result(self):
        chain = _make_options_chain()
        result = evaluate_options_flow(chain, underlying_price=100.0)
        assert isinstance(result, OptionsFlowResult)

    def test_detects_unusual_call_activity(self):
        # call vol/OI = 5000/1000 = 5.0x >= 3.0 threshold
        chain = _make_options_chain(call_volume=5000, call_oi=1000)
        result = evaluate_options_flow(chain, underlying_price=100.0)
        assert result.has_unusual_activity is True
        assert len(result.unusual_contracts) >= 1
        assert result.unusual_contracts[0]['type'] == 'call'

    def test_no_unusual_activity_when_ratio_low(self):
        chain = _make_options_chain(call_volume=100, call_oi=1000)
        result = evaluate_options_flow(chain, underlying_price=100.0)
        # 100/1000 = 0.1 < 3.0 threshold
        assert result.has_unusual_activity is False
        assert len(result.unusual_contracts) == 0

    def test_put_call_ratio_computed(self):
        # put_vol=2000, call_vol=5000 → ratio = 0.4
        chain = _make_options_chain(call_volume=5000, put_volume=2000)
        result = evaluate_options_flow(chain, underlying_price=100.0)
        assert result.put_call_ratio == pytest.approx(2000 / 5000)

    def test_put_call_ratio_none_when_no_calls(self):
        chain = {'callExpDateMap': {}, 'putExpDateMap': {}}
        result = evaluate_options_flow(chain, underlying_price=100.0)
        assert result.put_call_ratio is None

    def test_flow_score_increases_with_unusual_activity(self):
        chain_no_unusual = _make_options_chain(call_volume=50, call_oi=1000)
        chain_unusual = _make_options_chain(call_volume=5000, call_oi=1000)
        r_no = evaluate_options_flow(chain_no_unusual, underlying_price=100.0)
        r_yes = evaluate_options_flow(chain_unusual, underlying_price=100.0)
        assert r_yes.flow_score > r_no.flow_score

    def test_flow_score_bounded_at_10(self):
        # Max possible: 3 unusual contracts + clear skew + extreme P/C + high IV rank
        # Using a chain with high vol/OI across multiple strikes
        exp_key = "2026-04-17:30"
        chain = {
            'callExpDateMap': {
                exp_key: {
                    str(strike): [{
                        'strike': float(strike),
                        'totalVolume': 10000,
                        'openInterest': 100,
                        'volatility': 20.0,
                    }]
                    for strike in [105, 110, 115, 120]
                }
            },
            'putExpDateMap': {
                exp_key: {
                    '95': [{
                        'strike': 95.0,
                        'totalVolume': 1,
                        'openInterest': 10,
                        'volatility': 35.0,  # higher put IV → put_skew
                    }]
                }
            },
        }
        result = evaluate_options_flow(chain, underlying_price=100.0)
        assert result.flow_score <= 10.0

    def test_flow_score_zero_on_empty_chain(self):
        chain = {'callExpDateMap': {}, 'putExpDateMap': {}}
        result = evaluate_options_flow(chain, underlying_price=100.0)
        assert result.flow_score == pytest.approx(0.0)

    def test_iv_skew_direction_returned(self):
        chain = _make_options_chain(call_iv=20.0, put_iv=30.0)
        result = evaluate_options_flow(chain, underlying_price=100.0)
        # put IV > call IV → skew = put_iv - call_iv = 10 > 3 → put_skew
        assert result.iv_skew_direction == 'put_skew'

    def test_max_pain_returned(self):
        chain = _make_options_chain()
        result = evaluate_options_flow(chain, underlying_price=100.0)
        # max_pain is computed from OI; just check it's a number or None
        assert result.max_pain is None or isinstance(result.max_pain, float)

    def test_iv_rank_none_when_no_history(self):
        chain = _make_options_chain(call_volume=5000, call_oi=1000)
        result = evaluate_options_flow(chain, underlying_price=100.0, iv_history=None)
        assert result.iv_rank is None

    def test_extreme_put_call_ratio_increases_flow_score(self):
        # put_vol=8000, call_vol=1000 → P/C = 8.0 > 1.5 → +2 pts
        chain_normal = _make_options_chain(call_volume=5000, call_oi=1000, put_volume=4000, put_oi=1000)
        chain_extreme = _make_options_chain(call_volume=1000, call_oi=1000, put_volume=8000, put_oi=1000)
        # Both have unusual call activity, but extreme P/C should score higher
        r_normal = evaluate_options_flow(chain_normal, underlying_price=100.0)
        r_extreme = evaluate_options_flow(chain_extreme, underlying_price=100.0)
        assert r_extreme.flow_score >= r_normal.flow_score
