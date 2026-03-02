"""Tests for rocketstocks.core.analysis.popularity_signals."""
import math
from unittest.mock import patch

import pandas as pd
import pytest

from rocketstocks.core.analysis.popularity_signals import (
    SurgeType,
    evaluate_popularity_surge,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _surge(
    ticker='GME',
    current_rank=50,
    rank_24h_ago=200,
    mentions=3000,
    mentions_24h_ago=1000,
    history=None,
    **kwargs,
):
    return evaluate_popularity_surge(
        ticker=ticker,
        current_rank=current_rank,
        rank_24h_ago=rank_24h_ago,
        mentions=mentions,
        mentions_24h_ago=mentions_24h_ago,
        popularity_history=history,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# MENTION_SURGE
# ---------------------------------------------------------------------------

def test_mention_surge_3x_triggers():
    """mentions = 300, mentions_24h = 100 → MENTION_SURGE detected."""
    result = _surge(mentions=300, mentions_24h_ago=100, current_rank=50, rank_24h_ago=60)
    assert SurgeType.MENTION_SURGE in result.surge_types
    assert result.is_surging is True
    assert result.mention_ratio == pytest.approx(3.0)


def test_mention_surge_above_threshold_triggers():
    result = _surge(mentions=3000, mentions_24h_ago=900, current_rank=100, rank_24h_ago=150)
    assert SurgeType.MENTION_SURGE in result.surge_types
    assert result.mention_ratio == pytest.approx(3000 / 900)


def test_mention_surge_below_threshold_no_trigger():
    """2.5x ratio is below the default 3.0 threshold → no MENTION_SURGE."""
    result = _surge(mentions=250, mentions_24h_ago=100, current_rank=50, rank_24h_ago=60)
    assert SurgeType.MENTION_SURGE not in result.surge_types
    assert result.mention_ratio == pytest.approx(2.5)


def test_mention_surge_custom_threshold():
    """Custom threshold of 2.0 → 2.5x ratio triggers."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=60,
        mentions=250, mentions_24h_ago=100,
        mention_surge_threshold=2.0,
    )
    assert SurgeType.MENTION_SURGE in result.surge_types


# ---------------------------------------------------------------------------
# RANK_JUMP
# ---------------------------------------------------------------------------

def test_rank_jump_100_spots_triggers():
    """Rank improved from 200 → 50: gain = 150 → RANK_JUMP."""
    result = _surge(current_rank=50, rank_24h_ago=200, mentions=100, mentions_24h_ago=100)
    assert SurgeType.RANK_JUMP in result.surge_types
    assert result.rank_change == 150


def test_rank_jump_exact_threshold_triggers():
    """Gain of exactly 100 spots → RANK_JUMP (>= threshold)."""
    result = _surge(current_rank=100, rank_24h_ago=200, mentions=100, mentions_24h_ago=100)
    assert SurgeType.RANK_JUMP in result.surge_types
    assert result.rank_change == 100


def test_rank_jump_below_threshold_no_trigger():
    """Gain of 90 spots is below 100 → no RANK_JUMP."""
    result = _surge(current_rank=110, rank_24h_ago=200, mentions=100, mentions_24h_ago=100)
    assert SurgeType.RANK_JUMP not in result.surge_types
    assert result.rank_change == 90


def test_rank_drop_does_not_trigger():
    """Rank worsened (lost popularity) → no RANK_JUMP."""
    result = _surge(current_rank=300, rank_24h_ago=100, mentions=100, mentions_24h_ago=100)
    assert SurgeType.RANK_JUMP not in result.surge_types
    assert result.rank_change == -200


# ---------------------------------------------------------------------------
# NEW_ENTRANT
# ---------------------------------------------------------------------------

def test_new_entrant_top_500_triggers():
    """current_rank=400, rank_24h_ago=None → NEW_ENTRANT."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=400, rank_24h_ago=None,
        mentions=500, mentions_24h_ago=100,
    )
    assert SurgeType.NEW_ENTRANT in result.surge_types
    assert result.is_surging is True


def test_new_entrant_exactly_at_cutoff_triggers():
    """current_rank=500 with no prior rank → NEW_ENTRANT (<=cutoff)."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=500, rank_24h_ago=None,
        mentions=500, mentions_24h_ago=100,
    )
    assert SurgeType.NEW_ENTRANT in result.surge_types


def test_new_entrant_above_cutoff_no_trigger():
    """current_rank=600 > 500 → no NEW_ENTRANT even with no prior rank."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=600, rank_24h_ago=None,
        mentions=500, mentions_24h_ago=100,
    )
    assert SurgeType.NEW_ENTRANT not in result.surge_types


def test_new_entrant_requires_no_prior_rank():
    """If rank_24h_ago is set, it's not a new entrant even if rank is low."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=60,
        mentions=100, mentions_24h_ago=90,
    )
    assert SurgeType.NEW_ENTRANT not in result.surge_types


# ---------------------------------------------------------------------------
# VELOCITY_SPIKE
# ---------------------------------------------------------------------------

def test_velocity_spike_triggers_with_high_zscore():
    """Mocked rank_velocity_zscore >= 2.0 → VELOCITY_SPIKE."""
    history = pd.DataFrame({'rank': [100, 90, 80, 70, 60], 'datetime': pd.date_range('2026-01-01', periods=5)})
    with (
        patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind,
    ):
        mock_ind.popularity.rank_velocity.return_value = -5.0
        mock_ind.popularity.rank_velocity_zscore.return_value = 2.5
        result = evaluate_popularity_surge(
            ticker='GME', current_rank=60, rank_24h_ago=60,
            mentions=100, mentions_24h_ago=100,
            popularity_history=history,
        )
    assert SurgeType.VELOCITY_SPIKE in result.surge_types
    assert result.rank_velocity_zscore == pytest.approx(2.5)


def test_velocity_spike_negative_zscore_still_triggers():
    """abs(zscore) >= 2.0 — direction doesn't matter."""
    history = pd.DataFrame({'rank': [50, 60, 70, 80, 90], 'datetime': pd.date_range('2026-01-01', periods=5)})
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = 5.0
        mock_ind.popularity.rank_velocity_zscore.return_value = -2.5
        result = evaluate_popularity_surge(
            ticker='GME', current_rank=90, rank_24h_ago=50,
            mentions=100, mentions_24h_ago=100,
            popularity_history=history,
        )
    assert SurgeType.VELOCITY_SPIKE in result.surge_types


def test_velocity_spike_below_threshold_no_trigger():
    """zscore = 1.8 < 2.0 → no VELOCITY_SPIKE."""
    history = pd.DataFrame({'rank': [100, 95, 90, 85, 80], 'datetime': pd.date_range('2026-01-01', periods=5)})
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = -3.0
        mock_ind.popularity.rank_velocity_zscore.return_value = 1.8
        result = evaluate_popularity_surge(
            ticker='GME', current_rank=80, rank_24h_ago=80,
            mentions=100, mentions_24h_ago=100,
            popularity_history=history,
        )
    assert SurgeType.VELOCITY_SPIKE not in result.surge_types


def test_velocity_spike_nan_zscore_no_trigger():
    """NaN zscore → no VELOCITY_SPIKE (guarded by math.isnan check)."""
    history = pd.DataFrame({'rank': [100], 'datetime': pd.date_range('2026-01-01', periods=1)})
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = float('nan')
        mock_ind.popularity.rank_velocity_zscore.return_value = float('nan')
        result = evaluate_popularity_surge(
            ticker='GME', current_rank=100, rank_24h_ago=100,
            mentions=100, mentions_24h_ago=100,
            popularity_history=history,
        )
    assert SurgeType.VELOCITY_SPIKE not in result.surge_types


# ---------------------------------------------------------------------------
# Multiple surge types
# ---------------------------------------------------------------------------

def test_multiple_surge_types_detected():
    """Ticker with high mention ratio AND big rank jump gets both types."""
    history = pd.DataFrame({'rank': [200, 180, 150, 100, 50], 'datetime': pd.date_range('2026-01-01', periods=5)})
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = -30.0
        mock_ind.popularity.rank_velocity_zscore.return_value = 0.5  # below threshold
        result = evaluate_popularity_surge(
            ticker='GME', current_rank=50, rank_24h_ago=200,
            mentions=3000, mentions_24h_ago=500,
            popularity_history=history,
        )
    assert SurgeType.MENTION_SURGE in result.surge_types   # 6x mentions
    assert SurgeType.RANK_JUMP in result.surge_types       # 150 spot gain
    assert result.is_surging is True


def test_all_four_surge_types_detected():
    """All four surge types triggered simultaneously."""
    history = pd.DataFrame({'rank': [500, 400, 300, 200, 100], 'datetime': pd.date_range('2026-01-01', periods=5)})
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = -100.0
        mock_ind.popularity.rank_velocity_zscore.return_value = 3.0
        result = evaluate_popularity_surge(
            ticker='NEW', current_rank=50, rank_24h_ago=None,  # NEW_ENTRANT
            mentions=4000, mentions_24h_ago=500,               # MENTION_SURGE (8x)
            popularity_history=history,
        )
    # NEW_ENTRANT (rank<=500, no prior) + MENTION_SURGE + VELOCITY_SPIKE
    assert SurgeType.NEW_ENTRANT in result.surge_types
    assert SurgeType.MENTION_SURGE in result.surge_types
    assert SurgeType.VELOCITY_SPIKE in result.surge_types
    assert len(result.surge_types) >= 3


# ---------------------------------------------------------------------------
# No surge
# ---------------------------------------------------------------------------

def test_no_surge_when_nothing_unusual():
    """Flat mentions ratio and small rank change → is_surging=False."""
    result = _surge(
        current_rank=110, rank_24h_ago=120,   # rank change = 10 (below 100)
        mentions=110, mentions_24h_ago=100,    # ratio = 1.1 (below 3.0)
    )
    assert result.is_surging is False
    assert result.surge_types == []


# ---------------------------------------------------------------------------
# Missing / None data handling
# ---------------------------------------------------------------------------

def test_missing_mentions_does_not_crash():
    """None mentions → no MENTION_SURGE but no crash."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=200,
        mentions=None, mentions_24h_ago=None,
    )
    assert result.mention_ratio is None
    assert SurgeType.MENTION_SURGE not in result.surge_types


def test_missing_rank_does_not_crash():
    """None current_rank → no RANK_JUMP, no NEW_ENTRANT, no crash."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=None, rank_24h_ago=None,
        mentions=3000, mentions_24h_ago=500,
    )
    assert result.rank_change is None
    assert SurgeType.RANK_JUMP not in result.surge_types
    assert SurgeType.NEW_ENTRANT not in result.surge_types


def test_mentions_24h_zero_does_not_crash():
    """mentions_24h_ago=0 → division guarded, no crash."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=200,
        mentions=3000, mentions_24h_ago=0,
    )
    assert result.mention_ratio is None


def test_no_history_skips_velocity():
    """None popularity_history → rank_velocity / rank_velocity_zscore stay None."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=200,
        mentions=100, mentions_24h_ago=100,
        popularity_history=None,
    )
    assert result.rank_velocity is None
    assert result.rank_velocity_zscore is None
    assert SurgeType.VELOCITY_SPIKE not in result.surge_types


def test_empty_history_skips_velocity():
    """Empty DataFrame popularity_history → velocity stays None."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=200,
        mentions=100, mentions_24h_ago=100,
        popularity_history=pd.DataFrame(),
    )
    assert result.rank_velocity is None
    assert SurgeType.VELOCITY_SPIKE not in result.surge_types


# ---------------------------------------------------------------------------
# Result field correctness
# ---------------------------------------------------------------------------

def test_rank_change_positive_when_gaining_popularity():
    result = _surge(current_rank=50, rank_24h_ago=200)
    assert result.rank_change == 150  # gained 150 spots (lower rank = more popular)


def test_result_fields_populated():
    result = _surge(ticker='AAPL', current_rank=10, rank_24h_ago=20,
                    mentions=500, mentions_24h_ago=100)
    assert result.ticker == 'AAPL'
    assert result.current_rank == 10
    assert result.rank_24h_ago == 20
    assert result.mentions == 500
    assert result.mentions_24h_ago == 100
