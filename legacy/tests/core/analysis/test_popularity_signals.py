"""Tests for rocketstocks.core.analysis.popularity_signals."""
import math
from unittest.mock import patch

import pandas as pd
import pytest

from rocketstocks.core.analysis.popularity_signals import (
    SurgeType,
    _get_tier_thresholds,
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
    """mentions = 300, mentions_24h = 100 (>= min_base 15) → MENTION_SURGE detected."""
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
    """Custom threshold of 2.0 → 2.5x ratio triggers (base=100 >= min_base 15)."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=60,
        mentions=250, mentions_24h_ago=100,
        mention_surge_threshold=2.0,
    )
    assert SurgeType.MENTION_SURGE in result.surge_types


def test_mention_surge_requires_base_volume():
    """6 → 36 (6x ratio): base=6 < min_base 15 → no MENTION_SURGE despite high ratio."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=60,
        mentions=36, mentions_24h_ago=6,
    )
    assert SurgeType.MENTION_SURGE not in result.surge_types
    assert result.mention_ratio == pytest.approx(6.0)


def test_mention_surge_base_volume_met_triggers():
    """15 → 50 (3.33x ratio): base=15 meets min_base → MENTION_SURGE."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=60,
        mentions=50, mentions_24h_ago=15,
    )
    assert SurgeType.MENTION_SURGE in result.surge_types


def test_mention_surge_custom_min_base():
    """Custom mention_surge_min_base=30: base=20 blocked, base=30 triggers."""
    blocked = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=60,
        mentions=100, mentions_24h_ago=20,
        mention_surge_min_base=30,
    )
    assert SurgeType.MENTION_SURGE not in blocked.surge_types

    passes = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=60,
        mentions=100, mentions_24h_ago=30,
        mention_surge_min_base=30,
    )
    assert SurgeType.MENTION_SURGE in passes.surge_types


# ---------------------------------------------------------------------------
# RANK_JUMP
# ---------------------------------------------------------------------------

def test_rank_jump_100_spots_triggers():
    """Rank improved from 200 → 50: gain=150, ratio=3.0 → RANK_JUMP."""
    result = _surge(current_rank=50, rank_24h_ago=200, mentions=100, mentions_24h_ago=100)
    assert SurgeType.RANK_JUMP in result.surge_types
    assert result.rank_change == 150


def test_rank_jump_exact_threshold_triggers():
    """200 → 100: gain=100, ratio=1.0 — below ratio threshold of 1.5 → no RANK_JUMP."""
    result = _surge(current_rank=100, rank_24h_ago=200, mentions=100, mentions_24h_ago=100)
    assert SurgeType.RANK_JUMP not in result.surge_types
    assert result.rank_change == 100


def test_rank_jump_below_threshold_no_trigger():
    """200 → 110: gain=90, below min_spots of 50 AND ratio=0.82 → no RANK_JUMP."""
    result = _surge(current_rank=110, rank_24h_ago=200, mentions=100, mentions_24h_ago=100)
    assert SurgeType.RANK_JUMP not in result.surge_types
    assert result.rank_change == 90


def test_rank_drop_does_not_trigger():
    """Rank worsened (lost popularity) → no RANK_JUMP."""
    result = _surge(current_rank=300, rank_24h_ago=100, mentions=100, mentions_24h_ago=100)
    assert SurgeType.RANK_JUMP not in result.surge_types
    assert result.rank_change == -200


def test_rank_jump_relative_750_to_120_triggers():
    """750 → 120: gain=630, ratio=5.25 → RANK_JUMP."""
    result = _surge(current_rank=120, rank_24h_ago=750, mentions=100, mentions_24h_ago=100)
    assert SurgeType.RANK_JUMP in result.surge_types
    assert result.rank_change == 630


def test_rank_jump_relative_230_to_120_no_trigger():
    """230 → 120: gain=110, ratio=0.92 — ratio below 1.5 → no RANK_JUMP."""
    result = _surge(current_rank=120, rank_24h_ago=230, mentions=100, mentions_24h_ago=100)
    assert SurgeType.RANK_JUMP not in result.surge_types
    assert result.rank_change == 110


def test_rank_jump_relative_120_to_40_triggers():
    """120 → 40: gain=80, ratio=2.0 → RANK_JUMP."""
    result = _surge(current_rank=40, rank_24h_ago=120, mentions=100, mentions_24h_ago=100)
    assert SurgeType.RANK_JUMP in result.surge_types
    assert result.rank_change == 80


def test_rank_jump_below_min_spots_no_trigger():
    """gain=40 even with high ratio → blocked by min_spots=50."""
    result = _surge(current_rank=10, rank_24h_ago=50, mentions=100, mentions_24h_ago=100)
    assert SurgeType.RANK_JUMP not in result.surge_types
    assert result.rank_change == 40


# ---------------------------------------------------------------------------
# NEW_ENTRANT
# ---------------------------------------------------------------------------

def test_new_entrant_top_200_triggers():
    """current_rank=150, rank_24h_ago=None → NEW_ENTRANT (within top 200)."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=150, rank_24h_ago=None,
        mentions=500, mentions_24h_ago=100,
    )
    assert SurgeType.NEW_ENTRANT in result.surge_types
    assert result.is_surging is True


def test_new_entrant_exactly_at_cutoff_triggers():
    """current_rank=200 with no prior rank → NEW_ENTRANT (<=cutoff)."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=200, rank_24h_ago=None,
        mentions=500, mentions_24h_ago=100,
    )
    assert SurgeType.NEW_ENTRANT in result.surge_types


def test_new_entrant_above_cutoff_no_trigger():
    """current_rank=250 > 200 → no NEW_ENTRANT even with no prior rank."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=250, rank_24h_ago=None,
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


def test_new_entrant_rank_201_to_500_no_trigger():
    """Ranks 201-500 no longer qualify as NEW_ENTRANT with tightened cutoff."""
    for rank in [201, 300, 400, 500]:
        result = evaluate_popularity_surge(
            ticker='GME', current_rank=rank, rank_24h_ago=None,
            mentions=500, mentions_24h_ago=100,
        )
        assert SurgeType.NEW_ENTRANT not in result.surge_types, f"rank={rank} should not trigger"


# ---------------------------------------------------------------------------
# VELOCITY_SPIKE
# ---------------------------------------------------------------------------

def test_velocity_spike_triggers_with_high_zscore():
    """Mocked rank_velocity_zscore = -2.5 (gaining popularity) → VELOCITY_SPIKE.

    Using Tier 3 rank (150) so the default 2.5σ threshold applies unchanged.
    """
    history = pd.DataFrame({'rank': [200, 180, 160, 155, 150], 'datetime': pd.date_range('2026-01-01', periods=5)})
    with (
        patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind,
    ):
        mock_ind.popularity.rank_velocity.return_value = -5.0
        mock_ind.popularity.rank_velocity_zscore.return_value = -2.5
        mock_ind.popularity.mention_acceleration.return_value = float('nan')
        result = evaluate_popularity_surge(
            ticker='GME', current_rank=150, rank_24h_ago=150,
            mentions=100, mentions_24h_ago=100,
            popularity_history=history,
        )
    assert SurgeType.VELOCITY_SPIKE in result.surge_types
    assert result.rank_velocity_zscore == pytest.approx(-2.5)


def test_velocity_spike_gaining_popularity_triggers():
    """Negative z-score means gaining popularity — Tier 3 rank used (101-500)."""
    history = pd.DataFrame({'rank': [300, 280, 260, 240, 220], 'datetime': pd.date_range('2026-01-01', periods=5)})
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = -5.0
        mock_ind.popularity.rank_velocity_zscore.return_value = -2.5
        mock_ind.popularity.mention_acceleration.return_value = float('nan')
        result = evaluate_popularity_surge(
            ticker='GME', current_rank=220, rank_24h_ago=300,
            mentions=100, mentions_24h_ago=100,
            popularity_history=history,
        )
    assert SurgeType.VELOCITY_SPIKE in result.surge_types


def test_velocity_spike_below_threshold_no_trigger():
    """zscore = -2.0 is not <= -2.5 → no VELOCITY_SPIKE (Tier 3 rank)."""
    history = pd.DataFrame({'rank': [200, 195, 190, 185, 180], 'datetime': pd.date_range('2026-01-01', periods=5)})
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = -3.0
        mock_ind.popularity.rank_velocity_zscore.return_value = -2.0
        mock_ind.popularity.mention_acceleration.return_value = float('nan')
        result = evaluate_popularity_surge(
            ticker='GME', current_rank=180, rank_24h_ago=180,
            mentions=100, mentions_24h_ago=100,
            popularity_history=history,
        )
    assert SurgeType.VELOCITY_SPIKE not in result.surge_types


def test_velocity_spike_positive_zscore_no_trigger():
    """Positive z-score = losing popularity → no VELOCITY_SPIKE (Tier 3 rank)."""
    history = pd.DataFrame({'rank': [150, 160, 170, 180, 190], 'datetime': pd.date_range('2026-01-01', periods=5)})
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = 5.0
        mock_ind.popularity.rank_velocity_zscore.return_value = 3.0  # positive = losing popularity
        mock_ind.popularity.mention_acceleration.return_value = float('nan')
        result = evaluate_popularity_surge(
            ticker='GME', current_rank=190, rank_24h_ago=150,
            mentions=100, mentions_24h_ago=100,
            popularity_history=history,
        )
    assert SurgeType.VELOCITY_SPIKE not in result.surge_types


def test_velocity_spike_nan_zscore_no_trigger():
    """NaN zscore → no VELOCITY_SPIKE (guarded by math.isnan check)."""
    history = pd.DataFrame({'rank': [200], 'datetime': pd.date_range('2026-01-01', periods=1)})
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = float('nan')
        mock_ind.popularity.rank_velocity_zscore.return_value = float('nan')
        mock_ind.popularity.mention_acceleration.return_value = float('nan')
        result = evaluate_popularity_surge(
            ticker='GME', current_rank=200, rank_24h_ago=200,
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
        mock_ind.popularity.mention_acceleration.return_value = float('nan')
        result = evaluate_popularity_surge(
            ticker='GME', current_rank=50, rank_24h_ago=200,
            mentions=3000, mentions_24h_ago=500,
            popularity_history=history,
        )
    assert SurgeType.MENTION_SURGE in result.surge_types   # 6x mentions
    assert SurgeType.RANK_JUMP in result.surge_types       # 150 spot gain
    assert result.is_surging is True


def test_all_four_surge_types_detected():
    """All four surge types triggered simultaneously using a Tier 3 rank (101-500).

    Using rank=150 (Tier 3) so the default velocity threshold of 2.5σ applies,
    letting the -3.0 z-score trigger VELOCITY_SPIKE.
    """
    history = pd.DataFrame({'rank': [500, 400, 300, 200, 150], 'datetime': pd.date_range('2026-01-01', periods=5)})
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = -100.0
        mock_ind.popularity.rank_velocity_zscore.return_value = -3.0
        mock_ind.popularity.mention_acceleration.return_value = float('nan')
        result = evaluate_popularity_surge(
            ticker='NEW', current_rank=150, rank_24h_ago=None,  # NEW_ENTRANT (rank<=200)
            mentions=4000, mentions_24h_ago=500,                # MENTION_SURGE (8x)
            popularity_history=history,
        )
    # NEW_ENTRANT (rank<=200, no prior) + MENTION_SURGE + VELOCITY_SPIKE
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
    """None mentions → early return, no surge, no crash."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=200,
        mentions=None, mentions_24h_ago=None,
    )
    assert result.mention_ratio is None
    assert SurgeType.MENTION_SURGE not in result.surge_types
    assert result.is_surging is False


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
# Minimum mention filter
# ---------------------------------------------------------------------------

def test_min_mentions_filter_blocks_all_surges():
    """3 mentions with strong signals → is_surging=False (below min_mentions=5)."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=200,
        mentions=3, mentions_24h_ago=1,
    )
    assert result.is_surging is False
    assert result.surge_types == []


def test_min_mentions_filter_none_mentions():
    """None mentions → early return, is_surging=False."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=200,
        mentions=None, mentions_24h_ago=None,
    )
    assert result.is_surging is False
    assert result.mention_ratio is None
    assert result.rank_velocity is None


def test_min_mentions_exactly_fifteen_passes():
    """15 mentions passes the min_mentions filter; RANK_JUMP can still fire."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=200,
        mentions=15, mentions_24h_ago=1,         # passes min_mentions; base=1 < 15 blocks MENTION_SURGE
    )
    # min_mentions filter passed; RANK_JUMP fires (gain=150, ratio=3.0)
    assert result.is_surging is True
    assert SurgeType.RANK_JUMP in result.surge_types
    assert SurgeType.MENTION_SURGE not in result.surge_types  # base too low


def test_min_mentions_four_blocked():
    """4 mentions < 5 → early return even if ratio would trigger."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=200,
        mentions=4, mentions_24h_ago=1,
    )
    assert result.is_surging is False
    assert result.surge_types == []


def test_min_mentions_custom_threshold():
    """Custom min_mentions=10: 8 mentions blocked, 10 passes."""
    blocked = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=200,
        mentions=8, mentions_24h_ago=1,
        min_mentions=10,
    )
    assert blocked.is_surging is False

    passes = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=200,
        mentions=10, mentions_24h_ago=1,         # 10x ratio → MENTION_SURGE
        min_mentions=10,
    )
    assert passes.is_surging is True


def test_min_mentions_early_return_computes_rank_change():
    """Early return due to low mentions still populates rank_change."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=200,
        mentions=2, mentions_24h_ago=1,
    )
    assert result.rank_change == 150   # 200 - 50
    assert result.rank_velocity is None
    assert result.rank_velocity_zscore is None


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


# ---------------------------------------------------------------------------
# Tier damping (_get_tier_thresholds)
# ---------------------------------------------------------------------------

def test_tier_1_suppresses_velocity_and_tightens_mention():
    """Rank 1-25 → suppress VELOCITY_SPIKE, require 5x for MENTION_SURGE."""
    overrides = _get_tier_thresholds(15)
    assert overrides.get('suppress_velocity') is True
    assert overrides.get('mention_surge_threshold') == 5.0


def test_tier_2_tightens_velocity_threshold():
    """Rank 26-100 → tighten VELOCITY_SPIKE to 3.5σ."""
    overrides = _get_tier_thresholds(50)
    assert overrides.get('velocity_zscore_threshold') == 3.5
    assert not overrides.get('suppress_velocity')


def test_tier_3_has_no_overrides():
    """Rank 101-500 → no overrides (default sweet spot)."""
    for rank in [101, 250, 499, 500]:
        assert _get_tier_thresholds(rank) == {}


def test_tier_4_tightens_new_entrant_and_min_base():
    """Rank 501+ → NEW_ENTRANT cutoff=150, min_base=10."""
    overrides = _get_tier_thresholds(501)
    assert overrides.get('new_entrant_cutoff') == 150
    assert overrides.get('mention_surge_min_base') == 10


def test_tier_none_rank_has_no_overrides():
    """None rank → no overrides."""
    assert _get_tier_thresholds(None) == {}


def test_tier_1_velocity_spike_suppressed_in_full_eval():
    """Tier 1 stock (rank 20) does not trigger VELOCITY_SPIKE even at extreme zscore."""
    history = pd.DataFrame({'rank': [20, 19, 18, 17, 16], 'datetime': pd.date_range('2026-01-01', periods=5)})
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = -5.0
        mock_ind.popularity.rank_velocity_zscore.return_value = -10.0  # extreme
        mock_ind.popularity.mention_acceleration.return_value = float('nan')
        result = evaluate_popularity_surge(
            ticker='AAPL', current_rank=20, rank_24h_ago=20,
            mentions=50, mentions_24h_ago=50,
            popularity_history=history,
        )
    assert SurgeType.VELOCITY_SPIKE not in result.surge_types


def test_tier_1_mention_surge_requires_5x():
    """Tier 1 stock: 4x mention surge does NOT fire; 5x does."""
    history = pd.DataFrame({
        'rank': [5, 5, 5], 'datetime': pd.date_range('2026-01-01', periods=3),
        'mentions': [100, 200, 300],
    })
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = 0.0
        mock_ind.popularity.rank_velocity_zscore.return_value = 0.0
        mock_ind.popularity.mention_acceleration.return_value = 100.0  # positive — not gated

        # 4x ratio: base=100, current=400 — should NOT trigger (tier requires 5x)
        result_4x = evaluate_popularity_surge(
            ticker='TSLA', current_rank=10, rank_24h_ago=10,
            mentions=400, mentions_24h_ago=100,
            popularity_history=history,
        )
        assert SurgeType.MENTION_SURGE not in result_4x.surge_types

        # 5x ratio: base=100, current=500 — SHOULD trigger
        result_5x = evaluate_popularity_surge(
            ticker='TSLA', current_rank=10, rank_24h_ago=10,
            mentions=500, mentions_24h_ago=100,
            popularity_history=history,
        )
        assert SurgeType.MENTION_SURGE in result_5x.surge_types


def test_tier_2_velocity_spike_needs_higher_zscore():
    """Tier 2 stock (rank 60): zscore -2.5 does NOT fire; -3.5 does."""
    history = pd.DataFrame({
        'rank': [60, 58, 56], 'datetime': pd.date_range('2026-01-01', periods=3),
        'mentions': [100, 120, 145],
    })
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = -2.0
        mock_ind.popularity.rank_velocity_zscore.return_value = -2.5  # default threshold passes, tier 2 requires -3.5
        mock_ind.popularity.mention_acceleration.return_value = 10.0  # accelerating

        result = evaluate_popularity_surge(
            ticker='XYZ', current_rank=60, rank_24h_ago=60,
            mentions=50, mentions_24h_ago=50,
            popularity_history=history,
        )
        assert SurgeType.VELOCITY_SPIKE not in result.surge_types


def test_tier_4_new_entrant_cutoff_150():
    """Tier 4 stock (rank 510) with no prior rank: cutoff=150, but current rank 510 > 150 → not new entrant.

    Contrast with a Tier 3 stock at rank 160 (no prior): cutoff stays 200, 160 <= 200 → IS new entrant.
    """
    # Tier 4: rank 510, no prior rank — with cutoff=150, 510 > 150 → NOT new entrant
    tier4_result = evaluate_popularity_surge(
        ticker='NEWCO', current_rank=510, rank_24h_ago=None,
        mentions=30, mentions_24h_ago=10,
    )
    assert SurgeType.NEW_ENTRANT not in tier4_result.surge_types

    # Also verify the threshold override: _get_tier_thresholds(510) returns cutoff=150
    overrides = _get_tier_thresholds(510)
    assert overrides.get('new_entrant_cutoff') == 150


# ---------------------------------------------------------------------------
# Mention acceleration gate
# ---------------------------------------------------------------------------

def _make_accel_history(mentions: list[int]) -> pd.DataFrame:
    base = pd.Timestamp('2026-01-01')
    return pd.DataFrame({
        'datetime': [base + pd.Timedelta(hours=i) for i in range(len(mentions))],
        'rank': list(range(len(mentions))),
        'mentions': mentions,
    })


def test_acceleration_gate_blocks_when_decelerating():
    """When mention acceleration <= 0 and 3+ data points, surge is suppressed."""
    history = _make_accel_history([100, 150, 170, 180])  # slowing
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = -10.0
        mock_ind.popularity.rank_velocity_zscore.return_value = -3.0
        mock_ind.popularity.mention_acceleration.return_value = -5.0  # decelerating

        result = evaluate_popularity_surge(
            ticker='GME', current_rank=50, rank_24h_ago=200,
            mentions=180, mentions_24h_ago=50,
            popularity_history=history,
        )
    assert result.is_surging is False
    assert result.mention_acceleration == pytest.approx(-5.0)


def test_acceleration_gate_allows_when_accelerating():
    """When mention acceleration > 0, surge is NOT suppressed by the gate."""
    history = _make_accel_history([100, 150, 220, 350])  # accelerating
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = -30.0
        mock_ind.popularity.rank_velocity_zscore.return_value = -3.0
        mock_ind.popularity.mention_acceleration.return_value = 50.0  # accelerating

        result = evaluate_popularity_surge(
            ticker='GME', current_rank=50, rank_24h_ago=200,
            mentions=350, mentions_24h_ago=100,
            popularity_history=history,
        )
    # RANK_JUMP (gain=150, ratio=3.0) + MENTION_SURGE (3.5x) + VELOCITY_SPIKE (-3σ) should fire
    assert result.is_surging is True


def test_acceleration_gate_skipped_with_fewer_than_3_points():
    """With < 3 history points, the acceleration gate is skipped (sparse data fallback)."""
    history = _make_accel_history([100, 300])  # only 2 points
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = 0.0
        mock_ind.popularity.rank_velocity_zscore.return_value = 0.0
        mock_ind.popularity.mention_acceleration.return_value = float('nan')

        result = evaluate_popularity_surge(
            ticker='GME', current_rank=50, rank_24h_ago=200,
            mentions=300, mentions_24h_ago=100,
            popularity_history=history,
        )
    # RANK_JUMP + MENTION_SURGE should fire (no gate applied with NaN acceleration)
    assert SurgeType.MENTION_SURGE in result.surge_types


def test_acceleration_gate_skipped_with_none_history():
    """None history → acceleration gate is not applied."""
    result = evaluate_popularity_surge(
        ticker='GME', current_rank=50, rank_24h_ago=200,
        mentions=300, mentions_24h_ago=100,
        popularity_history=None,
    )
    assert SurgeType.MENTION_SURGE in result.surge_types
    assert result.mention_acceleration is None


def test_mention_acceleration_field_populated_in_result():
    """mention_acceleration field should be set in result when history has mentions column."""
    history = _make_accel_history([100, 150, 220, 350])
    with patch('rocketstocks.core.analysis.popularity_signals.indicators') as mock_ind:
        mock_ind.popularity.rank_velocity.return_value = 0.0
        mock_ind.popularity.rank_velocity_zscore.return_value = 0.0
        mock_ind.popularity.mention_acceleration.return_value = 20.0

        result = evaluate_popularity_surge(
            ticker='GME', current_rank=300, rank_24h_ago=300,
            mentions=350, mentions_24h_ago=100,
            popularity_history=history,
        )
    assert result.mention_acceleration == pytest.approx(20.0)
