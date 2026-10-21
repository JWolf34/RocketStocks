"""The raw score is a hit's distance from the stock's own Baseline."""

import math
from datetime import UTC, datetime, timedelta

import pytest

from rs_analysis.popularity_score import (
    MIN_BASELINE_READINGS,
    MIN_SPREAD,
    Baseline,
    baseline,
    raw_score,
    readings_over,
)


def readings(rank: int, mentions: int, n: int = 100) -> list[tuple[int, int]]:
    return [(rank, mentions)] * n


def wobbling(rank: int, mentions: int, n: int = 100) -> list[tuple[int, int]]:
    """Readings that move about their centre, so the spread is above the floor."""
    return [(rank + (i % 5) * rank // 5, mentions + (i % 5) * mentions // 3) for i in range(n)]


class TestBaseline:
    def test_is_none_with_too_few_readings_to_know_what_is_normal(self):
        assert baseline(readings(40, 30, MIN_BASELINE_READINGS - 1)) is None
        assert baseline(readings(40, 30, MIN_BASELINE_READINGS)) is not None

    def test_takes_the_median_not_the_mean_so_a_burst_does_not_lift_it(self):
        history = readings(40, 30, 90) + readings(2, 900, 10)

        normal = baseline(history)

        assert normal is not None
        assert normal.mentions_median == pytest.approx(math.log1p(30))
        assert normal.rank_median == pytest.approx(math.log(40))

    def test_a_perfectly_steady_stock_still_has_a_spread_floor(self):
        normal = baseline(readings(40, 30))

        assert normal is not None
        assert (normal.mentions_spread, normal.rank_spread) == (MIN_SPREAD, MIN_SPREAD)


class TestRawScore:
    def test_is_relative_to_the_stock_the_same_jump_is_further_out_for_a_steadier_stock(self):
        steady = baseline(readings(40, 30))
        restless = baseline(wobbling(40, 30))
        assert steady is not None and restless is not None

        assert raw_score(40, 90, steady) > raw_score(40, 90, restless) > 0

    def test_the_same_mentions_are_unusual_for_a_quiet_stock_and_not_for_a_busy_one(self):
        quiet = baseline(wobbling(300, 20))
        busy = baseline(wobbling(5, 400))
        assert quiet is not None and busy is not None

        assert raw_score(300, 400, quiet) > 3
        assert raw_score(7, 400, busy) == 0

    def test_is_zero_at_or_below_normal(self):
        normal = baseline(wobbling(40, 30))
        assert normal is not None

        assert raw_score(60, 10, normal) == 0

    def test_a_better_rank_adds_to_more_mentions(self):
        normal = baseline(wobbling(100, 30))
        assert normal is not None

        mentions_only = raw_score(100, 300, normal)
        both = raw_score(20, 300, normal)

        assert both > mentions_only > 0

    def test_grows_with_mentions_and_with_a_better_rank(self):
        normal = baseline(wobbling(100, 30))
        assert normal is not None

        assert raw_score(100, 200, normal) < raw_score(100, 400, normal)
        assert raw_score(60, 200, normal) < raw_score(30, 200, normal)

    def test_counts_in_spreads_of_the_log_scale(self):
        normal = Baseline(readings=100, mentions_median=math.log1p(30), mentions_spread=0.5,
                          rank_median=math.log(40), rank_spread=0.5)

        # 247 mentions is 8x the normal 30 (as 248 against 31), at the normal rank.
        assert raw_score(40, 8 * 31 - 1, normal) == pytest.approx(math.log(8) / 0.5)


T0 = datetime(2026, 10, 1, tzinfo=UTC)
BOUNDARIES = [T0 + timedelta(minutes=30 * i) for i in range(100)]


class TestReadingsOver:
    def test_a_stock_absent_from_a_polled_boundary_reads_zero_mentions_at_the_bottom_rank(self):
        bottoms = {BOUNDARIES[0]: 301, BOUNDARIES[1]: 305}

        got = readings_over(bottoms, {BOUNDARIES[0]: (40, 30)})

        assert got == [(40, 30), (305, 0)]

    def test_a_boundary_that_was_not_polled_gives_no_reading(self):
        # Boundary 1 has no snapshot for anyone: it is not in `bottoms`, so it is downtime, not absence.
        bottoms = {BOUNDARIES[0]: 301, BOUNDARIES[2]: 301}

        assert len(readings_over(bottoms, {})) == 2

    def test_a_newcomer_absent_from_every_polled_boundary_has_a_baseline_and_a_high_score(self):
        bottoms = {at: 301 for at in BOUNDARIES}

        newcomer = baseline(readings_over(bottoms, {}))
        steady = baseline(readings_over(bottoms, {at: (40, 30) for at in BOUNDARIES}))

        assert newcomer is not None and steady is not None
        assert newcomer.mentions_median == 0
        assert raw_score(40, 90, newcomer) > raw_score(40, 90, steady) > 0

    def test_a_fresh_install_with_too_few_polled_boundaries_has_no_baseline(self):
        bottoms = {at: 301 for at in BOUNDARIES[: MIN_BASELINE_READINGS - 1]}

        assert baseline(readings_over(bottoms, {})) is None
