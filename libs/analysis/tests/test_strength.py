"""Strength is a percentile among the reference hits; the bar gives a typical day about its target."""

from datetime import date, timedelta

import pytest

from rs_analysis.strength import (
    MIN_REFERENCE_DAYS,
    ReferenceUnavailable,
    ScoredHit,
    build_reference,
)

DAY0 = date(2026, 9, 1)
DAYS = [DAY0 + timedelta(days=i) for i in range(30)]


def hit(day_index: int, symbol: str, score: float) -> ScoredHit:
    return ScoredHit(day=DAYS[day_index], symbol=symbol, score=score)


def stocks_per_day(count: int) -> list[ScoredHit]:
    """`count` stocks on each of the 30 days, stock s always scoring 1 + s."""
    return [hit(d, f"S{s}", 1.0 + s) for d in range(30) for s in range(count)]


class TestStrength:
    def test_is_the_share_of_reference_hits_no_stronger(self):
        reference = build_reference(
            [hit(d, "A", float(d)) for d in range(30)], DAYS, target=1
        )

        assert reference.strength(14.5) == pytest.approx(15 / 30)
        assert reference.strength(29.0) == 1.0
        assert reference.strength(100.0) == 1.0
        assert reference.strength(-1.0) == 0.0

    def test_counts_every_hit_not_every_stock_day(self):
        # A stock hitting every poll of a day weighs on the ranking as the Trigger did.
        hits = [hit(d, "A", 10.0) for d in range(30)] * 3 + [hit(0, "B", 1.0)]

        assert build_reference(hits, DAYS, target=1).strength(5.0) == pytest.approx(1 / 91)


class TestBar:
    def test_a_typical_day_yields_the_target(self):
        reference = build_reference(stocks_per_day(12), DAYS, target=5)

        clearing = [
            sum(h.score >= reference.bar_score for h in stocks_per_day(12) if h.day == day) for day in DAYS
        ]
        assert sorted(clearing)[15] == 5  # the median day

    def test_is_set_in_strength_terms_the_target_scores_percentile(self):
        reference = build_reference(stocks_per_day(10), DAYS, target=5)

        assert reference.bar_score == 6.0  # stocks scoring 6..10 are the 5 that clear it
        assert reference.bar == reference.strength(6.0) == pytest.approx(0.6)  # 6 of 10 scores are at most 6

    def test_a_stock_counts_once_per_day_however_often_it_hits(self):
        # Two stocks a day, one of them hitting 40 times: a target of 2 still needs both.
        hits = [h for d in range(30) for h in (hit(d, "A", 9.0), hit(d, "B", 5.0))]
        hits += [hit(d, "A", 9.0) for d in range(30) for _ in range(40)]

        reference = build_reference(hits, DAYS, target=2)

        assert reference.bar_score == 5.0

    def test_a_target_a_day_never_reaches_lets_every_hit_clear(self):
        reference = build_reference(stocks_per_day(2), DAYS, target=5)

        assert reference.bar_score == 1.0
        assert reference.bar == 0.5

    def test_the_median_day_sets_it_so_a_few_extreme_days_do_not(self):
        quiet = [hit(d, f"S{s}", 10.0 + s) for d in range(30) for s in range(6)]
        storm = [hit(d, f"T{s}", 100.0 + s) for d in range(3) for s in range(50)]

        reference = build_reference(quiet + storm, DAYS, target=5)

        # 27 ordinary days have 6 stocks; the 3 storm days must not pull the bar up to the storm.
        assert reference.bar_score == 11.0

    def test_days_without_polls_are_left_out_not_counted_as_empty(self):
        polled = DAYS[:7] + DAYS[23:]  # sixteen days the daemon was down
        hits = [h for h in stocks_per_day(10) if h.day in polled]

        counted_as_empty = build_reference(hits, DAYS, target=5)
        left_out = build_reference(hits, polled, target=5)

        assert counted_as_empty.bar_score == 1.0  # a median of zero stocks asks nothing of one
        assert left_out.bar_score == 6.0  # the 5th strongest of the 10 stocks a polled day has

    def test_refuses_too_few_days(self):
        with pytest.raises(ReferenceUnavailable, match="day"):
            build_reference(stocks_per_day(3), DAYS[: MIN_REFERENCE_DAYS - 1], target=1)

    def test_refuses_no_hits(self):
        with pytest.raises(ReferenceUnavailable, match="hit"):
            build_reference([], DAYS, target=1)
