"""Strength and the bar: a hit's percentile among its Trigger's recent hits.

The reference is every hit the Trigger made over the REFERENCE_DAYS before an
Eastern day, scored by its raw score. A hit's strength is the share of those hits
no stronger than it, in [0, 1], so strengths of different Triggers are comparable
whatever their raw scales. The bar is the strength a hit must reach to be posted,
set so that the typical (median) day of the reference has `target` stocks clearing
it: because a stock counts once per day however often it hits, and a day with no
polls is not a day, an outage moves neither the bar nor the strengths much.
"""

from bisect import bisect_left, bisect_right
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import date
from statistics import median

REFERENCE_DAYS = 30
# Fewer days than this say too little about a typical day to set a bar.
MIN_REFERENCE_DAYS = 7


class ReferenceUnavailable(RuntimeError):
    """The stored Popularity covers too few days, or holds no hit, to rank a candidate."""


@dataclass(frozen=True)
class ScoredHit:
    """One Trigger hit of the reference: the stock, its Eastern day and raw score."""

    day: date
    symbol: str
    score: float


@dataclass(frozen=True)
class Reference:
    """The ranked reference hits and the bar they set."""

    scores: tuple[float, ...]  # ascending
    bar_score: float
    bar: float  # the strength of `bar_score`
    days: int

    def strength(self, score: float) -> float:
        """The share of reference hits whose score is at most `score`."""
        return bisect_right(self.scores, score) / len(self.scores)


def build_reference(hits: Sequence[ScoredHit], days: Collection[date], target: int) -> Reference:
    """Rank `hits` (those of the polled `days`) and set the bar for `target` stocks on a typical day.

    The bar is the strongest score that a typical day still has `target` stocks at
    or above; when even the weakest score leaves a typical day short of `target`,
    every hit clears it."""
    if len(days) < MIN_REFERENCE_DAYS:
        raise ReferenceUnavailable(
            f"{len(days)} day(s) of Popularity to rank against, need at least {MIN_REFERENCE_DAYS}"
        )
    if not hits:
        raise ReferenceUnavailable("no hit in the reference days to rank against")

    scores = sorted(hit.score for hit in hits)
    strongest: dict[tuple[date, str], float] = {}
    for hit in hits:
        key = (hit.day, hit.symbol)
        strongest[key] = max(hit.score, strongest.get(key, hit.score))
    by_day: dict[date, list[float]] = {day: [] for day in days}
    for (day, _), score in strongest.items():
        if day in by_day:
            by_day[day].append(score)
    ranked = [sorted(day_scores) for day_scores in by_day.values()]

    def typical_stocks_at_or_above(score: float) -> float:
        return median(len(day_scores) - bisect_left(day_scores, score) for day_scores in ranked)

    # The count falls as the score rises, so the strongest score still reaching `target` is a bisection.
    candidates = sorted(set(strongest.values()))
    low, high = 0, len(candidates) - 1
    if typical_stocks_at_or_above(candidates[0]) < target:
        high = 0
    while low < high:
        middle = (low + high + 1) // 2
        if typical_stocks_at_or_above(candidates[middle]) >= target:
            low = middle
        else:
            high = middle - 1
    bar_score = candidates[low]
    return Reference(
        scores=tuple(scores),
        bar_score=bar_score,
        bar=bisect_right(scores, bar_score) / len(scores),
        days=len(days),
    )
