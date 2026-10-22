"""Raw score of a Popularity hit: how far it is from the stock's own Baseline.

The Baseline is a stock's recent normal mentions and rank, taken as a median and a
robust spread of its readings at every polled boundary of the BASELINE_DAYS before
the hit's Eastern day began, so the hit's own build-up never lifts it. At a polled
boundary where the stock was absent from the listing its reading is zero mentions at
the bottom rank (one past the last rank stored at that boundary), so a newcomer has
a real Baseline: it was not discussed. A boundary with no stored snapshot at all is
downtime, not absence, and has no reading. A hit is Unusual by
its distance from that stock's normal, never by a cutoff shared by all stocks.
Mentions and rank are compared on a log scale (a stock normally at 20 mentions
doubling and one at 2,000 doubling are the same distance); the raw score adds the
two distances, each counted only in the unusual direction (more mentions, a better
rank), so a hit that moves both is further out than one that moves one.

Changing the window, the floor or how the two distances combine changes every
score and so every strength: a candidate is only comparable with others scored the
same way.
"""

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from statistics import median

BASELINE_DAYS = 14
# Fewer polled boundaries than a day of half-hourly polls say too little about anyone's
# normal, so a fresh install has no Baseline for a day.
MIN_BASELINE_READINGS = 48
# A very steady stock's spread is near zero and would make any move look infinite.
MIN_SPREAD = 0.25
# Scales a median absolute deviation to a standard deviation for normal data.
MAD_TO_SIGMA = 1.4826


@dataclass(frozen=True)
class Baseline:
    """A stock's normal mentions and rank: the median and spread of log(1 + mentions)
    and of log(rank), over `readings` stored readings."""

    readings: int
    mentions_median: float
    mentions_spread: float
    rank_median: float
    rank_spread: float


def _centre_and_spread(values: list[float]) -> tuple[float, float]:
    centre = median(values)
    spread = MAD_TO_SIGMA * median(abs(v - centre) for v in values)
    return centre, max(spread, MIN_SPREAD)


def readings_over(
    bottoms: Mapping[datetime, int], present: Mapping[datetime, tuple[int, int]]
) -> list[tuple[int, int]]:
    """A stock's (rank, mentions) reading at every polled boundary of a window.
    `bottoms` maps each polled boundary to the bottom rank there (one past the last
    stored rank) and `present` the boundaries the stock was listed at; at any other
    polled boundary it is absent: zero mentions at the bottom rank."""
    return [present.get(at, (bottom, 0)) for at, bottom in bottoms.items()]


def baseline(readings: Sequence[tuple[int, int]]) -> Baseline | None:
    """The Baseline of a stock's readings over a window's polled boundaries (see
    `readings_over`), or None when there are too few boundaries to say what is normal."""
    if len(readings) < MIN_BASELINE_READINGS:
        return None
    mentions_median, mentions_spread = _centre_and_spread([math.log1p(m) for _, m in readings])
    rank_median, rank_spread = _centre_and_spread([math.log(r) for r, _ in readings])
    return Baseline(len(readings), mentions_median, mentions_spread, rank_median, rank_spread)


def raw_score(rank: int, mentions: int, normal: Baseline) -> float:
    """Distance of a hit from the stock's Baseline, in spreads; 0 when it is no
    further out than normal in either measure."""
    mentions_distance = (math.log1p(mentions) - normal.mentions_median) / normal.mentions_spread
    rank_distance = (normal.rank_median - math.log(rank)) / normal.rank_spread
    return max(mentions_distance, 0.0) + max(rank_distance, 0.0)
