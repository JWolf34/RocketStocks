"""Scoring a hit: its raw score, its strength and the bar it is judged against.

The reference a strength ranks against is the Trigger's own hits over the 30
Eastern days before the hit's day, found by replaying the rule over the stored
Popularity rather than read from the candidate table. That table holds only what
the live poll saw (about a day at launch), has holes wherever ingest was down and
mixes rule versions, so a reference built from it would change with the outages
and releases of the past month; a replay of the current rule over every stored
reading is the same on any day it is computed and covers imported history from
the first day.

A stock absent from the listing at a polled boundary counts there as zero mentions at the
bottom rank, so a newcomer has a Baseline (see rs_analysis.popularity_score).
Every score is of a hit against its stock's Baseline as of the start of the hit's
Eastern day, so a day's scores and a day's reference never change once the day is
over, and each is computed once per process: a day's reference is the 30 cached
days before it, and a new Eastern day costs one more day's replay.
"""

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_analysis.popularity_score import (
    BASELINE_DAYS,
    Baseline,
    baseline,
    raw_score,
    readings_over,
)
from rs_analysis.popularity_surge import MIN_MENTIONS, SurgeEvent, evaluate_latest
from rs_analysis.strength import REFERENCE_DAYS, Reference, ScoredHit, build_reference
from rs_domain import eastern_day, eastern_day_start
from rs_persistence import PopularityRepository

logger = logging.getLogger(__name__)

# A polled Eastern day is one the daemon was up for at least half of (of 48 boundaries).
MIN_POLLS_PER_DAY = 24


@dataclass(frozen=True)
class Scored:
    """A hit's raw score and strength (None when its stock has no Baseline) and the bar
    its day is judged against (None, with no strength, when there is no reference)."""

    raw_score: float | None
    strength: float | None
    bar: float | None


class Scorer(Protocol):
    async def score(self, events: Sequence[SurgeEvent]) -> list[Scored]: ...


class PopularityScorer:
    """Scores hits against references replayed from the stored Popularity, caching
    each Eastern day's replay and each stock's Baseline for the day."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], *, bar_target: int) -> None:
        self._session_factory = session_factory
        self._bar_target = bar_target
        self._lock = asyncio.Lock()
        self._replayed: dict[date, tuple[bool, list[ScoredHit]]] = {}
        self._references: dict[date, Reference] = {}
        self._baselines: dict[tuple[date, str], Baseline | None] = {}
        self._bottoms: dict[date, dict[datetime, int]] = {}

    async def score(self, events: Sequence[SurgeEvent]) -> list[Scored]:
        """One Scored per event, in order. Raises ReferenceUnavailable when the stored
        Popularity is too thin to rank against."""
        async with self._lock:
            by_day: dict[date, set[str]] = {}
            for event in events:
                by_day.setdefault(eastern_day(event.polled_at), set()).add(event.ticker)
            normals: dict[date, dict[str, Baseline | None]] = {
                day: await self._normals(day, tickers) for day, tickers in by_day.items()
            }
            references = {day: await self._reference(day) for day in by_day}
            scored = []
            for event in events:
                day = eastern_day(event.polled_at)
                reference = references[day]
                normal = normals[day][event.ticker]
                if normal is None:
                    scored.append(Scored(raw_score=None, strength=None, bar=reference.bar))
                    continue
                score = raw_score(event.inputs.rank, event.inputs.mentions, normal)
                scored.append(Scored(raw_score=score, strength=reference.strength(score), bar=reference.bar))
            if by_day:
                self._prune(max(by_day))
            return scored

    async def _normals(self, day: date, tickers: set[str]) -> dict[str, Baseline | None]:
        """The Baselines of `tickers` as of the start of `day`."""
        missing = {ticker for ticker in tickers if (day, ticker) not in self._baselines}
        if missing:
            bottoms = await self._window_bottoms(day)
            async with self._session_factory() as session:
                present = await PopularityRepository(session).readings(
                    missing,
                    eastern_day_start(day - timedelta(days=BASELINE_DAYS)),
                    eastern_day_start(day),
                )
            for ticker in missing:
                self._baselines[(day, ticker)] = baseline(readings_over(bottoms, present.get(ticker, {})))
        return {ticker: self._baselines[(day, ticker)] for ticker in tickers}

    async def _window_bottoms(self, day: date) -> dict[datetime, int]:
        """Every polled boundary of the Baseline window before `day`, with its bottom rank."""
        window: dict[datetime, int] = {}
        async with self._session_factory() as session:
            repo = PopularityRepository(session)
            for past in (day - timedelta(days=back) for back in range(BASELINE_DAYS, 0, -1)):
                if past not in self._bottoms:
                    self._bottoms[past] = await repo.bottom_ranks(
                        eastern_day_start(past), eastern_day_start(past + timedelta(days=1))
                    )
                window.update(self._bottoms[past])
        return window

    async def _reference(self, day: date) -> Reference:
        """The reference for hits of `day`: the Trigger's hits of the 30 days before it."""
        if day not in self._references:
            window = [day - timedelta(days=back) for back in range(REFERENCE_DAYS, 0, -1)]
            hits: list[ScoredHit] = []
            polled: list[date] = []
            for past in window:
                was_polled, past_hits = await self._replay(past)
                hits.extend(past_hits)
                if was_polled:
                    polled.append(past)
            reference = build_reference(hits, polled, self._bar_target)
            logger.info(
                "reference for %s: %d hit(s) over %d polled day(s), bar %.3f (score %.2f)",
                day, len(hits), len(polled), reference.bar, reference.bar_score,
            )
            self._references[day] = reference
        return self._references[day]

    async def _replay(self, day: date) -> tuple[bool, list[ScoredHit]]:
        """Whether the daemon polled the Eastern `day`, and every hit the rule makes over
        its stored readings, scored."""
        if day not in self._replayed:
            start, end = eastern_day_start(day), eastern_day_start(day + timedelta(days=1))
            async with self._session_factory() as session:
                repo = PopularityRepository(session)
                polls = await repo.count_polls(start, end)
                # The rule ignores a reading under MIN_MENTIONS, so it is not loaded.
                snapshots = await repo.list_between(start, end, min_mentions=MIN_MENTIONS)
            events = [event for event in map(evaluate_latest, snapshots) if event is not None]
            normals = await self._normals(day, {event.ticker for event in events})
            hits = [
                ScoredHit(day=day, symbol=event.ticker, score=raw_score(event.inputs.rank, event.inputs.mentions, normal))
                for event in events
                if (normal := normals[event.ticker]) is not None
            ]
            self._replayed[day] = (polls >= MIN_POLLS_PER_DAY, hits)
        return self._replayed[day]

    def _prune(self, newest: date) -> None:
        """Forget what no reference of `newest` or a later day can use."""
        oldest = newest - timedelta(days=REFERENCE_DAYS + 1)
        self._replayed = {day: value for day, value in self._replayed.items() if day >= oldest}
        self._references = {day: value for day, value in self._references.items() if day >= oldest}
        self._baselines = {key: value for key, value in self._baselines.items() if key[0] >= oldest}
        self._bottoms = {day: value for day, value in self._bottoms.items() if day >= oldest - timedelta(days=BASELINE_DAYS)}
