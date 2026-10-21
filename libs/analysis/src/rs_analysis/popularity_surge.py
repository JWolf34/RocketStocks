"""Popularity-surge rule: pure, deterministic detection over ApeWisdom popularity snapshots.

`evaluate_latest` is the live entry point and `replay` the research one (it loops
evaluate_latest, so they cannot disagree). Changing any threshold below bumps
RULE_VERSION.
"""

from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from rs_domain import PopularitySnapshot, UtcDatetime

RULE_VERSION = "1"

MIN_MENTIONS = 15

MENTION_MULTIPLIER = 3.0
MENTION_MULTIPLIER_TOP = 5.0
TOP_RANK = 25
# minimum mentions_24h_ago for mention_surge
MENTION_MIN_PRIOR = 15
MENTION_MIN_PRIOR_TAIL = 10
TAIL_RANK = 500

RANK_JUMP_MIN_GAIN = 50
RANK_JUMP_MIN_RATIO = 1.5

COOLDOWN = timedelta(hours=24)


class SubRule(StrEnum):
    MENTION_SURGE = "mention_surge"
    RANK_JUMP = "rank_jump"


class SurgeInputs(BaseModel):
    """What the decision was made on, including the rank-tier thresholds in force."""

    model_config = ConfigDict(frozen=True)

    rank: int
    mentions: int
    rank_24h_ago: int | None
    mentions_24h_ago: int | None
    rank_gain: int | None
    mention_ratio: float | None
    mention_multiplier: float
    mention_min_prior: int


class SurgeEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    ticker: str
    polled_at: UtcDatetime
    fired: tuple[SubRule, ...]
    inputs: SurgeInputs
    rule_version: str = RULE_VERSION


def evaluate_latest(snapshot: PopularitySnapshot, prior_events: Sequence[SurgeEvent]) -> SurgeEvent | None:
    """Judge the newest snapshot of a ticker. `prior_events` are the events
    already emitted for it (other tickers' are ignored); one within COOLDOWN
    of this snapshot suppresses it."""
    if snapshot.mentions < MIN_MENTIONS:
        return None

    rank = snapshot.rank
    multiplier = MENTION_MULTIPLIER_TOP if rank <= TOP_RANK else MENTION_MULTIPLIER
    min_prior = MENTION_MIN_PRIOR_TAIL if rank > TAIL_RANK else MENTION_MIN_PRIOR
    prev_mentions = snapshot.mentions_24h_ago
    prev_rank = snapshot.rank_24h_ago
    gain = None if prev_rank is None else prev_rank - rank

    fired: list[SubRule] = []
    if prev_mentions is not None and prev_mentions >= min_prior and snapshot.mentions >= multiplier * prev_mentions:
        fired.append(SubRule.MENTION_SURGE)
    if gain is not None and gain >= RANK_JUMP_MIN_GAIN and gain >= RANK_JUMP_MIN_RATIO * rank:
        fired.append(SubRule.RANK_JUMP)
    if not fired:
        return None

    window_start = snapshot.polled_at - COOLDOWN
    if any(e.ticker == snapshot.ticker and e.polled_at > window_start for e in prior_events):
        return None

    return SurgeEvent(
        ticker=snapshot.ticker,
        polled_at=snapshot.polled_at,
        fired=tuple(fired),
        inputs=SurgeInputs(
            rank=rank,
            mentions=snapshot.mentions,
            rank_24h_ago=prev_rank,
            mentions_24h_ago=prev_mentions,
            rank_gain=gain,
            mention_ratio=snapshot.mentions / prev_mentions if prev_mentions else None,
            mention_multiplier=multiplier,
            mention_min_prior=min_prior,
        ),
    )


def replay(history: Iterable[PopularitySnapshot]) -> list[SurgeEvent]:
    """Every event the rule would have emitted over a full history (any number
    of tickers, any input order), ordered by (polled_at, ticker)."""
    ordered = sorted(history, key=lambda s: (s.polled_at, s.ticker))
    seen: set[tuple[str, datetime]] = set()
    emitted: dict[str, list[SurgeEvent]] = {}
    events: list[SurgeEvent] = []
    for snapshot in ordered:
        key = (snapshot.ticker, snapshot.polled_at)
        if key in seen:
            raise ValueError(f"duplicate snapshot for {snapshot.ticker} at {snapshot.polled_at}")
        seen.add(key)
        prior = emitted.setdefault(snapshot.ticker, [])
        event = evaluate_latest(snapshot, prior)
        if event is not None:
            prior.append(event)
            events.append(event)
    return events
