"""The selector: which Trigger hits become Alerts, and which reply to one.

A pure function from a hit, its strength and what its Eastern day already holds
to a verdict. A candidate posts at once when its strength clears the bar and the
day's budget is not used up: an online selector cannot know the day's strongest,
so the bar is tuned (from the Trigger's trailing 30 days) so a typical day yields
about the budget. A stock below the bar earlier can still get its first Alert
later that day; one over budget cannot, as the budget never frees within a day.
Once a stock has its Alert, later hits are repeats unless they escalate, in which
case they reply in its thread, up to a few times; replies never use the budget.
"""

from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime

from rs_analysis.popularity_surge import SubRule, SurgeEvent
from rs_domain import ALERT_STALE_AFTER, Verdict

# Alerts per Eastern day, all stocks; replies are not counted.
DAILY_BUDGET = 5
# Stocks a typical day has at or above the bar. The budget is what a typical day
# should yield, so the bar targets it; lowering this leaves headroom for busy days.
BAR_TARGET_PER_DAY = DAILY_BUDGET
# How far a hit's strength must rise above the highest strength already announced for
# the stock that day to reply on strength alone; strengths are percentiles in [0, 1].
REPLY_STRENGTH_MARGIN = 0.10
MAX_REPLIES_PER_STOCK_DAY = 3


@dataclass(frozen=True)
class StockDay:
    """What a stock's Alert and replies already said on the Eastern day: the sub-rules
    they carried, the highest strength announced (None when unknown, as for an Alert
    decided before strength was stored) and how many replies have been sent."""

    carried: frozenset[str]
    highest_strength: float | None
    replies: int


@dataclass(frozen=True)
class DayState:
    """What the hit's Eastern day holds: Alerts posted so far (all stocks, against the
    budget) and the hit's stock's Alert, None when the stock has none."""

    alerts_posted: int
    stock: StockDay | None


@dataclass(frozen=True)
class Escalation:
    """What a hit on an alerted stock adds: sub-rules not yet carried, and the strength
    it rose from when it rose by the margin."""

    new_rules: tuple[SubRule, ...]
    previous_strength: float | None
    strength_up: bool


def escalation(hit: SurgeEvent, strength: float | None, stock: StockDay) -> Escalation | None:
    """How the hit escalates beyond what the stock's Alert and replies carried, or None."""
    new_rules = tuple(rule for rule in hit.fired if rule.value not in stock.carried)
    strength_up = (
        strength is not None
        and stock.highest_strength is not None
        and strength >= stock.highest_strength + REPLY_STRENGTH_MARGIN
    )
    if not new_rules and not strength_up:
        return None
    return Escalation(new_rules=new_rules, previous_strength=stock.highest_strength, strength_up=strength_up)


def select(
    hit: SurgeEvent,
    *,
    strength: float | None,
    bar: float | None,
    day: DayState,
    decided_at: datetime,
) -> Verdict:
    """The verdict on a hit with `strength` (None when its stock has no Baseline, so
    it cannot be shown Unusual) against `bar` (None when there is no reference to rank
    against), decided at `decided_at`."""
    if decided_at - hit.polled_at > ALERT_STALE_AFTER:
        return Verdict.STALE
    if day.stock is not None:
        if escalation(hit, strength, day.stock) is not None and day.stock.replies < MAX_REPLIES_PER_STOCK_DAY:
            return Verdict.REPLY
        return Verdict.REPEAT
    if strength is None or bar is None or strength < bar:
        return Verdict.BELOW_BAR
    if day.alerts_posted >= DAILY_BUDGET:
        return Verdict.OVER_BUDGET
    return Verdict.POSTED


def announce(stock: StockDay | None, fired: Collection[str], strength: float | None, *, reply: bool) -> StockDay:
    """The stock's day after one more announcement (its Alert, or a reply to it)."""
    strengths = [s for s in (None if stock is None else stock.highest_strength, strength) if s is not None]
    return StockDay(
        carried=(frozenset() if stock is None else stock.carried) | frozenset(fired),
        highest_strength=max(strengths, default=None),
        replies=(0 if stock is None else stock.replies) + int(reply),
    )
