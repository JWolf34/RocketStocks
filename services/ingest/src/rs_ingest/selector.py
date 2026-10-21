"""The selector: which Trigger hits become Alerts.

A pure function from a hit and what its Eastern day already holds to a verdict, so
the rules that decide (one Alert per stock per day today; strength, the bar, the
budget, replies and staleness next) change here without touching how the poll
stores candidates. `DayState` grows with those rules.
"""

from dataclasses import dataclass

from rs_analysis.popularity_surge import SurgeEvent
from rs_domain import Verdict


@dataclass(frozen=True)
class DayState:
    """What the hit's Eastern day holds for the hit's stock."""

    alerted: bool


def select(hit: SurgeEvent, day: DayState) -> Verdict:
    """The first hit of a stock's day is posted; later hits that day repeat it."""
    return Verdict.REPEAT if day.alerted else Verdict.POSTED
