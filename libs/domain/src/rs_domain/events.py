"""Domain events — frozen, plain-data, JSON-serializable (ADR-0023).

Ordering contract: the total order over Bus-1 events is `(event_time,
sequence)`. `sequence` is monotonic within a single `event_time` and is
derived from the durable log on warm-up — never a resettable in-process
counter. A mid-day restart must not let a same-`event_time` pair sort
differently on replay than it did live.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, NonNegativeInt

from rs_domain._types import UtcDatetime
from rs_domain.models import Bar


class BarClosed(BaseModel):
    """A 5-minute bar closed; the sole Bus-1 event in Phase 0/1 (ADR-0021)."""

    model_config = ConfigDict(frozen=True)

    event_time: UtcDatetime
    sequence: NonNegativeInt
    bar: Bar

    @property
    def ordering_key(self) -> tuple[datetime, int]:
        return (self.event_time, self.sequence)
