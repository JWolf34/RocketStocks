"""The earnings calendar as stored: one row per ticker and report date, in a dated snapshot.

`snapshot_date` is the Eastern date the row was fetched, so a snapshot is what the
calendar said that day. `origin` tells the nightly snapshot (a week back to the end
of Nasdaq's forward window) from the one-time backfill of history; a reader of "the
latest snapshot" asks for the nightly origin only.
"""

from datetime import date
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, NonNegativeInt

from rs_domain._types import UtcDatetime


class EarningsTime(StrEnum):
    PRE_MARKET = "pre_market"
    AFTER_HOURS = "after_hours"


class EarningsOrigin(StrEnum):
    NIGHTLY = "nightly"
    BACKFILL = "backfill"


class StoredEarnings(BaseModel):
    """`fiscal_quarter` is the month the fiscal quarter ends, as `YYYY-MM`; `surprise_pct` is
    Nasdaq's percent surprise. Every vendor figure is None when Nasdaq had none (a forward
    day carries no actual EPS or surprise), and `fetched_at` is when Nasdaq answered."""

    model_config = ConfigDict(frozen=True)

    snapshot_date: date
    origin: EarningsOrigin
    ticker: str
    report_date: date
    time_of_day: EarningsTime | None
    fiscal_quarter: str | None
    eps_forecast: float | None
    estimate_count: NonNegativeInt | None
    eps_actual: float | None
    surprise_pct: float | None
    fetched_at: UtcDatetime
