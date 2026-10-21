"""Alert payload models: what ingest stores in `alerts.payload` and the bot
renders typed. An alert never changes after it is written, so every fact is the
value at write time, and a fact that could not be gathered says why instead of
being omitted. `version` is bumped on any breaking change to a payload shape.
"""

from datetime import date
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, PositiveInt

from rs_domain._types import UtcDatetime


class FactUnavailable(BaseModel):
    """A fact that could not be gathered; `reason` is shown to the reader."""

    model_config = ConfigDict(frozen=True)

    status: Literal["unavailable"] = "unavailable"
    reason: str


class PriceMoveFact(BaseModel):
    """Alert-time quote against the previous session's close."""

    model_config = ConfigDict(frozen=True)

    status: Literal["ok"] = "ok"
    last_price: float
    previous_close: float
    change_pct: float
    quote_time: UtcDatetime


class VolumeFact(BaseModel):
    """Quote volume against the mean of the last `sessions` stored daily bars.
    `intraday` is true when the quote came mid-session, so `volume` is a partial
    day and the ratio understates the day's final multiple."""

    model_config = ConfigDict(frozen=True)

    status: Literal["ok"] = "ok"
    volume: NonNegativeInt
    average_volume: float
    sessions: PositiveInt
    ratio: float
    intraday: bool


class BenchmarkFact(BaseModel):
    """The benchmark's price at alert time (SPY), so a later study can measure
    the move against the market from the same instant as `alert_price`."""

    model_config = ConfigDict(frozen=True)

    status: Literal["ok"] = "ok"
    symbol: str
    price: float
    quote_time: UtcDatetime


class NextEarningsFact(BaseModel):
    """`estimated` marks a date derived from the last results filing rather than
    listed by the earnings calendar. `time_of_day` is `pre_market`,
    `after_hours` or None when the source does not say."""

    model_config = ConfigDict(frozen=True)

    status: Literal["ok"] = "ok"
    date: date
    estimated: bool
    time_of_day: str | None = None


class SurgeFacts(BaseModel):
    """Unknown keys are ignored on read, so a version 1 payload (which also
    carried a `recent_filings` fact) still parses."""

    model_config = ConfigDict(frozen=True)

    price_move: Annotated[PriceMoveFact | FactUnavailable, Field(discriminator="status")]
    volume: Annotated[VolumeFact | FactUnavailable, Field(discriminator="status")]
    next_earnings: Annotated[NextEarningsFact | FactUnavailable, Field(discriminator="status")]
    benchmark: Annotated[BenchmarkFact | FactUnavailable, Field(discriminator="status")]


class AttentionSurgePayload(BaseModel):
    """The decision inputs (what the rule saw) plus the facts block."""

    model_config = ConfigDict(frozen=True)

    # 2 dropped the `recent_filings` fact; stored version 1 payloads stay readable.
    version: Literal[1, 2] = 2
    rule_version: str
    polled_at: UtcDatetime
    fired: tuple[Literal["mention_surge", "rank_jump"], ...]
    rank: PositiveInt
    mentions: NonNegativeInt
    rank_24h_ago: NonNegativeInt | None
    mentions_24h_ago: NonNegativeInt | None
    rank_gain: int | None
    mention_ratio: float | None
    mention_multiplier: float
    mention_min_prior: NonNegativeInt
    facts: SurgeFacts
