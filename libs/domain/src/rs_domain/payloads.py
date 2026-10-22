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
    """Unknown keys are ignored on read, so an older payload still parses: version 1 also
    carried a `recent_filings` fact, versions 1 and 2 a `volume` and a `benchmark` fact."""

    model_config = ConfigDict(frozen=True)

    price_move: Annotated[PriceMoveFact | FactUnavailable, Field(discriminator="status")]
    next_earnings: Annotated[NextEarningsFact | FactUnavailable, Field(discriminator="status")]


class PopularitySurgePayload(BaseModel):
    """The decision inputs (what the rule saw) plus the facts block."""

    model_config = ConfigDict(frozen=True)

    # 2 dropped the `recent_filings` fact, 3 the `volume` and `benchmark` facts; every stored
    # version stays readable.
    version: Literal[1, 2, 3] = 3
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


class EscalationPayload(BaseModel):
    """What a reply in an Alert's thread says escalated, written by the selector.
    `new_rules` are the sub-rules neither the Alert nor an earlier reply carried;
    `strength_before` is the highest strength announced for the stock that day and
    `strength_after` the reply's, both percentiles (0 to 1) among the Trigger's hits
    of the 30 Eastern days before, set only when strength rose by the margin."""

    model_config = ConfigDict(frozen=True)

    version: Literal[1] = 1
    polled_at: UtcDatetime
    new_rules: tuple[Literal["mention_surge", "rank_jump"], ...] = ()
    strength_before: float | None = None
    strength_after: float | None = None
