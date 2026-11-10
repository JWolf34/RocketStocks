"""Options chains as stored: one contract row per contract per session, one fetch record per
underlying per pass.

A contract row is an observation and never a placeholder: a chain that could not be fetched
is a fetch record with a failure status (a Tombstone), and an empty chain is a fetch record
with status `empty` and no contract rows. Values Schwab marks as absent (a `-999` greek, a
non-positive epoch time) are None, never 0.
"""

from datetime import date
from enum import StrEnum
from typing import Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PositiveInt,
    model_validator,
)

from rs_domain._types import UtcDatetime


class ChainPass(StrEnum):
    EVENING = "evening"
    MORNING = "morning"


class FetchStatus(StrEnum):
    """How one underlying's fetch ended in one pass.

    `ok` and `empty` are observations (`empty` is a chain with no contracts); `rejected`
    (the vendor answered 400 or 404 for the symbol) and `failed` (the ladder ran out) are
    Tombstones: no observation, never zero.
    """

    OK = "ok"
    EMPTY = "empty"
    REJECTED = "rejected"
    FAILED = "failed"

    @property
    def is_tombstone(self) -> bool:
        return self in (FetchStatus.REJECTED, FetchStatus.FAILED)


class OptionContract(BaseModel):
    """One contract of a chain as Schwab quoted it.

    `underlying` is the Universe's own symbol (`BRK.B`), whatever spelling the request used.
    `iv` is Schwab's implied volatility in percent, untransformed. `underlying_price` is the
    chain's, repeated on every row. Quote and flow fields are always present; the greeks, the
    times, the underlying price and the contract's own multiplier, expiration type and
    non-standard flag are None when Schwab sent none.
    """

    model_config = ConfigDict(frozen=True)

    underlying: str
    option_symbol: str
    expiry: date
    strike: float = Field(ge=0, allow_inf_nan=False)
    put_call: Literal["C", "P"]
    bid: float = Field(allow_inf_nan=False)
    ask: float = Field(allow_inf_nan=False)
    last: float = Field(allow_inf_nan=False)
    mark: float = Field(allow_inf_nan=False)
    bid_size: NonNegativeInt
    ask_size: NonNegativeInt
    last_size: NonNegativeInt
    volume: NonNegativeInt
    open_interest: NonNegativeInt
    quote_time: UtcDatetime | None
    trade_time: UtcDatetime | None
    iv: float | None = Field(allow_inf_nan=False)
    delta: float | None = Field(allow_inf_nan=False)
    gamma: float | None = Field(allow_inf_nan=False)
    theta: float | None = Field(allow_inf_nan=False)
    vega: float | None = Field(allow_inf_nan=False)
    rho: float | None = Field(allow_inf_nan=False)
    underlying_price: float | None = Field(gt=0, allow_inf_nan=False)
    multiplier: PositiveInt | None
    expiration_type: str | None = Field(min_length=1, max_length=1)
    non_standard: bool | None


class ChainFetchRecord(BaseModel):
    """The outcome of one pass's fetch of one underlying for one session.

    `attempts` counts the Schwab calls spent (0 for an underlying never tried before the
    deadline); `contract_count` is the contracts stored, so it is positive exactly when the
    status is `ok`.
    """

    model_config = ConfigDict(frozen=True)

    session_date: date
    chain_pass: ChainPass
    underlying: str
    status: FetchStatus
    attempts: NonNegativeInt
    contract_count: NonNegativeInt
    fetched_at: UtcDatetime

    @model_validator(mode="after")
    def _count_matches_status(self) -> Self:
        if (self.status is FetchStatus.OK) != (self.contract_count > 0):
            raise ValueError(f"status {self.status.value} cannot have {self.contract_count} contracts")
        return self
