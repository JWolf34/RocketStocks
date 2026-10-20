"""Domain models — the validated shapes the rest of v2 consumes (ADR-0004).

Prices are float64: the wire delivers JSON floats, detection math runs in
float64, and DOUBLE PRECISION round-trips them bit-exactly, which the replay
contract (ADR-0023) depends on. NUMERIC/Decimal is reserved for future
accounting-grade tables (auto-trader era), never this path.
"""

from collections.abc import Iterable
from datetime import date, datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, NonNegativeInt, PositiveInt

from rs_domain._types import UtcDatetime


class Bar(BaseModel):
    """One closed bar as detection saw it. `bucket` is the UTC start instant."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    bucket: UtcDatetime
    open: float
    high: float
    low: float
    close: float
    volume: NonNegativeInt


class Alert(BaseModel):
    """A detection decision to announce. The versioned-rule fields are optional so
    streaming detectors without a rule version keep working; a `dedup_key` makes
    the write idempotent (the same key is stored once)."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    alert_type: str
    triggered_at: UtcDatetime
    payload: dict = {}
    rule_version: str | None = None
    alert_price: float | None = None
    quote_time: UtcDatetime | None = None
    dedup_key: str | None = None


class Dataset(StrEnum):
    """Which scan produced a snapshot (ADR-0032 D5). Values are the DB enum
    values and double as the `source` metric-label key (ADR-0017)."""

    CURATED_5M = "curated_5m"
    EOD_FULL = "eod_full"


class SourceStatus(StrEnum):
    """Row-level fetch outcome (ADR-0017, extended by S008: `stale` for the
    quote_time-regression guard, `skipped` for overrun tombstones)."""

    OK = "ok"
    PARTIAL = "partial"
    FAILED = "failed"
    STALE = "stale"
    SKIPPED = "skipped"


class PopularityOrigin(StrEnum):
    """Where a popularity row came from. Values are the DB column values."""

    LIVE = "live"
    V1_IMPORT = "v1_import"


class PopularitySnapshot(BaseModel):
    """One ticker's ApeWisdom standing at one poll boundary. `polled_at` is the
    UTC half-hour boundary at or before the fetch; `fetched_at` is when the
    response actually arrived. The 24h-ago fields are None when ApeWisdom has
    no prior reading for the ticker; `upvotes` is None only on imported v1 rows
    that never carried it."""

    model_config = ConfigDict(frozen=True)

    polled_at: UtcDatetime
    ticker: str
    rank: PositiveInt
    mentions: NonNegativeInt
    upvotes: NonNegativeInt | None = None
    rank_24h_ago: NonNegativeInt | None = None
    mentions_24h_ago: NonNegativeInt | None = None
    fetched_at: UtcDatetime
    origin: PopularityOrigin


class StoredDailyBar(BaseModel):
    """One stored daily candle (the row ingest builds from the vendor-edge
    rs_sources.schwab.DailyBar). `session_date` is the exchange session (Eastern)
    date; `fetched_at` stamps when the vendor answered and `adjusted` records
    whether the prices are split/dividend adjusted."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    session_date: date
    open: float
    high: float
    low: float
    close: float
    volume: NonNegativeInt
    fetched_at: UtcDatetime
    adjusted: bool


class UnpricedSymbol(BaseModel):
    """A symbol the price vendor cannot price, kept so it is counted in the base
    rate's survivorship caveat and retried slowly instead of in a hot loop."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    first_seen: UtcDatetime
    last_attempt: UtcDatetime
    reason: str


class BaseRateInput(StrEnum):
    """What a base rate was measured from. Values are the DB column values."""

    RECONSTRUCTED = "reconstructed"
    LOGGED = "logged"


class BaseRate(BaseModel):
    """A signal's measured outcome statistics for one rule version. Hit rate is
    the share of events with a positive excess return versus SPY; both are None
    for a horizon with no priced events. `n_unpriced` counts events dropped
    because the vendor could not price the symbol."""

    model_config = ConfigDict(frozen=True)

    alert_type: str
    rule_version: str
    computed_at: UtcDatetime
    input: BaseRateInput
    n: NonNegativeInt
    n_unpriced: NonNegativeInt = 0
    hit_rate_1d: float | None = None
    hit_rate_5d: float | None = None
    hit_rate_10d: float | None = None
    median_excess_1d: float | None = None
    median_excess_5d: float | None = None
    median_excess_10d: float | None = None
    survivorship_caveat: str


class AuthRequestState(StrEnum):
    """Lifecycle of a Schwab re-auth request. Values are the DB enum values.

    requested -> url_ready (ingest filled the auth URL) -> callback_received (the
    bot stored the pasted callback URL) -> done | failed (ingest exchanged the
    code); any non-terminal state can become expired."""

    REQUESTED = "requested"
    URL_READY = "url_ready"
    CALLBACK_RECEIVED = "callback_received"
    DONE = "done"
    FAILED = "failed"
    EXPIRED = "expired"


class SchwabAuthRequest(BaseModel):
    """One `/setup schwab` request, shared between the bot and ingest."""

    model_config = ConfigDict(frozen=True)

    id: int
    created_at: UtcDatetime
    requested_by: int
    state: AuthRequestState
    auth_url: str | None = None
    callback_url: str | None = None
    error: str | None = None
    completed_at: UtcDatetime | None = None


def median_instant(values: Iterable[datetime]) -> datetime | None:
    """Median as an order statistic — the lower middle for even counts, so the
    result is always an observed instant (no midpoint arithmetic). The stale
    guard's one definition of "median quote_time" (S008): the scan loop applies
    it to fresh responses and the repository to restart-state rows; both sides
    must agree or the guard misfires across a restart."""
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[(len(ordered) - 1) // 2]


class OptionQuote(BaseModel):
    """One contract's row within a ChainSnapshot — the payload columns of
    options_chain_snapshot (S008's binding column list minus the snapshot-level
    provenance). Everything past the contract identity is optional: the wire
    may omit any of it, and a stored NULL is truthful point-in-time capture."""

    model_config = ConfigDict(frozen=True)

    # identity — option_symbol is the OCC symbol, S009's day-over-day OI-join key
    option_symbol: str
    expiry: date
    strike: float
    put_call: Literal["C", "P"]
    # quote
    quote_time: UtcDatetime | None = None
    bid: float | None = None
    ask: float | None = None
    last: float | None = None
    mark: float | None = None
    bid_size: int | None = None
    ask_size: int | None = None
    last_size: int | None = None
    trade_time: UtcDatetime | None = None
    # flow
    volume: NonNegativeInt | None = None
    open_interest: NonNegativeInt | None = None
    # risk
    iv: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    rho: float | None = None
    # chain context
    underlying_price: float | None = None
    multiplier: int | None = None
    expiration_type: str | None = None
    non_standard: bool | None = None


class ChainSnapshot(BaseModel):
    """One underlying's rows within one cycle (CONTEXT.md: Snapshot) — the unit
    a single chain request produces and the grain the stale guard accepts or
    rejects. `rows` empty means tombstone (CONTEXT.md: Tombstone): the fetch
    produced no data (failed/skipped) and persistence writes one payload-less
    membership row, keeping every (cycle, name) pair present (ADR-0030)."""

    model_config = ConfigDict(frozen=True)

    underlying: str
    dataset: Dataset
    cycle_id: UtcDatetime
    fetched_at: UtcDatetime
    source_status: SourceStatus
    rows: tuple[OptionQuote, ...] = ()

    @property
    def median_quote_time(self) -> datetime | None:
        return median_instant(q.quote_time for q in self.rows if q.quote_time is not None)
