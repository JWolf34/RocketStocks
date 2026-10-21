"""Domain models — the validated shapes the rest of v2 consumes.

Prices are float64: the wire delivers JSON floats, the analysis math runs in
float64, and DOUBLE PRECISION round-trips them bit-exactly. NUMERIC/Decimal is
reserved for future accounting-grade tables, never this path.
"""

from datetime import date, timedelta
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, NonNegativeInt, PositiveInt

from rs_domain._types import UtcDatetime


class Alert(BaseModel):
    """A decision to announce, produced by a Trigger. The versioned-rule fields are
    optional. `reply_to` is the id of the Alert a reply continues; it is None for
    an Alert, and the database holds one Alert per stock per Eastern day."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    trigger: str
    triggered_at: UtcDatetime
    payload: dict = {}
    rule_version: str | None = None
    alert_price: float | None = None
    quote_time: UtcDatetime | None = None
    reply_to: int | None = None


class Verdict(StrEnum):
    """What the selector decided about a candidate. Values are the DB enum values."""

    POSTED = "posted"
    REPLY = "reply"
    BELOW_BAR = "below_bar"
    OVER_BUDGET = "over_budget"
    REPEAT = "repeat"
    STALE = "stale"


# After an outage, an Alert or reply older than this (from its hit) is recorded as stale and
# never posted. The bot enforces it; the Outcomes job reads it to know a row can no longer post.
ALERT_STALE_AFTER = timedelta(hours=1)


class Delivery(StrEnum):
    """Where an Alert or reply is in being posted. `sending` means the bot marked it
    before a send, so a row still `sending` after a crash may or may not have posted.
    `stale` means it was too old to post and was recorded instead. Values are the DB
    enum values."""

    PENDING = "pending"
    SENDING = "sending"
    POSTED = "posted"
    STALE = "stale"


class Candidate(BaseModel):
    """One Trigger hit and what the selector made of it. `hit_at` is the Popularity
    boundary the hit was judged on; `inputs` is what the Trigger decided on,
    including the sub-rules that fired. `alert_id` is the Alert (or reply) the
    candidate produced, None when it produced none."""

    model_config = ConfigDict(frozen=True)

    trigger: str
    rule_version: str
    symbol: str
    hit_at: UtcDatetime
    inputs: dict
    verdict: Verdict
    decided_at: UtcDatetime
    alert_id: int | None = None


class Dataset(StrEnum):
    """Which scan produced a snapshot. Values are the DB enum
    values and double as the `source` metric-label key."""

    CURATED_5M = "curated_5m"
    EOD_FULL = "eod_full"


class SourceStatus(StrEnum):
    """Row-level fetch outcome (`stale` for the quote_time-regression guard,
    `skipped` for overrun tombstones)."""

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
    no prior reading for the ticker (never 0); `upvotes` is a net score, so it can
    be negative, and is None only on imported v1 rows that never carried it."""

    model_config = ConfigDict(frozen=True)

    polled_at: UtcDatetime
    ticker: str
    rank: PositiveInt
    mentions: NonNegativeInt
    upvotes: int | None = None
    rank_24h_ago: PositiveInt | None = None
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
    """A symbol the price vendor cannot price, kept so its cases are counted as
    unpriced and it is retried slowly instead of in a hot loop."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    first_seen: UtcDatetime
    last_attempt: UtcDatetime
    reason: str


class Outcome(BaseModel):
    """What one case did at one horizon. A case is the first candidate of a Trigger
    version on a stock on an Eastern day; `case_at` is its post (or its hit when it
    was not posted) and `live` says the live poll stored a candidate for it, as
    against a hit replayed from history. Entry is the open of the `entry_session`,
    the exit the close of the `exit_session`; returns are fractions (0.01 is one
    percentage point) and excess is the stock's less SPY's. `move_multiple` is the
    absolute excess over the stock's typical move and is None, with the typical
    move, when its history was too short. `ordinary_share` is the share of the
    stock's own windows that year that moved 2x typical or more."""

    model_config = ConfigDict(frozen=True)

    trigger: str
    rule_version: str
    symbol: str
    day: date
    horizon: PositiveInt
    case_at: UtcDatetime
    hit_at: UtcDatetime
    live: bool
    entry_session: date
    exit_session: date
    stock_return: float
    benchmark_return: float
    excess_return: float
    typical_move: float | None = None
    move_multiple: float | None = None
    ordinary_share: float | None = None


# A Track record says "too few cases" below this many Outcomes at a horizon.
TOO_FEW_CASES_BELOW = 30


class TrackRecord(BaseModel):
    """A Trigger version's Outcomes summarised at one horizon. `cases` and `live_cases`
    count every case, matured or not; the rest count this horizon: `outcomes` the
    measured ones, `unpriced` the matured ones with a missing bar, `split_excluded`
    the matured ones left out because unadjusted bars held what looks like a split,
    `no_move_size` the measured ones whose history was too short. The shares and
    medians are None when nothing backs them: `share_at_2x`, `ordinary_share` and
    `median_multiple` over the Outcomes with a move size, `median_excess` over all."""

    model_config = ConfigDict(frozen=True)

    trigger: str
    rule_version: str
    horizon: PositiveInt
    computed_at: UtcDatetime
    cases: NonNegativeInt
    live_cases: NonNegativeInt
    outcomes: NonNegativeInt
    unpriced: NonNegativeInt = 0
    split_excluded: NonNegativeInt = 0
    no_move_size: NonNegativeInt = 0
    share_at_2x: float | None = None
    ordinary_share: float | None = None
    median_multiple: float | None = None
    median_excess: float | None = None

    @property
    def too_few_cases(self) -> bool:
        return self.outcomes < TOO_FEW_CASES_BELOW


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


class OptionQuote(BaseModel):
    """One contract's row within a ChainSnapshot — the payload columns of
    options_chain_snapshot (the stored column list minus the snapshot-level
    provenance). Everything past the contract identity is optional: the wire
    may omit any of it, and a stored NULL is truthful point-in-time capture."""

    model_config = ConfigDict(frozen=True)

    # identity - option_symbol is the OCC symbol, the day-over-day open-interest join key
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
    """One underlying's rows within one cycle — the unit
    a single chain request produces and the grain the stale guard accepts or
    rejects. `rows` empty means tombstone: the fetch
    produced no data (failed/skipped) and persistence writes one payload-less
    membership row, keeping every (cycle, name) pair present."""

    model_config = ConfigDict(frozen=True)

    underlying: str
    dataset: Dataset
    cycle_id: UtcDatetime
    fetched_at: UtcDatetime
    source_status: SourceStatus
    rows: tuple[OptionQuote, ...] = ()
