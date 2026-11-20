"""The Universe as stored: every non-test listing of one session date, with the snapshot's
header (its As-of time, the source counts, whether the optionable names were carried over).
"""

from datetime import date

from pydantic import BaseModel, ConfigDict, NonNegativeInt

from rs_domain._types import UtcDatetime


class UniverseListing(BaseModel):
    """One listing of a Universe snapshot. `symbol` is the listing file's own (class shares
    with a dot, preferreds with a `$`); `cqs_symbol` and `nasdaq_symbol` are the other
    exchanges' spellings where the file gave them. `financial_status` is Nasdaq's code
    and None for a listing that file does not cover."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    security_name: str
    exchange: str
    etf: bool
    financial_status: str | None
    cqs_symbol: str | None
    nasdaq_symbol: str | None
    optionable: bool


class UniverseCounts(BaseModel):
    """How many entries each source file held in a snapshot's night: the non-test rows
    of the two listing files and the symbols of the two optionable sources (None for a
    source that was unusable that night). The next night's truncation guard reads them."""

    model_config = ConfigDict(frozen=True)

    nasdaq_listed: NonNegativeInt
    other_listed: NonNegativeInt
    options_symbols: NonNegativeInt | None
    cboe_symbols: NonNegativeInt | None


class UniverseHeader(BaseModel):
    """What a Universe snapshot says about itself: the session date it describes, its
    `as_of` (the earlier of the two listing files' File Creation Times), the source counts,
    and whether an optionable source was unusable that night so its names were carried
    over from the snapshot before."""

    model_config = ConfigDict(frozen=True)

    snapshot_date: date
    as_of: UtcDatetime
    counts: UniverseCounts
    optionable_carried: bool


class UniverseSnapshot(UniverseHeader):
    """Every listing RocketStocks tracked on one session date."""

    listings: tuple[UniverseListing, ...]
