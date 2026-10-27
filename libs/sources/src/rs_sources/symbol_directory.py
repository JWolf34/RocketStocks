"""Symbol directory adapters: Nasdaq Trader's listing and options files, and Cboe's options directory.

`nasdaqlisted.txt` and `otherlisted.txt` list every security with its flags;
`options.txt` has one row per listed options series, of which only the underlying
symbols matter here. All three are pipe-delimited with a header row, CRLF line ends
and a last line `File Creation Time: MMDDYYYYHH:MM|||...` in Eastern time, which is
the files' As-of. Cboe's equity options directory is a CSV with a padded header, quoted
fields and no As-of; it lags a business day.

`options.txt` is about 90 MB, so every file is read as a stream, line by line, and only
what is kept (the listings, the set of underlyings) is held in memory. Parsing yields to
the event loop between chunks, so the daemon's heartbeat is never starved.

Any deviation (a failed request, a changed header, no footer, which is also what a cut
download looks like) surfaces as SourceError, so a caller never reads a partial file as a
whole one.
"""

import asyncio
import csv
import logging
import re
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import aclosing
from datetime import UTC, datetime
from typing import Final

import httpx
from pydantic import BaseModel, ConfigDict

from rs_domain import EASTERN, UtcDatetime
from rs_sources.errors import SourceError

logger = logging.getLogger(__name__)

NASDAQ_TRADER_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/"
# The direct download; the page's `?download=csv` link redirects twice before reaching it.
CBOE_OPTIONS_URL = "https://www.cboe.com/us/options/symboldir/equity-index-options/download/"
CBOE_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
)
# nasdaqlisted.txt carries no exchange column; every row is a Nasdaq listing (the tape's code for Nasdaq).
NASDAQ_EXCHANGE: Final = "Q"

_NASDAQ_LISTED_HEADER = (
    "Symbol",
    "Security Name",
    "Market Category",
    "Test Issue",
    "Financial Status",
    "Round Lot Size",
    "ETF",
    "NextShares",
)
_OTHER_LISTED_HEADER = (
    "ACT Symbol",
    "Security Name",
    "Exchange",
    "CQS Symbol",
    "ETF",
    "Round Lot Size",
    "Test Issue",
    "NASDAQ Symbol",
)
_OPTIONS_HEADER = (
    "Root Symbol",
    "Options Closing Type",
    "Options Type",
    "Expiration Date",
    "Explicit Strike Price",
    "Underlying Symbol",
    "Underlying Issue Name",
    "Pending",
)
_UNDERLYING_COLUMN = _OPTIONS_HEADER.index("Underlying Symbol")
_CBOE_SYMBOL_COLUMN = "Stock Symbol"

_FOOTER = re.compile(r"File Creation Time:\s*(\d{8})(\d{2}:\d{2})")


class Listing(BaseModel):
    """One non-test security of a listing file, as the file gives it.

    `symbol` is the file's own (nasdaqlisted's Symbol, otherlisted's ACT Symbol).
    `cqs_symbol` and `nasdaq_symbol` are otherlisted.txt's other two symbologies,
    kept as given (the ACT preferred `ABR$D` is `ABRpD` on the consolidated tape and
    `ABR-D` at Nasdaq; the class share `BRK.B` is `BRK-B` at Nasdaq); both are None
    for a Nasdaq listing.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    security_name: str
    exchange: str
    etf: bool
    # Nasdaq's code (N normal, D deficient, E delinquent, ...); None for otherlisted.txt, which has none.
    financial_status: str | None
    cqs_symbol: str | None
    nasdaq_symbol: str | None


class ListingFile(BaseModel):
    model_config = ConfigDict(frozen=True)

    created_at: UtcDatetime
    listings: tuple[Listing, ...]


class OptionsFile(BaseModel):
    model_config = ConfigDict(frozen=True)

    created_at: UtcDatetime
    underlyings: frozenset[str]


class SymbolDirectory:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        nasdaq_trader_url: str = NASDAQ_TRADER_URL,
        cboe_options_url: str = CBOE_OPTIONS_URL,
    ) -> None:
        self._http = http
        self._nasdaq_trader_url = nasdaq_trader_url
        self._cboe_options_url = cboe_options_url

    async def nasdaq_listed(self) -> ListingFile:
        """Every non-test Nasdaq listing."""
        created_at: datetime | None = None
        listings: list[Listing] = []
        skipped = 0
        url = self._nasdaq_trader_url + "nasdaqlisted.txt"
        async for fields, footer in _rows(self._http, url, _NASDAQ_LISTED_HEADER):
            if footer is not None:
                created_at = footer
            elif fields is None or not fields[0].strip():
                skipped += 1
            elif fields[3] != "Y":
                listings.append(
                    Listing(
                        symbol=fields[0].strip(),
                        security_name=fields[1].strip(),
                        exchange=NASDAQ_EXCHANGE,
                        etf=fields[6] == "Y",
                        financial_status=fields[4].strip() or None,
                        cqs_symbol=None,
                        nasdaq_symbol=None,
                    )
                )
        return _listing_file("nasdaqlisted.txt", created_at, listings, skipped)

    async def other_listed(self) -> ListingFile:
        """Every non-test listing of the other exchanges; the exchange code is kept as given."""
        created_at: datetime | None = None
        listings: list[Listing] = []
        skipped = 0
        url = self._nasdaq_trader_url + "otherlisted.txt"
        async for fields, footer in _rows(self._http, url, _OTHER_LISTED_HEADER):
            if footer is not None:
                created_at = footer
            elif fields is None or not fields[0].strip():
                skipped += 1
            elif fields[6] != "Y":
                listings.append(
                    Listing(
                        symbol=fields[0].strip(),
                        security_name=fields[1].strip(),
                        exchange=fields[2].strip(),
                        etf=fields[4] == "Y",
                        financial_status=None,
                        cqs_symbol=fields[3].strip() or None,
                        nasdaq_symbol=fields[7].strip() or None,
                    )
                )
        return _listing_file("otherlisted.txt", created_at, listings, skipped)

    async def option_underlyings(self) -> OptionsFile:
        """The set of underlying symbols with a listed series; the 90 MB body is never held."""
        created_at: datetime | None = None
        underlyings: set[str] = set()
        url = self._nasdaq_trader_url + "options.txt"
        async for fields, footer in _rows(self._http, url, _OPTIONS_HEADER):
            if footer is not None:
                created_at = footer
            elif fields is not None:
                underlyings.add(fields[_UNDERLYING_COLUMN].strip())
        if created_at is None:
            raise SourceError("options.txt has no File Creation Time footer")
        underlyings.discard("")
        if not underlyings:
            raise SourceError("options.txt has no rows")
        return OptionsFile(created_at=created_at, underlyings=frozenset(underlyings))

    async def cboe_underlyings(self) -> frozenset[str]:
        """The symbols of Cboe's equity and index options directory (class shares with a dot)."""
        headers = {"User-Agent": CBOE_USER_AGENT}
        lines = [
            line
            async for batch in _line_batches(self._http, self._cboe_options_url, headers)
            for line in batch
            if line.strip()
        ]
        # Header cells are padded with a leading space and fields are quoted.
        rows = csv.reader(lines, skipinitialspace=True)
        header = [cell.strip() for cell in next(rows, [])]
        if _CBOE_SYMBOL_COLUMN not in header:
            raise SourceError(f"Cboe options directory has no {_CBOE_SYMBOL_COLUMN!r} column")
        column = header.index(_CBOE_SYMBOL_COLUMN)
        symbols = frozenset(row[column].strip() for row in rows if len(row) > column and row[column].strip())
        if not symbols:
            raise SourceError("Cboe options directory has no rows")
        return symbols


def _listing_file(name: str, created_at: datetime | None, listings: list[Listing], skipped: int) -> ListingFile:
    if created_at is None:
        raise SourceError(f"{name} has no File Creation Time footer")
    if not listings:
        raise SourceError(f"{name} has no listings")
    if skipped:
        logger.warning("%s: skipped %d malformed row(s)", name, skipped)
    return ListingFile(created_at=created_at, listings=tuple(listings))


async def _rows(
    http: httpx.AsyncClient, url: str, header: tuple[str, ...]
) -> AsyncIterator[tuple[list[str] | None, datetime | None]]:
    """Each line of a pipe-delimited file as (fields, None); the footer as (None, its instant);
    a row of the wrong width as (None, None). Raises SourceError when the header is not `header`."""
    first = True
    async with aclosing(_line_batches(http, url)) as batches:
        async for batch in batches:
            for line in batch:
                if first:
                    first = False
                    if tuple(cell.strip() for cell in line.split("|")) != header:
                        raise SourceError(f"{url} has an unexpected header")
                    continue
                if line.startswith("File Creation Time:"):
                    yield None, _parse_footer(line, url)
                    continue
                if not line:
                    continue
                fields = line.split("|")
                yield (fields if len(fields) == len(header) else None), None


def _parse_footer(line: str, url: str) -> datetime:
    match = _FOOTER.match(line)
    if match is None:
        raise SourceError(f"{url} has an unreadable File Creation Time footer")
    stamp = match.group(1) + match.group(2)  # MMDDYYYYHH:MM
    try:
        return datetime.strptime(stamp, "%m%d%Y%H:%M").replace(tzinfo=EASTERN).astimezone(UTC)
    except ValueError as exc:
        raise SourceError(f"{url} has an unreadable File Creation Time footer") from exc


async def _line_batches(
    http: httpx.AsyncClient, url: str, headers: dict[str, str] | None = None
) -> AsyncGenerator[list[str]]:
    """The body's lines, a network chunk's worth at a time, without their line ends.

    One chunk is under a millisecond of work; the zero-length sleep after each hands the
    loop back even when the whole body is already buffered.
    """
    pending = ""
    try:
        async with http.stream("GET", url, headers=headers) as response:
            response.raise_for_status()
            async for chunk in response.aiter_text():
                *lines, pending = (pending + chunk).split("\n")
                yield [line.rstrip("\r") for line in lines]
                await asyncio.sleep(0)
    except httpx.HTTPStatusError as exc:
        raise SourceError(f"GET {url} failed: {exc!r}", status_code=exc.response.status_code) from exc
    except httpx.HTTPError as exc:
        raise SourceError(f"GET {url} failed: {exc!r}") from exc
    if pending:
        yield [pending.rstrip("\r")]
