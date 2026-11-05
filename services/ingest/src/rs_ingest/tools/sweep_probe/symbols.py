"""A minimal reader of the Cboe equity options symbol directory, only to draw the probe's sample.

The directory's header cells carry leading spaces, its fields are quoted, class shares
keep their dot (`BRK.B`) and it lists index products beside equities and ETFs.
"""

import csv
import io
import random
import re
from dataclasses import dataclass

import httpx

CBOE_DIRECTORY_URL = "https://www.cboe.com/us/options/symboldir/equity-index-options/download/"

# Index roots, including those whose directory names do not say "index".
_INDEX_ROOTS = frozenset(
    {"SPX", "SPXW", "XSP", "XSPW", "XSPBX", "VIX", "VIXW", "DJX", "OEX", "XEO", "NDX", "NDXP", "XND"}
    | {"RUT", "RUTW", "MRUT"}
)
_INDEX_WORD = re.compile(r"\bindex\b", re.IGNORECASE)
# Funds that track an index carry "index" in their name too; these words mark them as not indexes.
_FUND_WORDS = re.compile(r"\b(ETF|ETN|TR|TRUST|FD|FDS|FUND|SHS|ADR|ADS|INC|CORP)\b", re.IGNORECASE)


@dataclass(frozen=True)
class Listing:
    symbol: str
    name: str


def is_index_product(symbol: str, name: str) -> bool:
    return symbol in _INDEX_ROOTS or (bool(_INDEX_WORD.search(name)) and not _FUND_WORDS.search(name))


def parse_directory(text: str) -> list[Listing]:
    """The equity and ETF listings of the directory CSV, index products excluded, sorted by symbol."""
    reader = csv.reader(io.StringIO(text), skipinitialspace=True)
    header = [cell.strip().lower() for cell in next(reader, [])]
    try:
        symbol_at, name_at = header.index("stock symbol"), header.index("company name")
    except ValueError:
        raise ValueError(f"unexpected Cboe directory header: {header}") from None
    listings: dict[str, Listing] = {}
    for row in reader:
        if len(row) <= max(symbol_at, name_at):
            continue
        symbol, name = row[symbol_at].strip().upper(), row[name_at].strip()
        if symbol and not is_index_product(symbol, name):
            listings.setdefault(symbol, Listing(symbol, name))
    return sorted(listings.values(), key=lambda listing: listing.symbol)


async def download_directory(http: httpx.AsyncClient, user_agent: str) -> str:
    response = await http.get(CBOE_DIRECTORY_URL, headers={"User-Agent": user_agent})
    response.raise_for_status()
    return response.text


def chain_spellings(listing: str) -> tuple[str, ...]:
    """The spellings to ask Schwab for a listing's chain, in order: a class share's slash spelling
    (`BRK.B` -> `BRK/B`), then the listing's own."""
    slash = listing.replace(".", "/")
    return (slash, listing) if slash != listing else (listing,)


def draw_sample(listings: list[Listing], seed: int, size: int, exclude: frozenset[str]) -> list[str]:
    """`size` symbols drawn with `seed`, in draw order; a symbol whose Schwab spelling is in
    `exclude` is not eligible."""
    eligible = [
        listing.symbol
        for listing in listings
        if not exclude.intersection(chain_spellings(listing.symbol))
    ]
    if size > len(eligible):
        raise ValueError(f"sample of {size} asked from {len(eligible)} eligible listings")
    return random.Random(seed).sample(eligible, size)
