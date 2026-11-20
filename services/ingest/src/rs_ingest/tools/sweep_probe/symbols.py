"""The probe's sample, drawn from the equities and funds of Cboe's equity options directory.

The directory is read through `rs_sources.symbol_directory`, as ingest reads it; it keeps class
shares' dot (`BRK.B`) and lists index products beside equities and ETFs.
"""

import random
import re

from rs_sources.symbol_directory import CboeEntry, parse_cboe_directory

# Index roots, including those whose directory names do not say "index".
_INDEX_ROOTS = frozenset(
    {"SPX", "SPXW", "XSP", "XSPW", "XSPBX", "VIX", "VIXW", "DJX", "OEX", "XEO", "NDX", "NDXP", "XND"}
    | {"RUT", "RUTW", "MRUT"}
)
_INDEX_WORD = re.compile(r"\bindex\b", re.IGNORECASE)
# Funds that track an index carry "index" in their name too; these words mark them as not indexes.
_FUND_WORDS = re.compile(r"\b(ETF|ETN|TR|TRUST|FD|FDS|FUND|SHS|ADR|ADS|INC|CORP)\b", re.IGNORECASE)


def is_index_product(symbol: str, name: str) -> bool:
    return symbol in _INDEX_ROOTS or (bool(_INDEX_WORD.search(name)) and not _FUND_WORDS.search(name))


def equity_listings(entries: tuple[CboeEntry, ...]) -> list[CboeEntry]:
    """The equity and ETF listings of the directory, index products excluded, one per symbol,
    sorted by symbol."""
    listings: dict[str, CboeEntry] = {}
    for entry in entries:
        symbol = entry.symbol.upper()
        if not is_index_product(symbol, entry.name):
            listings.setdefault(symbol, CboeEntry(symbol=symbol, name=entry.name))
    return sorted(listings.values(), key=lambda listing: listing.symbol)


def parse_directory(text: str) -> list[CboeEntry]:
    """`equity_listings` of a saved directory CSV."""
    return equity_listings(parse_cboe_directory(text.splitlines()))


def chain_spellings(listing: str) -> tuple[str, ...]:
    """The spellings to ask Schwab for a listing's chain, in order: a class share's slash spelling
    (`BRK.B` -> `BRK/B`), then the listing's own."""
    slash = listing.replace(".", "/")
    return (slash, listing) if slash != listing else (listing,)


def draw_sample(listings: list[CboeEntry], seed: int, size: int, exclude: frozenset[str]) -> list[str]:
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
