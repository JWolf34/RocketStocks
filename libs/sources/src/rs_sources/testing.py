"""Shared test helper: synthetic symbol-directory files in the vendors' wire format, and a
host that serves them through httpx.MockTransport.

The rows are hand-written shapes, never vendor data: pipe-delimited with a header, CRLF
line ends and the `File Creation Time` footer for Nasdaq Trader's files, a quoted CSV with a
padded header for Cboe's. Test-support only: it ships in the package, but only tests import it.
"""

from collections.abc import AsyncIterator, Iterable, Sequence
from datetime import datetime

import httpx

from rs_domain import EASTERN
from rs_sources.symbol_directory import CBOE_OPTIONS_URL, NASDAQ_TRADER_URL

NASDAQ_LISTED_HEADER = (
    "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares"
)
OTHER_LISTED_HEADER = "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol"
OPTIONS_HEADER = (
    "Root Symbol|Options Closing Type|Options Type|Expiration Date|Explicit Strike Price"
    "|Underlying Symbol|Underlying Issue Name|Pending"
)
CBOE_HEADER = " Company Name, Stock Symbol, DPM Name, Post/Station, Global Trading Hours DPM"


def footer(stamp: datetime) -> str:
    """The last line of a Nasdaq Trader file: Eastern wall clock as MMDDYYYYHH:MM, then pipes."""
    return f"File Creation Time: {stamp.astimezone(EASTERN):%m%d%Y%H:%M}||||||"


def nasdaq_row(symbol: str, *, test: bool = False, etf: bool = False, status: str = "N") -> str:
    return f"{symbol}|{symbol} Inc. - Common Stock|Q|{'Y' if test else 'N'}|{status}|100|{'Y' if etf else 'N'}|N"


def other_row(
    symbol: str,
    *,
    exchange: str = "N",
    etf: bool = False,
    test: bool = False,
    cqs: str | None = None,
    nasdaq: str | None = None,
) -> str:
    return (
        f"{symbol}|{symbol} Corp.|{exchange}|{cqs or symbol}|{'Y' if etf else 'N'}|100|"
        f"{'Y' if test else 'N'}|{nasdaq or symbol}"
    )


def listing_file(header: str, rows: Iterable[str], stamp: datetime) -> str:
    return "\r\n".join([header, *rows, footer(stamp)]) + "\r\n"


def options_file(underlyings: Iterable[str], stamp: datetime, *, series: int = 2) -> str:
    rows = [
        f"{symbol}|N|{kind}|01/15/2027|{10 + n}.000|{symbol}|{symbol} INC COM|N"
        for symbol in underlyings
        for n in range(series)
        for kind in "CP"
    ]
    return "\r\n".join([OPTIONS_HEADER, *rows, footer(stamp)]) + "\r\n"


def cboe_file(symbols: Sequence[str]) -> str:
    rows = [f'"{symbol} COMPANY","{symbol}","Example Market Maker LLC","8/1","-"' for symbol in symbols]
    return "\n".join([CBOE_HEADER, *rows]) + "\n"


class FakeDirectoryHost:
    """Serves the four files by name ("nasdaqlisted.txt", "otherlisted.txt", "options.txt",
    "cboe"). Set `bodies[name]`, or `statuses[name]` to answer with that error status; every
    request is appended to `requests`. Bodies stream in `chunk_size` pieces."""

    def __init__(self, *, chunk_size: int = 8192) -> None:
        self.bodies: dict[str, str] = {}
        self.statuses: dict[str, int] = {}
        self.requests: list[str] = []
        self._chunk_size = chunk_size

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self._handle))

    def _handle(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == CBOE_OPTIONS_URL:
            name = "cboe"
        elif url.startswith(NASDAQ_TRADER_URL):
            name = url.removeprefix(NASDAQ_TRADER_URL)
        else:
            return httpx.Response(404)
        self.requests.append(name)
        if name in self.statuses:
            return httpx.Response(self.statuses[name])
        if name not in self.bodies:
            return httpx.Response(404)
        return httpx.Response(200, content=self._stream(self.bodies[name].encode()))

    async def _stream(self, body: bytes) -> AsyncIterator[bytes]:
        for start in range(0, len(body), self._chunk_size):
            yield body[start : start + self._chunk_size]
