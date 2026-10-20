"""SEC EDGAR adapter: ticker-to-CIK map and recent 8-K filings.

EDGAR needs no key but requires a declared User-Agent and allows 10 requests
per second; the client spaces calls at 8/s to stay clear of the limit. Only
the submissions `recent` block is read (about a year of filings at least),
which covers every use in the facts block and the earnings-cadence estimate.
"""

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from rs_domain import Clock, UtcDatetime
from rs_sources._http import get_json
from rs_sources._ratelimit import MinInterval
from rs_sources.errors import SourceError

logger = logging.getLogger(__name__)

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}"

EIGHT_K_FORMS = frozenset({"8-K", "8-K/A"})
RESULTS_OF_OPERATIONS_ITEM = "2.02"


class Filing8K(BaseModel):
    """One 8-K (or 8-K/A) as EDGAR accepted it."""

    model_config = ConfigDict(frozen=True)

    accession_number: str
    filing_date: date
    accepted_at: UtcDatetime
    form: str
    items: tuple[str, ...]
    url: str


class EdgarClient:
    """EDGAR reads for one declared identity.

    `http` is owned by the caller (timeouts and lifecycle). The ticker map is
    cached in memory and reloaded once older than `tickers_ttl`; if a reload
    fails, the stale map keeps serving.
    """

    def __init__(
        self,
        http: httpx.AsyncClient,
        user_agent: str,
        clock: Clock,
        *,
        tickers_ttl: timedelta = timedelta(hours=24),
        min_interval: float = 0.125,
    ) -> None:
        if not user_agent.strip():
            raise ValueError("EDGAR requires a declared User-Agent (RS_EDGAR_USER_AGENT)")
        self._http = http
        self._headers = {"User-Agent": user_agent.strip(), "Accept-Encoding": "gzip, deflate"}
        self._clock = clock
        self._tickers_ttl = tickers_ttl
        self._limiter = MinInterval(min_interval)
        self._ciks: dict[str, int] | None = None
        self._ciks_loaded_at: datetime | None = None
        self._ciks_lock = asyncio.Lock()

    async def cik(self, ticker: str) -> int | None:
        """The ticker's CIK, or None when EDGAR does not list it."""
        ciks = await self._ticker_map()
        symbol = ticker.strip().upper()
        # EDGAR writes class shares with a hyphen (BRK-B); other sources use a dot.
        return ciks.get(symbol) or ciks.get(symbol.replace(".", "-"))

    async def recent_8ks(
        self, ticker: str, as_of: datetime, limit: int | None = 5
    ) -> list[Filing8K]:
        """8-K and 8-K/A filings accepted at or before `as_of`, newest first.

        An unlisted ticker yields an empty list; a failed call raises SourceError.
        `limit=None` returns every 8-K in the submissions `recent` block.
        """
        cik = await self.cik(ticker)
        if cik is None:
            return []
        await self._limiter.wait()
        payload = await get_json(self._http, SUBMISSIONS_URL.format(cik=cik), headers=self._headers)
        filings = [f for f in _parse_8ks(payload, cik) if f.accepted_at <= as_of]
        filings.sort(key=lambda f: f.accepted_at, reverse=True)
        return filings if limit is None else filings[:limit]

    async def _ticker_map(self) -> dict[str, int]:
        async with self._ciks_lock:
            now = self._clock.now()
            if (
                self._ciks is not None
                and self._ciks_loaded_at is not None
                and now - self._ciks_loaded_at < self._tickers_ttl
            ):
                return self._ciks
            try:
                await self._limiter.wait()
                payload = await get_json(self._http, TICKERS_URL, headers=self._headers)
                ciks = _parse_ticker_map(payload)
            except SourceError:
                if self._ciks is None:
                    raise
                logger.warning("EDGAR ticker map refresh failed; serving the stale map", exc_info=True)
                return self._ciks
            self._ciks, self._ciks_loaded_at = ciks, now
            return ciks


def _parse_ticker_map(payload: Any) -> dict[str, int]:
    if not isinstance(payload, dict):
        raise SourceError("EDGAR company_tickers.json is not an object")
    ciks: dict[str, int] = {}
    for entry in payload.values():
        try:
            ciks.setdefault(str(entry["ticker"]).upper(), int(entry["cik_str"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise SourceError(f"EDGAR company_tickers.json entry malformed: {entry!r}") from exc
    return ciks


def _parse_8ks(payload: Any, cik: int) -> list[Filing8K]:
    """Rows of the submissions `recent` block, which EDGAR serves column-wise."""
    try:
        recent = payload["filings"]["recent"]
        columns = {
            name: recent[name]
            for name in ("accessionNumber", "filingDate", "acceptanceDateTime", "form", "items", "primaryDocument")
        }
        count = len(columns["form"])
        if any(len(column) != count for column in columns.values()):
            raise SourceError("EDGAR submissions columns differ in length")
    except (KeyError, TypeError) as exc:
        raise SourceError("EDGAR submissions payload has an unexpected shape") from exc

    filings = []
    for i in range(count):
        if columns["form"][i] not in EIGHT_K_FORMS:
            continue
        accession = columns["accessionNumber"][i]
        try:
            accepted_at = datetime.fromisoformat(columns["acceptanceDateTime"][i])
            filing = Filing8K(
                accession_number=accession,
                filing_date=date.fromisoformat(columns["filingDate"][i]),
                accepted_at=accepted_at,
                form=columns["form"][i],
                items=tuple(item for item in columns["items"][i].split(",") if item),
                url=_filing_url(cik, accession, columns["primaryDocument"][i]),
            )
        except (ValueError, TypeError, AttributeError):
            logger.warning("skipping malformed EDGAR 8-K row %s", accession, exc_info=True)
            continue
        filings.append(filing)
    return filings


def _filing_url(cik: int, accession: str, primary_document: str) -> str:
    compact = accession.replace("-", "")
    # Some filings carry no primary document; the filing index is the stable link.
    document = primary_document or f"{accession}-index.htm"
    return ARCHIVE_URL.format(cik=cik, accession=compact, document=document)
