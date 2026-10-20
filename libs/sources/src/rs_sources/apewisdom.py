"""ApeWisdom all-stocks popularity adapter.

Fetches every page of the all-stocks filter and validates rows at the edge.
The adapter never reads the clock: the caller stamps polled_at / fetched_at
when it turns rows into snapshots.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

logger = logging.getLogger(__name__)

_PAGE_URL = "https://apewisdom.io/api/v1.0/filter/all-stocks/page/{page}"
# The live listing is about 10 pages; far more means the envelope is garbage.
_MAX_PAGES = 100
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class ApeWisdomError(Exception):
    """The fetch failed as a whole: no usable snapshot can be built."""


class ApeWisdomRow(BaseModel):
    """One validated ticker row; `name` is dropped, the 24h-ago fields may be null."""

    model_config = ConfigDict(frozen=True)

    rank: int = Field(gt=0)
    ticker: str = Field(min_length=1)
    mentions: int = Field(ge=0)
    upvotes: int = Field(ge=0)
    rank_24h_ago: int | None = Field(default=None, gt=0)
    mentions_24h_ago: int | None = Field(default=None, ge=0)

    @field_validator("ticker", mode="before")
    @classmethod
    def _normalise_ticker(cls, value: Any) -> Any:
        return value.strip().upper() if isinstance(value, str) else value


class ApeWisdomClient:
    """Paginated ApeWisdom fetch with bounded retry.

    The caller owns the httpx client's lifecycle; each request carries this
    client's timeout. A page that still fails after `max_attempts` fails the
    whole fetch - a partial listing would silently skew ranks.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        timeout: httpx.Timeout | float = 10.0,
        max_attempts: int = 3,
        backoff_seconds: float = 1.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._client = client
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._backoff = backoff_seconds
        self._sleep = sleep

    async def fetch_all(self) -> list[ApeWisdomRow]:
        first = await self._get_page(1)
        pages = _page_count(first)
        envelopes = [first]
        for page in range(2, pages + 1):
            envelopes.append(await self._get_page(page))

        rows: dict[str, ApeWisdomRow] = {}
        rejected = 0
        for envelope in envelopes:
            for raw in envelope["results"]:
                try:
                    row = ApeWisdomRow.model_validate(raw)
                except ValidationError as exc:
                    rejected += 1
                    logger.warning("rejected ApeWisdom row %r: %s", raw, exc.errors(include_url=False))
                    continue
                # Rankings shift between page requests, so a ticker can repeat.
                rows.setdefault(row.ticker, row)
        if rejected:
            logger.warning("ApeWisdom: kept %d rows, rejected %d", len(rows), rejected)
        return list(rows.values())

    async def _get_page(self, page: int) -> dict[str, Any]:
        failure: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            if attempt > 1:
                await self._sleep(self._backoff * 2 ** (attempt - 2))
            try:
                response = await self._client.get(_PAGE_URL.format(page=page), timeout=self._timeout)
            except httpx.RequestError as exc:
                failure = exc
                continue
            if response.status_code in _RETRYABLE_STATUS:
                failure = ApeWisdomError(f"HTTP {response.status_code}")
                continue
            if response.is_error:
                raise ApeWisdomError(f"page {page}: HTTP {response.status_code}")
            try:
                return _check_envelope(response.json())
            except ValueError as exc:
                failure = exc
        raise ApeWisdomError(f"page {page} failed after {self._max_attempts} attempts: {failure}") from failure


def _check_envelope(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict) or not isinstance(body.get("results"), list):
        raise ValueError("response has no results list")
    return body


def _page_count(first_page: dict[str, Any]) -> int:
    pages = first_page.get("pages")
    if not isinstance(pages, int) or isinstance(pages, bool) or not 1 <= pages <= _MAX_PAGES:
        raise ApeWisdomError(f"implausible page count {pages!r}")
    return pages
