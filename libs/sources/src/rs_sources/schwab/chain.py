"""Option-chain fetch over schwab-py (ChainScanner's ChainFetch port).

SchwabChainFetch shares the daemon's ONE schwab-py client (the token row's
single-writer constraint, ADR-0024) - never build a second client for the
scanner.
"""

from datetime import timedelta
from typing import Any

from rs_domain import Clock
from rs_sources.schwab.client import translate_auth_errors

# The server-side expiry bound (ADR-0032 D7): <=30 DTE, the bound that avoids
# the measured unfiltered-SPY/QQQ 502s. The client-side window/cap live in
# rs_ingest.chains.
DTE_BOUND = timedelta(days=30)


class ChainFetchError(RuntimeError):
    """Non-OK chain response (the scanner retries once, then tombstones)."""


class SchwabChainFetch:
    """ChainFetch port over schwab-py's async client: one expiry-bounded
    get_option_chain call (kwargs verified against schwab-py 1.5.1). The UTC
    date is the ET date whenever a scan can run (RTH), so `today` is safe as
    the fromDate."""

    def __init__(self, client: Any, clock: Clock) -> None:
        self._client = client
        self._clock = clock

    async def __call__(self, underlying: str) -> dict:
        today = self._clock.now().date()
        with translate_auth_errors():
            response = await self._client.get_option_chain(
                underlying, from_date=today, to_date=today + DTE_BOUND
            )
        if response.status_code != 200:
            raise ChainFetchError(f"{underlying}: HTTP {response.status_code}")
        return response.json()
