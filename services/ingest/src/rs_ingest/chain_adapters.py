"""Production adapters for ChainScanner's ports (S008 daemon wiring).

SchwabChainFetch shares the daemon's ONE schwab-py client (the token row's
single-writer constraint, ADR-0024) — never build a second client for the
scanner. SessionSnapshotStore owns transaction scope: one short commit per
snapshot, so each snapshot is durable the moment its train step completes and
a later failure in the same cycle cannot roll it back.
"""

import logging
from datetime import timedelta

from rs_domain import ChainSnapshot, Clock, Dataset
from rs_persistence import ChainSnapshotRepository

logger = logging.getLogger(__name__)

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

    def __init__(self, client, clock: Clock) -> None:
        self._client = client
        self._clock = clock

    async def __call__(self, underlying: str) -> dict:
        today = self._clock.now().date()
        response = await self._client.get_option_chain(
            underlying, from_date=today, to_date=today + DTE_BOUND
        )
        if response.status_code != 200:
            raise ChainFetchError(f"{underlying}: HTTP {response.status_code}")
        return response.json()


class SessionSnapshotStore:
    """SnapshotStore port over rs_persistence: session-per-call, commit on
    success (the repository's caller-owns-commit contract)."""

    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory

    async def insert(self, snapshot: ChainSnapshot) -> int:
        async with self._session_factory() as session:
            async with session.begin():
                return await ChainSnapshotRepository(session).insert(snapshot)

    async def last_accepted_median_quote_times(self, dataset: Dataset, now):
        async with self._session_factory() as session:
            return await ChainSnapshotRepository(session).last_accepted_median_quote_times(
                dataset, now
            )
