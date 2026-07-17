"""ChainScanner (S008): the curated 5-min REST chain scan train.

One instance runs one RTH session (the run_session lifecycle), as a second
asyncio task beside the stream. Cycles launch at wall-clock 5-min boundaries
(the session-open grid — ET-aligned by construction since XNYS opens on a
5-minute wall-clock boundary); `cycle_id` is the launch boundary as UTC. A
running train is never aborted: a boundary passing mid-run launches nothing
and gets one `skipped` tombstone per curated name, so snapshot presence stays
the scan-membership record (ADR-0030). Fetch failures retry once, then a
`failed` tombstone. The stale guard compares each snapshot's median quote_time
(rs_domain.median_instant — the one shared definition) against the last
ACCEPTED snapshot's; a regression stores the whole snapshot as `stale` and
leaves the baseline unmoved (the observed 2026-07-06 event was a chain-wide
200-OK cached response — findings-2026-07-06.md).

Fault posture (ADR-0008): per-name and per-cycle exceptions are logged +
counted and the train moves on; nothing here may take the stream down — the
daemon wires the task with its own boundary (step 4). No ambient time: every
instant comes from the injected Clock, waiting goes through the injected
sleep (tests drive a fake clock deterministically).
"""

import asyncio
import logging
import math
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timedelta
from typing import Protocol

from rs_domain import ChainSnapshot, Clock, Dataset, SourceStatus, TradingCalendar, median_instant
from rs_ingest.chains import NormalizedChain, normalize_chain

logger = logging.getLogger(__name__)

CYCLE_INTERVAL = timedelta(minutes=5)

# Train pacing: planning limit 120 req/min x 80% headroom (schwab-rest-budget)
# ~= 96 req/min -> one request start per 0.625s. Deliberately a constant, not
# config: the budget's headroom rule is roadmap policy, not a per-deploy knob.
REQUEST_SPACING_SECONDS = 0.625

ChainFetch = Callable[[str], Awaitable[dict]]
"""One bounded chain request (fromDate/toDate <=30 DTE — ADR-0032 D7): returns
the decoded response body; raises on any non-OK outcome."""


class SnapshotStore(Protocol):
    """Persistence seam — rs_persistence.ChainSnapshotRepository behind an
    adapter that owns sessions/commits (daemon wiring); in-memory in tests."""

    async def insert(self, snapshot: ChainSnapshot) -> int: ...

    async def last_accepted_median_quote_times(
        self, dataset: Dataset, now: datetime
    ) -> dict[str, datetime]: ...


class ChainScanner:
    def __init__(
        self,
        *,
        universe: Sequence[str],
        fetch: ChainFetch,
        store: SnapshotStore,
        clock: Clock,
        calendar: TradingCalendar,
        window: float,
        row_cap: int,
        request_timeout: float,
        dataset: Dataset = Dataset.CURATED_5M,
        cycle_interval: timedelta = CYCLE_INTERVAL,
        request_spacing: float = REQUEST_SPACING_SECONDS,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        on_cycle_duration: Callable[[float], None] | None = None,
    ) -> None:
        self._universe = sorted(universe)  # deterministic scan order (spec)
        self._fetch = fetch
        self._store = store
        self._clock = clock
        self._calendar = calendar
        self._window = window
        self._row_cap = row_cap
        self._request_timeout = request_timeout
        self._dataset = dataset
        self._interval = cycle_interval
        self._spacing = request_spacing
        self._sleep = sleep
        self._on_cycle_duration = on_cycle_duration
        # Stale-guard state: per name, the last ACCEPTED snapshot's median
        # quote_time; rebuilt from the DB at run() start (restart survival).
        self._medians: dict[str, datetime] = {}
        # Metric state (bare spec-pinned names; the daemon bridges to
        # collectors). Every label value is present from construction so the
        # exposition never has appearing-from-zero series.
        self.cycle_outcomes: dict[str, int] = {"completed": 0, "skipped": 0}
        self.fetch_outcomes: dict[str, int] = {s.value: 0 for s in SourceStatus}
        self.rows_written: dict[str, int] = {s.value: 0 for s in SourceStatus}
        self.retries = 0
        self.scan_errors = 0
        self.last_success_at: datetime | None = None

    async def run(self) -> None:
        """One session's scan train; returns when the session is over (or
        immediately when there is nothing to scan today)."""
        now = self._clock.now()
        # Session key from the UTC date — exact for a premarket start, the
        # documented operating procedure (same convention as LiveEventSource).
        day = now.date()
        if not self._calendar.is_session(day):
            logger.info("chain scan: %s is not a trading session — nothing to do", day)
            return
        session_open = self._calendar.session_open(day)
        session_close = self._calendar.session_close(day)
        if now >= session_close:
            logger.info("chain scan: session %s already closed — nothing to do", day)
            return

        self._medians = dict(
            await self._store.last_accepted_median_quote_times(self._dataset, now)
        )
        if self._medians:
            logger.info("chain scan: stale-guard state restored for %d names", len(self._medians))

        boundary = self._first_boundary(now, session_open)
        logger.info(
            "chain scan: session %s, %d names, cycles %s -> close %s UTC",
            day,
            len(self._universe),
            boundary.strftime("%H:%M:%S"),
            session_close.strftime("%H:%M:%S"),
        )
        while boundary < session_close:
            await self._sleep_until(boundary)
            await self._run_cycle(boundary)
            next_boundary = boundary + self._interval
            # A boundary that passed while the train ran launches nothing —
            # tombstone its membership and move to the next future boundary.
            while next_boundary < session_close and next_boundary < self._clock.now():
                await self._skip_cycle(next_boundary)
                next_boundary += self._interval
            boundary = next_boundary
        logger.info(
            "chain scan: session over — cycles %s, fetches %s, retries %d, errors %d",
            self.cycle_outcomes,
            self.fetch_outcomes,
            self.retries,
            self.scan_errors,
        )

    def _first_boundary(self, now: datetime, session_open: datetime) -> datetime:
        if now <= session_open:
            return session_open
        return session_open + math.ceil((now - session_open) / self._interval) * self._interval

    async def _sleep_until(self, target: datetime) -> None:
        while (delay := (target - self._clock.now()).total_seconds()) > 0:
            await self._sleep(delay)

    async def _run_cycle(self, cycle_id: datetime) -> None:
        started = self._clock.now()
        for position, underlying in enumerate(self._universe):
            if position and self._spacing:
                await self._sleep(self._spacing)
            try:
                await self._scan_name(underlying, cycle_id)
            except Exception:
                self.scan_errors += 1
                logger.exception("chain scan: %s failed in cycle %s", underlying, cycle_id)
        self.cycle_outcomes["completed"] += 1
        duration = (self._clock.now() - started).total_seconds()
        if self._on_cycle_duration is not None:
            self._on_cycle_duration(duration)

    async def _skip_cycle(self, cycle_id: datetime) -> None:
        logger.warning(
            "chain scan: boundary %s passed mid-train (overrun) — tombstoning %d names",
            cycle_id,
            len(self._universe),
        )
        for underlying in self._universe:
            try:
                await self._persist(
                    ChainSnapshot(
                        underlying=underlying,
                        dataset=self._dataset,
                        cycle_id=cycle_id,
                        fetched_at=self._clock.now(),
                        source_status=SourceStatus.SKIPPED,
                    )
                )
            except Exception:
                self.scan_errors += 1
                logger.exception("chain scan: tombstone failed for %s @ %s", underlying, cycle_id)
        self.cycle_outcomes["skipped"] += 1

    async def _scan_name(self, underlying: str, cycle_id: datetime) -> None:
        try:
            payload = await self._fetch_with_retry(underlying)
        except Exception as exc:
            logger.warning(
                "chain scan: %s fetch failed after retry (%s: %s) — failed tombstone",
                underlying,
                type(exc).__name__,
                exc,
            )
            snapshot = ChainSnapshot(
                underlying=underlying,
                dataset=self._dataset,
                cycle_id=cycle_id,
                fetched_at=self._clock.now(),
                source_status=SourceStatus.FAILED,
            )
        else:
            fetched_at = self._clock.now()
            normalized = normalize_chain(payload, window=self._window, row_cap=self._row_cap)
            if normalized.dropped_by_cap:
                logger.warning(
                    "chain scan: %s row cap bound — dropped %d rows (pathology guard; "
                    "measured books never hit it)",
                    underlying,
                    normalized.dropped_by_cap,
                )
            snapshot = ChainSnapshot(
                underlying=underlying,
                dataset=self._dataset,
                cycle_id=cycle_id,
                fetched_at=fetched_at,
                source_status=self._assess(underlying, normalized),
                rows=normalized.rows,
            )
        await self._persist(snapshot)

    def _assess(self, underlying: str, normalized: NormalizedChain) -> SourceStatus:
        """Snapshot-grain status. Stale wins: a regressed median means the
        whole response is a cached past, whatever else it looks like."""
        median = median_instant(
            q.quote_time for q in normalized.rows if q.quote_time is not None
        )
        baseline = self._medians.get(underlying)
        if median is not None and baseline is not None and median < baseline:
            logger.warning(
                "chain scan: %s stale response — median quote_time %s regressed vs accepted %s",
                underlying,
                median,
                baseline,
            )
            return SourceStatus.STALE
        if median is not None:
            self._medians[underlying] = median  # accepted -> new baseline
        if normalized.skipped_contracts or normalized.chain_truncated:
            logger.warning(
                "chain scan: %s partial — %d unmappable contracts, truncated=%s",
                underlying,
                normalized.skipped_contracts,
                normalized.chain_truncated,
            )
            return SourceStatus.PARTIAL
        return SourceStatus.OK

    async def _fetch_with_retry(self, underlying: str) -> dict:
        """Retry-once-then-tombstone (the measured 39/900 sporadic 502s). Each
        attempt is timeout-bounded so a hang can never hold a train open."""
        try:
            async with asyncio.timeout(self._request_timeout):
                return await self._fetch(underlying)
        except Exception as exc:
            self.retries += 1
            logger.info(
                "chain scan: %s fetch failed (%s: %s) — retrying once",
                underlying,
                type(exc).__name__,
                exc,
            )
        async with asyncio.timeout(self._request_timeout):
            return await self._fetch(underlying)

    async def _persist(self, snapshot: ChainSnapshot) -> None:
        written = await self._store.insert(snapshot)
        status = snapshot.source_status
        self.fetch_outcomes[status.value] += 1
        self.rows_written[status.value] += written
        if status in (SourceStatus.OK, SourceStatus.PARTIAL):
            self.last_success_at = self._clock.now()
