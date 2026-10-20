"""Entry point: `python -m rs_ingest`.

Lifecycle contract (spec, pinned #11): one process = one market session.
Connects immediately (premarket ticks warm the volume baseline), streams
until session close + grace, exits 0 — also when there is no session to
run. `run_session()` is shaped so an outer multi-session loop can wrap it
later without redesign.
"""

import asyncio
import logging
import sys
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import suppress
from datetime import timedelta

from authlib.integrations.base_client import OAuthError
from prometheus_client import Histogram
from pydantic import ValidationError

from rs_domain import Dataset, EventSource, SystemClock
from rs_ingest.aggregator import COUNTER_NAMES as AGGREGATOR_COUNTER_NAMES
from rs_ingest.chain_adapters import SchwabChainFetch, SessionSnapshotStore
from rs_ingest.chain_scanner import ChainScanner
from rs_ingest.detector import ThresholdDetector
from rs_ingest.pipeline import Pipeline
from rs_ingest.schwab_client import TokenMissingError, build_schwab_client
from rs_ingest.settings import IngestSettings
from rs_ingest.sources.live import LiveEventSource
from rs_ingest.sources.recording import FixtureRecorder, RecordingEventSource
from rs_observability import (
    CallableGaugeCollector,
    DictCounterCollector,
    LabeledDictCounterCollector,
    configure_logging,
    install_excepthooks,
    start_metrics_server,
)
from rs_persistence import NOTIFY_CHANNEL, SchwabTokenStore, build_engine, build_session_factory
from rs_sources import ExchangeCalendar

logger = logging.getLogger(__name__)

# Cycle trains run seconds (bootstrap) to ~4 minutes (full universe); the
# 5-minute cycle interval is the natural ceiling of interest.
CYCLE_DURATION_BUCKETS = (1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 180.0, 240.0, 300.0, 600.0)


class ScrapeTimeCounters(Mapping):
    """Mapping view over a dict-returning callable, so collectors read live
    counter values at each scrape instead of a snapshot from wiring time."""

    def __init__(self, read: Callable[[], Mapping[str, int]]) -> None:
        self._read = read

    def __getitem__(self, name: str) -> int:
        return self._read()[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._read())

    def __len__(self) -> int:
        return len(self._read())


def live_aggregator_counters(live: LiveEventSource) -> Mapping[str, int]:
    """The session aggregator exists only once a session opens (LiveEventSource
    creates it), so serve zeros until then — every metric name is present from
    process start."""
    if live.aggregator is None:
        return dict.fromkeys(AGGREGATOR_COUNTER_NAMES, 0)
    return live.aggregator.counters


def chain_scan_scalar_counters(scanner: ChainScanner) -> Mapping[str, int]:
    """The scanner's unlabeled counters (bare names per ADR-0015; the collector
    adds the _total sample suffix)."""
    return {
        "chain_scan_retries": scanner.retries,
        "chain_scan_errors": scanner.scan_errors,
    }


# Spec-pinned label for the freshness gauge (S008 metrics contract; ADR-0017's
# source key for the curated_5m dataset). S012 alarms on this exact pair.
CHAIN_SCAN_SOURCE = "options_curated_5m"


def chain_scan_collectors(scanner: ChainScanner, histogram: Histogram, clock: SystemClock) -> list:
    """The S012 metrics contract for the scan, exactly as the spec pins it."""

    def seconds_since_last_success() -> float | None:
        if scanner.last_success_at is None:
            return None  # series absent until the first accepted snapshot
        return (clock.now() - scanner.last_success_at).total_seconds()

    return [
        LabeledDictCounterCollector("chain_scan_cycles", "outcome", scanner.cycle_outcomes),
        LabeledDictCounterCollector("chain_scan_fetches", "outcome", scanner.fetch_outcomes),
        LabeledDictCounterCollector(
            "chain_snapshot_rows_written", "source_status", scanner.rows_written
        ),
        DictCounterCollector(
            ScrapeTimeCounters(lambda: chain_scan_scalar_counters(scanner)), namespace=""
        ),
        histogram,
        CallableGaugeCollector(
            "source_last_success_seconds",
            "Seconds since the source last delivered an accepted snapshot",
            {"source": CHAIN_SCAN_SOURCE},
            seconds_since_last_success,
        ),
    ]


async def _run_chain_scanner(
    scanner: ChainScanner, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
) -> None:
    """Own fault boundary (ADR-0008): an exception escaping the cycle machinery
    is logged + counted and the scan resumes at the next boundary — re-entering
    run() recomputes the boundary grid from the clock and reloads stale-guard
    baselines from the store, and run() returns normally once the session is
    over, ending the loop. curated_5m is forward-only, so a transient failure
    (e.g. a DB blip during the restart-state load) must cost one cycle, not the
    session. Never takes stream capture down; cancellation (session teardown)
    passes through, including during the between-attempts sleep."""
    while True:
        try:
            await scanner.run()
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            scanner.scan_errors += 1
            logger.exception(
                "chain scanner crashed — resuming at the next boundary; "
                "stream capture continues unaffected"
            )
        await sleep(scanner.cycle_interval.total_seconds())


async def run_session(settings: IngestSettings) -> int:
    from schwab.streaming import StreamClient

    install_excepthooks()  # the asyncio half — needs asyncio.run()'s loop
    engine = build_engine(settings.rs_db_url)
    metrics_server = None
    scanner_task: asyncio.Task | None = None
    try:
        session_factory = build_session_factory(engine)
        token_store = SchwabTokenStore(session_factory)
        try:
            client = await build_schwab_client(settings, token_store)
        except TokenMissingError as exc:
            logger.error("%s", exc)
            return 1
        clock = SystemClock()
        calendar = ExchangeCalendar()
        live = LiveEventSource(
            symbol=settings.symbol,
            stream_factory=lambda: StreamClient(client),
            calendar=calendar,
            clock=clock,
            grace=timedelta(seconds=settings.watchdog_grace_seconds),
        )
        source: EventSource = live
        if settings.fixture_path is not None:
            source = RecordingEventSource(live, FixtureRecorder(settings.fixture_path))
            logger.info("recording session fixture to %s", settings.fixture_path)
        pipeline = Pipeline(session_factory, [ThresholdDetector(settings.volume_threshold)])
        # :9090 side-port (ADR-0015). namespace="" — the ADR names the metric
        # vocabulary bare (bars_persisted_total, ...); scrape job labels, not
        # name prefixes, distinguish services.
        collectors = [
            DictCounterCollector(pipeline.counters, namespace=""),
            DictCounterCollector(
                ScrapeTimeCounters(lambda: live_aggregator_counters(live)), namespace=""
            ),
        ]
        scanner = None
        if settings.chain_scan_universe:
            cycle_histogram = Histogram(
                "chain_scan_cycle_duration_seconds",
                "Wall-clock duration of one completed scan cycle (train)",
                buckets=CYCLE_DURATION_BUCKETS,
                registry=None,
            )
            # The scanner shares the ONE schwab-py client (token single-writer,
            # ADR-0024), engine, calendar, clock, and metrics port.
            scanner = ChainScanner(
                universe=settings.chain_scan_universe,
                fetch=SchwabChainFetch(client, clock),
                store=SessionSnapshotStore(session_factory),
                clock=clock,
                calendar=calendar,
                window=settings.chain_scan_window,
                row_cap=settings.chain_scan_row_cap,
                request_timeout=settings.chain_scan_request_timeout_seconds,
                dataset=Dataset.CURATED_5M,
                on_cycle_duration=cycle_histogram.observe,
            )
            collectors += chain_scan_collectors(scanner, cycle_histogram, clock)
        metrics_server, _ = start_metrics_server(settings.metrics_port, collectors)
        if scanner is not None:
            scanner_task = asyncio.create_task(_run_chain_scanner(scanner), name="chain-scanner")
        try:
            await pipeline.run(source)
        except OAuthError:
            # Refresh token expired (the weekly re-login chore, pinned #9). The
            # remedy was already logged loudly in the reconnect loop; exit 1
            # without dumping a redundant traceback for an expected event.
            return 1
        if scanner_task is not None:
            # Session over. A train still mid-run finishes (never aborted) and
            # the scanner exits on its own — the guard swallowed any crash.
            await scanner_task
        await token_store.drain()  # a refresh scheduled near shutdown must not be lost
        if live.aggregator is not None:
            logger.info("aggregator counters: %s", live.aggregator.counters)
        logger.info("pipeline counters: %s", pipeline.counters)
        return 0
    finally:
        # An early return/raise must not leave a scan train running.
        if scanner_task is not None and not scanner_task.done():
            scanner_task.cancel()
            with suppress(asyncio.CancelledError):
                await scanner_task
        if metrics_server is not None:
            metrics_server.shutdown()
        await engine.dispose()


def main() -> int:
    configure_logging("ingest")
    install_excepthooks()  # sys/threading; the asyncio half joins in run_session
    try:
        settings = IngestSettings()
    except ValidationError as exc:
        logger.error("invalid configuration: %s", exc)
        return 1
    logger.info(
        "rs_ingest starting: symbol=%s threshold=%d channel=%s grace=%.0fs",
        settings.symbol,
        settings.volume_threshold,
        NOTIFY_CHANNEL,
        settings.watchdog_grace_seconds,
    )
    return asyncio.run(run_session(settings))


if __name__ == "__main__":
    sys.exit(main())
