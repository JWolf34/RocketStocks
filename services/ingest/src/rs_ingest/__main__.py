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
from collections.abc import Iterator, Mapping
from datetime import timedelta

from authlib.integrations.base_client import OAuthError
from pydantic import ValidationError

from rs_domain import EventSource, SystemClock
from rs_ingest.aggregator import COUNTER_NAMES as AGGREGATOR_COUNTER_NAMES
from rs_ingest.detector import ThresholdDetector
from rs_ingest.pipeline import Pipeline
from rs_ingest.schwab_client import TokenMissingError, build_schwab_client
from rs_ingest.settings import IngestSettings
from rs_ingest.sources.live import LiveEventSource
from rs_ingest.sources.recording import FixtureRecorder, RecordingEventSource
from rs_observability import (
    DictCounterCollector,
    configure_logging,
    install_excepthooks,
    start_metrics_server,
)
from rs_persistence import NOTIFY_CHANNEL, SchwabTokenStore, build_engine, build_session_factory
from rs_sources import PandasMarketCalendar

logger = logging.getLogger(__name__)


class LiveAggregatorCounters(Mapping):
    """Scrape-time view of the session aggregator's counters. The aggregator
    exists only once a session opens (LiveEventSource creates it), so the view
    serves zeros until then — every metric name is present from process start."""

    def __init__(self, live: LiveEventSource) -> None:
        self._live = live

    def _counters(self) -> Mapping[str, int]:
        if self._live.aggregator is None:
            return dict.fromkeys(AGGREGATOR_COUNTER_NAMES, 0)
        return self._live.aggregator.counters

    def __getitem__(self, name: str) -> int:
        return self._counters()[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._counters())

    def __len__(self) -> int:
        return len(self._counters())


async def run_session(settings: IngestSettings) -> int:
    from schwab.streaming import StreamClient

    install_excepthooks()  # the asyncio half — needs asyncio.run()'s loop
    engine = build_engine(settings.rs_db_url)
    metrics_server = None
    try:
        session_factory = build_session_factory(engine)
        token_store = SchwabTokenStore(session_factory)
        try:
            client = await build_schwab_client(settings, token_store)
        except TokenMissingError as exc:
            logger.error("%s", exc)
            return 1
        live = LiveEventSource(
            symbol=settings.symbol,
            stream_factory=lambda: StreamClient(client),
            calendar=PandasMarketCalendar(),
            clock=SystemClock(),
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
        metrics_server, _ = start_metrics_server(
            settings.metrics_port,
            [
                DictCounterCollector(pipeline.counters, namespace=""),
                DictCounterCollector(LiveAggregatorCounters(live), namespace=""),
            ],
        )
        try:
            await pipeline.run(source)
        except OAuthError:
            # Refresh token expired (the weekly re-login chore, pinned #9). The
            # remedy was already logged loudly in the reconnect loop; exit 1
            # without dumping a redundant traceback for an expected event.
            return 1
        await token_store.drain()  # a refresh scheduled near shutdown must not be lost
        if live.aggregator is not None:
            logger.info("aggregator counters: %s", live.aggregator.counters)
        logger.info("pipeline counters: %s", pipeline.counters)
        return 0
    finally:
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
