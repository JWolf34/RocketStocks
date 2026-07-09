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
from datetime import timedelta

from authlib.integrations.base_client import OAuthError
from pydantic import ValidationError

from rs_domain import EventSource, SystemClock
from rs_ingest.detector import ThresholdDetector
from rs_ingest.pipeline import Pipeline
from rs_ingest.schwab_client import TokenMissingError, build_schwab_client
from rs_ingest.settings import IngestSettings
from rs_ingest.sources.live import LiveEventSource
from rs_ingest.sources.recording import FixtureRecorder, RecordingEventSource
from rs_persistence import NOTIFY_CHANNEL, SchwabTokenStore, build_engine, build_session_factory
from rs_sources import PandasMarketCalendar

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )


async def run_session(settings: IngestSettings) -> int:
    from schwab.streaming import StreamClient

    engine = build_engine(settings.rs_db_url)
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
        await engine.dispose()


def main() -> int:
    configure_logging()
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
