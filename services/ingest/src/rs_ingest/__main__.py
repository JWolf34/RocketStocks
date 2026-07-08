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

from rs_domain import SystemClock
from rs_ingest.schwab_client import TokenMissingError, build_schwab_client
from rs_ingest.settings import IngestSettings
from rs_ingest.sources.live import LiveEventSource
from rs_persistence import SchwabTokenStore, build_engine, build_session_factory
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
        token_store = SchwabTokenStore(build_session_factory(engine))
        try:
            client = await build_schwab_client(settings, token_store)
        except TokenMissingError as exc:
            logger.error("%s", exc)
            return 1
        source = LiveEventSource(
            symbol=settings.symbol,
            stream_factory=lambda: StreamClient(client),
            calendar=PandasMarketCalendar(),
            clock=SystemClock(),
            grace=timedelta(seconds=settings.watchdog_grace_seconds),
        )
        try:
            async for event in source.events():
                # B6 replaces this log with the Bus-1 fan-out (persister → detector).
                logger.info(
                    "BarClosed %s seq=%d %s close=%.2f volume=%d",
                    event.event_time,
                    event.sequence,
                    event.bar.symbol,
                    event.bar.close,
                    event.bar.volume,
                )
        except OAuthError:
            # Refresh token expired (the weekly re-login chore, pinned #9). The
            # remedy was already logged loudly in the reconnect loop; exit 1
            # without dumping a redundant traceback for an expected event.
            return 1
        await token_store.drain()  # a refresh scheduled near shutdown must not be lost
        if source.aggregator is not None:
            logger.info("session counters: %s", source.aggregator.counters)
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
        settings.notify_channel,
        settings.watchdog_grace_seconds,
    )
    return asyncio.run(run_session(settings))


if __name__ == "__main__":
    sys.exit(main())
