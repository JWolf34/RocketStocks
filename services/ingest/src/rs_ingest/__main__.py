"""Entry point: `python -m rs_ingest`.

A supervised daemon: runs the cron table until SIGTERM, then lets in-flight
jobs finish (bounded), drains pending token saves and exits 0. A dead or
missing Schwab token pauses the Schwab jobs and sends one ops notice instead of
exiting; every other job keeps running and the token is picked up once a
re-login writes it.
"""

import asyncio
import logging
import signal
import sys

from pydantic import ValidationError

from rs_domain import SystemClock
from rs_ingest.cron import CronTable
from rs_ingest.daemon import Daemon, TokenGate
from rs_ingest.jobs import Dependencies, register_jobs
from rs_ingest.metrics import DaemonMetrics
from rs_ingest.rate_limit import RateLimiter
from rs_ingest.schwab_session import SchwabSession
from rs_ingest.settings import LOGIN_HINT, IngestSettings
from rs_observability import (
    configure_logging,
    configure_ops,
    install_excepthooks,
    start_metrics_server,
)
from rs_persistence import SchwabTokenStore, build_engine, build_session_factory
from rs_sources import ExchangeCalendar

logger = logging.getLogger(__name__)


async def run_daemon(settings: IngestSettings) -> int:
    install_excepthooks()  # the asyncio half - needs asyncio.run()'s loop
    clock = SystemClock()
    configure_ops(clock=clock)
    engine = build_engine(settings.rs_db_url)
    metrics_server = None
    session_factory = build_session_factory(engine)
    token_store = SchwabTokenStore(session_factory)
    try:
        schwab = SchwabSession(
            settings.schwab_api_key, settings.schwab_api_secret.get_secret_value(), token_store
        )
        metrics = DaemonMetrics()
        gate = TokenGate(token_store, schwab.reload, clock, metrics)
        cron = CronTable()
        register_jobs(
            cron,
            Dependencies(
                session_factory=session_factory,
                clock=clock,
                schwab=schwab,
                schwab_limiter=RateLimiter(settings.schwab_calls_per_minute),
                calendar=ExchangeCalendar(),
            ),
        )
        daemon = Daemon(
            cron,
            gate,
            clock,
            metrics,
            failure_threshold=settings.job_failure_threshold,
            token_poll_seconds=settings.token_poll_seconds,
            shutdown_grace_seconds=settings.shutdown_grace_seconds,
            finalizers=[token_store.drain],  # a refresh scheduled near shutdown must not be lost
        )
        metrics_server, _ = start_metrics_server(settings.metrics_port, metrics.collectors())
        if not await schwab.load():
            await gate.trip(f"no Schwab token stored; {LOGIN_HINT}")
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, daemon.request_stop)
        logger.info("rs_ingest daemon running: %d job(s)", len(cron))
        await daemon.run()
        logger.info("rs_ingest daemon stopped")
        return 0
    finally:
        # The daemon drains on its own exit; this covers a failure before it started.
        await token_store.drain()
        if metrics_server is not None:
            metrics_server.shutdown()
        await engine.dispose()


def main() -> int:
    configure_logging("ingest")
    install_excepthooks()  # sys/threading; the asyncio half joins in run_daemon
    try:
        settings = IngestSettings()
    except ValidationError as exc:
        logger.error("invalid configuration: %s", exc)
        return 1
    return asyncio.run(run_daemon(settings))


if __name__ == "__main__":
    sys.exit(main())
