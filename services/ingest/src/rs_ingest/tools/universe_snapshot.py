"""Take the current session's Universe snapshot by hand, with the size guard off.

    python -m rs_ingest.tools.universe_snapshot --accept-shrink

For when Ops says a file looks truncated and the file is genuinely that much smaller than the
last snapshot's. It runs the nightly code for the session whose evening last started, waiting
and retrying like the job: the stamp rule and the footer check still apply, so a stale or cut
file is still refused, and it does nothing if that session already has its snapshot. Exit 0
when the session's snapshot is stored, 1 when the night still has none.
"""

import argparse
import asyncio
import logging
import sys

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import Clock, SystemClock
from rs_ingest.settings import IngestSettings
from rs_ingest.universe import UniverseSnapshotter, UniverseUse
from rs_observability import HealthTracker, configure_ops
from rs_persistence import build_engine, build_session_factory
from rs_sources import ExchangeCalendar
from rs_sources.symbol_directory import SymbolDirectory


async def take_snapshot(
    session_factory: async_sessionmaker[AsyncSession], http: httpx.AsyncClient, clock: Clock
) -> UniverseUse:
    snapshotter = UniverseSnapshotter(
        session_factory, SymbolDirectory(http), clock, ExchangeCalendar(), HealthTracker("ingest")
    )
    return await snapshotter.take(accept_shrink=True)


async def _run(settings: IngestSettings) -> UniverseUse:
    clock = SystemClock()
    configure_ops(clock=clock)
    engine = build_engine(settings.rs_db_url)
    try:
        async with httpx.AsyncClient(timeout=15.0) as http:
            return await take_snapshot(build_session_factory(engine), http, clock)
    finally:
        await engine.dispose()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--accept-shrink",
        action="store_true",
        required=True,
        help="take the snapshot even if a file is far smaller than the previous snapshot's",
    )
    parser.parse_args()

    use = asyncio.run(_run(IngestSettings()))
    if use.fresh:
        print(f"Universe snapshot of {use.session} stored (as of {use.as_of})")
        return 0
    print(f"no Universe snapshot for {use.session}; {use.night_uses}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
