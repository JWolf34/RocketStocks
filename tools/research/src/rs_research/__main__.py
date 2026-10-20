"""Research CLI: `python -m rs_research event-study attention_surge`.

Connects as the `rs_research` role (read-only apart from INSERT on base_rate); the
DSN comes from RS_RESEARCH_DB_URL, never RS_DB_URL, so a misconfigured
container cannot run it with a wider role. Exit 0 on success (including
"nothing to measure"), 1 when the study fails, 2 on bad configuration.
"""

import argparse
import asyncio
import logging
import os
import sys
from collections.abc import Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import Clock, SystemClock
from rs_observability import configure_logging
from rs_persistence import build_engine, build_session_factory
from rs_research import attention_surge

logger = logging.getLogger(__name__)

DSN_VARIABLE = "RS_RESEARCH_DB_URL"

Study = Callable[[async_sessionmaker[AsyncSession], Clock], Awaitable[object]]
STUDIES: dict[str, Study] = {attention_surge.ALERT_TYPE: attention_surge.run}


async def _run(study: Study, dsn: str, clock: Clock) -> None:
    engine = build_engine(dsn)
    try:
        await study(build_session_factory(engine), clock)
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None, clock: Clock | None = None) -> int:
    parser = argparse.ArgumentParser(prog="rs_research", description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    event_study = sub.add_parser("event-study", help="measure a signal and write its base rates")
    event_study.add_argument("study", choices=sorted(STUDIES))
    args = parser.parse_args(argv)

    configure_logging("research")
    dsn = os.environ.get(DSN_VARIABLE)
    if not dsn:
        print(f"{DSN_VARIABLE} is not set", file=sys.stderr)
        return 2
    try:
        asyncio.run(_run(STUDIES[args.study], dsn, clock or SystemClock()))
    except Exception:
        logger.exception("event study %s failed", args.study)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
