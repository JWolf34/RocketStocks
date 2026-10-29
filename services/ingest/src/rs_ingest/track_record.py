"""The nightly Track record job: `rs_ingest.outcomes` as a subprocess of the same image.

It runs out of process on purpose. Replaying the rule over the stored Popularity is
CPU-heavy Python that would stall the daemon's event loop, and holding that history
would bloat the daemon's memory. The subprocess gets only what it needs (the database
URL and logging settings), never the Schwab secrets.
"""

import asyncio
import logging
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import time, timedelta

from rs_domain import EASTERN
from rs_ingest.cron import Daily, Job
from rs_ingest.outcomes import DSN_VARIABLE

logger = logging.getLogger(__name__)

JOB_NAME = "track_record"
COMMAND = (sys.executable, "-m", "rs_ingest.outcomes")
PASSED_ENVIRONMENT = ("PATH", "TZ", "RS_LOG_FORMAT", "RS_LOG_LEVEL")


def subprocess_environment(source: Mapping[str, str], db_url: str) -> dict[str, str]:
    return {name: source[name] for name in PASSED_ENVIRONMENT if name in source} | {DSN_VARIABLE: db_url}


async def run_outcomes(
    db_url: str, command: Sequence[str] = COMMAND, environ: Mapping[str, str] | None = None
) -> None:
    """Run the job to completion; a non-zero exit raises, so the daemon's failure
    metrics and Ops pings see it."""
    process = await asyncio.create_subprocess_exec(
        *command, env=subprocess_environment(os.environ if environ is None else environ, db_url)
    )
    try:
        code = await process.wait()
    except asyncio.CancelledError:
        # The daemon's timeout or shutdown: never leave the job running unsupervised.
        process.kill()
        await process.wait()
        raise
    if code != 0:
        raise RuntimeError(f"{JOB_NAME}: the outcomes job exited with status {code}")
    logger.info("%s finished", JOB_NAME)


def track_record_job(db_url: str) -> Job:
    return Job(
        JOB_NAME,
        # The daily bars job (17:30 Eastern) fetches SPY and the Triggered stocks, the only bars this
        # job reads, before the rest of the Universe, so it need not wait for the whole pass.
        Daily(time(20, 0), EASTERN),
        lambda: run_outcomes(db_url),
        needs_schwab=False,
        run_on_start=True,
        timeout=timedelta(hours=2),
    )
