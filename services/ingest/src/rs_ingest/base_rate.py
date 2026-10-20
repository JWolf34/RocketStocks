"""The weekly base-rate rerun: `rs_research` as a subprocess of the same image.

It runs out of process on purpose. The study is CPU-heavy pandas that would
stall the daemon's event loop, and it connects with its own read-only database
role (RS_RESEARCH_DB_URL), so ingest's wider credentials never reach it.
"""

import asyncio
import logging
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import time, timedelta
from zoneinfo import ZoneInfo

from rs_ingest.cron import Job, Weekly
from rs_observability import notify_ops

logger = logging.getLogger(__name__)

JOB_NAME = "base_rate_weekly"
COMMAND = (sys.executable, "-m", "rs_research", "event-study", "attention_surge")
# The subprocess gets only what it needs: never ingest's DSN or Schwab secrets.
PASSED_ENVIRONMENT = ("PATH", "TZ", "RS_RESEARCH_DB_URL", "RS_LOG_FORMAT", "RS_LOG_LEVEL")
SUNDAY = 6


def research_environment(source: Mapping[str, str]) -> dict[str, str]:
    return {name: source[name] for name in PASSED_ENVIRONMENT if name in source}


async def run_event_study(
    command: Sequence[str] = COMMAND, environ: Mapping[str, str] | None = None
) -> None:
    """Run the study to completion; a non-zero exit sends an ops notice and
    raises, so the daemon's failure metrics see it too."""
    process = await asyncio.create_subprocess_exec(
        *command, env=research_environment(os.environ if environ is None else environ)
    )
    try:
        code = await process.wait()
    except asyncio.CancelledError:
        # The daemon's timeout: never leave the study running unsupervised.
        process.kill()
        await process.wait()
        raise
    if code != 0:
        await notify_ops(
            f"Weekly base-rate study exited with status {code}; the stored base rate is stale.",
            key=f"job-failed:{JOB_NAME}",
        )
        raise RuntimeError(f"{JOB_NAME}: rs_research exited with status {code}")
    logger.info("%s finished", JOB_NAME)


def base_rate_job() -> Job:
    return Job(
        JOB_NAME,
        Weekly(SUNDAY, time(20, 0), ZoneInfo("America/Chicago")),
        run_event_study,
        needs_schwab=False,
        timeout=timedelta(hours=2),
    )
