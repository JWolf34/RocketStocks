"""One-time backfill of Nasdaq's earnings calendar from 2008-01-01 (the earliest year it serves).

    python -m rs_ingest.tools.earnings_backfill                  # 2008-01-01 through yesterday
    python -m rs_ingest.tools.earnings_backfill --start 2026-01-05 --end 2026-01-16

One request per weekday at the adapter's spacing. Each day's rows are stored in one
transaction with origin `backfill` and the real fetch time, dated by the Eastern
day they were fetched, so a run over several days is one snapshot per day it ran.
It resumes: a weekday that already has backfill rows is skipped, so a rerun after
a stop, a failure or a dead endpoint carries on where it left off (a weekday Nasdaq
had nothing for leaves no rows and is asked again). A day that fails is asked again twice,
after 30 seconds and after 2 minutes; one that still fails is left for the rerun, and ten
failed days in a row stop the run.
"""

import argparse
import asyncio
import logging
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import EarningsOrigin, StoredEarnings, SystemClock, eastern_day
from rs_ingest.earnings_snapshot import DaySource, RetryingDays, weekdays
from rs_observability import configure_logging
from rs_persistence import (
    EarningsSnapshotRepository,
    build_engine,
    build_session_factory,
)
from rs_sources.errors import SourceError
from rs_sources.nasdaq import NasdaqEarningsCalendar

logger = logging.getLogger(__name__)

DSN_VARIABLE = "RS_DB_URL"
EARLIEST_DAY = date(2008, 1, 1)
# Progress is logged this often.
PROGRESS_EVERY = 100
# The endpoint is down or refusing us; stop instead of asking thousands more times.
MAX_CONSECUTIVE_FAILURES = 10


@dataclass
class BackfillReport:
    days_total: int = 0
    skipped: int = 0
    fetched: int = 0
    rows: int = 0
    failed: list[date] = field(default_factory=list)
    unparsed: Counter[str] = field(default_factory=Counter)
    stopped_early: bool = False


async def backfill_earnings(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    source: DaySource,
    start: date,
    end: date,
) -> BackfillReport:
    """Store every weekday from `start` to `end` that has no backfill rows yet."""
    async with session_factory() as session:
        done = await EarningsSnapshotRepository(session).report_dates(EarningsOrigin.BACKFILL, start, end)
    report = BackfillReport()
    consecutive_failures = 0
    for day in weekdays(start, end):
        report.days_total += 1
        if day in done:
            report.skipped += 1
            continue
        try:
            fetched = await source.day(day)
        except SourceError:
            logger.warning("Nasdaq earnings calendar failed for %s", day, exc_info=True)
            report.failed.append(day)
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                report.stopped_early = True
                break
            continue
        consecutive_failures = 0
        rows = [
            StoredEarnings(
                snapshot_date=eastern_day(fetched.fetched_at),
                origin=EarningsOrigin.BACKFILL,
                fetched_at=fetched.fetched_at,
                **listing.model_dump(),
            )
            for listing in fetched.listings
        ]
        if rows:
            async with session_factory() as session, session.begin():
                report.rows += await EarningsSnapshotRepository(session).insert_many(rows)
        report.fetched += 1
        report.unparsed.update(fetched.unparsed)
        if report.fetched % PROGRESS_EVERY == 0:
            logger.info(
                "earnings backfill: %d days fetched, now at %s of %s (%d rows)", report.fetched, day, end, report.rows
            )
    return report


async def _run(dsn: str, start: date, end: date) -> BackfillReport:
    engine = build_engine(dsn)
    clock = SystemClock()
    try:
        async with httpx.AsyncClient(timeout=15.0) as http:
            return await backfill_earnings(
                session_factory=build_session_factory(engine),
                source=RetryingDays(NasdaqEarningsCalendar(http, clock)),
                start=start,
                end=end,
            )
    finally:
        await engine.dispose()


def main() -> int:
    """Exit 0 when every day is stored, 1 when some failed or the run stopped early, 2 on bad configuration."""
    configure_logging("earnings-backfill")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat, default=EARLIEST_DAY, help="first day (default 2008-01-01)")
    parser.add_argument("--end", type=date.fromisoformat, default=None, help="last day (default: yesterday, Eastern)")
    args = parser.parse_args()
    dsn = os.environ.get(DSN_VARIABLE)
    if not dsn:
        print(f"{DSN_VARIABLE} is not set", file=sys.stderr)
        return 2
    end = args.end or eastern_day(SystemClock().now()) - timedelta(days=1)
    report = asyncio.run(_run(dsn, args.start, end))
    print(
        f"{args.start} to {end}: {report.days_total} weekdays, {report.skipped} already stored, "
        f"{report.fetched} fetched ({report.rows} rows), {len(report.failed)} failed"
    )
    if report.unparsed:
        print(f"values not parsed, by field: {dict(sorted(report.unparsed.items()))}")
    if report.failed:
        print(f"rerun to retry the failed days, first {report.failed[0]}", file=sys.stderr)
    if report.stopped_early:
        print(f"stopped after {MAX_CONSECUTIVE_FAILURES} failed days in a row; Nasdaq may be refusing requests", file=sys.stderr)
    return 1 if report.failed else 0


if __name__ == "__main__":
    sys.exit(main())
