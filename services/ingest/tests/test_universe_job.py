"""The registered universe_snapshot job: its schedule and how a night without a snapshot ends."""

from datetime import UTC, date, datetime, time, timedelta

import pytest

from rs_domain import EASTERN
from rs_ingest.cron import CronTable, Daily
from rs_ingest.jobs import Dependencies, register_jobs
from rs_ingest.rate_limit import RateLimiter
from rs_observability import HealthTracker, QuietFailure
from rs_persistence import UniverseRepository
from rs_sources import ExchangeCalendar
from rs_sources.testing import (
    NASDAQ_LISTED_HEADER,
    OTHER_LISTED_HEADER,
    FakeDirectoryHost,
    cboe_file,
    listing_file,
    nasdaq_row,
    options_file,
    other_row,
)

# Monday 2026-10-19 20:30 ET, past the 20:00 give-up: a run makes one attempt and does not wait.
LATE_EVENING = datetime(2026, 10, 20, 0, 30, tzinfo=UTC)


class FixedClock:
    def now(self) -> datetime:
        return LATE_EVENING


class _NoSchwab:
    client = None


async def _trip(reason: str) -> None: ...


def _job(session_factory, host: FakeDirectoryHost):
    deps = Dependencies(
        session_factory=session_factory,
        db_url="postgresql+psycopg://ingest@db/rocketstocks",
        clock=FixedClock(),
        schwab=_NoSchwab(),  # type: ignore[arg-type]
        schwab_limiter=RateLimiter(6000),
        calendar=ExchangeCalendar(),
        http=host.client(),
        edgar_user_agent="RocketStocks test@example.com",
        trip_token_gate=_trip,
        health=HealthTracker("ingest"),
    )
    cron = CronTable()
    register_jobs(cron, deps)
    return next(job for job in cron if job.name == "universe_snapshot")


async def test_the_universe_snapshot_runs_each_evening_and_at_startup_without_schwab(session_factory):
    job = _job(session_factory, FakeDirectoryHost())

    assert job.schedule == Daily(time(18, 5), EASTERN)
    assert job.needs_schwab is False
    assert job.run_on_start is True
    # From 18:05 the wait lasts until 20:00 ET (115 minutes) before the downloads and the write.
    assert job.time_limit >= timedelta(minutes=115) + timedelta(minutes=15)


async def test_a_night_that_falls_back_fails_quietly_because_ops_was_already_pinged(session_factory, notices):
    host = FakeDirectoryHost()
    host.statuses["nasdaqlisted.txt"] = 503

    with pytest.raises(QuietFailure):
        await _job(session_factory, host).run()

    assert len(notices) == 1 and notices[0].startswith("Nasdaq Trader is failing")


async def test_a_fresh_snapshot_ends_the_run_normally_and_stores_it(session_factory, notices):
    host = FakeDirectoryHost()
    stamp = datetime(2026, 10, 19, 22, 1, tzinfo=UTC)
    host.bodies["nasdaqlisted.txt"] = listing_file(NASDAQ_LISTED_HEADER, [nasdaq_row("AAA")], stamp)
    host.bodies["otherlisted.txt"] = listing_file(OTHER_LISTED_HEADER, [other_row("BBB")], stamp)
    host.bodies["options.txt"] = options_file(["AAA"], stamp)
    host.bodies["cboe"] = cboe_file(["BBB"])

    await _job(session_factory, host).run()

    async with session_factory() as s:
        latest = await UniverseRepository(s).latest()
    assert latest is not None and latest.snapshot_date == date(2026, 10, 19)
    assert {x.symbol: x.optionable for x in latest.listings} == {"AAA": True, "BBB": True}
    assert notices == []
