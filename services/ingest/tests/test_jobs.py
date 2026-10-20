"""The job registry: what register_jobs schedules and how a job reaches Schwab."""

from datetime import UTC, datetime, time, timedelta

import httpx
import pytest

from rs_ingest.cron import CronTable, Daily, Every
from rs_ingest.jobs import Dependencies, register_jobs
from rs_ingest.rate_limit import RateLimiter
from rs_ingest.surge_bars import EASTERN, SurgeBarsError
from rs_sources import ExchangeCalendar

NOW = datetime(2026, 10, 20, 22, 0, tzinfo=UTC)


class FixedClock:
    def now(self) -> datetime:
        return NOW


class SwappableSession:
    """Stands in for SchwabSession: `client` is whatever was last (re)loaded."""

    def __init__(self, client) -> None:
        self.client = client


class RecordingClient:
    def __init__(self) -> None:
        self.symbols: list[str] = []

    async def get_price_history_every_day(self, symbol, **kwargs):
        self.symbols.append(symbol)
        raise RuntimeError("stop after the first call")


async def _trip(reason: str) -> None: ...


def _deps(session_factory, schwab) -> Dependencies:
    return Dependencies(
        session_factory=session_factory,
        clock=FixedClock(),
        schwab=schwab,
        schwab_limiter=RateLimiter(6000),
        calendar=ExchangeCalendar(),
        http=httpx.AsyncClient(),
        edgar_user_agent="RocketStocks test@example.com",
        trip_token_gate=_trip,
    )


async def test_surge_bars_runs_daily_after_the_close_and_once_at_startup(session_factory):
    cron = CronTable()
    register_jobs(cron, _deps(session_factory, SwappableSession(RecordingClient())))

    job = next(job for job in cron if job.name == "surge_bars")

    assert job.schedule == Daily(time(17, 30), EASTERN)
    assert job.needs_schwab and job.run_on_start
    assert job.time_limit == timedelta(hours=3)


async def test_the_poll_is_a_non_schwab_half_hourly_job_that_catches_up_at_start(session_factory):
    cron = CronTable()
    register_jobs(cron, _deps(session_factory, SwappableSession(RecordingClient())))

    job = next(job for job in cron if job.name == "popularity_poll")

    assert job.schedule == Every(30)
    assert job.needs_schwab is False
    assert job.run_on_start is True
    assert job.time_limit < timedelta(minutes=30)


async def test_a_run_uses_the_client_current_at_run_time_not_registration_time(session_factory):
    old, rebuilt = RecordingClient(), RecordingClient()
    session = SwappableSession(old)
    cron = CronTable()
    register_jobs(cron, _deps(session_factory, session))
    job = next(job for job in cron if job.name == "surge_bars")

    session.client = rebuilt  # a re-login rebuilt the client
    with pytest.raises(SurgeBarsError):  # the recording client fails its first call on purpose
        await job.run()

    assert old.symbols == []
    assert rebuilt.symbols == ["SPY"]
