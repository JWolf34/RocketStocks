"""The job registry: what register_jobs schedules and how a job reaches Schwab."""

from datetime import UTC, datetime, time, timedelta

import httpx
import pytest

from rs_domain import EASTERN
from rs_ingest.cron import CronTable, Daily, Every
from rs_ingest.jobs import Dependencies, register_jobs
from rs_ingest.rate_limit import RateLimiter
from rs_observability import HealthTracker, QuietFailure, SourceFailed
from rs_sources import ExchangeCalendar
from rs_sources.nasdaq import EARNINGS_URL

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


class RespondingClient:
    """A Schwab client whose every history request is answered with `status_code`."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    async def get_price_history_every_day(self, symbol, **kwargs):
        return httpx.Response(self.status_code, json={})


async def _trip(reason: str) -> None: ...


def _deps(session_factory, schwab, http: httpx.AsyncClient | None = None) -> Dependencies:
    return Dependencies(
        session_factory=session_factory,
        db_url="postgresql+psycopg://ingest@db/rocketstocks",
        clock=FixedClock(),
        schwab=schwab,
        schwab_limiter=RateLimiter(6000),
        calendar=ExchangeCalendar(),
        http=http or httpx.AsyncClient(),
        edgar_user_agent="RocketStocks test@example.com",
        trip_token_gate=_trip,
        health=HealthTracker("ingest"),
    )


def _job(session_factory, name: str, *, schwab=None, handler=None):
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler)) if handler else None
    cron = CronTable()
    register_jobs(cron, _deps(session_factory, SwappableSession(schwab or RecordingClient()), http))
    return next(job for job in cron if job.name == name)


@pytest.fixture(autouse=True)
def no_request_spacing(monkeypatch):
    async def instant(self) -> None: ...

    monkeypatch.setattr("rs_sources._ratelimit.MinInterval.wait", instant)


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
    with pytest.raises(QuietFailure):  # the recording client fails its first call on purpose
        await job.run()

    assert old.symbols == []
    assert rebuilt.symbols == ["SPY"]


async def test_earnings_calendar_warm_runs_at_startup_and_often_enough_to_beat_the_cache_ttl(session_factory):
    cron = CronTable()
    register_jobs(cron, _deps(session_factory, SwappableSession(RecordingClient())))

    job = next(job for job in cron if job.name == "earnings_calendar_warm")

    assert job.schedule == Every(120)
    assert job.run_on_start
    assert not job.needs_schwab


async def test_a_single_failed_symbol_fails_the_bars_run_without_a_ping(session_factory, notices):
    job = _job(session_factory, "surge_bars")  # only SPY is needed, and the recording client fails it

    with pytest.raises(QuietFailure):
        await job.run()

    assert notices == []


async def test_schwab_rate_limiting_the_bars_run_pings_schwab_once_and_fails_the_job_quietly(
    session_factory, notices
):
    job = _job(session_factory, "surge_bars", schwab=RespondingClient(429))

    for _ in range(2):
        with pytest.raises(SourceFailed):
            await job.run()

    assert len(notices) == 1
    assert notices[0].startswith("Schwab is failing (1 of 1 requests failed)")


async def test_a_symbol_schwab_refuses_is_not_a_failure_and_does_not_fail_the_bars_run(session_factory, notices):
    job = _job(session_factory, "surge_bars", schwab=RespondingClient(404))

    await job.run()

    assert notices == []


def _earnings_handler(failing_days: set[str] | None = None, all_failing: bool = False):
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url).startswith(EARNINGS_URL)
        if all_failing or request.url.params["date"] in (failing_days or set()):
            return httpx.Response(503)
        return httpx.Response(200, json={"data": {"rows": None}})

    return handler


async def test_a_dead_nasdaq_calendar_pings_nasdaq_once_and_the_warm_job_fails_without_its_own_ping(
    session_factory, notices
):
    job = _job(session_factory, "earnings_calendar_warm", handler=_earnings_handler(all_failing=True))

    for _ in range(2):
        with pytest.raises(SourceFailed):
            await job.run()

    assert len(notices) == 1
    assert notices[0].startswith("Nasdaq is failing (30 of 30 requests failed)")


async def test_one_failed_calendar_day_pings_nothing(session_factory, notices):
    job = _job(session_factory, "earnings_calendar_warm", handler=_earnings_handler({"2026-10-21"}))

    with pytest.raises(QuietFailure):
        await job.run()

    assert notices == []


async def test_a_healthy_warm_recovers_nasdaq(session_factory, notices):
    failing = {"on": True}

    def handler(request: httpx.Request) -> httpx.Response:
        return _earnings_handler(all_failing=failing["on"])(request)

    job = _job(session_factory, "earnings_calendar_warm", handler=handler)
    with pytest.raises(SourceFailed):
        await job.run()

    failing["on"] = False
    await job.run()

    assert [n.split(" (")[0] for n in notices] == ["Nasdaq is failing", "Nasdaq recovered."]


async def test_a_failed_apewisdom_listing_pings_apewisdom_and_fails_the_poll_without_its_own_ping(
    session_factory, notices
):
    job = _job(session_factory, "popularity_poll", handler=lambda request: httpx.Response(400))

    for _ in range(2):
        with pytest.raises(SourceFailed):
            await job.run()

    assert notices == ["ApeWisdom is failing (ApeWisdomError). See the ingest logs."]
