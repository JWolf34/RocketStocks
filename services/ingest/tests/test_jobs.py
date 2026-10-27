"""The job registry: what register_jobs schedules and how a job reaches Schwab."""

from datetime import UTC, date, datetime, time, timedelta

import httpx
import pytest
from sqlalchemy import func, select

from rs_domain import EASTERN
from rs_ingest.cron import CronTable, Daily, Every
from rs_ingest.jobs import Dependencies, register_jobs
from rs_ingest.rate_limit import RateLimiter
from rs_observability import HealthTracker, QuietFailure, SourceFailed
from rs_persistence import EarningsSnapshotRow, PopularitySnapshotRow
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


class Sleeps:
    """A `sleep` stand-in that records the requested delays and returns at once."""

    def __init__(self) -> None:
        self.seconds: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.seconds.append(seconds)


def _deps(session_factory, schwab, http: httpx.AsyncClient | None = None, clock=None, sleep=None) -> Dependencies:
    return Dependencies(
        session_factory=session_factory,
        db_url="postgresql+psycopg://ingest@db/rocketstocks",
        clock=clock or FixedClock(),
        schwab=schwab,
        schwab_limiter=RateLimiter(6000),
        calendar=ExchangeCalendar(),
        http=http or httpx.AsyncClient(),
        edgar_user_agent="RocketStocks test@example.com",
        trip_token_gate=_trip,
        health=HealthTracker("ingest"),
        sleep=sleep or Sleeps(),
    )


def _job(session_factory, name: str, *, schwab=None, handler=None, clock=None, sleep=None):
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler)) if handler else None
    cron = CronTable()
    register_jobs(cron, _deps(session_factory, SwappableSession(schwab or RecordingClient()), http, clock, sleep))
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


async def test_the_earnings_snapshot_runs_each_evening_after_the_close_and_once_at_startup(session_factory):
    cron = CronTable()
    register_jobs(cron, _deps(session_factory, SwappableSession(RecordingClient())))

    job = next(job for job in cron if job.name == "earnings_snapshot")

    assert job.schedule == Daily(time(20, 30), EASTERN)
    assert job.run_on_start
    assert not job.needs_schwab
    assert job.time_limit == timedelta(minutes=30)


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


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now


# Tuesday 2026-10-20, 20:45 Eastern: past the snapshot slot.
EVENING = FakeClock(datetime(2026, 10, 21, 0, 45, tzinfo=UTC))
FORWARD_ROW = {
    "symbol": "AAA",
    "time": "time-pre-market",
    "fiscalQuarterEnding": "Dec/2026",
    "epsForecast": "$0.64",
    "noOfEsts": "12",
    "lastYearRptDt": "11/20/2025",
    "lastYearEPS": "$0.62",
}


def _earnings_handler(failing_days: set[str] | None = None, all_failing: bool = False, listed: bool = True):
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url).startswith(EARNINGS_URL)
        day = request.url.params["date"]
        if all_failing or day in (failing_days or set()):
            return httpx.Response(503)
        rows = [FORWARD_ROW] if listed and day == "2026-10-22" else None
        return httpx.Response(200, json={"data": {"rows": rows}})

    return handler


async def test_a_night_that_stores_nothing_pings_nasdaq_once_and_fails_the_job_without_its_own_ping(
    session_factory, notices
):
    sleeps = Sleeps()
    job = _job(
        session_factory,
        "earnings_snapshot",
        handler=_earnings_handler(all_failing=True),
        clock=EVENING,
        sleep=sleeps,
    )

    for _ in range(2):
        with pytest.raises(SourceFailed):
            await job.run()

    assert notices == ["Nasdaq is failing (a calendar day failed). See the ingest logs."]
    assert sleeps.seconds == [30.0, 120.0, 30.0, 120.0]  # the first day alone, asked three times each night


async def test_one_failed_calendar_day_after_its_retries_pings_nasdaq_and_stores_nothing(session_factory, notices):
    sleeps = Sleeps()
    job = _job(
        session_factory, "earnings_snapshot", handler=_earnings_handler({"2026-10-21"}), clock=EVENING, sleep=sleeps
    )

    with pytest.raises(SourceFailed):
        await job.run()

    assert sleeps.seconds == [30.0, 120.0]
    assert len(notices) == 1 and notices[0].startswith("Nasdaq is failing")
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(EarningsSnapshotRow)) == 0


async def test_a_day_that_fails_once_is_retried_and_the_snapshot_is_stored(session_factory, notices):
    attempts: dict[str, int] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        day = request.url.params["date"]
        attempts[day] = attempts.get(day, 0) + 1
        if day == "2026-10-21" and attempts[day] == 1:
            return httpx.Response(503)
        return _earnings_handler()(request)

    sleeps = Sleeps()
    job = _job(session_factory, "earnings_snapshot", handler=handler, clock=EVENING, sleep=sleeps)

    await job.run()

    assert sleeps.seconds == [30.0] and attempts["2026-10-21"] == 2
    assert notices == []
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(EarningsSnapshotRow)) == 1


async def test_a_calendar_listing_nothing_anywhere_is_nasdaq_failing(session_factory, notices):
    job = _job(session_factory, "earnings_snapshot", handler=_earnings_handler(listed=False), clock=EVENING)

    with pytest.raises(SourceFailed):
        await job.run()

    assert notices == ["Nasdaq is failing (no earnings listed). See the ingest logs."]


async def test_a_healthy_snapshot_is_stored_and_recovers_nasdaq(session_factory, notices):
    failing = {"on": True}

    def handler(request: httpx.Request) -> httpx.Response:
        return _earnings_handler(all_failing=failing["on"])(request)

    job = _job(session_factory, "earnings_snapshot", handler=handler, clock=EVENING)
    with pytest.raises(SourceFailed):
        await job.run()

    failing["on"] = False
    await job.run()

    assert [n.split(" (")[0] for n in notices] == ["Nasdaq is failing", "Nasdaq recovered."]
    async with session_factory() as session:
        [row] = (await session.scalars(select(EarningsSnapshotRow))).all()
    assert (row.ticker, row.report_date, row.snapshot_date) == ("AAA", date(2026, 10, 22), date(2026, 10, 20))


async def test_the_snapshot_asks_nothing_when_no_evening_is_due(session_factory, notices):
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(503)

    job = _job(session_factory, "earnings_snapshot", handler=handler)  # 18:00 Eastern, before the slot

    await job.run()

    assert requests == [] and notices == []


async def test_a_failed_apewisdom_listing_pings_apewisdom_and_fails_the_poll_without_its_own_ping(
    session_factory, notices
):
    job = _job(session_factory, "popularity_poll", handler=lambda request: httpx.Response(400))

    for _ in range(2):
        with pytest.raises(SourceFailed):
            await job.run()

    assert notices == ["ApeWisdom is failing (ApeWisdomError). See the ingest logs."]


async def test_an_empty_apewisdom_listing_is_the_source_failing_and_stores_no_boundary(session_factory, notices):
    job = _job(
        session_factory,
        "popularity_poll",
        handler=lambda request: httpx.Response(200, json={"pages": 1, "results": []}),
    )

    with pytest.raises(SourceFailed):
        await job.run()

    assert notices == ["ApeWisdom is failing (ApeWisdomError). See the ingest logs."]
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(PopularitySnapshotRow)) == 0
