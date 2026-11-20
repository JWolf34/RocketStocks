"""The job registry: what register_jobs schedules and how a job reaches Schwab."""

from datetime import UTC, date, datetime, time, timedelta

import httpx
import pytest
from sqlalchemy import func, select

from rs_domain import EASTERN, ChainPass, FetchStatus
from rs_ingest.cron import CronTable, Daily, Every
from rs_ingest.jobs import Dependencies, register_jobs
from rs_ingest.rate_limit import RateLimiter
from rs_observability import HealthTracker, QuietFailure, SourceFailed
from rs_persistence import ChainRepository, EarningsSnapshotRow, PopularitySnapshotRow
from rs_sources import ExchangeCalendar
from rs_sources.nasdaq import EARNINGS_URL
from rs_sources.schwab import SchwabAuthError

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


class OutcomesRuns:
    """Stands in for the Outcomes subprocess: notes in `calls` that it ran."""

    def __init__(self, calls: list[str] | None = None, error: Exception | None = None) -> None:
        self.calls = calls if calls is not None else []
        self.error = error

    async def __call__(self, db_url: str) -> None:
        self.calls.append("outcomes")
        if self.error is not None:
            raise self.error


def _deps(
    session_factory,
    schwab,
    http: httpx.AsyncClient | None = None,
    clock=None,
    sleep=None,
    outcomes: OutcomesRuns | None = None,
) -> Dependencies:
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
        run_outcomes=outcomes or OutcomesRuns(),
    )


def _job(session_factory, name: str, *, schwab=None, handler=None, clock=None, sleep=None, outcomes=None):
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler)) if handler else None
    cron = CronTable()
    register_jobs(
        cron, _deps(session_factory, SwappableSession(schwab or RecordingClient()), http, clock, sleep, outcomes)
    )
    return next(job for job in cron if job.name == name)


@pytest.fixture(autouse=True)
def no_request_spacing(monkeypatch):
    async def instant(self) -> None: ...

    monkeypatch.setattr("rs_sources._ratelimit.MinInterval.wait", instant)


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
    register_jobs(cron, _deps(session_factory, session, clock=EVE))
    job = next(job for job in cron if job.name == "evening_sweep")

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
    job = _job(session_factory, "evening_sweep", clock=EVE)  # only SPY is needed, and the recording client fails it

    with pytest.raises(QuietFailure):
        await job.run()

    assert notices == []


async def test_schwab_rate_limiting_the_bars_run_pings_schwab_once_and_fails_the_job_quietly(
    session_factory, notices
):
    job = _job(session_factory, "evening_sweep", schwab=RespondingClient(429), clock=EVE)

    for _ in range(2):
        with pytest.raises(SourceFailed):
            await job.run()

    assert len(notices) == 1
    assert notices[0].startswith("Schwab is failing (1 of 1 requests failed)")


async def test_a_symbol_schwab_refuses_is_not_a_failure_and_does_not_fail_the_bars_run(session_factory, notices):
    job = _job(session_factory, "evening_sweep", schwab=RespondingClient(404), clock=EVE)

    await job.run()

    assert notices == []


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now


# Tuesday 2026-10-20, 18:30 Eastern: the evening has started. Chains are owed until 19:00 Eastern
# (23:00 UTC) and the bars stop at 03:00 Central (08:00 UTC the next day).
EVENING_SESSION = date(2026, 10, 20)
EVE = FakeClock(datetime(2026, 10, 20, 22, 30, tzinfo=UTC))
# The same evening at 19:30 Eastern, and the next day at 07:30 and 10:00 Eastern.
LATE_EVE = FakeClock(datetime(2026, 10, 20, 23, 30, tzinfo=UTC))
MORNING = FakeClock(datetime(2026, 10, 21, 11, 30, tzinfo=UTC))
AFTER_OPEN = FakeClock(datetime(2026, 10, 21, 14, 0, tzinfo=UTC))


async def test_the_evening_sweep_runs_daily_from_the_evenings_start_and_once_at_startup(session_factory):
    cron = CronTable()
    register_jobs(cron, _deps(session_factory, SwappableSession(RecordingClient())))

    job = next(job for job in cron if job.name == "evening_sweep")

    assert job.schedule == Daily(time(18, 5), EASTERN)
    assert job.needs_schwab and job.run_on_start
    assert job.time_limit == timedelta(hours=13)
    # The Universe and the earnings snapshot are their own jobs, beside it; the passes it
    # sequences are no jobs of their own.
    assert {j.name for j in cron} == {
        "evening_sweep",
        "morning_open_interest",
        "universe_snapshot",
        "earnings_snapshot",
        "popularity_poll",
    }


async def test_the_morning_pass_runs_at_7am_eastern_every_day_and_once_at_startup(session_factory):
    cron = CronTable()
    register_jobs(cron, _deps(session_factory, SwappableSession(RecordingClient())))

    job = next(job for job in cron if job.name == "morning_open_interest")

    assert job.schedule == Daily(time(7, 0), EASTERN)
    assert job.needs_schwab and job.run_on_start
    # About 2.5 hours from the slot to the open, with room past it.
    assert job.time_limit == timedelta(hours=3)


class EveningClient:
    """A Schwab client for the whole evening: every call goes into `calls`, in order."""

    class Options:
        class ContractType:
            ALL = "ALL"

    def __init__(self, calls: list[str] | None = None) -> None:
        self.calls = calls if calls is not None else []
        self.chain_error: Exception | None = None
        # When set, only this underlying's chain raises `chain_error`.
        self.chain_error_for: str | None = None

    async def get_option_chain(self, symbol, *, contract_type=None, from_date=None, to_date=None):
        self.calls.append(f"chain {symbol}")
        if self.chain_error is not None and self.chain_error_for in (None, symbol):
            raise self.chain_error
        empty = {"symbol": symbol, "status": "SUCCESS", "underlyingPrice": 10.0, "callExpDateMap": {}, "putExpDateMap": {}}
        return httpx.Response(200, json=empty)

    async def get_price_history_every_day(self, symbol, **kwargs):
        self.calls.append(f"bars {symbol}")
        return httpx.Response(200, json={"symbol": symbol, "empty": True, "candles": []})


async def _recorded(session_factory, chain_pass: ChainPass, symbol: str):
    async with session_factory() as s:
        return await ChainRepository(s).record(EVENING_SESSION, chain_pass, symbol)


async def test_the_evening_runs_chains_then_bars_then_outcomes_each_after_the_one_before(
    session_factory, seed_previous
):
    await seed_previous(day=EVENING_SESSION)  # AAA and OLD are optionable; BBB, CCC and DDD are not
    calls: list[str] = []

    job = _job(session_factory, "evening_sweep", schwab=EveningClient(calls), clock=EVE, outcomes=OutcomesRuns(calls))
    await job.run()

    assert calls == [
        "chain AAA",
        "chain OLD",
        *(f"bars {symbol}" for symbol in ("SPY", "AAA", "BBB", "CCC", "DDD", "OLD")),
        "outcomes",
    ]
    for symbol in ("AAA", "OLD"):
        assert (await _recorded(session_factory, ChainPass.EVENING, symbol)).status is FetchStatus.EMPTY


async def test_a_restart_after_the_chain_deadline_tombstones_the_chains_and_still_runs_bars_and_outcomes(
    session_factory, seed_previous
):
    await seed_previous(day=EVENING_SESSION)
    calls: list[str] = []

    job = _job(
        session_factory, "evening_sweep", schwab=EveningClient(calls), clock=LATE_EVE, outcomes=OutcomesRuns(calls)
    )
    with pytest.raises(SourceFailed):  # every underlying a Tombstone: the night's chains failed
        await job.run()

    assert "chain AAA" not in calls and "chain OLD" not in calls
    assert calls[0] == "bars SPY" and calls[-1] == "outcomes"
    for symbol in ("AAA", "OLD"):
        assert (await _recorded(session_factory, ChainPass.EVENING, symbol)).status is FetchStatus.FAILED


async def test_a_restart_after_the_bars_stop_skips_recorded_chains_and_the_bars_and_reruns_outcomes(
    session_factory, seed_previous
):
    await seed_previous(day=EVENING_SESSION)
    calls: list[str] = []
    await _job(
        session_factory, "evening_sweep", schwab=EveningClient(calls), clock=EVE, outcomes=OutcomesRuns(calls)
    ).run()
    calls.clear()

    # 07:30 Eastern the next morning: the chains are recorded and the bars' stop (04:00 Eastern) has passed.
    await _job(
        session_factory, "evening_sweep", schwab=EveningClient(calls), clock=MORNING, outcomes=OutcomesRuns(calls)
    ).run()

    assert calls == ["outcomes"]


async def test_a_dead_token_ends_the_evening_at_once_and_the_rerun_resumes_it(session_factory, seed_previous):
    await seed_previous(day=EVENING_SESSION)
    calls: list[str] = []
    client = EveningClient(calls)
    client.chain_error = SchwabAuthError("refresh token rejected")
    outcomes = OutcomesRuns(calls)

    with pytest.raises(SchwabAuthError):
        await _job(session_factory, "evening_sweep", schwab=client, clock=EVE, outcomes=outcomes).run()

    assert calls == ["chain AAA"]  # no bars, no Outcomes, and no Tombstone for the underlying it died on
    assert await _recorded(session_factory, ChainPass.EVENING, "AAA") is None

    client.chain_error = None  # signed in again
    calls.clear()
    await _job(session_factory, "evening_sweep", schwab=client, clock=EVE, outcomes=outcomes).run()

    assert calls[:2] == ["chain AAA", "chain OLD"] and calls[-1] == "outcomes"


async def test_a_rerun_after_a_dead_token_fetches_only_the_underlyings_not_yet_recorded(
    session_factory, seed_previous
):
    await seed_previous(day=EVENING_SESSION)
    calls: list[str] = []
    client = EveningClient(calls)
    client.chain_error, client.chain_error_for = SchwabAuthError("refresh token rejected"), "OLD"

    with pytest.raises(SchwabAuthError):
        await _job(session_factory, "evening_sweep", schwab=client, clock=EVE).run()
    assert await _recorded(session_factory, ChainPass.EVENING, "AAA") is not None
    assert await _recorded(session_factory, ChainPass.EVENING, "OLD") is None

    client.chain_error = None
    calls.clear()
    await _job(session_factory, "evening_sweep", schwab=client, clock=EVE).run()

    assert calls[0] == "chain OLD" and "chain AAA" not in calls
    assert calls[1] == "bars SPY"


async def test_a_failed_stage_does_not_hold_back_the_stages_after_it(session_factory, seed_previous):
    await seed_previous(day=EVENING_SESSION)
    calls: list[str] = []
    outcomes = OutcomesRuns(calls, error=RuntimeError("the outcomes job exited with status 1"))

    job = _job(session_factory, "evening_sweep", schwab=EveningClient(calls), clock=EVE, outcomes=outcomes)
    with pytest.raises(RuntimeError, match="status 1"):
        await job.run()

    assert "bars SPY" in calls and calls[-1] == "outcomes"


async def test_the_morning_pass_records_open_interest_for_the_session_it_settled(session_factory, seed_previous):
    await seed_previous(day=EVENING_SESSION)
    calls: list[str] = []

    await _job(session_factory, "morning_open_interest", schwab=EveningClient(calls), clock=MORNING).run()

    assert calls == ["chain AAA", "chain OLD"]
    for symbol in ("AAA", "OLD"):
        assert (await _recorded(session_factory, ChainPass.MORNING, symbol)).status is FetchStatus.EMPTY


async def test_a_morning_started_after_the_open_records_the_unfetched_chains_as_tombstones(
    session_factory, seed_previous
):
    await seed_previous(day=EVENING_SESSION)
    calls: list[str] = []

    with pytest.raises(SourceFailed):  # every underlying a Tombstone: the morning's pass failed
        await _job(session_factory, "morning_open_interest", schwab=EveningClient(calls), clock=AFTER_OPEN).run()

    assert calls == []
    for symbol in ("AAA", "OLD"):
        assert (await _recorded(session_factory, ChainPass.MORNING, symbol)).status is FetchStatus.FAILED


async def test_a_dead_token_in_the_morning_propagates_so_the_daemon_pauses_it(session_factory, seed_previous):
    await seed_previous(day=EVENING_SESSION)
    client = EveningClient()
    client.chain_error = SchwabAuthError("refresh token rejected")

    with pytest.raises(SchwabAuthError):
        await _job(session_factory, "morning_open_interest", schwab=client, clock=MORNING).run()


async def test_a_morning_failing_above_the_share_pings_schwab_once(session_factory, seed_previous, notices):
    await seed_previous(day=EVENING_SESSION)
    client = EveningClient()
    client.chain_error = RuntimeError("bad gateway")  # every ladder rung fails: both underlyings are Tombstones

    job = _job(session_factory, "morning_open_interest", schwab=client, clock=MORNING)
    with pytest.raises(SourceFailed):
        await job.run()
    await job.run()  # the rerun finds every underlying recorded and judges nothing

    assert len(notices) == 1 and notices[0].startswith("Schwab is failing (2 of 2 requests failed)")


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


async def test_a_restart_the_morning_after_a_missed_slot_takes_that_evenings_snapshot_once(session_factory, notices):
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.params["date"])
        return _earnings_handler()(request)

    # Ingest was down over Tuesday's 20:30 slot and starts again at 07:30 Eastern on Wednesday.
    await _job(session_factory, "earnings_snapshot", handler=handler, clock=MORNING).run()

    async with session_factory() as session:
        [row] = (await session.scalars(select(EarningsSnapshotRow))).all()
    assert (row.snapshot_date, row.fetched_at) == (EVENING_SESSION, MORNING.now())
    assert requests[0] == "2026-10-13"  # Tuesday evening's window, a week back from the 20th

    # Another restart that morning finds Tuesday's snapshot and asks Nasdaq nothing.
    requests.clear()
    await _job(session_factory, "earnings_snapshot", handler=handler, clock=AFTER_OPEN).run()

    assert requests == [] and notices == []
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(EarningsSnapshotRow)) == 1


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
