"""The supervised cron loop under a fake clock: dispatch, fault isolation, the
Schwab token gate, the heartbeat and graceful shutdown. Time is virtual - the
rig's sleep advances the clock - so hours of schedule run in milliseconds."""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest
from prometheus_client import CollectorRegistry

from rs_ingest.cron import CronTable, Every, Job, Weekly
from rs_ingest.daemon import Daemon, TokenGate
from rs_ingest.metrics import DaemonMetrics
from rs_ingest.monitored import MonitoredPopularitySource
from rs_observability import HealthTracker, QuietFailure, Source, SourceFailed
from rs_sources.schwab import SchwabAuthError

START = datetime(2026, 10, 19, 12, 7, tzinfo=UTC)
SETTLE_TURNS = 30


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, 19, hour, minute, tzinfo=UTC)


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self.current = now

    def now(self) -> datetime:
        return self.current


class Rig:
    """A daemon wired to a fake clock, a scriptable token store and a timeline
    of callbacks that fire as virtual time passes."""

    def __init__(self, start: datetime = START) -> None:
        self.clock = FakeClock(start)
        self.cron = CronTable()
        self.metrics = DaemonMetrics()
        self.registry = CollectorRegistry()
        for collector in self.metrics.collectors():
            self.registry.register(collector)
        self.token_updated_at: datetime | None = None
        self.renewals = 0
        self.renewal_error: Exception | None = None
        self.gate = TokenGate(self, self._renewed, self.clock, self.metrics)
        self.daemon: Daemon
        self._stop_at: datetime | None = None
        self._timeline: list[tuple[datetime, Callable[[], None]]] = []

    async def updated_at(self) -> datetime | None:
        return self.token_updated_at

    async def _renewed(self) -> None:
        if self.renewal_error is not None:
            raise self.renewal_error
        self.renewals += 1

    def build(self, **overrides: object) -> None:
        options: dict = {
            "health": HealthTracker("ingest"),
            "tick_seconds": 60.0,
            "token_poll_seconds": 60.0,
            "sleep": self._sleep,
        }
        self.daemon = Daemon(
            self.cron, self.gate, self.clock, self.metrics, **(options | overrides)
        )

    def when(self, moment: datetime, action: Callable[[], None]) -> None:
        self._timeline.append((moment, action))

    async def _sleep(self, seconds: float) -> None:
        self.clock.current += timedelta(seconds=seconds)
        for _ in range(SETTLE_TURNS):
            await asyncio.sleep(0)
        for moment, action in list(self._timeline):
            if moment <= self.clock.now():
                self._timeline.remove((moment, action))
                action()
        if self._stop_at is not None and self.clock.now() >= self._stop_at:
            self.daemon.request_stop()

    async def run_until(self, moment: datetime) -> None:
        self._stop_at = moment
        await self.daemon.run()

    def sample(self, name: str, **labels: str) -> float | None:
        return self.registry.get_sample_value(name, labels)

    def runs(self, job_name: str, outcome: str) -> float | None:
        return self.sample("cron_job_runs_total", job_name=job_name, outcome=outcome)


class Recorder:
    """A job body that logs when it ran and plays a script of outcomes
    (None = succeed, an exception = raise it; the last entry repeats)."""

    def __init__(self, clock: FakeClock, script: list[Exception | None] | None = None) -> None:
        self._clock = clock
        self._script = script or [None]
        self.started: list[datetime] = []

    async def __call__(self) -> None:
        self.started.append(self._clock.now())
        outcome = self._script[min(len(self.started), len(self._script)) - 1]
        if outcome is not None:
            raise outcome


@pytest.fixture
def rig() -> Rig:
    return Rig()


class TestDispatch:
    async def test_fires_on_utc_half_hour_boundaries(self, rig, notices):
        poll = Recorder(rig.clock)
        rig.cron.register(Job("poll", Every(30), poll))
        rig.build()

        await rig.run_until(at(14, 1))

        assert poll.started == [at(12, 30), at(13, 0), at(13, 30), at(14, 0)]

    async def test_run_on_start_runs_once_then_follows_the_schedule(self, rig, notices):
        catch_up = Recorder(rig.clock)
        rig.cron.register(Job("catch_up", Every(30), catch_up, run_on_start=True))
        rig.build()

        await rig.run_until(at(13, 1))

        assert catch_up.started == [START, at(12, 30), at(13, 0)]

    async def test_a_schwab_catch_up_job_runs_at_start_once_the_token_is_back(self, rig, notices):
        catch_up = Recorder(rig.clock)
        rig.cron.register(Job("catch_up", Every(1440), catch_up, needs_schwab=True, run_on_start=True))
        rig.build()
        await rig.gate.trip("no Schwab token stored")
        rig.when(at(12, 20), lambda: setattr(rig, "token_updated_at", at(12, 15)))

        await rig.run_until(at(12, 40))

        assert len(catch_up.started) == 1
        assert at(12, 20) <= catch_up.started[0] <= at(12, 22)

    async def test_a_late_start_does_not_back_fill_missed_slots(self, notices):
        rig = Rig(start=at(12, 59))
        poll = Recorder(rig.clock)
        rig.cron.register(Job("poll", Every(30), poll))
        rig.build()

        await rig.run_until(at(13, 1))

        assert poll.started == [at(13, 0)]

    async def test_weekly_job_keeps_local_time_across_a_dst_change(self, notices):
        rig = Rig(start=datetime(2026, 3, 2, 14, 0, tzinfo=UTC))
        weekly = Recorder(rig.clock)
        monday_nine = Weekly(0, time(9, 0), ZoneInfo("America/Chicago"))
        rig.cron.register(Job("research", monday_nine, weekly, timeout=timedelta(hours=1)))
        rig.build(tick_seconds=86_400.0)

        await rig.run_until(datetime(2026, 3, 9, 15, 0, tzinfo=UTC))

        assert weekly.started == [
            datetime(2026, 3, 2, 15, 0, tzinfo=UTC),  # 09:00 CST
            datetime(2026, 3, 9, 14, 0, tzinfo=UTC),  # 09:00 CDT, DST began on the 8th
        ]

    async def test_a_run_still_going_at_its_next_slot_is_skipped_not_stacked(self, rig, notices):
        release = asyncio.Event()
        starts: list[datetime] = []

        async def slow() -> None:
            starts.append(rig.clock.now())
            await release.wait()

        rig.cron.register(Job("slow", Every(30), slow, timeout=timedelta(minutes=45)))
        rig.build()
        rig.when(at(13, 10), release.set)

        await rig.run_until(at(13, 31))

        assert starts == [at(12, 30), at(13, 30)]
        assert rig.runs("slow", "skipped_overlap") == 1


class TestFaultIsolation:
    async def test_a_raising_job_does_not_disturb_its_siblings_or_the_loop(self, rig, notices):
        bad = Recorder(rig.clock, [RuntimeError("boom")])
        good = Recorder(rig.clock)
        rig.cron.register(Job("bad", Every(30), bad))
        rig.cron.register(Job("good", Every(30), good))
        rig.build()

        await rig.run_until(at(13, 1))

        assert bad.started == [at(12, 30), at(13, 0)]
        assert good.started == [at(12, 30), at(13, 0)]
        assert rig.runs("bad", "failure") == 2
        assert rig.runs("good", "success") == 2

    async def test_a_hung_job_does_not_stop_other_jobs(self, rig, notices):
        async def hang() -> None:
            await asyncio.Future()

        good = Recorder(rig.clock)
        rig.cron.register(Job("hang", Every(30), hang))
        rig.cron.register(Job("good", Every(30), good))
        rig.build(shutdown_grace_seconds=0.0)

        await rig.run_until(at(13, 31))

        assert good.started == [at(12, 30), at(13, 0), at(13, 30)]

    async def test_a_job_pings_on_its_first_failure_and_once_on_recovery(self, rig, notices):
        failing = [RuntimeError("secret-token")] * 3 + [None]
        flaky = Recorder(rig.clock, failing)
        rig.cron.register(Job("flaky", Every(30), flaky))
        rig.build()

        await rig.run_until(at(14, 1))  # fails 12:30-13:30, recovers 14:00

        assert len(flaky.started) == 4
        assert len(notices) == 2
        assert notices[0].startswith("Ingest job flaky is failing (RuntimeError)")
        assert "secret-token" not in notices[0]  # exception text can carry credentials
        assert notices[1] == "Ingest job flaky recovered."

    async def test_a_job_that_keeps_succeeding_never_pings(self, rig, notices):
        fine = Recorder(rig.clock)
        rig.cron.register(Job("fine", Every(30), fine))
        rig.build()

        await rig.run_until(at(14, 1))

        assert len(fine.started) == 4
        assert notices == []

    async def test_a_job_that_fails_again_after_recovering_pings_again(self, rig, notices):
        script = [RuntimeError("x"), None, RuntimeError("x")]
        flaky = Recorder(rig.clock, script)
        rig.cron.register(Job("flaky", Every(30), flaky))
        rig.build()

        await rig.run_until(at(13, 31))

        assert [n.split(" (")[0] for n in notices] == [
            "Ingest job flaky is failing",
            "Ingest job flaky recovered.",
            "Ingest job flaky is failing",
        ]

    async def test_a_failure_already_pinged_as_a_source_sends_no_job_ping(self, rig, notices):
        failing = [SourceFailed(Source.APEWISDOM)] * 2 + [None]
        poll = Recorder(rig.clock, failing)
        rig.cron.register(Job("poll", Every(30), poll))
        rig.build()

        await rig.run_until(at(14, 1))

        assert len(poll.started) == 4
        assert notices == []
        assert rig.runs("poll", "failure") == 2  # still logged and counted as failed runs

    async def test_one_cause_is_one_ping_from_the_source_through_the_daemon(self, rig, notices):
        health = HealthTracker("ingest")

        class Listing:
            def __init__(self) -> None:
                self.script = [ConnectionError("down")] * 3 + [[object()]]

            async def fetch_all(self):
                outcome = self.script.pop(0) if len(self.script) > 1 else self.script[0]
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome

        source = MonitoredPopularitySource(Listing(), health)
        rig.cron.register(Job("poll", Every(30), source.fetch_all))
        rig.build(health=health)

        await rig.run_until(at(14, 1))  # three failed polls, then one that recovers

        assert rig.runs("poll", "failure") == 3
        assert notices == [
            "ApeWisdom is failing (ConnectionError). See the ingest logs.",
            "ApeWisdom recovered.",
        ]

    async def test_a_quiet_failure_such_as_a_few_stocks_sends_no_job_ping(self, rig, notices):
        poll = Recorder(rig.clock, [QuietFailure("2 of 900 symbols failed"), None])
        rig.cron.register(Job("poll", Every(30), poll))
        rig.build()

        await rig.run_until(at(13, 1))

        assert notices == []
        assert rig.runs("poll", "failure") == 1

    async def test_a_job_failing_for_its_own_reason_after_a_quiet_failure_still_pings(self, rig, notices):
        poll = Recorder(rig.clock, [SourceFailed(Source.SCHWAB), RuntimeError("database down")])
        rig.cron.register(Job("poll", Every(30), poll))
        rig.build()

        await rig.run_until(at(13, 1))

        assert len(notices) == 1
        assert notices[0].startswith("Ingest job poll is failing (RuntimeError)")


class TestTimeout:
    async def test_a_hung_run_is_cancelled_and_the_next_slot_runs(self, rig, notices):
        cancelled: list[datetime] = []
        starts: list[datetime] = []

        async def hang_once() -> None:
            starts.append(rig.clock.now())
            if len(starts) > 1:
                return
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.append(rig.clock.now())
                raise

        rig.cron.register(Job("hang", Every(30), hang_once))  # timeout defaults to 30 minutes
        rig.build()

        await rig.run_until(at(13, 31))

        assert cancelled == [at(13, 0)]
        assert starts == [at(12, 30), at(13, 0), at(13, 30)]
        assert rig.runs("hang", "timeout") == 1
        assert rig.runs("hang", "success") == 2

    async def test_a_timeout_pings_once_and_the_next_success_recovers(self, rig, notices):
        starts: list[datetime] = []

        async def hang_then_recover() -> None:
            starts.append(rig.clock.now())
            if len(starts) <= 3:
                await asyncio.Future()

        rig.cron.register(Job("hang", Every(30), hang_then_recover))
        rig.build()

        await rig.run_until(at(14, 31))  # timeouts at 13:00, 13:30, 14:00; success at 14:30

        assert len(notices) == 2
        assert notices[0].startswith("Ingest job hang is failing (TimeoutError)")
        assert notices[1] == "Ingest job hang recovered."

    async def test_a_run_within_its_timeout_still_skips_the_overlapping_slot(self, rig, notices):
        starts: list[datetime] = []

        async def hang() -> None:
            starts.append(rig.clock.now())
            await asyncio.Future()

        rig.cron.register(Job("hang", Every(30), hang, timeout=timedelta(minutes=45)))
        rig.build(shutdown_grace_seconds=0.0)

        await rig.run_until(at(13, 31))

        # 13:00 overlaps the 12:30 run (deadline 13:15); 13:30 follows its timeout.
        assert starts == [at(12, 30), at(13, 30)]
        assert rig.runs("hang", "skipped_overlap") == 1
        assert rig.runs("hang", "timeout") == 1

    async def test_the_cancelled_run_does_not_end_the_daemon_or_its_siblings(self, rig, notices):
        async def hang() -> None:
            await asyncio.Future()

        good = Recorder(rig.clock)
        rig.cron.register(Job("hang", Every(30), hang))
        rig.cron.register(Job("good", Every(30), good))
        rig.build(shutdown_grace_seconds=0.0)

        await rig.run_until(at(14, 1))

        assert good.started == [at(12, 30), at(13, 0), at(13, 30), at(14, 0)]


class TestTokenGate:
    async def test_auth_error_pauses_schwab_jobs_and_keeps_the_rest_running(self, rig, notices):
        schwab = Recorder(rig.clock, [None, SchwabAuthError("refresh token rejected")])
        poll = Recorder(rig.clock)
        rig.cron.register(Job("bars", Every(30), schwab, needs_schwab=True))
        rig.cron.register(Job("poll", Every(30), poll))
        rig.token_updated_at = at(12, 45)  # older than the 13:00 failure: not a renewal
        rig.build()

        await rig.run_until(at(14, 1))

        assert schwab.started == [at(12, 30), at(13, 0)]
        assert poll.started == [at(12, 30), at(13, 0), at(13, 30), at(14, 0)]
        assert rig.gate.dead
        assert len(notices) == 1
        assert "Schwab token is dead" in notices[0]
        assert rig.sample("schwab_token_dead") == 1
        assert rig.runs("bars", "skipped_token_dead") == 2
        assert rig.runs("bars", "failure") == 0  # a dead token is not a job failure streak

    async def test_a_token_newer_than_the_failure_resumes_schwab_jobs(self, rig, notices):
        schwab = Recorder(rig.clock, [None, SchwabAuthError("rejected")] + [None] * 10)
        rig.cron.register(Job("bars", Every(30), schwab, needs_schwab=True))
        rig.build()
        rig.when(at(14, 10), lambda: setattr(rig, "token_updated_at", at(14, 5)))

        await rig.run_until(at(14, 31))

        assert schwab.started == [at(12, 30), at(13, 0), at(14, 30)]
        assert not rig.gate.dead
        assert rig.renewals == 1
        assert [("dead" in n) or ("renewed" in n) for n in notices] == [True, True]
        assert rig.sample("schwab_token_dead") == 0

    async def test_auth_error_from_a_job_not_marked_needs_schwab_still_trips_the_gate(
        self, rig, notices
    ):
        sneaky = Recorder(rig.clock, [SchwabAuthError("rejected")])
        rig.cron.register(Job("sneaky", Every(30), sneaky))
        rig.build()

        await rig.run_until(at(12, 31))

        assert rig.gate.dead

    async def test_a_failed_client_rebuild_keeps_the_gate_closed_and_retries(self, rig, notices):
        schwab = Recorder(rig.clock, [SchwabAuthError("rejected")] + [None] * 10)
        rig.cron.register(Job("bars", Every(30), schwab, needs_schwab=True))
        rig.token_updated_at = at(12, 40)
        rig.renewal_error = RuntimeError("token row unreadable")
        rig.build()
        rig.when(at(13, 10), lambda: setattr(rig, "renewal_error", None))

        await rig.run_until(at(13, 31))

        assert rig.renewals == 1
        assert schwab.started == [at(12, 30), at(13, 30)]

    async def test_starts_dead_when_no_token_is_stored_and_resumes_when_one_appears(
        self, rig, notices
    ):
        schwab = Recorder(rig.clock)
        poll = Recorder(rig.clock)
        rig.cron.register(Job("bars", Every(30), schwab, needs_schwab=True))
        rig.cron.register(Job("poll", Every(30), poll))
        rig.build()
        await rig.gate.trip("no Schwab token stored")
        rig.when(at(13, 10), lambda: setattr(rig, "token_updated_at", at(13, 5)))

        await rig.run_until(at(13, 31))

        assert schwab.started == [at(13, 30)]
        assert poll.started == [at(12, 30), at(13, 0), at(13, 30)]
        assert len(notices) == 2  # dead at start, renewed later


class TestHeartbeat:
    async def test_loop_tick_and_per_job_success_are_exposed(self, rig, notices):
        poll = Recorder(rig.clock)
        rig.cron.register(Job("poll", Every(30), poll))
        rig.build(tick_seconds=15.0)

        await rig.run_until(at(12, 40))

        last_tick = rig.sample("daemon_loop_last_tick_timestamp_seconds")
        assert last_tick is not None
        assert rig.clock.now().timestamp() - last_tick <= 15.0
        assert rig.sample("cron_job_last_success_timestamp_seconds", job_name="poll") == (
            at(12, 30).timestamp()
        )

    async def test_job_series_exist_before_the_first_run(self, rig, notices):
        rig.cron.register(Job("poll", Every(30), Recorder(rig.clock)))
        rig.build()

        await rig.run_until(at(12, 8))

        assert rig.runs("poll", "success") == 0
        assert rig.runs("poll", "failure") == 0


class TestShutdown:
    async def test_an_in_flight_job_finishes_before_finalizers_run(self, rig, notices):
        order: list[str] = []
        release = asyncio.Event()

        async def job() -> None:
            await release.wait()
            order.append("job finished")

        async def finalizer() -> None:
            order.append("finalizer")

        rig.cron.register(Job("slow", Every(30), job))
        rig.build(finalizers=[finalizer], shutdown_grace_seconds=5.0)
        rig.when(at(12, 40), rig.daemon.request_stop)
        asyncio.get_running_loop().call_later(0.05, release.set)

        await rig.daemon.run()

        assert order == ["job finished", "finalizer"]

    async def test_a_job_outliving_the_grace_is_cancelled(self, rig, notices):
        cancelled: list[bool] = []
        finalized: list[bool] = []

        async def hang() -> None:
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

        async def finalizer() -> None:
            finalized.append(True)

        rig.cron.register(Job("hang", Every(30), hang))
        rig.build(finalizers=[finalizer], shutdown_grace_seconds=0.05)
        rig.when(at(12, 40), rig.daemon.request_stop)

        await rig.daemon.run()

        assert cancelled == [True]
        assert finalized == [True]

    async def test_finalizers_run_when_the_loop_crashes(self, rig, notices):
        finalized: list[bool] = []

        async def finalizer() -> None:
            finalized.append(True)

        async def broken_sleep(seconds: float) -> None:
            raise RuntimeError("loop bug")

        rig.build(finalizers=[finalizer], sleep=broken_sleep)

        with pytest.raises(RuntimeError, match="loop bug"):
            await rig.daemon.run()

        assert finalized == [True]

    async def test_finalizers_run_when_the_daemon_is_cancelled(self, rig, notices):
        finalized: list[bool] = []

        async def finalizer() -> None:
            finalized.append(True)

        rig.build(finalizers=[finalizer], sleep=asyncio.sleep)
        task = asyncio.create_task(rig.daemon.run())
        await asyncio.sleep(0.01)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert finalized == [True]

    async def test_a_failing_finalizer_does_not_skip_the_next(self, rig, notices):
        finalized: list[str] = []

        async def failing() -> None:
            raise RuntimeError("drain failed")

        async def second() -> None:
            finalized.append("second")

        rig.build(finalizers=[failing, second])
        rig.daemon.request_stop()

        await rig.daemon.run()

        assert finalized == ["second"]

    async def test_a_stop_request_interrupts_a_long_sleep(self, rig, notices):
        rig.build(sleep=asyncio.sleep, tick_seconds=3600.0)
        task = asyncio.create_task(rig.daemon.run())
        await asyncio.sleep(0.01)

        rig.daemon.request_stop()

        await asyncio.wait_for(task, timeout=1.0)

