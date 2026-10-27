"""The supervised cron loop and the Schwab token gate.

One loop dispatches due jobs; each run is its own task and fault boundary, so a
raising or hanging job never stalls the loop or its siblings. The loop ticks at
least every `tick_seconds` even when nothing is due: that tick is the heartbeat
the container healthcheck reads.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from rs_domain import Clock
from rs_ingest.cron import CronTable, Job
from rs_ingest.metrics import DaemonMetrics, Outcome
from rs_observability import HealthTracker, QuietFailure, notify_ops, unit_of_work
from rs_sources.schwab import SchwabAuthError

logger = logging.getLogger(__name__)

Sleep = Callable[[float], Awaitable[None]]


class TokenFreshness(Protocol):
    """Reads when the stored Schwab token was last written."""

    async def updated_at(self) -> datetime | None: ...


class TokenGate:
    """Shared "Schwab token is dead" state.

    A SchwabAuthError from any job trips it; needs_schwab jobs are skipped
    while it is tripped. It clears when the stored token is newer than the
    failure (polled) or when `renewed()` is called directly, after
    `on_renewed` has rebuilt whatever holds the old token.
    """

    def __init__(
        self,
        freshness: TokenFreshness,
        on_renewed: Callable[[], Awaitable[None]],
        clock: Clock,
        metrics: DaemonMetrics,
    ) -> None:
        self._freshness = freshness
        self._on_renewed = on_renewed
        self._clock = clock
        self._metrics = metrics
        self._dead_since: datetime | None = None

    @property
    def dead(self) -> bool:
        return self._dead_since is not None

    async def trip(self, reason: str) -> None:
        if self._dead_since is not None:
            return
        self._dead_since = self._clock.now()
        self._metrics.token_dead.set(1)
        logger.error("Schwab token dead - pausing Schwab jobs: %s", reason)
        await notify_ops(
            f"Schwab token is dead ({reason}). Schwab jobs are paused and other jobs keep "
            "running. Run /setup schwab in Discord to resume.",
            key="schwab-token-dead",
        )

    async def poll(self) -> None:
        dead_since = self._dead_since
        if dead_since is None:
            return
        try:
            updated_at = await self._freshness.updated_at()
        except Exception:
            logger.exception("could not read the Schwab token age; gate stays closed")
            return
        if updated_at is not None and updated_at > dead_since:
            await self.renewed()

    async def renewed(self) -> None:
        if self._dead_since is None:
            return
        try:
            await self._on_renewed()
        except Exception:
            logger.exception("could not rebuild the Schwab client from the new token; gate stays closed")
            return
        self._dead_since = None
        self._metrics.token_dead.set(0)
        logger.info("Schwab token renewed - resuming Schwab jobs")
        await notify_ops("Schwab token renewed. Schwab jobs resumed.", key="schwab-token-renewed")


@dataclass
class _JobState:
    job: Job
    next_fire: datetime
    task: asyncio.Task[None] | None = None
    deadline: datetime | None = None  # when the run in `task` times out
    # One run is owed: the startup run of a run_on_start job, a slot that came due while the
    # job was still running or the Schwab token was dead, or a Schwab run the dead token killed.
    # Slots missed meanwhile coalesce into it.
    run_pending: bool = False


def _subject(job: Job) -> str:
    return f"Ingest job {job.name}"


class Daemon:
    def __init__(
        self,
        cron: CronTable,
        gate: TokenGate,
        clock: Clock,
        metrics: DaemonMetrics,
        *,
        health: HealthTracker,
        sleep: Sleep = asyncio.sleep,
        tick_seconds: float = 15.0,
        token_poll_seconds: float = 60.0,
        shutdown_grace_seconds: float = 30.0,
        finalizers: Sequence[Callable[[], Awaitable[None]]] = (),
    ) -> None:
        self._cron = cron
        self._gate = gate
        self._clock = clock
        self._metrics = metrics
        self._sleep = sleep
        self._health = health
        self._tick = timedelta(seconds=tick_seconds)
        self._token_poll = timedelta(seconds=token_poll_seconds)
        self._grace = shutdown_grace_seconds
        self._finalizers = finalizers
        self._stop = asyncio.Event()
        self._inflight: set[asyncio.Task[None]] = set()
        self._timed_out: set[asyncio.Task[None]] = set()
        self._gate_poll: asyncio.Task[None] | None = None
        self._next_gate_poll: datetime | None = None

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        """Run until `request_stop()`; finalizers run on every exit path."""
        try:
            states = self._initial_states()
            while not self._stop.is_set():
                now = self._clock.now()
                self._metrics.loop_last_tick.set(now.timestamp())
                self._expire_timeouts(states, now)
                self._dispatch_due(states, now)
                self._maybe_poll_gate(now)
                await self._wait(self._time_to_next_event(states))
        finally:
            await self._shutdown()

    def _initial_states(self) -> list[_JobState]:
        now = self._clock.now()
        states = []
        for job in self._cron:
            self._metrics.register_job(job.name)
            states.append(_JobState(job, job.schedule.next_after(now), run_pending=job.run_on_start))
        return states

    def _dispatch_due(self, states: list[_JobState], now: datetime) -> None:
        for state in states:
            name = state.job.name
            running = state.task is not None and not state.task.done()
            if state.next_fire <= now:
                # Slots missed while the daemon was down are dropped, not replayed.
                state.next_fire = state.job.schedule.next_after(now)
                if running:
                    logger.warning("job %s still running at its next slot - running it again when it finishes", name)
                    self._metrics.job_runs.labels(name, Outcome.DEFERRED_OVERLAP).inc()
                elif self._paused(state.job):
                    logger.info("job %s due while the Schwab token is dead - running it once the token is back", name)
                    self._metrics.job_runs.labels(name, Outcome.DEFERRED_TOKEN_DEAD).inc()
                state.run_pending = True
            if state.run_pending and not running and not self._paused(state.job):
                state.run_pending = False
                self._start(state, now)

    def _start(self, state: _JobState, now: datetime) -> None:
        state.deadline = now + state.job.time_limit
        state.task = self._spawn(self._execute(state), state.job.name)

    def _expire_timeouts(self, states: list[_JobState], now: datetime) -> None:
        for state in states:
            task = state.task
            if task is None or task.done() or state.deadline is None or now < state.deadline:
                continue
            name = state.job.name
            logger.error("job %s exceeded its %s timeout - cancelling", name, state.job.time_limit)
            self._metrics.job_runs.labels(name, Outcome.TIMEOUT).inc()
            self._timed_out.add(task)
            task.cancel()
            # The slot is free at once; the cancelled run unwinds on its own.
            state.task = None
            self._spawn(self._record_failure(state, TimeoutError()), f"{name}-timeout")

    def _paused(self, job: Job) -> bool:
        return job.needs_schwab and self._gate.dead

    def _maybe_poll_gate(self, now: datetime) -> None:
        if not self._gate.dead:
            self._next_gate_poll = None
            return
        if self._next_gate_poll is None:
            self._next_gate_poll = now + self._token_poll
        if now < self._next_gate_poll:
            return
        self._next_gate_poll = now + self._token_poll
        if self._gate_poll is None or self._gate_poll.done():
            self._gate_poll = self._spawn(self._gate.poll(), "token-gate-poll")

    def _time_to_next_event(self, states: list[_JobState]) -> float:
        now = self._clock.now()
        horizon = now + self._tick
        for state in states:
            horizon = min(horizon, state.next_fire)
            if state.task is not None and not state.task.done() and state.deadline is not None:
                horizon = min(horizon, state.deadline)
        if self._gate.dead and self._next_gate_poll is not None:
            horizon = min(horizon, self._next_gate_poll)
        return max(0.0, (horizon - now).total_seconds())

    def _spawn(self, coro: Coroutine[Any, Any, None], name: str) -> asyncio.Task[None]:
        task = asyncio.create_task(coro, name=name)
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)
        task.add_done_callback(self._timed_out.discard)
        return task

    async def _wait(self, seconds: float) -> None:
        """Sleep, but wake at once for a stop request."""
        sleeper = asyncio.ensure_future(self._sleep(seconds))
        stopper = asyncio.ensure_future(self._stop.wait())
        try:
            await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
            if sleeper.done():
                sleeper.result()
        finally:
            for task in (sleeper, stopper):
                task.cancel()
            await asyncio.gather(sleeper, stopper, return_exceptions=True)

    async def _execute(self, state: _JobState) -> None:
        job = state.job
        with unit_of_work(job_name=job.name):
            try:
                await job.run()
            except asyncio.CancelledError:
                if asyncio.current_task() in self._timed_out:
                    return  # cancelled by the timeout check, which already recorded it
                raise
            except SchwabAuthError as exc:
                if job.needs_schwab:
                    # The run died half done: it is owed again once the gate reopens. A job that is
                    # not paused by the gate must not be re-queued, or it would loop while dead.
                    state.run_pending = True
                self._metrics.job_runs.labels(job.name, Outcome.AUTH_ERROR).inc()
                await self._gate.trip(f"job {job.name}: {exc}")
            except Exception as exc:
                logger.exception("job %s failed", job.name)
                self._metrics.job_runs.labels(job.name, Outcome.FAILURE).inc()
                await self._record_failure(state, exc)
            else:
                self._metrics.job_runs.labels(job.name, Outcome.SUCCESS).inc()
                self._metrics.job_last_success.labels(job.name).set(self._clock.now().timestamp())
                await self._record_success(state)

    async def _record_failure(self, state: _JobState, exc: Exception) -> None:
        if isinstance(exc, QuietFailure):
            return  # its cause is a source's or a stock's, which are not job failures to ping about
        # The type only: exception text can carry URLs or credentials.
        await self._health.report_failed(_subject(state.job), type(exc).__name__)

    async def _record_success(self, state: _JobState) -> None:
        await self._health.report_ok(_subject(state.job))

    async def _shutdown(self) -> None:
        # In-flight work gets the grace period, then is cancelled.
        pending = set(self._inflight)
        if pending:
            _, pending = await asyncio.wait(pending, timeout=self._grace)
        for task in pending:
            logger.warning("cancelling %s after the %.0fs shutdown grace", task.get_name(), self._grace)
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for finalizer in self._finalizers:
            try:
                await finalizer()
            except Exception:
                logger.exception("shutdown finalizer failed")
