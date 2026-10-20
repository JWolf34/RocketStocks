"""The cron table: what runs when. Pure data and schedule arithmetic - the
daemon (rs_ingest.daemon) owns dispatch, fault isolation and the Schwab gate.

Register a job with `CronTable.register(Job(...))`. The job's `run` is a
zero-argument coroutine function; close over whatever it needs. A scheduled
slot that passes while the daemon is down or the job is still running is
skipped, never back-filled; set `run_on_start` for a job that must run once at
startup to catch up (it still runs one time, not once per missed slot). A run
that exceeds its `timeout` is cancelled and counts as a failure.
"""

from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

MINUTES_PER_DAY = 24 * 60


class Schedule(Protocol):
    def next_after(self, now: datetime) -> datetime:
        """The first fire instant strictly after `now` (tz-aware UTC)."""
        ...

    @property
    def default_timeout(self) -> timedelta | None:
        """The run timeout a job gets when it sets none; None means it must."""
        ...


@dataclass(frozen=True)
class Every:
    """Every `minutes`, aligned to UTC midnight boundaries (30 -> :00 and :30)."""

    minutes: int

    def __post_init__(self) -> None:
        # A divisor of the day keeps the grid identical on every day.
        if not 1 <= self.minutes <= MINUTES_PER_DAY or MINUTES_PER_DAY % self.minutes:
            raise ValueError(f"minutes must divide {MINUTES_PER_DAY}, got {self.minutes}")

    def next_after(self, now: datetime) -> datetime:
        now = now.astimezone(UTC)
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        step = timedelta(minutes=self.minutes)
        return midnight + ((now - midnight) // step + 1) * step

    @property
    def default_timeout(self) -> timedelta:
        return timedelta(minutes=self.minutes)


def _wall_time_utc(day: date, at: time, zone: ZoneInfo) -> datetime:
    """The instant of wall-clock `at` on `day` in `zone`. A time the zone skips
    resolves to the equivalent instant after the gap; a repeated one to its first occurrence."""
    return datetime.combine(day, at, tzinfo=zone).astimezone(UTC)


@dataclass(frozen=True)
class Daily:
    """Every day at the wall-clock time `at` in `zone` (DST rules as for `Weekly`).

    Every calendar day, sessions or not: a job that must act only after a
    session works out from the exchange calendar what is due, so a weekend or
    holiday run finds nothing to do.
    """

    at: time
    zone: ZoneInfo

    def __post_init__(self) -> None:
        if self.at.tzinfo is not None:
            raise ValueError("`at` is wall-clock time in `zone`; pass it without a tzinfo")

    def next_after(self, now: datetime) -> datetime:
        today = now.astimezone(self.zone).date()
        # A candidate on `today` can already be past; tomorrow's never is.
        for offset in range(2):
            candidate = _wall_time_utc(today + timedelta(days=offset), self.at, self.zone)
            if candidate > now:
                return candidate
        raise AssertionError("unreachable: a daily slot exists in every 2-day window")

    @property
    def default_timeout(self) -> None:
        return None  # a daily job has no natural bound, so the job must choose one


@dataclass(frozen=True)
class Weekly:
    """Weekly on `weekday` (Monday = 0) at the wall-clock time `at` in `zone`.

    A wall time that does not exist on a spring-forward day fires at the
    equivalent instant after the gap (02:30 becomes 03:30); a repeated
    fall-back time fires on its first occurrence.
    """

    weekday: int
    at: time
    zone: ZoneInfo

    def __post_init__(self) -> None:
        if not 0 <= self.weekday <= 6:
            raise ValueError(f"weekday must be 0 (Monday) to 6, got {self.weekday}")
        if self.at.tzinfo is not None:
            raise ValueError("`at` is wall-clock time in `zone`; pass it without a tzinfo")

    def next_after(self, now: datetime) -> datetime:
        today = now.astimezone(self.zone).date()
        # The slot is at most 7 days out, and a candidate on `today` can already be past.
        for offset in range(8):
            day = today + timedelta(days=offset)
            if day.weekday() != self.weekday:
                continue
            candidate = _wall_time_utc(day, self.at, self.zone)
            if candidate > now:
                return candidate
        raise AssertionError("unreachable: a weekly slot exists in every 8-day window")

    @property
    def default_timeout(self) -> None:
        return None  # a weekly job has no natural bound, so the job must choose one


@dataclass(frozen=True)
class Job:
    name: str
    schedule: Schedule
    run: Callable[[], Awaitable[None]]
    # Paused while the Schwab token is dead; non-Schwab jobs keep running.
    needs_schwab: bool = False
    run_on_start: bool = False
    # Defaults to the interval for `Every`; required for `Daily` and `Weekly`.
    timeout: timedelta | None = None

    def __post_init__(self) -> None:
        if self.timeout is None:
            default = self.schedule.default_timeout
            if default is None:
                raise ValueError(f"job {self.name!r} needs an explicit timeout")
            object.__setattr__(self, "timeout", default)
        elif self.timeout <= timedelta(0):
            raise ValueError(f"job {self.name!r} timeout must be positive")

    @property
    def time_limit(self) -> timedelta:
        """The resolved timeout (`timeout` is None only before __post_init__)."""
        assert self.timeout is not None
        return self.timeout


class CronTable:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}

    def register(self, job: Job) -> None:
        if job.name in self._jobs:
            raise ValueError(f"job {job.name!r} is already registered")
        self._jobs[job.name] = job

    def __iter__(self) -> Iterator[Job]:
        return iter(self._jobs.values())

    def __len__(self) -> int:
        return len(self._jobs)
