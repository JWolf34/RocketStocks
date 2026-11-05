"""earnings_snapshot: the nightly snapshot of Nasdaq's earnings calendar, and its reader.

Each trading evening the job reads every weekday from a week back to a fixed
horizon past Nasdaq's forward window and stores the lot as one snapshot dated that
Eastern day. A past day carries actual EPS and surprise, so the week back picks up
what reporters published after an earlier snapshot. The horizon is fixed because
the window's end cannot be told from a quiet stretch inside it (a holiday week
answers the same way as the days past the end); the last listed day is logged.
A snapshot is stored whole or not at all: a day that still fails after its retries
ends the night, nothing is stored, and the previous snapshot stays the latest.

The Alert's next Catalyst reads the latest nightly snapshot through
`StoredEarningsCalendar`, and trusts it for `FRESH_FOR_SESSIONS` trading sessions;
past that, for a ticker the snapshot does not list, or when the database cannot be
read, the lookup estimates from EDGAR.
"""

import asyncio
import logging
from collections import Counter
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Protocol

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import (
    EASTERN,
    Clock,
    EarningsOrigin,
    StoredEarnings,
    TradingCalendar,
    eastern_day,
)
from rs_persistence import EarningsSnapshotRepository
from rs_sources.errors import SourceError
from rs_sources.nasdaq import EarningsDay

logger = logging.getLogger(__name__)

# After the close and after most after-hours reporters have published (they report from about
# 16:05 to 19:00); the week back reads any later actuals on the nights that follow.
SNAPSHOT_AT = time(20, 30)
# How far back each snapshot reaches, in calendar days.
PAST_DAYS = 7
# Forward weekdays read each night: about 13 weeks, well past the about 8 weeks Nasdaq lists.
FORWARD_WEEKDAYS = 65
# A day that fails is asked again after each of these pauses (seconds) before it counts as failed.
RETRY_DELAYS = (30.0, 120.0)
# A snapshot is trusted this many trading sessions after the one it was taken on.
FRESH_FOR_SESSIONS = 2


class DaySource(Protocol):
    async def day(self, day: date) -> EarningsDay: ...


class RetryingDays:
    """A day source that asks again, after each pause in `delays`, when a day fails; the last
    failure is raised. Wrapped by the tally, so a day that fails every time counts once."""

    def __init__(
        self,
        source: DaySource,
        *,
        delays: tuple[float, ...] = RETRY_DELAYS,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._source = source
        self._delays = delays
        self._sleep = sleep

    async def day(self, day: date) -> EarningsDay:
        for delay in self._delays:
            try:
                return await self._source.day(day)
            except SourceError:
                logger.info("Nasdaq earnings calendar failed for %s; asking again in %gs", day, delay)
                await self._sleep(delay)
        return await self._source.day(day)


class EarningsSnapshotError(Exception):
    """The night stored no snapshot; the next night tries again. `reason` is short and free of
    vendor text, for the Ops ping."""

    def __init__(self, message: str, *, reason: str) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class SnapshotReport:
    snapshot_date: date
    days_read: int
    last_listed_day: date
    rows: int
    unparsed: Counter[str] = field(default_factory=Counter)


def weekdays(start: date, end: date) -> Iterator[date]:
    """Monday to Friday dates from `start` to `end`, both inclusive."""
    day = start
    while day <= end:
        if day.weekday() < 5:
            yield day
        day += timedelta(days=1)


def _next_weekday(day: date) -> date:
    day += timedelta(days=1)
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return day


def _snapshot_due(calendar: TradingCalendar, now: datetime) -> bool:
    """A snapshot is taken on a trading evening, from SNAPSHOT_AT; a weekend or holiday has none."""
    return calendar.is_session(eastern_day(now)) and now.astimezone(EASTERN).time() >= SNAPSHOT_AT


async def snapshot_earnings(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    source: DaySource,
    calendar: TradingCalendar,
    clock: Clock,
) -> SnapshotReport | None:
    """Take tonight's snapshot; None when nothing is due (not a trading evening, or already taken)."""
    now = clock.now()
    today = eastern_day(now)
    if not _snapshot_due(calendar, now):
        return None
    async with session_factory() as session:
        if await EarningsSnapshotRepository(session).has_snapshot(EarningsOrigin.NIGHTLY, today):
            return None

    days = await _read_window(source, today)
    listings = [(day, listing) for day in days for listing in day.listings]
    if not listings:
        raise EarningsSnapshotError(
            f"Nasdaq earnings snapshot for {today}: no earnings listed on any of {len(days)} days; nothing stored",
            reason="no earnings listed",
        )
    rows = [
        StoredEarnings(
            snapshot_date=today,
            origin=EarningsOrigin.NIGHTLY,
            fetched_at=day.fetched_at,
            **listing.model_dump(),
        )
        for day, listing in listings
    ]
    async with session_factory() as session, session.begin():
        stored = await EarningsSnapshotRepository(session).insert_many(rows)
    unparsed = sum((Counter(day.unparsed) for day in days), Counter[str]())
    report = SnapshotReport(
        snapshot_date=today,
        days_read=len(days),
        last_listed_day=max(day.day for day in days if day.listings),
        rows=stored,
        unparsed=unparsed,
    )
    logger.info(
        "earnings snapshot %s: %d rows over %d days, last listed day %s; values not parsed: %s",
        today,
        report.rows,
        report.days_read,
        report.last_listed_day,
        dict(sorted(unparsed.items())) or "none",
    )
    return report


async def _read_window(source: DaySource, today: date) -> list[EarningsDay]:
    """Every day of the window, in order. A day that fails ends the read: the snapshot is whole
    or nothing, so there is no use reading on (and a dead endpoint costs one day's retries)."""
    wanted = list(weekdays(today - timedelta(days=PAST_DAYS), today))
    day = today
    for _ in range(FORWARD_WEEKDAYS):
        day = _next_weekday(day)
        wanted.append(day)
    days: list[EarningsDay] = []
    for day in wanted:
        try:
            days.append(await source.day(day))
        except SourceError as exc:
            logger.warning("Nasdaq earnings calendar failed for %s", day, exc_info=True)
            raise EarningsSnapshotError(
                f"Nasdaq earnings snapshot for {today}: {day} failed to read after {len(days)} good day(s); nothing stored",
                reason="a calendar day failed",
            ) from exc
    return days


class StoredEarningsCalendar:
    """The latest nightly snapshot as the earnings lookup reads it."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], calendar: TradingCalendar) -> None:
        self._session_factory = session_factory
        self._calendar = calendar

    async def next_listed(self, ticker: str, today: date) -> StoredEarnings | None:
        """The ticker's first listing on or after `today` in the latest nightly snapshot, or None
        when there is no snapshot, it is more than FRESH_FOR_SESSIONS sessions old, or it does
        not list the ticker. A database error is logged and also None, so the lookup still
        estimates from EDGAR."""
        try:
            async with self._session_factory() as session:
                repository = EarningsSnapshotRepository(session)
                latest = await repository.latest_snapshot_date(EarningsOrigin.NIGHTLY)
                if latest is None:
                    return None
                age = len(self._calendar.sessions_in_range(latest + timedelta(days=1), today))
                if age > FRESH_FOR_SESSIONS:
                    logger.info("earnings snapshot of %s is %d sessions old; not used", latest, age)
                    return None
                return await repository.next_report(EarningsOrigin.NIGHTLY, latest, ticker.strip().upper(), today)
        except SQLAlchemyError:
            logger.warning("stored earnings calendar unreadable for %s; estimating from EDGAR", ticker, exc_info=True)
            return None

