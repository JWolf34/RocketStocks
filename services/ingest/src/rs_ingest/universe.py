"""universe: the daily Universe snapshot, taken each trading evening.

The snapshot of a session is every non-test listing of `nasdaqlisted.txt` and
`otherlisted.txt`, each marked optionable when its symbol is an underlying in Nasdaq's
`options.txt` or a symbol of Cboe's options directory (an exact match; there is no alias
table). It is stored whole, by the session's date.

The files are updated through the day, so a file belongs to a session only when its File
Creation Time is after that session's close (an early close counts) and before the next
session's open. Cboe's directory carries no stamp, so it counts as the day's. `take` keeps
retrying whatever it has not yet accepted (the listing files, `options.txt`, Cboe's directory)
every 15 minutes and gives up at 20:00 ET. A file far smaller than the previous snapshot's is
treated like a stale or unreadable one (a cut download is already caught by its missing footer,
so this only fires on a complete but much smaller file); an operator who knows the shrink is
real runs the snapshot with the size guard off.

At the give-up, listing files that never qualified mean the night uses the previous snapshot and
Ops is pinged. An optionable source that never qualified must never flip thousands of listings
to non-optionable: the snapshot is taken with every symbol the previous snapshot called
optionable plus what the working source says, it records that it did so, and Ops is pinged.
A source can therefore only add optionable names on a bad night, never remove them.
A run that starts after the give-up (a restart the next afternoon, or the first deploy) makes
one attempt without waiting and reports failures like any other run. The one exception is a file
already stamped after the next session's open: it is a later day's file, nothing of this night
can be fetched any more, so that is logged and not pinged.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import (
    EASTERN,
    Clock,
    TradingCalendar,
    UniverseCounts,
    UniverseListing,
    UniverseSnapshot,
    eastern_day,
)
from rs_observability import HealthTracker, Source
from rs_persistence import UniverseRepository
from rs_sources.errors import SourceError
from rs_sources.symbol_directory import ListingFile, OptionsFile, SymbolDirectory

logger = logging.getLogger(__name__)

# The evening's start, Eastern wall clock on the session's date: the job's daily slot. A run
# before it belongs to the session before, so a restart in the afternoon never waits for hours.
EVENING_STARTS_AT = time(18, 5)
RETRY_EVERY = timedelta(minutes=15)
# Eastern wall clock on the session's date; the last attempt is made at this moment.
GIVE_UP_AT = time(20, 0)
# The longest a run waits: from the evening's start to the give-up.
MAX_WAIT = datetime.combine(date.min, GIVE_UP_AT) - datetime.combine(date.min, EVENING_STARTS_AT)
# Calendar days that always hold a session (the longest market closure is four days).
SESSION_SEARCH = timedelta(days=10)
# A file with less than this share of the previous snapshot's entries is truncated. Day to day
# the listings move by tens of names in about 13,000 (the largest real change in one night
# is a few hundred), so 10 percent is far outside ordinary change and well inside any cut-off
# download. A genuine change that large keeps rejecting until the operator accepts it.
MIN_SHARE_OF_PREVIOUS = 0.9

Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class UniverseUse:
    """Which snapshot the night's passes read.

    `session` is the session the night belongs to (the last one whose evening has started). `fresh`
    is True when the snapshot is that session's own and False when the night falls back to the one
    before. `snapshot_date` and `as_of` are None only when no snapshot exists at all, and a later
    pass must then skip the night: it has no Universe to read.
    """

    session: date
    snapshot_date: date | None
    as_of: datetime | None
    fresh: bool

    @property
    def night_uses(self) -> str:
        """What the night reads, in words for a failure message."""
        if self.snapshot_date is None:
            return "no Universe snapshot exists yet"
        return f"the night uses the snapshot of {self.snapshot_date}"


class _Unusable(Exception):
    """A file that cannot be used tonight; `reason` is short and safe to send to Ops."""

    def __init__(self, reason: str, *, superseded: bool = False) -> None:
        super().__init__(reason)
        self.reason = reason
        # The file is a later day's: the night's own can no longer be fetched.
        self.superseded = superseded


@dataclass
class _Attempt[T]:
    """One file the night needs: its accepted value, or why it cannot be used yet."""

    value: T | None = None
    problem: str = ""
    superseded: bool = False

    @property
    def accepted(self) -> bool:
        return self.value is not None

    async def run(self, call: Awaitable[T]) -> None:
        try:
            self.value, self.problem, self.superseded = await call, "", False
        except _Unusable as exc:
            self.value, self.problem, self.superseded = None, exc.reason, exc.superseded


@dataclass(frozen=True)
class _Window:
    """The instants a file may be stamped to belong to the session: after its close, before the next open."""

    close: datetime
    next_open: datetime


@dataclass(frozen=True)
class _File:
    """A source file: its name for the logs and the count the previous snapshot kept of it."""

    name: str
    previous_count: Callable[[UniverseCounts], int | None]


_NASDAQ_LISTED = _File("nasdaqlisted.txt", lambda counts: counts.nasdaq_listed)
_OTHER_LISTED = _File("otherlisted.txt", lambda counts: counts.other_listed)
_OPTIONS = _File("options.txt", lambda counts: counts.options_symbols)
_CBOE = _File("Cboe options directory", lambda counts: counts.cboe_symbols)


class _SizeGuard:
    """Rejects a file far smaller than the previous snapshot's; with no previous counts, accepts any size."""

    def __init__(self, previous: UniverseCounts | None) -> None:
        self._previous = previous

    def accept(self, file: _File, size: int) -> None:
        before = None if self._previous is None else file.previous_count(self._previous)
        if before is not None and size < before * MIN_SHARE_OF_PREVIOUS:
            logger.warning("%s holds %d entries against %d the time before", file.name, size, before)
            raise _Unusable(f"{file.name} looks truncated ({size} entries against {before})")


@dataclass
class _Night:
    """One run's progress: the session and, per source, the file accepted so far or why not."""

    session: date
    listings: _Attempt[tuple[ListingFile, ListingFile]]
    options: _Attempt[OptionsFile]
    cboe: _Attempt[frozenset[str]]


class UniverseSnapshotter:
    """Takes the Universe of the session whose evening last started, once, waiting for its files.

    Reads the vendors through `directory`, stores through `session_factory`, and reports each
    source's health (listing files, `options.txt`, Cboe) to `health`. `sleep` is injectable so the
    15-minute waits run instantly in tests.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        directory: SymbolDirectory,
        clock: Clock,
        calendar: TradingCalendar,
        health: HealthTracker,
        *,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self._session_factory = session_factory
        self._directory = directory
        self._clock = clock
        self._calendar = calendar
        self._health = health
        self._sleep = sleep

    async def take(self, *, accept_shrink: bool = False) -> UniverseUse:
        """Take the snapshot of the session whose evening last started if it has none yet.

        Weekends, holidays, the hours before 18:05 ET and a session whose snapshot is stored find
        nothing to do. Past the give-up time one attempt is made without waiting. `accept_shrink`
        turns the size guard off for this call; the stamp rule and the footer still apply.
        """
        session = _last_evening_session(self._calendar, self._clock.now())
        async with self._session_factory() as db:
            repo = UniverseRepository(db)
            existing = await repo.header(session)
            previous = await repo.reference_counts()
        if existing is not None:
            return UniverseUse(session, existing.snapshot_date, existing.as_of, fresh=True)

        if accept_shrink:
            logger.warning("Universe of %s: the size guard is off for this run", session)
        guard = _SizeGuard(None if accept_shrink else previous)
        window = self._window(session)
        give_up = datetime.combine(session, GIVE_UP_AT, tzinfo=EASTERN).astimezone(UTC)
        night = _Night(session, _Attempt(), _Attempt(), _Attempt())
        while True:
            if not night.listings.accepted:
                await night.listings.run(self._listing_files(window, guard))
            # The big file is only fetched once the listings qualify, so the wait costs it nothing.
            if night.listings.accepted and not night.options.accepted:
                await night.options.run(self._options_file(window, guard))
            if night.listings.accepted and not night.cboe.accepted:
                await night.cboe.run(self._cboe_file(guard))
            now = self._clock.now()
            if (night.listings.accepted and night.options.accepted and night.cboe.accepted) or now >= give_up:
                break
            await self._sleep(min(RETRY_EVERY, give_up - now).total_seconds())

        if not night.listings.accepted:
            logger.warning("Universe of %s: the listing files never qualified (%s)", session, night.listings.problem)
            await self._report_failed(Source.NASDAQ_TRADER, night.listings)
            return await self._previous(session)
        return await self._store(night)

    def _window(self, session: date) -> _Window:
        following = self._calendar.sessions_in_range(session + timedelta(days=1), session + SESSION_SEARCH)
        return _Window(self._calendar.session_close(session), self._calendar.session_open(following[0]))

    async def _previous(self, session: date) -> UniverseUse:
        async with self._session_factory() as db:
            latest = await UniverseRepository(db).latest_header()
        if latest is None:
            logger.error("Universe of %s: no snapshot exists to fall back to", session)
            return UniverseUse(session, None, None, fresh=False)
        logger.warning("Universe of %s: using the snapshot of %s", session, latest.snapshot_date)
        return UniverseUse(session, latest.snapshot_date, latest.as_of, fresh=False)

    async def _store(self, night: _Night) -> UniverseUse:
        assert night.listings.value is not None
        nasdaq, other = night.listings.value
        options, cboe = night.options.value, night.cboe.value
        carried = options is None or cboe is None
        live: set[str] = set()
        if options is not None:
            live |= options.underlyings
        if cboe is not None:
            live |= cboe
        optionable = set(live)

        async with self._session_factory() as db:
            repo = UniverseRepository(db)
            if carried and (latest := await repo.latest_header()) is not None:
                optionable |= await repo.optionable_symbols(latest.snapshot_date)

            rows: dict[str, UniverseListing] = {}
            for listing in (*nasdaq.listings, *other.listings):
                # The first file's row wins a symbol both files list.
                rows.setdefault(
                    listing.symbol,
                    UniverseListing(**listing.model_dump(), optionable=listing.symbol in optionable),
                )
            snapshot = UniverseSnapshot(
                snapshot_date=night.session,
                as_of=min(nasdaq.created_at, other.created_at),
                counts=UniverseCounts(
                    nasdaq_listed=len(nasdaq.listings),
                    other_listed=len(other.listings),
                    options_symbols=None if options is None else len(options.underlyings),
                    cboe_symbols=None if cboe is None else len(cboe),
                ),
                optionable_carried=carried,
                listings=tuple(rows.values()),
            )
            stored = await repo.save(snapshot)
            await db.commit()

        logger.info(
            "Universe of %s%s: %d listings (%d Nasdaq + %d other, %d duplicate symbols), "
            "%d optionable, %d optionable symbols joined no listing",
            night.session,
            "" if stored else " (already stored)",
            len(rows),
            len(nasdaq.listings),
            len(other.listings),
            len(nasdaq.listings) + len(other.listings) - len(rows),
            sum(row.optionable for row in rows.values()),
            len(live - rows.keys()),
        )
        await self._health.report_ok(Source.NASDAQ_TRADER)  # the listing files qualified
        for source, attempt in ((Source.NASDAQ_OPTIONS, night.options), (Source.CBOE, night.cboe)):
            if attempt.accepted:
                await self._health.report_ok(source)
            else:
                await self._report_failed(source, attempt)
        return UniverseUse(night.session, night.session, snapshot.as_of, fresh=True)

    async def _report_failed(self, source: Source, attempt: _Attempt[Any]) -> None:
        if attempt.superseded:
            logger.warning("Universe: %s has nothing left to fetch for this night (%s)", source, attempt.problem)
            return
        await self._health.report_failed(source, attempt.problem)

    async def _listing_files(self, window: _Window, guard: _SizeGuard) -> tuple[ListingFile, ListingFile]:
        nasdaq = await _read(_NASDAQ_LISTED, self._directory.nasdaq_listed())
        _accept_stamp(_NASDAQ_LISTED, nasdaq.created_at, window)
        guard.accept(_NASDAQ_LISTED, len(nasdaq.listings))
        other = await _read(_OTHER_LISTED, self._directory.other_listed())
        _accept_stamp(_OTHER_LISTED, other.created_at, window)
        guard.accept(_OTHER_LISTED, len(other.listings))
        return nasdaq, other

    async def _options_file(self, window: _Window, guard: _SizeGuard) -> OptionsFile:
        options = await _read(_OPTIONS, self._directory.option_underlyings())
        _accept_stamp(_OPTIONS, options.created_at, window)
        guard.accept(_OPTIONS, len(options.underlyings))
        return options

    async def _cboe_file(self, guard: _SizeGuard) -> frozenset[str]:
        symbols = await _read(_CBOE, self._directory.cboe_underlyings())
        guard.accept(_CBOE, len(symbols))
        return symbols


async def _read[T](file: _File, call: Awaitable[T]) -> T:
    try:
        return await call
    except SourceError as exc:
        logger.warning("%s could not be read: %s", file.name, exc)
        raise _Unusable(f"{file.name} could not be read") from exc


def _accept_stamp(file: _File, created_at: datetime, window: _Window) -> None:
    if window.close < created_at < window.next_open:
        return
    stamp = created_at.astimezone(EASTERN).strftime("%Y-%m-%d %H:%M ET")
    logger.info("%s is stamped %s, outside the session's evening", file.name, stamp)
    if created_at <= window.close:
        raise _Unusable(f"{file.name} not updated since the close")
    raise _Unusable(f"{file.name} is stamped after the next open", superseded=True)


def _last_evening_session(calendar: TradingCalendar, now: datetime) -> date:
    today = eastern_day(now)
    for day in reversed(calendar.sessions_in_range(today - SESSION_SEARCH, today)):
        if datetime.combine(day, EVENING_STARTS_AT, tzinfo=EASTERN) <= now:
            return day
    raise RuntimeError(f"no session evening has started in the {SESSION_SEARCH.days} days before {today}")
