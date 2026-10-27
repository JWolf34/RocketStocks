"""universe: the daily Universe snapshot, taken each trading evening.

The snapshot of a session is every non-test listing of `nasdaqlisted.txt` and
`otherlisted.txt`, each marked optionable when its symbol is an underlying in Nasdaq's
`options.txt` or a symbol of Cboe's options directory (an exact match; there is no alias
table). It is stored whole, by the session's date.

The listing files are updated through the day, so a snapshot only takes files whose File
Creation Time is after that session's close (an early close counts). `take` retries every
15 minutes and gives up at 20:00 ET: the night then uses the previous snapshot and Ops is
pinged. A listing file far smaller than the previous snapshot's is a cut download and is
treated like a stale one.

Optionable sources are held to a softer rule, because a failure there must never flip
thousands of listings to non-optionable. `options.txt` older than the close is waited for
like the listings. Anything else wrong with either source (a failed request, a truncated
file) is not waited for: the night keeps every symbol the previous snapshot called
optionable, adds what the working source says, records that it did so, and pings Ops.
A source can therefore only add optionable names on a bad night, never remove them.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

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
# A file with less than this share of the previous snapshot's entries is truncated. Day to day
# the listings move by tens of names in about 13,000 (the largest real change in one night
# is a few hundred), so 10 percent is far outside ordinary change and well inside any cut-off
# download. A genuine change that large keeps rejecting until the operator looks, which the
# Ops ping asks for.
MIN_SHARE_OF_PREVIOUS = 0.9

Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class UniverseUse:
    """Which snapshot the night's passes read.

    `session` is the session the night belongs to (the last one whose evening has started). `fresh` is True when
    the snapshot is that session's own and False when the night falls back to the one before;
    `snapshot_date` and `as_of` are None only when no snapshot exists at all.
    """

    session: date
    snapshot_date: date | None
    as_of: datetime | None
    fresh: bool


class _Unusable(Exception):
    """A file that cannot be used tonight; `reason` is short and safe to send to Ops."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _Stale(_Unusable):
    """A whole file stamped before the session's close: it may simply not be updated yet."""


class UniverseSnapshotter:
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

    async def take(self) -> UniverseUse:
        """Take the snapshot of the session whose evening last started if it has none yet.

        Weekends, holidays, the hours before 18:05 ET and a session whose snapshot is stored find
        nothing to do. Past the give-up time one attempt is made without waiting.
        """
        session = _last_evening_session(self._calendar, self._clock.now())
        async with self._session_factory() as db:
            repo = UniverseRepository(db)
            existing = await repo.header(session)
            previous = await repo.reference_counts()
        if existing is not None:
            return UniverseUse(session, existing.snapshot_date, existing.as_of, fresh=True)

        close = self._calendar.session_close(session)
        give_up = datetime.combine(session, GIVE_UP_AT, tzinfo=EASTERN).astimezone(UTC)
        listings: tuple[ListingFile, ListingFile] | None = None
        options: OptionsFile | None = None
        listings_problem = options_problem = ""
        options_settled = False  # accepted, or failed in a way waiting will not mend
        while True:
            if listings is None:
                try:
                    listings = await self._listing_files(close, previous)
                except _Unusable as exc:
                    listings_problem = exc.reason
            if listings is not None and not options_settled:
                try:
                    options = await self._options_file(close, previous)
                    options_settled = True
                except _Stale as exc:
                    options_problem = exc.reason
                except _Unusable as exc:
                    options_problem = exc.reason
                    options_settled = True
            now = self._clock.now()
            if (listings is not None and options_settled) or now >= give_up:
                break
            await self._sleep(min(RETRY_EVERY, give_up - now).total_seconds())

        if listings is None:
            logger.warning("Universe of %s: the listing files never qualified (%s)", session, listings_problem)
            await self._health.report_failed(Source.NASDAQ_TRADER, f"listing files: {listings_problem}")
            return await self._previous(session)

        cboe, cboe_problem = await self._cboe(previous)
        return await self._store(session, listings, options, options_problem, cboe, cboe_problem)

    async def _previous(self, session: date) -> UniverseUse:
        async with self._session_factory() as db:
            latest = await UniverseRepository(db).latest_header()
        if latest is None:
            logger.error("Universe of %s: no snapshot exists to fall back to", session)
            return UniverseUse(session, None, None, fresh=False)
        logger.warning("Universe of %s: using the snapshot of %s", session, latest.snapshot_date)
        return UniverseUse(session, latest.snapshot_date, latest.as_of, fresh=False)

    async def _store(
        self,
        session: date,
        listings: tuple[ListingFile, ListingFile],
        options: OptionsFile | None,
        options_problem: str,
        cboe: frozenset[str] | None,
        cboe_problem: str,
    ) -> UniverseUse:
        nasdaq, other = listings
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
                snapshot_date=session,
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
            session,
            "" if stored else " (already stored)",
            len(rows),
            len(nasdaq.listings),
            len(other.listings),
            len(nasdaq.listings) + len(other.listings) - len(rows),
            sum(row.optionable for row in rows.values()),
            len(live - rows.keys()),
        )
        if options is not None:
            await self._health.report_ok(Source.NASDAQ_TRADER)
        else:
            await self._health.report_failed(Source.NASDAQ_TRADER, options_problem)
        if cboe is not None:
            await self._health.report_ok(Source.CBOE)
        else:
            await self._health.report_failed(Source.CBOE, cboe_problem)
        return UniverseUse(session, session, snapshot.as_of, fresh=True)

    async def _listing_files(
        self, close: datetime, previous: UniverseCounts | None
    ) -> tuple[ListingFile, ListingFile]:
        nasdaq = await _read("nasdaqlisted.txt", self._directory.nasdaq_listed())
        _accept_stamp("nasdaqlisted.txt", nasdaq.created_at, close)
        _accept_size("nasdaqlisted.txt", len(nasdaq.listings), previous.nasdaq_listed if previous else None)
        other = await _read("otherlisted.txt", self._directory.other_listed())
        _accept_stamp("otherlisted.txt", other.created_at, close)
        _accept_size("otherlisted.txt", len(other.listings), previous.other_listed if previous else None)
        return nasdaq, other

    async def _options_file(self, close: datetime, previous: UniverseCounts | None) -> OptionsFile:
        options = await _read("options.txt", self._directory.option_underlyings())
        _accept_stamp("options.txt", options.created_at, close)
        _accept_size("options.txt", len(options.underlyings), previous.options_symbols if previous else None)
        return options

    async def _cboe(self, previous: UniverseCounts | None) -> tuple[frozenset[str] | None, str]:
        try:
            symbols = await _read("Cboe options directory", self._directory.cboe_underlyings())
            _accept_size("Cboe options directory", len(symbols), previous.cboe_symbols if previous else None)
        except _Unusable as exc:
            return None, exc.reason
        return symbols, ""


async def _read[T](name: str, call: Awaitable[T]) -> T:
    try:
        return await call
    except SourceError as exc:
        logger.warning("%s could not be read: %s", name, exc)
        raise _Unusable(f"{name} could not be read") from exc


def _accept_stamp(name: str, created_at: datetime, close: datetime) -> None:
    if created_at <= close:
        stamp = created_at.astimezone(EASTERN).strftime("%Y-%m-%d %H:%M ET")
        logger.info("%s is stamped %s, not after the close yet", name, stamp)
        raise _Stale(f"{name} not updated since the close")


def _accept_size(name: str, size: int, previous: int | None) -> None:
    if previous is not None and size < previous * MIN_SHARE_OF_PREVIOUS:
        logger.warning("%s holds %d entries against %d the time before", name, size, previous)
        raise _Unusable(f"{name} looks truncated ({size} entries against {previous})")


def _last_evening_session(calendar: TradingCalendar, now: datetime) -> date:
    today = eastern_day(now)
    for day in reversed(calendar.sessions_in_range(today - timedelta(days=10), today)):
        if datetime.combine(day, EVENING_STARTS_AT, tzinfo=EASTERN) <= now:
            return day
    raise RuntimeError(f"no session evening has started in the 10 days before {today}")
