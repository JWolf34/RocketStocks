"""chains: the evening chain pass and the morning open-interest pass, one fetch record per
optionable underlying.

The roster is the optionable listings of the latest Universe snapshot. Every chain is fetched
whole and unfiltered, and what Schwab will not serve whole goes down a ladder: fetch, retry once, split into three expiry
ranges (the last open-ended), retry each range once, then a Tombstone. A chain is stored only
whole: when any range fails twice the underlying is a Tombstone and none of its ranges is kept.

A 400 or 404 is Schwab's answer about the symbol and an empty chain is an observation; both are
recorded at once and never retried. A dead token (`SchwabAuthError`) propagates out of the pass
and is never a Tombstone, and so does a 429, so a pass that ran out of budget or token stops
and the next run resumes with what is not recorded.

An underlying and its record are committed together, so a rerun, a restart or a pass a dead
token killed skips every underlying already recorded for the session and pass. A pass is owed
until its deadline; past it the pass writes only Tombstones, so a night that was missed is
recorded as missed and is never caught up.

The evening pass stores each whole contract for the session and is owed until the first 03:00
Central after the session's evening starts, unless configured. The morning pass runs the same
ladder from 07:00 ET, once Schwab's open interest has settled, on each trading day's morning,
and stores only each contract's open interest, named by the session it settled for (the last
session before the morning) in its own table; its deadline is that morning's open. A failed
morning loses nothing, because the evening pass carries the same open interest a session later.

The pass reports every underlying's outcome into the `SweepTally` it is given, one entry per
underlying (not per call), for the job to judge once the pass ends: above about 5% of the
underlyings Tombstoned is Schwab failing.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import (
    EASTERN,
    ChainFetchRecord,
    ChainPass,
    Clock,
    FetchStatus,
    OpenInterest,
    OptionContract,
    TradingCalendar,
    eastern_day,
)
from rs_ingest.daily_bars import stop_time
from rs_ingest.monitored import RATE_LIMITED
from rs_ingest.rate_limit import RateLimiter
from rs_ingest.universe import EVENING_STARTS_AT, SESSION_SEARCH, last_evening_session
from rs_observability import SweepTally
from rs_persistence import ChainRepository, UniverseRepository
from rs_sources.errors import PER_SYMBOL_STATUSES
from rs_sources.schwab import (
    OptionChain,
    SchwabAuthError,
    SchwabResponseError,
    TokenMissingError,
)

logger = logging.getLogger(__name__)

# Central clock time the pass's deadline falls at, until the probe gives the settle time.
DEADLINE_AT = time(3, 0)
# Eastern clock time the morning pass starts, once open interest has settled (the sweep probe saw
# it settle between 06:00 and 07:00 and hold until the open).
MORNING_STARTS_AT = time(7, 0)
# Days after the session that end the first two expiry ranges of the ladder; the third is open-ended.
LADDER_DAYS = (30, 180)
# Calls spent on one request of the ladder (the whole chain, then each range): fetch, retry once.
ATTEMPTS = 2
# Progress is logged this often during a long pass.
PROGRESS_EVERY = 500


class ChainSource(Protocol):
    async def chain(self, symbol: str, start: date | None = None, end: date | None = None) -> OptionChain: ...


@dataclass
class ChainPassReport:
    session: date | None = None
    roster: int = 0
    # Underlyings an earlier run of the pass had already recorded.
    already_recorded: int = 0
    ok: int = 0
    empty: int = 0
    rejected: int = 0
    # The ladder ran out of attempts.
    failed: int = 0
    # Tombstones written because the deadline came before the chain could be fetched.
    past_deadline: int = 0
    # Contracts stored (the morning pass's open-interest rows, one per contract).
    contracts: int = 0
    calls: int = 0

    @property
    def tombstones(self) -> int:
        return self.rejected + self.failed + self.past_deadline


class _Rejected(Exception):
    """Schwab answered 400 or 404 for the symbol."""


class _PastDeadline(Exception):
    """The deadline came before a call could be made."""


@dataclass
class _Fetched:
    status: FetchStatus
    contracts: tuple[OptionContract, ...] = ()
    attempts: int = 0
    past_deadline: bool = False


def chain_deadline(session: date, at: time = DEADLINE_AT) -> datetime:
    """The first `at` Central after `session`'s evening starts."""
    return stop_time(datetime.combine(session, EVENING_STARTS_AT, tzinfo=EASTERN), at)


def expiry_ranges(session: date) -> tuple[tuple[date, date | None], ...]:
    """The ladder's three expiry ranges: within 30 days of the session, 31 to 180 days, and everything after."""
    near, mid = LADDER_DAYS
    return (
        (session, session + timedelta(days=near)),
        (session + timedelta(days=near + 1), session + timedelta(days=mid)),
        (session + timedelta(days=mid + 1), None),
    )


def morning_slot(calendar: TradingCalendar, now: datetime) -> tuple[date, datetime]:
    """The morning pass owed at `now`: the session its open interest settled for and its deadline.

    The latest trading day whose 07:00 ET has passed owns the morning; the session is the one
    before it and the deadline is its open. A weekend or holiday owns no morning, so it belongs
    to the last trading day's.
    """
    today = eastern_day(now)
    for day in reversed(calendar.sessions_in_range(today - SESSION_SEARCH, today)):
        if datetime.combine(day, MORNING_STARTS_AT, tzinfo=EASTERN) <= now:
            settled = calendar.sessions_in_range(day - SESSION_SEARCH, day - timedelta(days=1))
            if not settled:
                raise RuntimeError(f"no session in the {SESSION_SEARCH.days} days before the morning of {day}")
            return settled[-1], calendar.session_open(day)
    raise RuntimeError(f"no session morning has started in the {SESSION_SEARCH.days} days before {today}")


async def sweep_evening_chains(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    source: ChainSource,
    limiter: RateLimiter,
    calendar: TradingCalendar,
    clock: Clock,
    tally: SweepTally,
    deadline_at: time = DEADLINE_AT,
) -> ChainPassReport:
    session = last_evening_session(calendar, clock.now())
    return await _sweep(
        ChainPass.EVENING,
        session,
        chain_deadline(session, deadline_at),
        session_factory=session_factory,
        source=source,
        limiter=limiter,
        clock=clock,
        tally=tally,
    )


async def sweep_morning_open_interest(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    source: ChainSource,
    limiter: RateLimiter,
    calendar: TradingCalendar,
    clock: Clock,
    tally: SweepTally,
) -> ChainPassReport:
    session, deadline = morning_slot(calendar, clock.now())
    return await _sweep(
        ChainPass.MORNING,
        session,
        deadline,
        session_factory=session_factory,
        source=source,
        limiter=limiter,
        clock=clock,
        tally=tally,
    )


async def _sweep(
    chain_pass: ChainPass,
    session: date,
    deadline: datetime,
    *,
    session_factory: async_sessionmaker[AsyncSession],
    source: ChainSource,
    limiter: RateLimiter,
    clock: Clock,
    tally: SweepTally,
) -> ChainPassReport:
    report = ChainPassReport(session=session)
    label = f"{chain_pass.value} chains of {session}"
    async with session_factory() as db:
        universe = UniverseRepository(db)
        header = await universe.latest_header()
        if header is None:
            logger.warning("%s: no Universe stored yet, nothing to fetch", label)
            return report
        roster = sorted(await universe.optionable_symbols(header.snapshot_date))
        recorded = await ChainRepository(db).recorded_underlyings(session, chain_pass)
    report.roster = len(roster)
    todo = [symbol for symbol in roster if symbol not in recorded]
    report.already_recorded = len(roster) - len(todo)

    logger.info(
        "%s: %d optionable underlyings from the Universe of %s, %d already recorded, deadline %s",
        label, len(roster), header.snapshot_date, report.already_recorded, deadline,
    )
    ladder = _Ladder(source, limiter, clock, deadline, session, report)
    try:  # the partial report is worth logging when a 429 or a dead token ends the pass
        for done, symbol in enumerate(todo):
            if done and done % PROGRESS_EVERY == 0:
                logger.info("%s: %d of %d underlyings done", label, done, len(todo))
            if clock.now() >= deadline:
                fetched = _Fetched(FetchStatus.FAILED, past_deadline=True)
            else:
                try:
                    fetched = await ladder.fetch(symbol)
                except SchwabResponseError as exc:
                    if exc.status_code != RATE_LIMITED:
                        raise
                    tally.record_failure(source_wide=True)
                    raise
            await _store(session_factory, clock, chain_pass, session, symbol, fetched)
            _count(report, tally, fetched)
    finally:
        _log(label, report)
    return report


class _Ladder:
    """One underlying's calls: the whole chain, and when that fails twice its three expiry ranges."""

    def __init__(
        self,
        source: ChainSource,
        limiter: RateLimiter,
        clock: Clock,
        deadline: datetime,
        session: date,
        report: ChainPassReport,
    ) -> None:
        self._source = source
        self._limiter = limiter
        self._clock = clock
        self._deadline = deadline
        self._session = session
        self._report = report
        self._attempts = 0

    async def fetch(self, symbol: str) -> _Fetched:
        """The underlying's chain, or the Tombstone or rejection that explains why there is none."""
        self._attempts = 0
        try:
            whole = await self._request(symbol, None, None)
            if whole is not None:
                return _fetched(whole.contracts, self._attempts)
            parts: list[OptionChain] = []
            for start, end in expiry_ranges(self._session):
                part = await self._request(symbol, start, end)
                if part is None:
                    logger.warning("%s: the chain failed whole and in ranges, recording a Tombstone", symbol)
                    return _Fetched(FetchStatus.FAILED, attempts=self._attempts)
                parts.append(part)
        except _Rejected:
            return _Fetched(FetchStatus.REJECTED, attempts=self._attempts)
        except _PastDeadline:
            return _Fetched(FetchStatus.FAILED, attempts=self._attempts, past_deadline=True)
        # Ranges never overlap, but a repeated option symbol must not fail the whole underlying's store.
        merged = {c.option_symbol: c for part in parts for c in part.contracts}
        return _fetched(tuple(merged.values()), self._attempts)

    async def _request(self, symbol: str, start: date | None, end: date | None) -> OptionChain | None:
        """One request of the ladder, tried again once; None when both tries failed."""
        for _ in range(ATTEMPTS):
            await self._limiter.acquire()
            if self._clock.now() >= self._deadline:
                raise _PastDeadline
            self._attempts += 1
            self._report.calls += 1
            try:
                chain = await self._source.chain(symbol, start, end)
            except (SchwabAuthError, TokenMissingError):
                raise  # the daemon pauses Schwab jobs on this; never a Tombstone
            except SchwabResponseError as exc:
                if exc.status_code in PER_SYMBOL_STATUSES:
                    logger.info("%s: Schwab refuses the chain: %s", symbol, exc)
                    raise _Rejected from exc
                if exc.status_code == RATE_LIMITED:
                    raise  # over budget: stop, the next run resumes
                logger.warning("%s: chain request failed: %s", symbol, exc)
            except Exception:
                logger.exception("%s: chain request failed", symbol)
            else:
                if chain.truncated:
                    logger.warning("%s: Schwab flags the chain as truncated", symbol)
                return chain
        return None


def _fetched(contracts: Sequence[OptionContract], attempts: int) -> _Fetched:
    status = FetchStatus.OK if contracts else FetchStatus.EMPTY
    return _Fetched(status, tuple(contracts), attempts)


async def _store(
    session_factory: async_sessionmaker[AsyncSession],
    clock: Clock,
    chain_pass: ChainPass,
    session: date,
    symbol: str,
    fetched: _Fetched,
) -> None:
    record = ChainFetchRecord(
        session_date=session,
        chain_pass=chain_pass,
        underlying=symbol,
        status=fetched.status,
        attempts=fetched.attempts,
        contract_count=len(fetched.contracts),
        fetched_at=clock.now(),
    )
    async with session_factory() as db:
        repository = ChainRepository(db)
        if chain_pass is ChainPass.EVENING:
            await repository.store(record, fetched.contracts)
        else:
            await repository.store_open_interest(record, [OpenInterest.of(c) for c in fetched.contracts])
        await db.commit()


def _count(report: ChainPassReport, tally: SweepTally, fetched: _Fetched) -> None:
    report.contracts += len(fetched.contracts)
    if fetched.past_deadline:
        report.past_deadline += 1
        tally.record_failure()
    elif fetched.status is FetchStatus.FAILED:
        report.failed += 1
        tally.record_failure()
    else:
        # Schwab answered, even if with a 400 or an empty chain: that is an observation or the stock's, not Schwab's.
        tally.record_ok()
        match fetched.status:
            case FetchStatus.OK:
                report.ok += 1
            case FetchStatus.EMPTY:
                report.empty += 1
            case _:
                report.rejected += 1


def _log(label: str, report: ChainPassReport) -> None:
    logger.info(
        "%s: %d underlyings in the roster, %d already recorded, %d ok, %d empty, %d rejected, "
        "%d failed, %d past the deadline, %d contracts or open-interest rows stored, %d calls",
        label,
        report.roster,
        report.already_recorded,
        report.ok,
        report.empty,
        report.rejected,
        report.failed,
        report.past_deadline,
        report.contracts,
        report.calls,
    )
