"""The nightly Outcomes job: find every case, measure it against the stored daily
bars and rewrite the Trigger version's Outcomes and Track record.

A case is the first candidate of the Trigger version on a stock on an Eastern day,
timed at its post (the Alert's `posted_at`) when it was posted and at its hit
otherwise. Cases come from a replay of the rule over every stored Popularity
snapshot, both origins, merged with the candidates the live poll stored: one case
per stock and day however many of those saw it. A case is live when the live poll
stored a candidate for it; the rest are replayed history, which the Track record
discloses because the rule was written having seen it. A stock whose Alert has not
been delivered yet is held back, since its post time is not known; a later run takes it.

The measure itself is `rs_analysis.outcomes`. Everything is recomputed from the
stored Popularity, candidates, Alerts and bars, then written in one transaction,
so a rerun, a restart or a crash mid-run leaves either the last Track record or the
new one, and a late bar, a late import or a corrected bar flows into the next run.

It runs as a subprocess (`python -m rs_ingest.outcomes`, see rs_ingest.track_record):
reading a year of Popularity and replaying the rule over it is CPU-heavy Python that
would stall the daemon's event loop.
"""

import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_analysis.outcomes import (
    HORIZONS,
    HorizonResult,
    SessionGrid,
    Status,
    align,
    measure_case,
    summarize,
)
from rs_analysis.popularity_surge import RULE_VERSION
from rs_domain import (
    Clock,
    Outcome,
    SystemClock,
    TrackRecord,
    TradingCalendar,
    eastern_day,
)
from rs_ingest.popularity_poll import TRIGGER
from rs_ingest.replay import replayed_hits
from rs_ingest.surge_bars import (
    BENCHMARK,
    EASTERN,
    LOOKBACK_CALENDAR_DAYS,
    last_settled_session,
)
from rs_observability import configure_logging
from rs_persistence import (
    AlertRepository,
    CandidateRepository,
    DailyBarRepository,
    TrackRecordRepository,
    build_engine,
    build_session_factory,
)
from rs_sources import ExchangeCalendar

logger = logging.getLogger(__name__)

DSN_VARIABLE = "RS_DB_URL"
# Calendar days of sessions past today the grid holds: a case entered tomorrow
# still needs its 20th session, with holidays to spare.
GRID_AHEAD_DAYS = 60


class OutcomesError(RuntimeError):
    """The Outcomes cannot be measured on what is stored (the message says what to fix)."""


@dataclass(frozen=True)
class Case:
    symbol: str
    day: date
    hit_at: datetime
    case_at: datetime
    live: bool


async def discover_cases(
    session_factory: async_sessionmaker[AsyncSession], now: datetime
) -> tuple[list[Case], int]:
    """Every case of the current rule version, oldest day first, and how many were held back."""
    replayed: dict[tuple[str, date], datetime] = {}
    async for event in replayed_hits(session_factory, now):
        key = (event.ticker, eastern_day(event.polled_at))
        replayed[key] = min(event.polled_at, replayed.get(key, event.polled_at))
    async with session_factory() as session:
        stored = await CandidateRepository(session).first_hits_by_day(TRIGGER, RULE_VERSION)
        posts = await AlertRepository(session).posts_by_day(TRIGGER, RULE_VERSION)

    cases: list[Case] = []
    held_back = 0
    for key in sorted(replayed.keys() | stored.keys(), key=lambda k: (k[1], k[0])):
        symbol, day = key
        hit_at = min(t for t in (replayed.get(key), stored.get(key)) if t is not None)
        case_at = hit_at
        if key in posts:
            posted_at = posts[key]
            if posted_at is None:
                held_back += 1
                continue
            case_at = posted_at
        cases.append(Case(symbol, day, hit_at, case_at, live=key in stored))
    return cases, held_back


async def measure_cases(
    session_factory: async_sessionmaker[AsyncSession],
    calendar: TradingCalendar,
    now: datetime,
    cases: list[Case],
) -> tuple[dict[int, list[HorizonResult]], list[Outcome]]:
    """Each horizon's result for every case, and the Outcomes among them."""
    results: dict[int, list[HorizonResult]] = {horizon: [] for horizon in HORIZONS}
    outcomes: list[Outcome] = []
    if not cases:
        return results, outcomes

    start = min(case.day for case in cases) - timedelta(days=LOOKBACK_CALENDAR_DAYS)
    grid = SessionGrid.build(calendar, start, now.astimezone(EASTERN).date() + timedelta(days=GRID_AHEAD_DAYS))
    settled = grid.index_of(last_settled_session(calendar, now))
    if settled is None:
        raise OutcomesError("the last settled session is outside the session grid")

    by_symbol: dict[str, list[Case]] = {}
    for case in cases:
        by_symbol.setdefault(case.symbol, []).append(case)

    async with session_factory() as session:
        bars = DailyBarRepository(session)
        benchmark_bars = await bars.list_range(BENCHMARK, grid.days[0], grid.days[-1])
        if not benchmark_bars:
            raise OutcomesError(f"no {BENCHMARK} daily bars stored; the surge-bars job has not fetched the benchmark")
        benchmark = align(grid, benchmark_bars)
        for symbol in sorted(by_symbol):
            stock = align(grid, await bars.list_range(symbol, grid.days[0], grid.days[-1]))
            for case in by_symbol[symbol]:
                entry = grid.entry_index(case.case_at)
                if entry is None:
                    measured = [HorizonResult(horizon, Status.NOT_MATURED) for horizon in HORIZONS]
                else:
                    measured = measure_case(grid, stock, benchmark, entry, settled)
                for result in measured:
                    results[result.horizon].append(result)
                    if result.measure is not None:
                        outcomes.append(
                            Outcome(
                                trigger=TRIGGER,
                                rule_version=RULE_VERSION,
                                symbol=case.symbol,
                                day=case.day,
                                horizon=result.horizon,
                                case_at=case.case_at,
                                hit_at=case.hit_at,
                                live=case.live,
                                **result.measure.model_dump(),
                            )
                        )
    return results, outcomes


def track_records(
    cases: list[Case], results: dict[int, list[HorizonResult]], computed_at: datetime
) -> list[TrackRecord]:
    live = sum(case.live for case in cases)
    records = []
    for horizon in HORIZONS:
        summary = summarize(results[horizon])
        records.append(
            TrackRecord(
                trigger=TRIGGER,
                rule_version=RULE_VERSION,
                horizon=horizon,
                computed_at=computed_at,
                cases=len(cases),
                live_cases=live,
                **summary.model_dump(),
            )
        )
    return records


async def refresh_outcomes(
    session_factory: async_sessionmaker[AsyncSession], calendar: TradingCalendar, clock: Clock
) -> list[TrackRecord]:
    """Measure the current rule version and replace its stored Outcomes and Track record."""
    now = clock.now()
    cases, held_back = await discover_cases(session_factory, now)
    results, outcomes = await measure_cases(session_factory, calendar, now, cases)
    records = track_records(cases, results, now)
    async with session_factory() as session:
        repo = TrackRecordRepository(session)
        await repo.replace_outcomes(TRIGGER, RULE_VERSION, outcomes)
        await repo.upsert(records)
        await session.commit()
    logger.info(
        "outcomes for %s v%s: %d cases (%d live, %d held back for an undelivered Alert); Outcomes %s",
        TRIGGER,
        RULE_VERSION,
        len(cases),
        sum(case.live for case in cases),
        held_back,
        {record.horizon: record.outcomes for record in records},
    )
    for record in records:
        if record.unpriced or record.split_excluded:
            logger.warning(
                "%dd: %d matured cases could not be priced, %d excluded for a suspected split in unadjusted bars",
                record.horizon,
                record.unpriced,
                record.split_excluded,
            )
    return records


async def _run(dsn: str) -> None:
    engine = build_engine(dsn)
    try:
        await refresh_outcomes(build_session_factory(engine), ExchangeCalendar(), SystemClock())
    finally:
        await engine.dispose()


def main() -> int:
    """Exit 0 on success (including no cases), 1 when the job fails, 2 on bad configuration."""
    configure_logging("track-record")
    dsn = os.environ.get(DSN_VARIABLE)
    if not dsn:
        print(f"{DSN_VARIABLE} is not set", file=sys.stderr)
        return 2
    try:
        asyncio.run(_run(dsn))
    except Exception:
        logger.exception("outcomes job failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
