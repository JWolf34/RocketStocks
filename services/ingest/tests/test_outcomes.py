"""The nightly Outcomes job against a real database, the real exchange calendar and
synthetic bars whose answers are worked out by hand.

Every stock's history before its entry is +1% and -1% on alternate sessions (median
0, deviation 0.01), so its typical one-session move is 1.4826 x 0.01 and its typical
h-session move that times sqrt(h). SPY sits at 100 unless a test moves it, so excess
is the stock's own move less SPY's.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import select, text

from rs_analysis.outcomes import HORIZONS
from rs_analysis.popularity_surge import RULE_VERSION
from rs_domain import (
    Alert,
    Candidate,
    PopularityOrigin,
    PopularitySnapshot,
    StoredDailyBar,
    Verdict,
)
from rs_ingest.outcomes import OutcomesError, refresh_outcomes
from rs_persistence import (
    AlertRepository,
    CandidateRepository,
    DailyBarRepository,
    OutcomeRow,
    PopularityRepository,
    TrackRecordRow,
)
from rs_sources import ExchangeCalendar

CALENDAR = ExchangeCalendar()
SESSIONS = CALENDAR.sessions_in_range(date(2024, 1, 1), date(2026, 12, 31))
TRIGGER = "popularity_surge"
# Friday 2026-10-23, 18:00 ET: two hours past the close, so that day's candle is settled.
NOW = datetime(2026, 10, 23, 22, 0, tzinfo=UTC)
LAST_SESSION = date(2026, 10, 23)
FETCHED = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)
SIGMA = 1.4826 * 0.01


class FakeClock:
    def __init__(self, now: datetime = NOW) -> None:
        self.current = now

    def now(self) -> datetime:
        return self.current


def _bar(symbol: str, day: date, open_: float, close: float, adjusted: bool = True) -> StoredDailyBar:
    return StoredDailyBar(
        symbol=symbol,
        session_date=day,
        open=open_,
        high=max(open_, close),
        low=min(open_, close),
        close=close,
        volume=1000,
        fetched_at=FETCHED,
        adjusted=adjusted,
    )


def stock_bars(
    symbol: str,
    entry: date,
    after: Mapping[int, float],
    *,
    history: int = 250,
    squeeze: tuple[int, float] | None = None,
    adjusted: bool = True,
    skip: frozenset[date] = frozenset(),
    through: date = LAST_SESSION,
) -> list[StoredDailyBar]:
    """Bars for the `history` sessions before `entry` and every session from it to `through`.
    The entry open is the prior close; the close of the k-th session from entry (counting it)
    is `after[k]` x that open, and a session without one closes where the last did.
    `squeeze=(offset, factor)` multiplies the close at that offset into the 250 sessions
    before entry by `factor` and the next session by 1/factor, undoing it."""
    index = SESSIONS.index(entry)
    bars, previous, entry_open = [], 100.0, 100.0
    for i in range(index - history - 1, SESSIONS.index(through) + 1):
        day = SESSIONS[i]
        if i < index:
            offset = i - (index - 250)
            move = 0.0 if i == index - history - 1 else (0.01 if offset % 2 == 0 else -0.01)
            if squeeze is not None and offset == squeeze[0]:
                move = squeeze[1] - 1
            if squeeze is not None and offset == squeeze[0] + 1:
                move = 1 / squeeze[1] - 1
            close = previous * (1 + move)
        else:
            if i == index:
                entry_open = previous
            k = i - index + 1
            close = entry_open * after[k] if k in after else previous
        if day not in skip:
            bars.append(_bar(symbol, day, previous, close, adjusted))
        previous = close
    return bars


def spy_bars(entry: date, after: Mapping[int, float] | None = None, *, through: date = LAST_SESSION) -> list[StoredDailyBar]:
    """SPY at 100 on every session from well before `entry` to `through`, except that the
    k-th session from entry (counting it) closes at after[k]."""
    index = SESSIONS.index(entry)
    moved = {SESSIONS[index + k - 1]: close for k, close in (after or {}).items()}
    return [
        _bar("SPY", SESSIONS[i], 100.0, moved.get(SESSIONS[i], 100.0))
        for i in range(index - 300, SESSIONS.index(through) + 1)
    ]


def surge(symbol: str, at: datetime, origin: PopularityOrigin = PopularityOrigin.V1_IMPORT) -> PopularitySnapshot:
    """A snapshot the rule fires on: three times the mentions of a day ago."""
    return PopularitySnapshot(
        polled_at=at,
        ticker=symbol,
        rank=100,
        mentions=60,
        upvotes=0,
        rank_24h_ago=None,
        mentions_24h_ago=20,
        fetched_at=at + timedelta(seconds=20),
        origin=origin,
    )


@dataclass
class Seed:
    snapshots: list[PopularitySnapshot] = field(default_factory=list)
    bars: list[StoredDailyBar] = field(default_factory=list)


async def _store(session_factory, seed: Seed) -> None:
    async with session_factory() as s:
        await PopularityRepository(s).insert_many(seed.snapshots)
        await DailyBarRepository(s).insert_many(seed.bars)
        await s.commit()


async def _refresh(session_factory, clock: FakeClock | None = None):
    return await refresh_outcomes(session_factory, CALENDAR, clock or FakeClock())


async def _outcomes(session_factory) -> dict[tuple[str, date, int], OutcomeRow]:
    async with session_factory() as s:
        rows = (await s.scalars(select(OutcomeRow))).all()
    return {(row.symbol, row.day, row.horizon): row for row in rows}


async def _records(session_factory) -> dict[int, TrackRecordRow]:
    async with session_factory() as s:
        rows = (await s.scalars(select(TrackRecordRow).where(TrackRecordRow.rule_version == RULE_VERSION))).all()
    return {row.horizon: row for row in rows}


async def _single_case(session_factory, symbol: str, at: datetime, entry: date, after, spy_after, **kwargs):
    await _store(
        session_factory,
        Seed([surge(symbol, at)], stock_bars(symbol, entry, after, **kwargs) + spy_bars(entry, spy_after)),
    )
    await _refresh(session_factory)
    return await _outcomes(session_factory)


# --- entry and exit sessions, and the hand-computed Outcomes ------------------------------

# Stock +5% on the entry session, -8% at the fifth and +10% at the twentieth;
# SPY +1%, +2% and +4% over the same sessions.
STOCK_AFTER = {1: 1.05, 5: 0.92, 20: 1.10}
SPY_AFTER = {1: 101.0, 5: 102.0, 20: 104.0}
EXCESS = {1: 0.05 - 0.01, 5: -0.08 - 0.02, 20: 0.10 - 0.04}
TIMINGS = [
    # (case, the first session that begins after it, the exit sessions at 1, 5 and 20 sessions)
    pytest.param(
        datetime(2026, 7, 7, 15, 0, tzinfo=UTC),  # Tuesday 11:00 ET, in session
        date(2026, 7, 8),
        (date(2026, 7, 8), date(2026, 7, 14), date(2026, 8, 4)),
        id="in-session-enters-the-next-open",
    ),
    pytest.param(
        datetime(2026, 7, 7, 12, 0, tzinfo=UTC),  # Tuesday 08:00 ET, before the open
        date(2026, 7, 7),
        (date(2026, 7, 7), date(2026, 7, 13), date(2026, 8, 3)),
        id="pre-market-enters-that-days-open",
    ),
    pytest.param(
        datetime(2026, 7, 11, 15, 0, tzinfo=UTC),  # Saturday
        date(2026, 7, 13),
        (date(2026, 7, 13), date(2026, 7, 17), date(2026, 8, 7)),
        id="weekend-enters-monday",
    ),
    pytest.param(
        datetime(2026, 7, 8, 21, 0, tzinfo=UTC),  # Wednesday 17:00 ET, after the close
        date(2026, 7, 9),
        (date(2026, 7, 9), date(2026, 7, 15), date(2026, 8, 5)),
        id="after-hours-enters-the-next-open",
    ),
]


@pytest.mark.parametrize(("case_at", "entry", "exits"), TIMINGS)
async def test_an_outcome_enters_at_the_next_open_and_exits_at_the_hth_close(session_factory, case_at, entry, exits):
    found = await _single_case(session_factory, "ABC", case_at, entry, STOCK_AFTER, SPY_AFTER)

    day = case_at.date()  # the Eastern day is the UTC date for each of these
    assert {horizon: found[("ABC", day, horizon)].exit_session for horizon in HORIZONS} == dict(
        zip(HORIZONS, exits, strict=True)
    )
    for horizon in HORIZONS:
        row = found[("ABC", day, horizon)]
        assert row.entry_session == entry
        assert row.stock_return == pytest.approx({1: 0.05, 5: -0.08, 20: 0.10}[horizon])
        assert row.benchmark_return == pytest.approx({1: 0.01, 5: 0.02, 20: 0.04}[horizon])
        assert row.excess_return == pytest.approx(EXCESS[horizon])
        typical = SIGMA * horizon**0.5
        assert row.typical_move == pytest.approx(typical)
        assert row.move_multiple == pytest.approx(abs(EXCESS[horizon]) / typical)
        assert row.case_at == case_at
        assert row.live is False
    # 0.04 / 0.014826, 0.10 / (0.014826 x sqrt 5) and 0.06 / (0.014826 x sqrt 20)
    assert [round(found[("ABC", day, h)].move_multiple, 2) for h in HORIZONS] == [2.70, 3.02, 0.90]


async def test_the_track_record_summarises_the_outcomes_per_horizon(session_factory):
    await _single_case(
        session_factory, "ABC", datetime(2026, 7, 7, 15, 0, tzinfo=UTC), date(2026, 7, 8), STOCK_AFTER, SPY_AFTER
    )

    records = await _records(session_factory)

    assert sorted(records) == [1, 5, 20]
    for horizon, record in records.items():
        assert (record.cases, record.live_cases, record.outcomes) == (1, 0, 1)
        assert (record.unpriced, record.split_excluded, record.no_move_size) == (0, 0, 0)
        assert record.median_excess == pytest.approx(EXCESS[horizon])
        assert record.median_multiple == pytest.approx(abs(EXCESS[horizon]) / (SIGMA * horizon**0.5))
        # One case moved 2x typical or more at 1 and 5 sessions, not at 20.
        assert record.share_at_2x == (1.0 if horizon in (1, 5) else 0.0)
        # An ordinary +-1% day or window never reaches 2x typical.
        assert record.ordinary_share == 0.0
        assert record.computed_at == NOW


async def test_a_squeeze_in_the_history_leaves_the_typical_move_sensible(session_factory):
    entry = date(2026, 7, 8)
    # +80% on one session of the year before and the -44.4% that undoes it.
    found = await _single_case(
        session_factory, "SQZ", datetime(2026, 7, 7, 15, 0, tzinfo=UTC), entry, STOCK_AFTER, SPY_AFTER,
        squeeze=(100, 1.8),
    )

    one, five = found[("SQZ", date(2026, 7, 7), 1)], found[("SQZ", date(2026, 7, 7), 5)]
    # The median absolute deviation ignores the two squeeze days: typical is still 1.4826 x 0.01.
    assert one.typical_move == pytest.approx(SIGMA)
    assert one.move_multiple == pytest.approx(0.04 / SIGMA)
    # Only the squeeze day and its undoing beat 2x typical among the 250 one-session windows,
    # and among the 246 five-session ones only the window starting just before the +80% and
    # the one starting at the -44.4% (the others hold both days and net out near zero).
    assert one.ordinary_share == pytest.approx(2 / 250)
    assert five.ordinary_share == pytest.approx(2 / 246)


async def test_fewer_than_120_sessions_of_history_has_no_move_size_and_is_counted(session_factory):
    found = await _single_case(
        session_factory, "NEW", datetime(2026, 7, 7, 15, 0, tzinfo=UTC), date(2026, 7, 8), STOCK_AFTER, SPY_AFTER,
        history=100,
    )

    row = found[("NEW", date(2026, 7, 7), 5)]
    assert row.excess_return == pytest.approx(EXCESS[5])
    assert (row.typical_move, row.move_multiple, row.ordinary_share) == (None, None, None)
    record = (await _records(session_factory))[5]
    assert (record.outcomes, record.no_move_size) == (1, 1)
    assert (record.share_at_2x, record.ordinary_share, record.median_multiple) == (None, None, None)
    assert record.median_excess == pytest.approx(EXCESS[5])


# --- one case per stock and Eastern day -----------------------------------------------------


def _candidate(symbol: str, hit_at: datetime, verdict: Verdict = Verdict.POSTED) -> Candidate:
    return Candidate(
        trigger=TRIGGER,
        rule_version=RULE_VERSION,
        symbol=symbol,
        hit_at=hit_at,
        inputs={"fired": ["mention_surge"]},
        verdict=verdict,
        decided_at=hit_at + timedelta(seconds=20),
    )


async def _candidates(session_factory, *candidates: Candidate) -> None:
    async with session_factory() as s:
        repo = CandidateRepository(s)
        for candidate in candidates:
            await repo.insert(candidate)
        await s.commit()


async def test_live_and_replayed_hits_of_a_stock_day_are_one_case(session_factory):
    tuesday = date(2026, 7, 7)
    at = lambda hour, minute=0: datetime(2026, 7, 7, hour, minute, tzinfo=UTC)  # noqa: E731
    entry = date(2026, 7, 8)
    seed = Seed(
        [
            # a live hit the poll also stored, then a second hit that day: one live case at the first
            surge("LIV", at(14), PopularityOrigin.LIVE),
            surge("LIV", at(16), PopularityOrigin.LIVE),
            # imported history only: one replayed case at the first of its two hits
            surge("OLD", at(14, 30)),
            surge("OLD", at(15)),
            # a replayed hit before the live poll's stored one: still one case, live, timed at the earlier
            surge("MIX", at(13), PopularityOrigin.LIVE),
            surge("MIX", at(15), PopularityOrigin.LIVE),
        ],
    )
    for symbol in ("LIV", "OLD", "MIX"):
        seed.bars += stock_bars(symbol, entry, STOCK_AFTER)
    seed.bars += spy_bars(entry, SPY_AFTER)
    await _store(session_factory, seed)
    await _candidates(
        session_factory,
        _candidate("LIV", at(14)),
        _candidate("LIV", at(16), Verdict.REPEAT),
        _candidate("MIX", at(15)),
        # a stored hit on a day of its own that no snapshot replays
        _candidate("LIV", datetime(2026, 7, 9, 15, 0, tzinfo=UTC)),
    )

    await _refresh(session_factory)

    found = await _outcomes(session_factory)
    cases = {(symbol, day) for symbol, day, _ in found}
    assert cases == {("LIV", tuesday), ("LIV", date(2026, 7, 9)), ("OLD", tuesday), ("MIX", tuesday)}
    assert len(found) == 4 * len(HORIZONS)  # one Outcome per case and horizon, however many hits
    assert found[("LIV", tuesday, 5)].hit_at == at(14)
    assert found[("OLD", tuesday, 5)].hit_at == at(14, 30)
    assert found[("MIX", tuesday, 5)].hit_at == at(13)
    live = {(symbol, day): found[(symbol, day, 5)].live for symbol, day, _ in found}
    assert live == {("LIV", tuesday): True, ("LIV", date(2026, 7, 9)): True, ("OLD", tuesday): False, ("MIX", tuesday): True}
    records = await _records(session_factory)
    assert {(r.cases, r.live_cases) for r in records.values()} == {(4, 3)}


async def test_a_posted_case_is_timed_at_its_post_not_its_hit(session_factory):
    hit = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)  # 08:00 ET, before the open
    posted = datetime(2026, 7, 7, 15, 0, tzinfo=UTC)  # the bot delivered it in session
    await _store(session_factory, Seed([surge("PST", hit)], stock_bars("PST", date(2026, 7, 8), STOCK_AFTER) + spy_bars(date(2026, 7, 8), SPY_AFTER)))
    async with session_factory() as s:
        alert_id = await AlertRepository(s).insert(
            Alert(symbol="PST", trigger=TRIGGER, triggered_at=hit, rule_version=RULE_VERSION)
        )
        await s.execute(text("UPDATE alerts SET posted_at = :at, delivery = 'posted' WHERE id = :id"), {"at": posted, "id": alert_id})
        await s.commit()

    await _refresh(session_factory)

    row = (await _outcomes(session_factory))[("PST", date(2026, 7, 7), 1)]
    # Timed at the hit it would have entered that day's open; the post puts it at the next.
    assert (row.hit_at, row.case_at, row.entry_session) == (hit, posted, date(2026, 7, 8))
    assert row.excess_return == pytest.approx(EXCESS[1])


async def _alert_in(session_factory, symbol: str, hit: datetime, delivery: str) -> None:
    async with session_factory() as s:
        alert_id = await AlertRepository(s).insert(
            Alert(symbol=symbol, trigger=TRIGGER, triggered_at=hit, rule_version=RULE_VERSION)
        )
        await s.execute(
            text("UPDATE alerts SET delivery = CAST(:delivery AS alert_delivery) WHERE id = :id"),
            {"delivery": delivery, "id": alert_id},
        )
        await s.commit()


@pytest.mark.parametrize("delivery", ["pending", "sending"])
async def test_a_case_whose_alert_may_still_post_waits_for_its_post(session_factory, delivery):
    hit = NOW - timedelta(minutes=30)  # Friday 17:30 ET, inside the bot's stale limit
    await _store(session_factory, Seed([surge("WAIT", hit)], spy_bars(date(2026, 10, 26))))
    await _alert_in(session_factory, "WAIT", hit, delivery)

    await _refresh(session_factory)

    assert await _outcomes(session_factory) == {}
    assert {r.cases for r in (await _records(session_factory)).values()} == {0}


async def test_once_the_stale_limit_passes_an_unposted_alert_is_timed_at_its_hit(session_factory):
    hit = NOW - timedelta(minutes=30)
    await _store(session_factory, Seed([surge("LATE", hit)], spy_bars(date(2026, 10, 26))))
    await _alert_in(session_factory, "LATE", hit, "pending")
    await _refresh(session_factory)
    assert {r.cases for r in (await _records(session_factory)).values()} == {0}

    # An hour and a minute after the hit the bot would record it stale, so it can never post.
    await _refresh(session_factory, FakeClock(hit + timedelta(hours=1, minutes=1)))

    assert {(r.cases, r.live_cases) for r in (await _records(session_factory)).values()} == {(1, 0)}


@pytest.mark.parametrize("delivery", ["stale", "pending", "sending"])
async def test_an_alert_that_never_posts_still_counts_timed_at_its_hit(session_factory, delivery):
    # Stale, or abandoned after its sends failed (still pending or sending, long past the limit).
    hit = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)  # 08:00 ET: enters that day's open
    await _store(
        session_factory,
        Seed([surge("GHOST", hit)], stock_bars("GHOST", date(2026, 7, 7), STOCK_AFTER) + spy_bars(date(2026, 7, 7), SPY_AFTER)),
    )
    await _alert_in(session_factory, "GHOST", hit, delivery)

    await _refresh(session_factory)

    row = (await _outcomes(session_factory))[("GHOST", date(2026, 7, 7), 1)]
    assert (row.case_at, row.entry_session) == (hit, date(2026, 7, 7))
    assert row.excess_return == pytest.approx(EXCESS[1])


async def test_the_benchmark_is_never_a_case(session_factory):
    hit = datetime(2026, 7, 7, 15, 0, tzinfo=UTC)
    await _store(session_factory, Seed([surge("SPY", hit)], spy_bars(date(2026, 7, 8))))

    await _refresh(session_factory)

    assert await _outcomes(session_factory) == {}
    assert {r.cases for r in (await _records(session_factory)).values()} == {0}


async def test_an_old_rule_versions_candidates_are_not_this_versions_cases(session_factory):
    hit = datetime(2026, 7, 7, 15, 0, tzinfo=UTC)
    await _candidates(session_factory, _candidate("OLDV", hit).model_copy(update={"rule_version": "0"}))
    await _store(session_factory, Seed(bars=spy_bars(date(2026, 7, 8))))

    await _refresh(session_factory)

    assert await _outcomes(session_factory) == {}


# --- matured horizons ----------------------------------------------------------------------


async def test_a_horizon_has_an_outcome_once_its_exit_session_is_settled(session_factory):
    case = datetime(2026, 10, 12, 15, 0, tzinfo=UTC)  # Monday: enters Tuesday 10-13, the 9th session by 10-23
    not_entered = datetime(2026, 10, 23, 21, 30, tzinfo=UTC)  # after Friday's close: enters Monday 10-26
    entry = date(2026, 10, 13)
    await _store(
        session_factory,
        Seed(
            [surge("MID", case), surge("LATE", not_entered)],
            stock_bars("MID", entry, {1: 1.05, 5: 0.92}) + stock_bars("LATE", date(2026, 10, 26), {}, through=LAST_SESSION)
            + spy_bars(entry),
        ),
    )

    await _refresh(session_factory)

    found = await _outcomes(session_factory)
    assert sorted(found) == [("MID", date(2026, 10, 12), 1), ("MID", date(2026, 10, 12), 5)]
    assert found[("MID", date(2026, 10, 12), 5)].exit_session == date(2026, 10, 19)
    records = await _records(session_factory)
    assert {h: (r.cases, r.outcomes, r.unpriced) for h, r in records.items()} == {1: (2, 1, 0), 5: (2, 1, 0), 20: (2, 0, 0)}
    assert records[20].median_excess is None


async def test_a_later_run_adds_the_matured_outcome_and_keeps_the_rest(session_factory):
    case = datetime(2026, 10, 12, 15, 0, tzinfo=UTC)
    entry = date(2026, 10, 13)
    early = Seed([surge("MAT", case)], stock_bars("MAT", entry, {1: 1.05, 5: 0.92, 20: 1.10}) + spy_bars(entry))
    await _store(session_factory, early)
    await _refresh(session_factory)
    first = await _outcomes(session_factory)
    assert sorted(h for _, _, h in first) == [1, 5]

    later = date(2026, 11, 9)  # the 20th session from 10-13
    clock = FakeClock(datetime(2026, 11, 9, 22, 0, tzinfo=UTC))
    more = [b for b in stock_bars("MAT", entry, {1: 1.05, 5: 0.92, 20: 1.10}, through=later) + spy_bars(entry, through=later) if b.session_date > LAST_SESSION]
    await _store(session_factory, Seed(bars=more))
    await _refresh(session_factory, clock)

    second = await _outcomes(session_factory)
    assert sorted(h for _, _, h in second) == [1, 5, 20]
    assert second[("MAT", date(2026, 10, 12), 20)].exit_session == later
    for key, row in first.items():
        assert second[key].excess_return == row.excess_return
    assert (await _records(session_factory))[20].computed_at == clock.current


async def test_a_rerun_changes_nothing(session_factory):
    await _single_case(
        session_factory, "ABC", datetime(2026, 7, 7, 15, 0, tzinfo=UTC), date(2026, 7, 8), STOCK_AFTER, SPY_AFTER
    )
    snapshot = {k: (r.excess_return, r.move_multiple, r.case_at) for k, r in (await _outcomes(session_factory)).items()}
    records = {h: (r.cases, r.outcomes, r.median_excess) for h, r in (await _records(session_factory)).items()}

    await _refresh(session_factory)

    assert {k: (r.excess_return, r.move_multiple, r.case_at) for k, r in (await _outcomes(session_factory)).items()} == snapshot
    assert {h: (r.cases, r.outcomes, r.median_excess) for h, r in (await _records(session_factory)).items()} == records


# --- missing bars, unadjusted bars and an unmeasurable store ----------------------------------


async def test_a_due_horizon_with_a_missing_bar_is_counted_as_unpriced(session_factory):
    entry = date(2026, 7, 8)
    exit_5 = date(2026, 7, 14)
    await _store(
        session_factory,
        Seed(
            [surge("GAP", datetime(2026, 7, 7, 15, 0, tzinfo=UTC)), surge("GONE", datetime(2026, 7, 7, 15, 0, tzinfo=UTC))],
            stock_bars("GAP", entry, STOCK_AFTER, skip=frozenset({exit_5})) + spy_bars(entry, SPY_AFTER),
        ),
    )

    await _refresh(session_factory)

    found = await _outcomes(session_factory)
    assert sorted(found) == [("GAP", date(2026, 7, 7), 1), ("GAP", date(2026, 7, 7), 20)]
    records = await _records(session_factory)
    # GONE has no bars at all, so every horizon is unpriced for it; GAP only lacks the 5-session exit.
    assert {h: (r.cases, r.outcomes, r.unpriced) for h, r in records.items()} == {1: (2, 1, 1), 5: (2, 0, 2), 20: (2, 1, 1)}
    assert records[5].median_excess is None


async def test_without_benchmark_bars_nothing_is_measured_and_the_last_track_record_stays(session_factory):
    await _single_case(
        session_factory, "ABC", datetime(2026, 7, 7, 15, 0, tzinfo=UTC), date(2026, 7, 8), STOCK_AFTER, SPY_AFTER
    )
    async with session_factory() as s:
        await s.execute(text("DELETE FROM daily_bar WHERE symbol = 'SPY'"))
        await s.commit()

    with pytest.raises(OutcomesError, match="SPY"):
        await _refresh(session_factory)

    assert len(await _outcomes(session_factory)) == 3
    assert (await _records(session_factory))[5].outcomes == 1


async def test_unadjusted_bars_with_a_split_shaped_jump_are_excluded_and_counted(session_factory):
    entry = date(2026, 7, 8)
    bars = stock_bars("SPL", entry, STOCK_AFTER, adjusted=False)
    # A 2-for-1 split shows as a halving between two history closes when bars are not adjusted.
    halved = SESSIONS.index(entry) - 60
    bars = [
        b.model_copy(update={"open": b.open * 0.5, "close": b.close * 0.5, "high": b.high * 0.5, "low": b.low * 0.5})
        if SESSIONS.index(b.session_date) >= halved
        else b
        for b in bars
    ]
    await _store(
        session_factory,
        Seed([surge("SPL", datetime(2026, 7, 7, 15, 0, tzinfo=UTC))], bars + spy_bars(entry, SPY_AFTER)),
    )

    await _refresh(session_factory)

    assert await _outcomes(session_factory) == {}
    records = await _records(session_factory)
    assert {h: (r.cases, r.outcomes, r.split_excluded, r.unpriced) for h, r in records.items()} == {
        1: (1, 0, 1, 0), 5: (1, 0, 1, 0), 20: (1, 0, 1, 0)
    }


async def test_with_no_cases_every_horizon_says_zero_outcomes(session_factory):
    await _refresh(session_factory)

    records = await _records(session_factory)
    assert {h: (r.cases, r.live_cases, r.outcomes) for h, r in records.items()} == {h: (0, 0, 0) for h in HORIZONS}
