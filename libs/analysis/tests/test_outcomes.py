"""Outcome maths on small synthetic histories whose answers are worked out by hand."""

import math
from datetime import UTC, date, datetime, time, timedelta

import numpy as np
import pytest

from rs_analysis.outcomes import (
    AlignedBars,
    HorizonResult,
    Measure,
    SessionGrid,
    Status,
    align,
    measure_case,
    summarize,
)
from rs_domain import StoredDailyBar

FIRST_DAY = date(2025, 1, 6)  # a Monday
SESSIONS = 330
ENTRY = 290
# The 250 sessions before ENTRY.
FIRST = ENTRY - 250
FETCHED = datetime(2026, 1, 1, tzinfo=UTC)


class WeekdayCalendar:
    """Every weekday is a session, opening at 14:30 UTC."""

    def is_session(self, day: date) -> bool:
        return day.weekday() < 5

    def session_open(self, day: date) -> datetime:
        return datetime.combine(day, time(14, 30), tzinfo=UTC)

    def session_close(self, day: date) -> datetime:
        return datetime.combine(day, time(21, 0), tzinfo=UTC)

    def sessions_in_range(self, start: date, end: date) -> list[date]:
        days = (start + timedelta(days=n) for n in range((end - start).days + 1))
        return [d for d in days if self.is_session(d)]


GRID = SessionGrid.build(WeekdayCalendar(), FIRST_DAY, FIRST_DAY + timedelta(days=SESSIONS * 2))


def flat(price: float = 100.0) -> AlignedBars:
    size = len(GRID.days)
    return AlignedBars(np.full(size, price), np.full(size, price), np.ones(size, dtype=bool))


def stock_from(returns: dict[int, float], start: int = FIRST) -> AlignedBars:
    """Opens at the previous close and closes at close x (1 + return), from session `start`
    on, so a one-session window's move is that session's return. Sessions before it have no bar."""
    size = len(GRID.days)
    opens, closes = np.full(size, np.nan), np.full(size, np.nan)
    previous = 100.0
    closes[start - 1] = previous
    for session in range(start, size):
        opens[session] = previous
        previous = previous * (1.0 + returns.get(session, 0.0))
        closes[session] = previous
    return AlignedBars(opens, closes, np.ones(size, dtype=bool))


def alternating(squeeze: bool = False) -> dict[int, float]:
    """+1% and -1% on alternate sessions over the 250 before entry; a squeeze swaps a
    +1% day for +80% and the next -1% day for the -44.44% that undoes it."""
    returns = {FIRST + i: 0.01 if i % 2 == 0 else -0.01 for i in range(250)}
    if squeeze:
        returns[FIRST + 100] = 0.8
        returns[FIRST + 101] = 1 / 1.8 - 1
    return returns


def with_entry_moves(returns: dict[int, float], first_day: float, fifth_day: float) -> dict[int, float]:
    """Entry-session return `first_day`; the fifth session ends at `fifth_day` against the entry open."""
    out = dict(returns)
    out[ENTRY] = first_day
    # sessions ENTRY+1..ENTRY+3 flat, then the move that lands the fifth close on (1 + fifth_day) x entry open
    out[ENTRY + 4] = (1 + fifth_day) / (1 + first_day) - 1
    return out


SIGMA = 1.4826 * 0.01


class TestEntry:
    def test_before_the_open_enters_that_days_open(self):
        monday = datetime(2025, 3, 3, 13, 0, tzinfo=UTC)
        assert GRID.days[GRID.entry_index(monday)] == date(2025, 3, 3)

    def test_in_session_enters_the_next_open(self):
        monday = datetime(2025, 3, 3, 18, 0, tzinfo=UTC)
        assert GRID.days[GRID.entry_index(monday)] == date(2025, 3, 4)

    def test_at_the_open_exactly_is_already_begun(self):
        monday = datetime(2025, 3, 3, 14, 30, tzinfo=UTC)
        assert GRID.days[GRID.entry_index(monday)] == date(2025, 3, 4)

    def test_a_weekend_enters_monday(self):
        saturday = datetime(2025, 3, 8, 15, 0, tzinfo=UTC)
        assert GRID.days[GRID.entry_index(saturday)] == date(2025, 3, 10)

    def test_past_the_grid_has_no_entry(self):
        assert GRID.entry_index(datetime(2030, 1, 1, tzinfo=UTC)) is None


class TestMeasure:
    def test_excess_return_and_move_size_against_a_robust_typical_move(self):
        stock = stock_from(with_entry_moves(alternating(), first_day=0.06, fifth_day=-0.10))

        results = measure_case(GRID, stock, flat(), ENTRY, last_settled=ENTRY + 10)

        one, five, twenty = results
        # SPY is flat, so excess is the stock's own move: +6% on the entry session, -10% after five.
        assert one.status is Status.MEASURED
        assert one.measure.entry_session == GRID.days[ENTRY]
        assert one.measure.exit_session == GRID.days[ENTRY]
        assert one.measure.excess_return == pytest.approx(0.06)
        assert five.measure.exit_session == GRID.days[ENTRY + 4]
        assert five.measure.excess_return == pytest.approx(-0.10)
        # Daily moves are +-1% in equal numbers: median 0, deviation 0.01, typical 1.4826 x 0.01 x sqrt(h).
        assert one.measure.typical_move == pytest.approx(SIGMA)
        assert one.measure.move_multiple == pytest.approx(0.06 / SIGMA)
        assert five.measure.typical_move == pytest.approx(SIGMA * math.sqrt(5))
        assert five.measure.move_multiple == pytest.approx(0.10 / (SIGMA * math.sqrt(5)))
        assert twenty.status is Status.NOT_MATURED

    def test_the_benchmark_is_subtracted_over_the_same_sessions(self):
        size = len(GRID.days)
        opens, closes = np.full(size, 100.0), np.full(size, 100.0)
        closes[ENTRY] = 102.0  # SPY +2% on the entry session
        spy = AlignedBars(opens, closes, np.ones(size, dtype=bool))
        stock = stock_from(with_entry_moves(alternating(), first_day=0.06, fifth_day=0.0))

        [one, *_] = measure_case(GRID, stock, spy, ENTRY, last_settled=ENTRY)

        assert one.measure.stock_return == pytest.approx(0.06)
        assert one.measure.benchmark_return == pytest.approx(0.02)
        assert one.measure.excess_return == pytest.approx(0.04)

    def test_a_squeeze_in_the_history_leaves_the_typical_move_alone(self):
        plain = stock_from(with_entry_moves(alternating(), 0.06, 0.0))
        squeezed = stock_from(with_entry_moves(alternating(squeeze=True), 0.06, 0.0))

        [plain_one, *_] = measure_case(GRID, plain, flat(), ENTRY, last_settled=ENTRY)
        [squeezed_one, *_] = measure_case(GRID, squeezed, flat(), ENTRY, last_settled=ENTRY)

        assert squeezed_one.measure.typical_move == pytest.approx(SIGMA)
        assert squeezed_one.measure.typical_move == pytest.approx(plain_one.measure.typical_move)
        # A standard deviation of the same history would call 'typical' several times larger.
        closes = squeezed.close
        daily = closes[FIRST:ENTRY] / closes[FIRST - 1 : ENTRY - 1] - 1
        assert float(np.std(daily)) > 3 * SIGMA

    def test_ordinary_share_counts_the_windows_that_were_big(self):
        stock = stock_from(with_entry_moves(alternating(squeeze=True), 0.06, -0.10))

        [one, five, _] = measure_case(GRID, stock, flat(), ENTRY, last_settled=ENTRY + 10)

        # One-session windows: only the squeeze day (+80%) and its undoing (-44%) beat 2 x 1.48%; 2 of 250.
        assert one.measure.ordinary_share == pytest.approx(2 / 250)
        # Five-session windows: 246 of them. Of those holding either squeeze day, the ones holding
        # both net out near zero; the window starting at 96 holds only the +80% and the one at 101 only the -44%.
        assert five.measure.ordinary_share == pytest.approx(2 / 246)

    def test_fewer_than_120_sessions_has_no_move_size(self):
        young = stock_from(with_entry_moves(alternating(), 0.06, 0.0), start=ENTRY - 100)

        [one, *_] = measure_case(GRID, young, flat(), ENTRY, last_settled=ENTRY)

        assert one.status is Status.MEASURED
        assert one.measure.excess_return == pytest.approx(0.06)
        assert one.measure.typical_move is None
        assert one.measure.move_multiple is None
        assert one.measure.ordinary_share is None

    def test_120_valid_sessions_is_enough(self):
        enough = stock_from(with_entry_moves(alternating(), 0.06, 0.0), start=ENTRY - 120)

        [one, *_] = measure_case(GRID, enough, flat(), ENTRY, last_settled=ENTRY)

        assert one.measure.typical_move == pytest.approx(SIGMA)

    def test_a_dead_stock_has_no_move_size(self):
        dead = stock_from({ENTRY: 0.06})

        [one, *_] = measure_case(GRID, dead, flat(), ENTRY, last_settled=ENTRY)

        assert one.measure.excess_return == pytest.approx(0.06)
        assert one.measure.typical_move is None

    def test_gaps_in_the_history_are_skipped_not_filled(self):
        stock = stock_from(with_entry_moves(alternating(), 0.06, 0.0))
        for session in range(FIRST + 10, FIRST + 41):  # 31 missing closes lose 32 daily moves, 16 of each sign
            stock.close[session] = np.nan

        [one, *_] = measure_case(GRID, stock, flat(), ENTRY, last_settled=ENTRY)

        assert one.measure.typical_move == pytest.approx(SIGMA)


class TestMaturity:
    def test_a_horizon_is_due_once_its_exit_session_is_settled(self):
        stock = stock_from(with_entry_moves(alternating(), 0.06, -0.10))

        before = measure_case(GRID, stock, flat(), ENTRY, last_settled=ENTRY + 3)
        on_the_day = measure_case(GRID, stock, flat(), ENTRY, last_settled=ENTRY + 4)

        assert [r.status for r in before] == [Status.MEASURED, Status.NOT_MATURED, Status.NOT_MATURED]
        assert [r.status for r in on_the_day] == [Status.MEASURED, Status.MEASURED, Status.NOT_MATURED]

    def test_an_entry_that_has_not_begun_has_no_outcome(self):
        stock = stock_from(alternating())

        results = measure_case(GRID, stock, flat(), ENTRY, last_settled=ENTRY - 1)

        assert {r.status for r in results} == {Status.NOT_MATURED}

    def test_a_due_horizon_missing_the_stock_exit_bar_is_unpriced(self):
        stock = stock_from(with_entry_moves(alternating(), 0.06, -0.10))
        stock.close[ENTRY + 4] = np.nan

        one, five, _ = measure_case(GRID, stock, flat(), ENTRY, last_settled=ENTRY + 4)

        assert one.status is Status.MEASURED
        assert five.status is Status.UNPRICED
        assert five.measure is None

    def test_a_missing_entry_open_leaves_every_due_horizon_unpriced(self):
        stock = stock_from(with_entry_moves(alternating(), 0.06, -0.10))
        stock.open[ENTRY] = np.nan

        results = measure_case(GRID, stock, flat(), ENTRY, last_settled=ENTRY + 4)

        assert [r.status for r in results] == [Status.UNPRICED, Status.UNPRICED, Status.NOT_MATURED]

    def test_a_missing_benchmark_bar_is_unpriced(self):
        stock = stock_from(with_entry_moves(alternating(), 0.06, -0.10))
        spy = flat()
        spy.close[ENTRY] = np.nan

        [one, *_] = measure_case(GRID, stock, spy, ENTRY, last_settled=ENTRY)

        assert one.status is Status.UNPRICED


class TestSplitGuard:
    def halved(self, adjusted: bool) -> AlignedBars:
        stock = stock_from(with_entry_moves(alternating(), 0.06, 0.0))
        # A 2-for-1 split shows as a halving between two closes when bars are not adjusted.
        stock.close[FIRST + 60 :] *= 0.5
        stock.open[FIRST + 61 :] *= 0.5
        if not adjusted:
            stock.adjusted[:] = False
        return stock

    def test_a_split_shaped_jump_in_unadjusted_bars_excludes_the_case(self):
        [one, *_] = measure_case(GRID, self.halved(adjusted=False), flat(), ENTRY, last_settled=ENTRY)

        assert one.status is Status.SUSPECTED_SPLIT
        assert one.measure is None

    def test_adjusted_bars_are_trusted(self):
        [one, *_] = measure_case(GRID, self.halved(adjusted=True), flat(), ENTRY, last_settled=ENTRY)

        assert one.status is Status.MEASURED

    def test_unadjusted_bars_without_a_split_shaped_jump_are_measured(self):
        stock = stock_from(with_entry_moves(alternating(squeeze=True), 0.06, 0.0))
        stock.adjusted[:] = False

        [one, *_] = measure_case(GRID, stock, flat(), ENTRY, last_settled=ENTRY)

        assert one.status is Status.MEASURED


class TestAlign:
    def test_bars_land_on_their_sessions_and_the_rest_is_nan(self):
        day = GRID.days[5]
        bar = StoredDailyBar(
            symbol="ABC", session_date=day, open=10.0, high=12.0, low=9.0, close=11.0,
            volume=1, fetched_at=FETCHED, adjusted=False,
        )
        off_grid = bar.model_copy(update={"session_date": date(1999, 1, 4)})

        aligned = align(GRID, [bar, off_grid])

        assert aligned.open[5] == 10.0 and aligned.close[5] == 11.0
        assert not aligned.adjusted[5]
        assert int(np.isfinite(aligned.close).sum()) == 1


def result(horizon: int, excess: float, multiple: float | None, ordinary: float | None = 0.1) -> HorizonResult:
    return HorizonResult(
        horizon,
        Status.MEASURED,
        Measure(
            entry_session=date(2026, 1, 5), exit_session=date(2026, 1, 5), stock_return=excess,
            benchmark_return=0.0, excess_return=excess, typical_move=None if multiple is None else 0.01,
            move_multiple=multiple, ordinary_share=None if multiple is None else ordinary,
        ),
    )


class TestSummarize:
    def test_shares_and_medians_over_the_sized_outcomes(self):
        results = [
            result(5, -0.02, 3.0, ordinary=0.10),
            result(5, -0.01, 1.0, ordinary=0.20),
            result(5, 0.03, 2.0, ordinary=0.30),
            result(5, 0.50, None),  # an Outcome with no move size
            HorizonResult(5, Status.UNPRICED),
            HorizonResult(5, Status.SUSPECTED_SPLIT),
            HorizonResult(5, Status.NOT_MATURED),
        ]

        summary = summarize(results)

        assert summary.outcomes == 4
        assert summary.unpriced == 1
        assert summary.split_excluded == 1
        assert summary.no_move_size == 1
        assert summary.share_at_2x == pytest.approx(2 / 3)  # 3.0x and 2.0x of three sized
        assert summary.ordinary_share == pytest.approx(0.20)  # each case weighs the same
        assert summary.median_multiple == pytest.approx(2.0)
        # Direction counts every Outcome, sized or not: median of -0.02, -0.01, 0.03, 0.50.
        assert summary.median_excess == pytest.approx(0.01)

    def test_nothing_measured_has_no_statistics(self):
        summary = summarize([HorizonResult(1, Status.NOT_MATURED)])

        assert summary.outcomes == 0
        assert summary.share_at_2x is None
        assert summary.ordinary_share is None
        assert summary.median_multiple is None
        assert summary.median_excess is None
