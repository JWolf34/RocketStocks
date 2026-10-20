"""Session placement and summary edge cases, without a database."""

from datetime import UTC, date, datetime

import numpy as np
import pandas as pd
import pytest

from rs_research.returns import first_session_after, last_close_before, measure

# Thu Oct 1, Fri Oct 2, Mon Oct 5, Tue Oct 6 2026
SESSIONS = np.array([date(2026, 10, d).toordinal() for d in (1, 2, 5, 6)], dtype=np.int64)


def utc(day: int, hour: int) -> datetime:
    return datetime(2026, 10, day, hour, tzinfo=UTC)


def test_the_first_session_after_an_event_skips_the_weekend():
    assert list(first_session_after(SESSIONS, [utc(1, 15), utc(2, 15), utc(3, 15), utc(6, 15)])) == [1, 2, 2, 4]


def test_event_date_is_the_eastern_date():
    # 02:00 UTC Oct 2 is 22:00 ET Oct 1: still the Oct 1 event date.
    assert list(first_session_after(SESSIONS, [utc(2, 2)])) == [1]


def test_the_reference_close_depends_on_whether_the_session_has_closed():
    instants = [utc(2, 15), utc(2, 21), utc(3, 12), utc(1, 12)]  # 11:00, 17:00 ET Fri; Sat; Thu morning
    assert list(last_close_before(SESSIONS, instants)) == [0, 1, 1, -1]


def test_events_without_a_price_or_a_reference_are_counted_and_excluded():
    closes = pd.Series(
        [10.0, 11.0],
        index=pd.MultiIndex.from_arrays([["AAA", "AAA"], SESSIONS[:2]], names=["symbol", "session"]),
    )
    spy = np.array([100.0, 100.0, 100.0, 100.0])

    summary = measure(
        ["AAA", "BBB", "CCC"], np.array([0, 0, -1]), np.array([10.0, np.nan, 10.0]), SESSIONS, spy, closes
    )

    assert (summary.n, summary.n_unpriced) == (1, 2)
    assert summary.hit_rate[1] == 1.0  # +10% against a flat SPY
    assert summary.median_excess[1] == pytest.approx(0.1)
    assert summary.hit_rate[5] is None and summary.median_excess[10] is None



def test_a_matured_horizon_without_an_exit_bar_is_counted_not_hidden():
    # AAA has its entry bar but no bar on the next session (delisted, halted).
    closes = pd.Series(
        [10.0],
        index=pd.MultiIndex.from_arrays([["AAA"], SESSIONS[:1]], names=["symbol", "session"]),
    )
    spy = np.array([100.0, 100.0, 100.0, 100.0])

    summary = measure(["AAA"], np.array([0]), np.array([10.0]), SESSIONS, spy, closes)

    assert summary.n == 1
    assert summary.n_missing_exit == 1  # the 1-day horizon; 5 and 10 have not matured
    assert summary.hit_rate[1] is None


def test_a_spy_entry_override_replaces_the_reference_close_and_nan_falls_back():
    closes = pd.Series(
        [10.0, 10.0, 11.0, 11.0],
        index=pd.MultiIndex.from_arrays([["AAA"] * 2 + ["BBB"] * 2, np.tile(SESSIONS[:2], 2)], names=["symbol", "session"]),
    )
    spy = np.array([100.0, 110.0, 100.0, 100.0])

    summary = measure(
        ["AAA", "BBB"], np.array([0, 0]), np.array([10.0, 10.0]), SESSIONS, spy, closes, np.array([110.0, np.nan])
    )

    # AAA: flat, SPY from 110 to 110 is flat too; BBB: +10%, SPY +10% from its close.
    assert summary.n_approx_benchmark == 1
    assert summary.median_excess[1] == pytest.approx(0.0)


def test_a_session_without_a_spy_bar_is_a_missing_exit():
    closes = pd.Series(
        [10.0, 11.0],
        index=pd.MultiIndex.from_arrays([["AAA", "AAA"], SESSIONS[:2]], names=["symbol", "session"]),
    )
    spy = np.array([100.0, np.nan, 100.0, 100.0])

    summary = measure(["AAA"], np.array([0]), np.array([10.0]), SESSIONS, spy, closes)

    assert summary.n_missing_exit == 1
    assert summary.hit_rate[1] is None
