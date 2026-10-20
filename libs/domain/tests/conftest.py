"""Shared canonical-object builders for the rs_domain suites (as fixtures —
importlib mode makes sibling-module imports unreliable, fixtures always work)."""

from datetime import UTC, datetime, timedelta

import pytest

from rs_domain import Bar, BarClosed

SESSION_OPEN = datetime(2026, 7, 6, 13, 30, tzinfo=UTC)


@pytest.fixture
def make_bar():
    def _make(**overrides) -> Bar:
        fields = dict(
            symbol="SPY",
            bucket=SESSION_OPEN,
            open=560.10,
            high=561.25,
            low=559.90,
            close=560.75,
            volume=1_200_000,
        )
        fields.update(overrides)
        return Bar(**fields)

    return _make


@pytest.fixture
def make_event(make_bar):
    def _make(minutes_after_open: int, sequence: int = 0) -> BarClosed:
        close_time = SESSION_OPEN + timedelta(minutes=minutes_after_open)
        return BarClosed(
            event_time=close_time,
            sequence=sequence,
            bar=make_bar(bucket=close_time - timedelta(minutes=5)),
        )

    return _make
