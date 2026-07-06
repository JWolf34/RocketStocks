import random
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rs_domain import Bar, BarClosed

SESSION_OPEN = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)


def make_event(minutes_after_open: int, sequence: int = 0) -> BarClosed:
    close_time = SESSION_OPEN + timedelta(minutes=minutes_after_open)
    bar = Bar(
        symbol="SPY",
        bucket=close_time - timedelta(minutes=5),
        open=560.10,
        high=561.25,
        low=559.90,
        close=560.75,
        volume=1_200_000,
    )
    return BarClosed(event_time=close_time, sequence=sequence, bar=bar)


class TestJsonRoundTrip:
    def test_lossless(self):
        event = make_event(5, sequence=3)
        restored = BarClosed.model_validate_json(event.model_dump_json())
        assert restored == event

    def test_float_prices_bit_exact(self):
        bar = make_event(5).bar
        restored = Bar.model_validate_json(bar.model_dump_json())
        # Bit-exactness is the replay contract (ADR-0023), not approximate equality.
        assert restored.open.hex() == bar.open.hex()
        assert restored.close.hex() == bar.close.hex()

    def test_datetime_preserves_utc(self):
        event = make_event(5)
        restored = BarClosed.model_validate_json(event.model_dump_json())
        assert restored.event_time.utcoffset() == timedelta(0)


class TestOrdering:
    def test_shuffled_events_resort_deterministically(self):
        events = [make_event(5 * i) for i in range(20)]
        # Same event_time, distinct sequence — the tie the key must break.
        events += [make_event(100, sequence=s) for s in (2, 0, 1)]
        expected = sorted(events, key=lambda e: e.ordering_key)
        for seed in (1, 7, 42):
            shuffled = events[:]
            random.Random(seed).shuffle(shuffled)
            assert sorted(shuffled, key=lambda e: e.ordering_key) == expected

    def test_sequence_breaks_event_time_ties(self):
        first = make_event(100, sequence=0)
        second = make_event(100, sequence=1)
        assert sorted([second, first], key=lambda e: e.ordering_key) == [first, second]

    def test_rejects_negative_sequence(self):
        with pytest.raises(ValidationError):
            make_event(5, sequence=-1)

    def test_frozen(self):
        with pytest.raises(ValidationError):
            make_event(5).sequence = 9
