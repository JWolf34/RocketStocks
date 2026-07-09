"""Fixture recorder + replay (S003 B7, ADR-0023): a session's BarClosed events
round-trip through JSONL preserving the (event_time, sequence) Bus-1 order, and
the recording tee passes events through unchanged."""

import logging
from datetime import datetime, timedelta, timezone

from rs_domain import Bar, BarClosed
from rs_ingest.sources.recording import (
    FixtureRecorder,
    RecordedEventSource,
    RecordingEventSource,
    load_events,
)

BUCKET0 = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)


def closed(i: int, volume: int, close: float, sequence: int = 0) -> BarClosed:
    bucket = BUCKET0 + timedelta(minutes=5 * i)
    bar = Bar(symbol="SPY", bucket=bucket, open=100.0, high=101.5, low=99.25, close=close, volume=volume)
    return BarClosed(event_time=bucket + timedelta(minutes=5), sequence=sequence, bar=bar)


# non-round floats and a wide volume range to catch any serialization drift
EVENTS = [closed(0, 480_137, 100.27), closed(1, 1_204_991, 101.73), closed(2, 91_468, 99.51)]


class FakeSource:
    def __init__(self, events: list[BarClosed]) -> None:
        self._events = events

    async def events(self):
        for e in self._events:
            yield e


def test_recorder_round_trips_events_in_order(tmp_path):
    path = tmp_path / "session.jsonl"
    recorder = FixtureRecorder(path)
    for event in EVENTS:
        recorder.record(event)

    loaded = load_events(path)
    assert loaded == EVENTS  # full equality: floats, volume, and UTC tz all survive
    assert [e.ordering_key for e in loaded] == [e.ordering_key for e in EVENTS]


def test_recorder_writes_one_json_line_per_event(tmp_path):
    path = tmp_path / "session.jsonl"
    recorder = FixtureRecorder(path)
    for event in EVENTS:
        recorder.record(event)

    assert len(path.read_text(encoding="utf-8").splitlines()) == len(EVENTS)


def test_recorder_truncates_a_stale_file_on_construction(tmp_path):
    path = tmp_path / "session.jsonl"
    path.write_text("stale-from-a-previous-run\n", encoding="utf-8")
    FixtureRecorder(path)  # fresh file for this session
    assert path.read_text(encoding="utf-8") == ""


async def test_recording_source_tees_events_through_unchanged(tmp_path):
    path = tmp_path / "session.jsonl"
    source = RecordingEventSource(FakeSource(EVENTS), FixtureRecorder(path))

    yielded = [e async for e in source.events()]
    assert yielded == EVENTS  # the pipeline sees exactly what the inner source emitted
    assert load_events(path) == EVENTS  # and the fixture recorded exactly that


async def test_recorded_source_replays_the_fixture_in_order(tmp_path):
    path = tmp_path / "session.jsonl"
    recorder = FixtureRecorder(path)
    for event in EVENTS:
        recorder.record(event)

    replayed = [e async for e in RecordedEventSource(path).events()]
    assert replayed == EVENTS
    keys = [e.ordering_key for e in replayed]
    assert keys == sorted(keys)  # replay yields the total Bus-1 order it was recorded in


class BrokenRecorder:
    """A recorder whose every write fails — stands in for a full/unwritable disk."""

    def record(self, event: BarClosed) -> None:
        raise OSError("disk full")


async def test_recording_tolerates_write_failure_and_keeps_capturing(caplog):
    # Tier-2: a fixture-write failure is logged and skipped; every event still
    # flows downstream so bar capture is never stopped by a recording problem.
    source = RecordingEventSource(FakeSource(EVENTS), BrokenRecorder())

    with caplog.at_level(logging.ERROR):
        yielded = [e async for e in source.events()]

    assert yielded == EVENTS  # capture continues, unaffected
    assert "fixture recording failed" in caplog.text  # loud, so the operator knows
