"""Fixture recording + replay behind the EventSource port (S003 B7, ADR-0023).

A live session's BarClosed events are teed to a JSONL file — one event per line,
pydantic's canonical JSON — as they flow to the pipeline; the same file replays
back through RecordedEventSource for warm-up, backtest, and replay tests. The
total Bus-1 order (`event_time, sequence` — rs_domain.events) is preserved by
append-on-emit and read-in-order, so a replay sorts identically to the live run.

Recording is a tee at the *source*, upstream of persistence: the fixture is what
the source emitted, independent of what the pipeline then did with it. That is
the replay contract — swap the live source for the recorded one and the pipeline
reproduces the same run. The tee is Tier-2 (like the pipeline's alert path): a
fixture-write failure is logged and skipped, never allowed to stop Tier-1 bar
capture — the fixture is a replayable artifact, the bar stream is not.
"""

import logging
from collections.abc import AsyncIterator
from pathlib import Path

from rs_domain import BarClosed, EventSource

logger = logging.getLogger(__name__)


class FixtureRecorder:
    """Appends BarClosed events to a JSONL file, one canonical JSON per line.

    Truncates on construction so each session starts a fresh recording; appends
    per event (reopened each time) so a mid-session crash still leaves every
    event written before it as complete, valid lines.
    """

    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text("", encoding="utf-8")

    def record(self, event: BarClosed) -> None:
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(event.model_dump_json() + "\n")


def load_events(path: Path | str) -> list[BarClosed]:
    """Parse a recorded JSONL fixture back into BarClosed events, in file order."""
    text = Path(path).read_text(encoding="utf-8")
    return [BarClosed.model_validate_json(line) for line in text.splitlines() if line]


class RecordingEventSource:
    """EventSource decorator: tees each event of an inner source to a recorder as
    it passes through, unchanged. A recorder failure never stops capture (Tier-2)."""

    def __init__(self, inner: EventSource, recorder: FixtureRecorder) -> None:
        self._inner = inner
        self._recorder = recorder
        self._record_failed = False

    async def events(self) -> AsyncIterator[BarClosed]:
        async for event in self._inner.events():
            try:
                self._recorder.record(event)
            except Exception:
                # Tier-2: a fixture-write failure (disk full, unwritable path)
                # must not stop Tier-1 capture. Log loudly once — the operator
                # learns the fixture is incomplete — then keep yielding bars.
                if not self._record_failed:
                    self._record_failed = True
                    logger.error("fixture recording failed — capture continues without it", exc_info=True)
            yield event


class RecordedEventSource:
    """EventSource that replays a recorded JSONL fixture (ADR-0023 replay seam)."""

    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)

    async def events(self) -> AsyncIterator[BarClosed]:
        for event in load_events(self._path):
            yield event
