"""Pipeline: the Bus-1 persister -> detector fan-out (S003 B6c).

These are orchestration tests - the repositories' DB behavior (atomic alert row +
NOTIFY, rollback) is proven in libs/persistence's test_alert_repository
against a real database. Here BarRepository/AlertRepository are faked so we can
pin the *ordering and failure semantics* the pipeline owns: persist-before-detect,
two independent transactions per bar, the ADR-0008 error boundary (a raising
detector and a failed alert write both tolerated), and crash-loud bar persistence.
A shared Recorder captures the ordered sequence of DB operations across both
transactions so the two-transaction structure is visible.
"""

import logging
from datetime import UTC, datetime, timedelta

import pytest

import rs_ingest.pipeline as pipeline_mod
from rs_domain import Bar, BarClosed
from rs_ingest.detector import ThresholdDetector
from rs_ingest.pipeline import Pipeline
from rs_observability.testing import captured_json_stream, json_records
from rs_persistence import NOTIFY_CHANNEL

BUCKET0 = datetime(2026, 7, 6, 13, 30, tzinfo=UTC)


def bar(volume: int, *, i: int = 0, close: float = 100.0) -> Bar:
    return Bar(
        symbol="SPY",
        bucket=BUCKET0 + timedelta(minutes=5 * i),
        open=100.0,
        high=100.0,
        low=100.0,
        close=close,
        volume=volume,
    )


def closed(b: Bar) -> BarClosed:
    return BarClosed(event_time=b.bucket + timedelta(minutes=5), sequence=0, bar=b)


# -- test doubles -------------------------------------------------------------


class Recorder:
    """Shared sink: `log` is the ordered DB-operation trace across every session
    the pipeline opens, so a whole run's transaction structure reads top to bottom."""

    def __init__(self) -> None:
        self.log: list = []
        self.sessions_opened = 0
        self.alert_channels: list[str] = []

    def factory(self):  # the async_sessionmaker seam
        self.sessions_opened += 1
        return FakeSession(self)


class FakeSession:
    def __init__(self, rec: Recorder) -> None:
        self.rec = rec

    async def __aenter__(self) -> "FakeSession":
        return self

    async def __aexit__(self, *exc) -> bool:
        return False

    async def commit(self) -> None:
        self.rec.log.append("commit")


class FakeBarRepo:
    def __init__(self, session: FakeSession) -> None:
        self._rec = session.rec

    async def insert(self, bar: Bar) -> None:
        self._rec.log.append(("bar", bar.volume))


class FakeAlertRepo:
    def __init__(self, session: FakeSession, notify_channel: str = NOTIFY_CHANNEL) -> None:
        self._rec = session.rec
        self._rec.alert_channels.append(notify_channel)

    async def insert(self, alert) -> int:
        self._rec.log.append(("alert", alert.symbol, alert.payload["volume"]))
        return len(self._rec.log)


class RaisingBarRepo:
    def __init__(self, session: FakeSession) -> None:
        pass

    async def insert(self, bar: Bar) -> None:
        raise RuntimeError("bar insert failed")


class RaisingAlertRepo:
    def __init__(self, session: FakeSession, notify_channel: str = NOTIFY_CHANNEL) -> None:
        session.rec.alert_channels.append(notify_channel)

    async def insert(self, alert) -> int:
        raise RuntimeError("alert insert failed")


class RaisingDetector:
    def decide(self, window):
        raise RuntimeError("boom")


class RecordingDetector:
    """Captures a snapshot of the window it was handed on each call."""

    def __init__(self) -> None:
        self.windows: list[list[Bar]] = []

    def decide(self, window):
        self.windows.append(list(window))
        return None


class FakeSource:
    def __init__(self, events: list[BarClosed]) -> None:
        self._events = events

    async def events(self):
        for event in self._events:
            yield event


@pytest.fixture
def rec(monkeypatch) -> Recorder:
    monkeypatch.setattr(pipeline_mod, "BarRepository", FakeBarRepo)
    monkeypatch.setattr(pipeline_mod, "AlertRepository", FakeAlertRepo)
    return Recorder()


# -- persist path -------------------------------------------------------------


async def test_persists_each_bar_in_its_own_transaction(rec):
    p = Pipeline(rec.factory, [ThresholdDetector(500_000)])
    await p.run(FakeSource([closed(bar(100, i=0)), closed(bar(200, i=1))]))

    assert p.counters["bars_persisted"] == 2
    assert p.counters["alerts_emitted"] == 0
    # Two below-threshold bars: one persist+commit each, no alert transaction.
    assert rec.log == [("bar", 100), "commit", ("bar", 200), "commit"]
    assert rec.sessions_opened == 2
    assert rec.alert_channels == []


async def test_detector_sees_growing_window_newest_last(rec):
    detector = RecordingDetector()
    p = Pipeline(rec.factory, [detector])
    b0, b1, b2 = bar(1, i=0), bar(2, i=1), bar(3, i=2)
    for b in (b0, b1, b2):
        await p.handle(closed(b))

    # The window is cumulative, newest last, and every bar in it is already
    # persisted (append happens after the bar commit).
    assert detector.windows == [[b0], [b0, b1], [b0, b1, b2]]


# -- alert path (two transactions) --------------------------------------------


async def test_alert_written_in_a_second_transaction_after_the_bar(rec):
    p = Pipeline(rec.factory, [ThresholdDetector(500_000)])
    await p.handle(closed(bar(750_000)))

    # Bar commits alone first; then the alert lands in a *separate* transaction.
    assert rec.log == [("bar", 750_000), "commit", ("alert", "SPY", 750_000), "commit"]
    assert rec.sessions_opened == 2
    assert p.counters == {
        "bars_persisted": 1,
        "alerts_emitted": 1,
        "detector_errors": 0,
        "alerts_dropped": 0,
    }
    assert rec.alert_channels == [NOTIFY_CHANNEL]  # Option A default threaded through


async def test_no_second_transaction_when_no_detector_fires(rec):
    p = Pipeline(rec.factory, [ThresholdDetector(500_000)])
    await p.handle(closed(bar(499_999)))

    assert rec.log == [("bar", 499_999), "commit"]
    assert rec.sessions_opened == 1  # the alert transaction is never opened
    assert p.counters["alerts_emitted"] == 0


async def test_notify_channel_override_threads_to_the_alert_repo(rec):
    p = Pipeline(rec.factory, [ThresholdDetector(0)], notify_channel="custom_alerts")
    await p.handle(closed(bar(1_000)))

    assert rec.alert_channels == ["custom_alerts"]


# -- ADR-0008 detector error boundary -----------------------------------------


async def test_detector_error_is_swallowed_and_capture_continues(rec):
    p = Pipeline(rec.factory, [RaisingDetector()])
    await p.run(FakeSource([closed(bar(10, i=0)), closed(bar(20, i=1))]))

    # Both bars still persist; the raising detector is logged + counted, never
    # fatal - capture is not stopped by a misbehaving rule.
    assert p.counters["bars_persisted"] == 2
    assert p.counters["detector_errors"] == 2
    assert p.counters["alerts_emitted"] == 0
    assert rec.log == [("bar", 10), "commit", ("bar", 20), "commit"]


# -- crash-loud persistence ---------------------------------------------------


async def test_persister_failure_crashes_loud(rec, monkeypatch):
    monkeypatch.setattr(pipeline_mod, "BarRepository", RaisingBarRepo)
    p = Pipeline(rec.factory, [ThresholdDetector(100_000)])

    with pytest.raises(RuntimeError, match="bar insert failed"):
        await p.handle(closed(bar(200_000)))

    assert rec.log == []  # nothing committed
    assert p.counters["bars_persisted"] == 0


async def test_alert_write_failure_is_tolerated_and_capture_continues(rec, monkeypatch, caplog):
    # Wide boundary (criticality tiering): a failed alert write is Tier-2 -
    # tolerated, counted, and logged with its payload, never fatal to Tier-1 bar
    # capture. Both bars stay durable and the second still flows after the first
    # bar's alert write blows up.
    monkeypatch.setattr(pipeline_mod, "AlertRepository", RaisingAlertRepo)
    p = Pipeline(rec.factory, [ThresholdDetector(100_000)])

    with caplog.at_level(logging.ERROR):
        await p.run(FakeSource([closed(bar(200_000, i=0)), closed(bar(300_000, i=1))]))

    assert p.counters["bars_persisted"] == 2  # both bars durable
    assert p.counters["alerts_dropped"] == 2  # both alert writes tolerated
    assert p.counters["alerts_emitted"] == 0
    assert p.counters["detector_errors"] == 0  # decide() succeeded; the *write* failed
    # each bar committed in its own transaction, independent of the alert failure
    assert rec.log == [("bar", 200_000), "commit", ("bar", 300_000), "commit"]
    # the drop is auditable, not silent: the payload lands in the ERROR log
    assert "DROPPED" in caplog.text
    assert "200000" in caplog.text and "300000" in caplog.text


# -- S006 unit of work: one event dispatch = one correlation_id ----------------


@pytest.fixture
def json_stream():
    with captured_json_stream("ingest") as buffer:
        yield buffer


async def test_dispatch_records_carry_correlation_id_and_domain_keys(rec, json_stream):
    p = Pipeline(rec.factory, [ThresholdDetector(100_000)])
    await p.handle(closed(bar(200_000)))

    (record,) = [r for r in json_records(json_stream) if r["event"].startswith("alert ")]
    assert record["service"] == "ingest"
    assert record["symbol"] == "SPY"
    assert record["bucket"] == BUCKET0.isoformat()
    assert record["correlation_id"]
    # The cross-service join key (S006 pin #3): ingest's insert log carries the
    # alert id the bot's post log will also carry - tracing joins on it.
    assert record["alert_id"] == 3  # FakeAlertRepo returns len(rec.log)


async def test_each_dispatch_is_its_own_unit_of_work(rec, json_stream):
    p = Pipeline(rec.factory, [ThresholdDetector(100_000)])
    await p.run(FakeSource([closed(bar(200_000, i=0)), closed(bar(300_000, i=1))]))

    first, second = [r for r in json_records(json_stream) if r["event"].startswith("alert ")]
    assert first["correlation_id"] != second["correlation_id"]
    assert first["bucket"] != second["bucket"]


async def test_dropped_alert_record_is_correlated_too(rec, monkeypatch, json_stream):
    monkeypatch.setattr(pipeline_mod, "AlertRepository", RaisingAlertRepo)
    p = Pipeline(rec.factory, [ThresholdDetector(100_000)])
    await p.handle(closed(bar(200_000)))

    (record,) = [r for r in json_records(json_stream) if "DROPPED" in r["event"]]
    assert record["level"] == "error"
    assert record["correlation_id"]
    assert record["symbol"] == "SPY"
