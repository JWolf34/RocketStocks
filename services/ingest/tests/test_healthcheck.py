"""The healthcheck's staleness rule over the daemon's real metrics exposition."""

from datetime import UTC, datetime, timedelta

from prometheus_client import CollectorRegistry, generate_latest

from rs_ingest.healthcheck import heartbeat_age_seconds
from rs_ingest.metrics import DaemonMetrics

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)


class FakeClock:
    def now(self) -> datetime:
        return NOW


def exposition(metrics: DaemonMetrics) -> str:
    registry = CollectorRegistry()
    for collector in metrics.collectors():
        registry.register(collector)
    return generate_latest(registry).decode()


def test_age_is_the_time_since_the_last_loop_tick():
    metrics = DaemonMetrics()
    metrics.loop_last_tick.set((NOW - timedelta(seconds=42)).timestamp())

    assert heartbeat_age_seconds(exposition(metrics), FakeClock()) == 42.0


def test_no_tick_yet_reads_as_no_heartbeat():
    assert heartbeat_age_seconds(exposition(DaemonMetrics()), FakeClock()) is None


def test_unrelated_text_reads_as_no_heartbeat():
    assert heartbeat_age_seconds("# nothing here\n", FakeClock()) is None
