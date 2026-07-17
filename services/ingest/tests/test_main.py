"""Entry-point metrics wiring (S006 + S008): the scrape-time views of the
session aggregator's and chain scanner's counters. Both exist from process
start, so every metric name is present before any session activity and
rate() sees continuous series. The chain-scan collector set is the S012
metrics contract — names asserted here exactly as the spec pins them."""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from prometheus_client import CollectorRegistry, Histogram, generate_latest

from rs_domain import SourceStatus
from rs_ingest.__main__ import (
    CYCLE_DURATION_BUCKETS,
    LiveAggregatorCounters,
    _run_chain_scanner,
    chain_scan_collectors,
)
from rs_ingest.aggregator import COUNTER_NAMES


def test_serves_all_zero_counters_before_the_session_starts():
    view = LiveAggregatorCounters(SimpleNamespace(aggregator=None))
    assert dict(view) == dict.fromkeys(COUNTER_NAMES, 0)


def test_reads_through_to_the_live_aggregator_at_scrape_time():
    live = SimpleNamespace(aggregator=None)
    view = LiveAggregatorCounters(live)
    live.aggregator = SimpleNamespace(counters={"bars_built": 7, "ticks_late_dropped": 1})
    assert dict(view) == {"bars_built": 7, "ticks_late_dropped": 1}
    live.aggregator.counters["bars_built"] += 1
    assert view["bars_built"] == 8


def scanner_stub(last_success_at=None):
    """The attribute surface chain_scan_collectors reads (ChainScanner's
    metric state, seeded exactly as the scanner seeds it)."""
    return SimpleNamespace(
        cycle_outcomes={"completed": 0, "skipped": 0},
        fetch_outcomes={s.value: 0 for s in SourceStatus},
        rows_written={s.value: 0 for s in SourceStatus},
        retries=0,
        scan_errors=0,
        last_success_at=last_success_at,
    )


class TestChainScanCollectors:
    NOW = datetime(2026, 7, 16, 15, 0, tzinfo=timezone.utc)

    def scrape(self, scanner, clock=None):
        histogram = Histogram(
            "chain_scan_cycle_duration_seconds",
            "test",
            buckets=CYCLE_DURATION_BUCKETS,
            registry=None,
        )
        registry = CollectorRegistry()
        for collector in chain_scan_collectors(
            scanner, histogram, clock or SimpleNamespace(now=lambda: self.NOW)
        ):
            registry.register(collector)
        return generate_latest(registry).decode()

    def test_every_pinned_metric_name_present_from_start(self):
        text = self.scrape(scanner_stub())
        assert 'chain_scan_cycles_total{outcome="completed"} 0.0' in text
        assert 'chain_scan_cycles_total{outcome="skipped"} 0.0' in text
        for status in ("ok", "partial", "failed", "stale", "skipped"):
            assert f'chain_scan_fetches_total{{outcome="{status}"}} 0.0' in text
            assert f'chain_snapshot_rows_written_total{{source_status="{status}"}} 0.0' in text
        assert "chain_scan_retries_total 0.0" in text
        assert "chain_scan_errors_total 0.0" in text
        assert "# TYPE chain_scan_cycle_duration_seconds histogram" in text
        # Freshness gauge: family declared, sample absent before first success.
        assert "# TYPE source_last_success_seconds gauge" in text
        assert 'source_last_success_seconds{source="options_curated_5m"}' not in text

    def test_freshness_gauge_measures_at_scrape_time(self):
        scanner = scanner_stub(last_success_at=self.NOW - timedelta(seconds=42))
        text = self.scrape(scanner)
        assert 'source_last_success_seconds{source="options_curated_5m"} 42.0' in text


class TestScannerGuard:
    async def test_scanner_crash_is_swallowed_and_logged(self, caplog):
        async def exploding_run():
            raise RuntimeError("scanner bug")

        with caplog.at_level("ERROR"):
            await _run_chain_scanner(SimpleNamespace(run=exploding_run))
        assert "stream capture continues" in caplog.text

    async def test_cancellation_passes_through(self):
        started = asyncio.Event()

        async def hanging_run():
            started.set()
            await asyncio.Future()

        task = asyncio.create_task(_run_chain_scanner(SimpleNamespace(run=hanging_run)))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
