"""DictCounterCollector + start_metrics_server: the existing
counter dicts stay the increment API; exposition reads them at scrape time
with proper Counter type metadata so rate()/increase() work."""

import urllib.request

from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.registry import Collector

from rs_observability import (
    CallableGaugeCollector,
    DictCounterCollector,
    LabeledDictCounterCollector,
    start_metrics_server,
)


def scrape(registry: CollectorRegistry) -> str:
    return generate_latest(registry).decode()


def registry_with(collector: Collector) -> CollectorRegistry:
    registry = CollectorRegistry()
    registry.register(collector)
    return registry


def test_counters_expose_with_counter_type_metadata():
    counters = {"discord_posts_total": 3, "sweeps_total": 1}
    text = scrape(registry_with(DictCounterCollector(counters, namespace="")))
    assert "# TYPE discord_posts_total counter" in text
    assert "discord_posts_total 3.0" in text
    assert "sweeps_total 1.0" in text


def test_scrape_reads_current_values_not_registration_snapshot():
    counters = {"sweeps_total": 0}
    registry = registry_with(DictCounterCollector(counters, namespace=""))
    counters["sweeps_total"] += 5
    assert "sweeps_total 5.0" in scrape(registry)


def test_names_without_total_suffix_are_normalized():
    """Ingest's pipeline dict uses bare names (bars_persisted, alerts_emitted);
    Prometheus counters must end in _total for
    rate()/increase(), so the bridge normalizes at exposition."""
    counters = {"bars_persisted": 2, "alerts_emitted": 1}
    text = scrape(registry_with(DictCounterCollector(counters, namespace="")))
    assert "# TYPE bars_persisted_total counter" in text
    assert "bars_persisted_total 2.0" in text
    assert "alerts_emitted_total 1.0" in text


def test_namespace_prefixes_metric_names():
    counters = {"bars_built": 4}
    text = scrape(registry_with(DictCounterCollector(counters, namespace="aggregator")))
    assert "aggregator_bars_built_total 4.0" in text


def test_labeled_counters_expose_one_family_with_label_per_key():
    counters = {"completed": 0, "skipped": 0}
    registry = registry_with(LabeledDictCounterCollector("chain_scan_cycles", "outcome", counters))
    text = scrape(registry)
    # Both label values present from process start (seeded dict), zeros included.
    assert "# TYPE chain_scan_cycles_total counter" in text
    assert 'chain_scan_cycles_total{outcome="completed"} 0.0' in text
    assert 'chain_scan_cycles_total{outcome="skipped"} 0.0' in text

    counters["completed"] += 3
    assert 'chain_scan_cycles_total{outcome="completed"} 3.0' in scrape(registry)


def test_callable_gauge_evaluates_at_scrape_time():
    state = {"value": None}
    registry = registry_with(
        CallableGaugeCollector(
            "source_last_success_seconds",
            "Seconds since the source last delivered accepted data",
            {"source": "options_curated_5m"},
            lambda: state["value"],
        )
    )
    text = scrape(registry)
    # None -> family present (type metadata), sample absent until first success.
    assert "# TYPE source_last_success_seconds gauge" in text
    assert 'source_last_success_seconds{source="options_curated_5m"}' not in text

    state["value"] = 12.5
    assert 'source_last_success_seconds{source="options_curated_5m"} 12.5' in scrape(registry)


def test_start_metrics_server_serves_collectors_and_defaults():
    counters = {"sweeps_total": 2}
    server, thread = start_metrics_server(0, [DictCounterCollector(counters, namespace="")])
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{server.server_port}/metrics") as response:
            body = response.read().decode()
        assert "sweeps_total 2.0" in body
        # Platform/GC defaults prove the registry carries the standard
        # collectors (process_* appears only on Linux — asserted in-container).
        assert "python_info" in body
    finally:
        server.shutdown()
        thread.join(timeout=5)
