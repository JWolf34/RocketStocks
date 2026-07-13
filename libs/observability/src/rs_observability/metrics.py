"""Prometheus exposition for the side-port services (ADR-0015, S006 pin #6).

The one home for exposition decisions (S004 pin #8): ingest and bot keep
their plain counter dicts as the increment API — and their counter log
lines — while DictCounterCollector reads the same dicts at scrape time and
emits them with Counter type metadata, so rate()/increase() work without
reopening the accepted S003/S004 internals. serve is untouched: its per-app
registry behind API-key auth (S005) stands.
"""

import threading
from collections.abc import Iterable, Mapping
from wsgiref.simple_server import WSGIServer

from prometheus_client import CollectorRegistry, start_http_server
from prometheus_client import GCCollector, PlatformCollector, ProcessCollector
from prometheus_client.core import CounterMetricFamily
from prometheus_client.registry import Collector


class DictCounterCollector(Collector):
    """Read-only bridge from a counter dict to Counter-typed samples.

    Values are read live on every scrape (plain int reads — safe across the
    exposition thread). Names are normalized to the Prometheus counter
    convention: CounterMetricFamily strips a `_total` suffix from the family
    name and every sample gets one, so ingest's bare names (`bars_persisted`)
    expose as `bars_persisted_total` while bot's already-suffixed names pass
    through unchanged. A non-empty namespace prefixes `<namespace>_`.
    """

    def __init__(self, counters: Mapping[str, int], namespace: str) -> None:
        self._counters = counters
        self._namespace = namespace

    def collect(self):
        for name, value in self._counters.items():
            full_name = f"{self._namespace}_{name}" if self._namespace else name
            yield CounterMetricFamily(
                full_name, f"Value of the {name} counter, read at scrape time", value=value
            )


def start_metrics_server(
    port: int, collectors: Iterable[Collector]
) -> tuple[WSGIServer, threading.Thread]:
    """Threaded prometheus_client exposition on its own registry: the given
    collectors plus the process/platform/GC defaults. Returns the server and
    its thread (daemonized by prometheus_client) — tests bind port 0 and read
    back `server.server_port`; daemons just let process exit reap it."""
    registry = CollectorRegistry()
    ProcessCollector(registry=registry)
    PlatformCollector(registry=registry)
    GCCollector(registry=registry)
    for collector in collectors:
        registry.register(collector)
    return start_http_server(port, registry=registry)
