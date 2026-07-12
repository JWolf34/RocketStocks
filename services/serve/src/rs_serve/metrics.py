"""Prometheus metrics (ADR-0015): process defaults + HTTP throughput/latency.

A per-app CollectorRegistry (not the module-global REGISTRY) so app factories
can be created repeatedly — tests build one app per case — without duplicate
collector registration errors.

The path label is the ROUTE TEMPLATE (`/v1/bars/{ticker}`), never the raw
path: raw tickers would make label cardinality unbounded. Requests rejected
before routing (e.g. the middleware's 401s) share the "unrouted" label.
"""

import time

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
)
from prometheus_client import GCCollector, PlatformCollector, ProcessCollector
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class HttpMetrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        ProcessCollector(registry=self.registry)
        PlatformCollector(registry=self.registry)
        GCCollector(registry=self.registry)
        self.requests_total = Counter(
            "http_requests_total",
            "HTTP requests served",
            ["method", "path", "status"],
            registry=self.registry,
        )
        self.request_duration = Histogram(
            "http_request_duration_seconds",
            "HTTP request latency",
            ["method", "path"],
            registry=self.registry,
        )

    def exposition(self) -> Response:
        return Response(generate_latest(self.registry), media_type=CONTENT_TYPE_LATEST)


class MetricsMiddleware(BaseHTTPMiddleware):
    """Outermost middleware: counts every request, including auth rejections."""

    def __init__(self, app, metrics: HttpMetrics) -> None:
        super().__init__(app)
        self._metrics = metrics

    async def dispatch(self, request: Request, call_next) -> Response:
        started = time.perf_counter()  # monotonic duration, not ambient wall time
        response = await call_next(request)
        route = request.scope.get("route")
        path = route.path if route is not None else "unrouted"
        labels = {"method": request.method, "path": path}
        self._metrics.requests_total.labels(**labels, status=str(response.status_code)).inc()
        self._metrics.request_duration.labels(**labels).observe(time.perf_counter() - started)
        return response
