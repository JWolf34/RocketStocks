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
    GCCollector,
    Histogram,
    PlatformCollector,
    ProcessCollector,
    generate_latest,
)
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp


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

    def __init__(self, app: ASGIApp, metrics: HttpMetrics) -> None:
        super().__init__(app)
        self._metrics = metrics

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        started = time.perf_counter()  # monotonic duration, not ambient wall time
        # Pre-bound: a BaseException (CancelledError) skips both assignments
        # below and must not turn into an UnboundLocalError in the finally.
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
        except Exception:
            # The catch-all 500 handler lives in Starlette's OUTERMOST
            # ServerErrorMiddleware, so unhandled errors pass through here as
            # exceptions — record them as the 500s they become, then re-raise
            # for the handler to answer.
            status = 500
            raise
        finally:
            route = request.scope.get("route")
            labels = {
                "method": request.method,
                "path": route.path if route is not None else "unrouted",
            }
            self._metrics.requests_total.labels(**labels, status=str(status)).inc()
            self._metrics.request_duration.labels(**labels).observe(time.perf_counter() - started)
        return response
