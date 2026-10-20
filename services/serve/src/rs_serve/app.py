"""FastAPI app factory. The engine/session factory live on app.state so the
middleware and routes share one pool; the lifespan only disposes it (creation
is eager, so test clients that skip lifespan startup still get a working app).
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from rs_observability import install_excepthooks
from rs_persistence import build_engine, build_session_factory
from rs_serve.auth import ApiKeyMiddleware
from rs_serve.bars import router as bars_router
from rs_serve.correlation import CORRELATION_HEADER, CorrelationMiddleware
from rs_serve.metrics import HttpMetrics, MetricsMiddleware
from rs_serve.settings import ServeSettings

logger = logging.getLogger(__name__)


async def _unhandled_error(request: Request, exc: Exception) -> JSONResponse:
    """ADR-0015 FastAPI hook, S005 depth: full traceback to the log, clean
    JSON to the caller (public-posture surface — internals never leak).
    notifications.send(SYSTEM_CRITICAL, ...) joins here when ADR-0016 lands.

    Runs in Starlette's outermost ServerErrorMiddleware — the unit_of_work
    context is already gone, so the correlation id rides request.state (S006):
    the failure record and the 500 response stay joinable to the request."""
    correlation_id = getattr(request.state, "correlation_id", None)
    logger.exception(
        "unhandled error serving %s %s",
        request.method,
        request.url.path,
        exc_info=exc,
        extra={"correlation_id": correlation_id} if correlation_id else None,
    )
    headers = {CORRELATION_HEADER: correlation_id} if correlation_id else None
    return JSONResponse({"detail": "internal server error"}, status_code=500, headers=headers)


def create_app(settings: ServeSettings | None = None) -> FastAPI:
    settings = settings or ServeSettings()
    metrics = HttpMetrics()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # The running-loop half of the S006 excepthook wiring: asyncio's
        # handler can only attach once uvicorn's loop exists.
        install_excepthooks()
        yield
        await app.state.engine.dispose()

    app = FastAPI(title="rs-serve", lifespan=lifespan)
    app.state.settings = settings
    app.state.engine = build_engine(settings.rs_db_url)
    app.state.session_factory = build_session_factory(app.state.engine)
    # add_middleware order: last added runs first. Correlation is OUTERMOST —
    # every request is one unit of work, rejections included (S006); metrics
    # sits OUTSIDE auth so rejected requests are traffic too; /metrics itself
    # is a routed endpoint behind the auth middleware (ADR-0015 / S005 Q3).
    app.add_middleware(ApiKeyMiddleware)
    app.add_middleware(MetricsMiddleware, metrics=metrics)
    app.add_middleware(CorrelationMiddleware)
    app.include_router(bars_router)
    app.get("/metrics", include_in_schema=False)(metrics.exposition)
    app.add_exception_handler(Exception, _unhandled_error)
    return app
