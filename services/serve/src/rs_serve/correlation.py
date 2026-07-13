"""One HTTP request = one unit of work (S006 pin #3).

A fresh opaque correlation_id is bound for the request's records and echoed
as X-Correlation-ID so an API consumer's report can be joined to the exact
records that served it. `route` is bound as the concrete request path: logs
are grepped by what was actually requested, and the cardinality constraint
that forces the route TEMPLATE onto metrics labels doesn't apply to log
fields. The one "request completed" record replaces uvicorn's access line
(which is emitted outside the bound context and would carry no id).
"""

import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from rs_observability import unit_of_work

logger = logging.getLogger(__name__)

CORRELATION_HEADER = "X-Correlation-ID"


class CorrelationMiddleware(BaseHTTPMiddleware):
    """Outermost middleware: auth rejections are units of work too."""

    async def dispatch(self, request: Request, call_next) -> Response:
        with unit_of_work(method=request.method, route=request.url.path) as correlation_id:
            # Stashed for the catch-all 500 handler, which runs in Starlette's
            # outermost ServerErrorMiddleware — after this context has exited.
            request.state.correlation_id = correlation_id
            response = await call_next(request)
            response.headers[CORRELATION_HEADER] = correlation_id
            logger.info("request completed", extra={"status": response.status_code})
            return response
