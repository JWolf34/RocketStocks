"""Run the serve daemon: python -m rs_serve (host/port via RS_SERVE_*)."""

import logging

import uvicorn
from pydantic import ValidationError

from rs_observability import configure_logging, install_excepthooks
from rs_serve.app import create_app
from rs_serve.settings import ServeSettings

logger = logging.getLogger(__name__)


def main() -> int:
    configure_logging("serve")
    install_excepthooks()  # sys/threading; the asyncio half joins in lifespan
    try:
        settings = ServeSettings()
    except ValidationError as exc:
        # Loud exit-1, never a silent default (settings convention).
        logger.error("invalid configuration: %s", exc)
        return 1
    logger.info("rs_serve starting: %s:%d", settings.host, settings.port)
    # log_config=None: uvicorn leaves logging alone, so its loggers propagate
    # to the root handler and render on the one S006 stream. Its access line
    # is off — CorrelationMiddleware's "request completed" record is the
    # access log, emitted inside the unit of work where it carries the id.
    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_config=None,
        access_log=False,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
