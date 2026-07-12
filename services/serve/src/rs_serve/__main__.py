"""Run the serve daemon: python -m rs_serve (host/port via RS_SERVE_*)."""

import logging
import sys

import uvicorn
from pydantic import ValidationError

from rs_serve.app import create_app
from rs_serve.settings import ServeSettings

logger = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    try:
        settings = ServeSettings()
    except ValidationError as exc:
        # Loud exit-1, never a silent default (settings convention).
        logger.error("invalid configuration: %s", exc)
        return 1
    logger.info("rs_serve starting: %s:%d", settings.host, settings.port)
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
