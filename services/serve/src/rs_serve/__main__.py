"""Run the serve daemon: python -m rs_serve (host/port via RS_SERVE_*)."""

import logging

import uvicorn

from rs_serve.app import create_app
from rs_serve.settings import ServeSettings


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = ServeSettings()
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
