"""Entry point: `python -m rs_ingest`.

S003 skeleton is built in slices; until the session runner lands this is a
runnable no-op that proves config + logging wiring. Lifecycle contract
(spec, pinned #11): one process = one market session; exits 0 when there is
nothing to do.
"""

import logging
import sys

from pydantic import ValidationError

from rs_ingest.settings import IngestSettings

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )


def main() -> int:
    configure_logging()
    try:
        settings = IngestSettings()
    except ValidationError as exc:
        logger.error("invalid configuration: %s", exc)
        return 1
    logger.info(
        "rs_ingest starting: symbol=%s threshold=%d channel=%s grace=%.0fs",
        settings.symbol,
        settings.volume_threshold,
        settings.notify_channel,
        settings.watchdog_grace_seconds,
    )
    logger.info("skeleton scaffold: no session runner yet — exiting")
    return 0


if __name__ == "__main__":
    sys.exit(main())
