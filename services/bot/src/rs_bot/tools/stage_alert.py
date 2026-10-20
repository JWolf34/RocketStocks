"""Dev tool: `python -m rs_bot.tools.stage_alert`.

Commits one clearly-marked test alert through the real AlertRepository insert
path - alerts row + transactional NOTIFY, exactly what
ingest produces - so the acceptance drills (live notify-wake, severed LISTEN,
kill/restart) stage their inputs on the real Bus-2 write path (spec pin #11).
"""

import asyncio
import logging
import sys

from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import Alert, SystemClock
from rs_persistence import AlertRepository, build_engine, build_session_factory

logger = logging.getLogger(__name__)


class _StageSettings(BaseSettings):
    """Only the DSN - staging an alert must not require the Discord credential."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    rs_db_url: str


async def stage(session_factory: async_sessionmaker[AsyncSession]) -> int:
    """Insert + commit one staged test alert; returns its id (also the NOTIFY
    payload a running bot logs as the wake)."""
    alert = Alert(
        symbol="TEST",
        alert_type="staged_test",
        triggered_at=SystemClock().now(),
        payload={"volume": 999_999, "threshold": 500_000, "staged": True},
    )
    async with session_factory() as session:
        alert_id = await AlertRepository(session).insert(alert)
        await session.commit()
    assert alert_id is not None  # no dedup_key, so never deduplicated
    return alert_id


async def _run(settings: _StageSettings) -> int:
    engine = build_engine(settings.rs_db_url)
    try:
        alert_id = await stage(build_session_factory(engine))
    finally:
        await engine.dispose()
    logger.info("staged test alert id=%d (alerts row + NOTIFY committed)", alert_id)
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        settings = _StageSettings()
    except ValidationError as exc:
        logger.error("invalid configuration: %s", exc)
        return 1
    return asyncio.run(_run(settings))


if __name__ == "__main__":
    sys.exit(main())
