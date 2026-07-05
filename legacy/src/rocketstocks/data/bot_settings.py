"""Repository for the bot_settings table — runtime key/value config store."""
import logging

from rocketstocks.data.db import Postgres

logger = logging.getLogger(__name__)


class BotSettingsRepository:
    """Key/value store for runtime bot settings (tz, notification_filter, etc.)."""

    def __init__(self, db: Postgres):
        self._db = db

    async def get(self, key: str) -> str | None:
        """Return the value for *key*, or None if not set."""
        row = await self._db.execute(
            "SELECT value FROM bot_settings WHERE key = %s",
            [key],
            fetchone=True,
        )
        return row[0] if row else None

    async def set(self, key: str, value: str) -> None:
        """Insert or update *key* with *value*."""
        await self._db.execute(
            """
            INSERT INTO bot_settings (key, value)
            VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            [key, value],
        )
        logger.debug(f"bot_settings: set {key!r} = {value!r}")

    async def delete(self, key: str) -> None:
        """Remove *key* from bot_settings (no-op if absent)."""
        await self._db.execute(
            "DELETE FROM bot_settings WHERE key = %s",
            [key],
        )
        logger.debug(f"bot_settings: deleted {key!r}")

    async def get_all(self) -> dict[str, str]:
        """Return all stored settings as {key: value}."""
        rows = await self._db.execute("SELECT key, value FROM bot_settings", [])
        return {row[0]: row[1] for row in (rows or [])}
