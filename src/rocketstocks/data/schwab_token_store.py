"""Schwab OAuth token persistence — stores token in the database."""
import asyncio
import json
import logging

from rocketstocks.data.db import Postgres

logger = logging.getLogger(__name__)


class SchwabTokenRepository:
    def __init__(self, db: Postgres):
        self._db = db

    async def load_token(self) -> dict | None:
        rows = await self._db.execute(
            "SELECT token_data FROM schwab_tokens WHERE id = 1"
        )
        return rows[0][0] if rows else None

    async def save_token(self, token_dict: dict) -> None:
        await self._db.execute(
            "INSERT INTO schwab_tokens (id, token_data, updated_at) VALUES (1, %s, NOW()) "
            "ON CONFLICT (id) DO UPDATE SET token_data = EXCLUDED.token_data, updated_at = NOW()",
            (json.dumps(token_dict),)
        )
        logger.info("Schwab token saved to database")

    def schedule_save(self, token_dict: dict, **kwargs) -> None:
        """Sync callback for schwab-py's auto-refresh token_write_func.

        Called from inside an async context (schwab-py wraps it in ``async def``),
        so the event loop is always running here. Uses create_task for non-blocking save.

        ``**kwargs`` absorbs extra keyword arguments (e.g. ``refresh_token``) that
        schwab-py passes during an access-token auto-refresh. The token_dict already
        contains the new refresh token, so the extra kwarg can be safely ignored.
        """
        asyncio.get_running_loop().create_task(self.save_token(token_dict))
