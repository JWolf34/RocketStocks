"""Schwab OAuth token persistence — Postgres single-row store (S003 OQ#2).

Owns its transactions (unlike the per-session repositories): schwab-py fires
the write callback at arbitrary refresh moments, so the store opens a short
session per operation from the injected factory. v1 pattern:
legacy/src/rocketstocks/data/schwab_token_store.py.
"""

import asyncio
import logging
from typing import Any

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_persistence.models import SchwabTokenRow

logger = logging.getLogger(__name__)

_ROW_ID = 1


class SchwabTokenStore:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def load(self) -> dict[str, Any] | None:
        async with self._session_factory() as session:
            row = await session.get(SchwabTokenRow, _ROW_ID)
            return None if row is None else row.token_data

    async def save(self, token: dict[str, Any]) -> None:
        stmt = insert(SchwabTokenRow).values(id=_ROW_ID, token_data=token)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={"token_data": stmt.excluded.token_data, "updated_at": func.now()},
        )
        async with self._session_factory() as session:
            await session.execute(stmt)
            await session.commit()
        logger.info("Schwab token saved")

    def schedule_save(self, token: dict[str, Any], **kwargs: Any) -> None:
        """Sync callback for schwab-py's token_write_func.

        schwab-py invokes it from inside the running event loop, so the save
        is scheduled as a task rather than awaited. **kwargs absorbs extras
        (e.g. refresh_token) schwab-py passes on auto-refresh; the token dict
        already contains them (v1-proven behavior).
        """
        asyncio.get_running_loop().create_task(self.save(token))
