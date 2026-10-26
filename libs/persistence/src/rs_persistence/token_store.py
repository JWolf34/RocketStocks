"""Schwab OAuth token persistence - Postgres single-row store.

Owns its transactions (unlike the per-session repositories): schwab-py fires
the write callback at arbitrary refresh moments, so the store opens a short
session per operation from the injected factory.
"""

import asyncio
import logging
from datetime import datetime
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
        # Serializes saves; asyncio.Lock wakes waiters FIFO, and save() hits
        # the lock before its first await, so schedule order = write order.
        self._save_lock = asyncio.Lock()
        self._pending: set[asyncio.Task[None]] = set()

    async def load(self) -> dict[str, Any] | None:
        async with self._session_factory() as session:
            row = await session.get(SchwabTokenRow, _ROW_ID)
            return None if row is None else row.token_data

    async def updated_at(self) -> datetime | None:
        """When the row was last written, or None before the first login."""
        async with self._session_factory() as session:
            row = await session.get(SchwabTokenRow, _ROW_ID)
            return None if row is None else row.updated_at

    async def save(self, token: dict[str, Any]) -> None:
        async with self._save_lock:
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
        already contains them.
        """
        task = asyncio.get_running_loop().create_task(self.save(token))
        # The loop holds tasks weakly - without this ref a pending save can be
        # GC'd mid-flight and a rotated refresh_token silently lost.
        self._pending.add(task)
        task.add_done_callback(self._on_save_done)

    def _on_save_done(self, task: asyncio.Task[None]) -> None:
        self._pending.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error(
                "Schwab token save failed - DB row may hold a stale token; "
                "a restart may require re-login (python -m rs_ingest.tools.login)",
                exc_info=task.exception(),
            )

    async def drain(self) -> None:
        """Await all scheduled saves; call before shutdown so none are lost."""
        while self._pending:
            await asyncio.gather(*tuple(self._pending), return_exceptions=True)
