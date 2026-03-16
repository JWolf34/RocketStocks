"""Async PostgreSQL wrapper using psycopg3 + psycopg-pool."""
import logging
from contextlib import asynccontextmanager

from psycopg_pool import AsyncConnectionPool

from rocketstocks.core.config.settings import settings

logger = logging.getLogger(__name__)


def _build_conninfo() -> str:
    return (
        f"host={settings.postgres_host} "
        f"dbname={settings.postgres_db} "
        f"user={settings.postgres_user} "
        f"password={settings.postgres_password} "
        f"port={settings.postgres_port}"
    )


class Postgres:
    def __init__(self, minconn: int = 2, maxconn: int = 10):
        """Create the async connection pool (call await open() before use)."""
        self._pool = AsyncConnectionPool(
            conninfo=_build_conninfo(),
            min_size=minconn,
            max_size=maxconn,
            open=False,
        )
        logger.debug(f"Connection pool configured (min={minconn}, max={maxconn})")

    async def open(self) -> None:
        """Open the connection pool. Call once at startup."""
        await self._pool.open()
        logger.debug("Connection pool opened")

    async def close(self) -> None:
        """Close the pool (call on shutdown)."""
        await self._pool.close()
        logger.debug("Connection pool closed")

    async def execute(self, query: str, params=None, fetchone: bool = False):
        """Execute a parameterized query.

        Returns rows for SELECT, None for DML.
        Uses %s placeholders (psycopg3 server-side binding via Extended Query Protocol).
        """
        async with self._pool.connection() as conn:
            cur = await conn.execute(query, params)
            if cur.description is not None:
                return await cur.fetchone() if fetchone else await cur.fetchall()
            return None

    async def execute_batch(self, query: str, values) -> None:
        """Bulk insert/upsert using executemany."""
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(query, values)

    @asynccontextmanager
    async def transaction(self):
        """Yield a connection for multi-statement atomic operations.

        Usage::

            async with self._db.transaction() as conn:
                await conn.execute("SELECT ...", params)
                await conn.execute("UPDATE ...", params)
        """
        async with self._pool.connection() as conn:
            async with conn.transaction():
                yield conn
