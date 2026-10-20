"""Async engine + session factory. DSNs use the psycopg3 dialect
(postgresql+psycopg://) — one driver family repo-wide (S002 decision).

The engine pins the session timezone to UTC: psycopg3 decodes timestamptz
into the connection's TimeZone, so without the pin, reads from a server
configured to any other zone would hand non-UTC datetimes to the domain
validators. Correctness must not depend on ambient DB config.
"""

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def psycopg_conninfo(sqlalchemy_url: str) -> str:
    """The driverless DSN raw psycopg wants (LISTEN connections) from the shared SQLAlchemy one."""
    return sqlalchemy_url.replace("+psycopg", "", 1)


def build_engine(dsn: str) -> AsyncEngine:
    # hide_parameters: a DB error must not log bound values (Schwab token data).
    return create_async_engine(
        dsn, connect_args={"options": "-c timezone=UTC"}, hide_parameters=True
    )


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
