"""Async engine + session factory. DSNs use the psycopg3 dialect
(postgresql+psycopg://) — one driver family repo-wide (S002 decision)."""

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


def create_engine(dsn: str) -> AsyncEngine:
    return create_async_engine(dsn)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
