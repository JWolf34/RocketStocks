"""Alembic environment. Runs sync via the same psycopg3 driver the app uses
async (postgresql+psycopg handles both modes — S002 driver decision).

DSN resolution: RS_DB_URL env var wins; falls back to -x db_url=... ;
alembic.ini's placeholder is never a real target.
"""

import os

from alembic import context
from sqlalchemy import create_engine

from rs_persistence.models import Base

config = context.config
target_metadata = Base.metadata


def _database_url() -> str:
    url = os.environ.get("RS_DB_URL") or context.get_x_argument(as_dictionary=True).get("db_url")
    if not url:
        raise RuntimeError("set RS_DB_URL (or -x db_url=...) to run migrations")
    return url


def run_migrations_offline() -> None:
    context.configure(url=_database_url(), target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_database_url())
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
