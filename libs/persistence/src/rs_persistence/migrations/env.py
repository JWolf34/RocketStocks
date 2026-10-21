"""Alembic environment. Runs sync via the same psycopg3 driver the app uses
async (postgresql+psycopg handles both modes).

The DSN arrives on the config's sqlalchemy.url, set by
rs_persistence.migration_config() (rs_migrate CLI and tests both build
their config through it).
"""

from typing import Any

from alembic import context
from sqlalchemy import create_engine

from rs_persistence.models import Base

config = context.config
target_metadata = Base.metadata


def include_object(
    obj: Any, name: str | None, type_: str, reflected: bool, compare_to: Any
) -> bool:
    """Autogenerate/`check` filter: skip the `<table>_<column>_idx` indexes
    TimescaleDB creates on a hypertable's time column - they exist in the
    database but are deliberately not modelled."""
    if type_ == "index" and reflected and compare_to is None:
        column_names = [column.name for column in obj.columns]
        return name != f"{obj.table.name}_{column_names[0]}_idx"
    return True


def _database_url() -> str:
    url = config.get_main_option("sqlalchemy.url")
    if not url:
        raise RuntimeError("no sqlalchemy.url — build the config via rs_persistence.migration_config()")
    return url


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_database_url())
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
        )
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
