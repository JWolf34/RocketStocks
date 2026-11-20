"""Bulk inserts in as few statements as the driver allows, inside the caller's transaction."""

from collections.abc import Iterable, Mapping
from itertools import batched
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from rs_persistence.models import Base

# Bound parameters one statement may carry: half the protocol's 65535 limit, so a row with every
# column bound stays far below it.
_PARAMETERS_PER_STATEMENT = 32_767


async def insert_rows(
    session: AsyncSession,
    table: type[Base],
    rows: Iterable[Mapping[str, Any]],
    *,
    skip_existing: bool = False,
) -> int:
    """Insert `rows` into `table`, a statement per chunk sized by the table's column count;
    returns how many were inserted. With `skip_existing` a row whose key is already stored is
    skipped (first write wins) and not counted. `rows` is consumed a chunk at a time."""
    per_statement = _PARAMETERS_PER_STATEMENT // len(table.__table__.columns)
    key = next(iter(table.__table__.primary_key))
    inserted = 0
    for chunk in batched(rows, per_statement, strict=False):
        statement = pg_insert(table).values(list(chunk))
        if skip_existing:
            stored = await session.scalars(statement.on_conflict_do_nothing().returning(key))
            inserted += len(stored.all())
        else:
            await session.execute(statement)
            inserted += len(chunk)
    return inserted
