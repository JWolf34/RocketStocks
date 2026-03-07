import logging
from contextlib import contextmanager

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool

from rocketstocks.core.config.secrets import secrets

logger = logging.getLogger(__name__)

_ALLOWED_DIRECTIONS = frozenset({'ASC', 'DESC'})
_ALLOWED_OPERATORS = frozenset({'=', '!=', '<>', '<', '>', '<=', '>=', 'LIKE', 'ILIKE', 'IN', 'NOT IN'})
_COLLECTION_OPERATORS = frozenset({'IN', 'NOT IN'})


class Postgres:
    def __init__(self, minconn: int = 2, maxconn: int = 10):
        self._pool = ThreadedConnectionPool(
            minconn,
            maxconn,
            host=secrets.db_host,
            dbname=secrets.db_name,
            user=secrets.db_user,
            password=secrets.db_password,
            port=secrets.db_port if secrets.db_port else 5432,
        )
        logger.debug(f"Connection pool created (min={minconn}, max={maxconn})")

    @contextmanager
    def _cursor(self):
        """Acquire a pooled connection, yield a cursor, commit or rollback, return connection."""
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def close_pool(self):
        """Close all connections in the pool (call on shutdown)."""
        self._pool.closeall()
        logger.debug("Connection pool closed")

    # ------------------------------------------------------------------
    # CRUD helpers
    # ------------------------------------------------------------------

    def insert(self, table: str, fields: list, values: list):
        """Insert rows into *table*.

        Args:
            table: Target table name.
            fields: Column names to insert into.
            values: List of tuples, one per row, matching *fields* order.
        """
        insert_script = sql.SQL("INSERT INTO {sql_table} ({sql_fields})").format(
            sql_table=sql.Identifier(table),
            sql_fields=sql.SQL(",").join([sql.Identifier(f) for f in fields]),
        )
        insert_script += sql.SQL(" VALUES %s ON CONFLICT DO NOTHING;")

        with self._cursor() as cur:
            execute_values(cur=cur, sql=insert_script, argslist=values)

    def select(
        self,
        table: str,
        fields: list,
        where_conditions: list = None,
        order_by: tuple = tuple(),
        fetchall: bool = True,
        limit: int | None = None,
    ):
        """Select rows from *table*.

        Args:
            table: Table to query.
            fields: Column names to return.
            where_conditions: Optional list of condition tuples:
                - ``(col, val)``           → ``col = val``
                - ``(col, op, val)``       → ``col op val``
                - ``(col, 'IN', [v, …])``  → ``col = ANY(%s)``
                - ``(col, 'NOT IN', […])`` → ``col <> ALL(%s)``
                Allowed operators: ``=``, ``!=``, ``<>``, ``<``, ``>``,
                ``<=``, ``>=``, ``LIKE``, ``ILIKE``, ``IN``, ``NOT IN``.
            order_by: ``(col, direction)`` tuple; direction must be ``'ASC'``
                or ``'DESC'``.
            fetchall: If ``True`` return all rows; if ``False`` return one.
            limit: Maximum number of rows to return.
        """
        if where_conditions is None:
            where_conditions = []

        vals = tuple()

        select_script = sql.SQL("SELECT {sql_fields} FROM {sql_table} ").format(
            sql_fields=sql.SQL(",").join([sql.Identifier(f) for f in fields]),
            sql_table=sql.Identifier(table),
        )

        if where_conditions:
            where_script, where_vals = self.where_clauses(where_conditions)
            select_script += where_script
            vals += where_vals

        if order_by:
            direction = order_by[1].upper()
            if direction not in _ALLOWED_DIRECTIONS:
                raise ValueError(f"Invalid ORDER BY direction: {order_by[1]!r}")
            select_script += sql.SQL(" ORDER BY {sql_field} {sql_order}").format(
                sql_field=sql.Identifier(order_by[0]),
                sql_order=sql.SQL(direction),
            )

        if limit is not None:
            select_script += sql.SQL(" LIMIT %s")
            vals += (limit,)

        select_script += sql.SQL(";")

        with self._cursor() as cur:
            cur.execute(select_script, vals)
            return cur.fetchall() if fetchall else cur.fetchone()

    def update(
        self,
        table: str,
        set_fields: list,
        where_conditions: list = None,
        force_bulk: bool = False,
    ):
        """Update rows in *table*.

        Args:
            table: Target table name.
            set_fields: List of ``(col, val)`` pairs to set.
            where_conditions: Condition tuples (see :meth:`where_clauses`).
                Required unless *force_bulk* is ``True``.
            force_bulk: Set to ``True`` to allow updating every row without
                a WHERE clause.  Use with caution.

        Raises:
            ValueError: If *set_fields* is empty.
            ValueError: If *where_conditions* is empty/None and *force_bulk*
                is ``False``.
        """
        if not set_fields:
            raise ValueError("update() requires at least one set_field")

        if where_conditions is None:
            where_conditions = []

        if not where_conditions and not force_bulk:
            raise ValueError(
                "update() requires where_conditions; pass force_bulk=True to update all rows"
            )

        vals = tuple()

        update_script = sql.SQL("UPDATE {sql_table} ").format(
            sql_table=sql.Identifier(table),
        )

        set_columns = [field for (field, _) in set_fields]
        set_vals = tuple(value for (_, value) in set_fields)
        vals += set_vals

        update_script += sql.SQL("SET ")
        update_script += sql.SQL(",").join([
            sql.SQL("{sql_field} = %s").format(sql_field=sql.Identifier(field))
            for field in set_columns
        ])

        if where_conditions:
            where_script, where_vals = self.where_clauses(where_conditions)
            update_script += where_script
            vals += where_vals

        update_script += sql.SQL(";")

        with self._cursor() as cur:
            cur.execute(update_script, vals)

    def delete(self, table: str, where_conditions: list, force_bulk: bool = False):
        """Delete rows from *table*.

        Args:
            table: Target table name.
            where_conditions: Condition tuples (see :meth:`where_clauses`).
                Required unless *force_bulk* is ``True``.
            force_bulk: Set to ``True`` to allow deleting every row without
                a WHERE clause.  Use with caution.

        Raises:
            ValueError: If *where_conditions* is empty and *force_bulk* is
                ``False``.
        """
        if not where_conditions and not force_bulk:
            raise ValueError(
                "delete() requires where_conditions; pass force_bulk=True to delete all rows"
            )

        vals = tuple()

        delete_script = sql.SQL("DELETE FROM {sql_table} ").format(
            sql_table=sql.Identifier(table),
        )

        if where_conditions:
            where_script, where_vals = self.where_clauses(where_conditions)
            delete_script += where_script
            vals += where_vals

        delete_script += sql.SQL(";")

        with self._cursor() as cur:
            cur.execute(delete_script, vals)

    def where_clauses(self, where_conditions: list):
        """Build a parameterized WHERE clause from a list of conditions.

        Each condition is a tuple:

        - ``(col, val)``           → ``col = %s``
        - ``(col, op, val)``       → ``col op %s``
        - ``(col, 'IN', list)``    → ``col = ANY(%s)``   (list must be non-empty)
        - ``(col, 'NOT IN', list)``→ ``col <> ALL(%s)``  (list must be non-empty)

        Allowed operators: ``=``, ``!=``, ``<>``, ``<``, ``>``, ``<=``,
        ``>=``, ``LIKE``, ``ILIKE``, ``IN``, ``NOT IN``.

        Returns:
            Tuple of ``(sql.SQL fragment, values tuple)``.

        Raises:
            ValueError: For disallowed operators or empty ``IN``/``NOT IN``
                collections.
        """
        where_script = sql.SQL(" WHERE ")
        vals = tuple()
        clauses = []

        for condition in where_conditions:
            if len(condition) == 2:
                clauses.append(
                    sql.SQL("{sql_field} = %s").format(sql_field=sql.Identifier(condition[0]))
                )
                vals += (condition[1],)
            elif len(condition) == 3:
                operator = condition[1].upper()
                if operator not in _ALLOWED_OPERATORS:
                    raise ValueError(f"Disallowed WHERE operator: {condition[1]!r}")

                if operator in _COLLECTION_OPERATORS:
                    collection = condition[2]
                    if not isinstance(collection, (list, tuple)) or len(collection) == 0:
                        raise ValueError(
                            f"{operator} requires a non-empty list or tuple of values"
                        )
                    if operator == 'IN':
                        clauses.append(
                            sql.SQL("{sql_field} = ANY(%s)").format(
                                sql_field=sql.Identifier(condition[0])
                            )
                        )
                    else:  # NOT IN
                        clauses.append(
                            sql.SQL("{sql_field} <> ALL(%s)").format(
                                sql_field=sql.Identifier(condition[0])
                            )
                        )
                    vals += (list(collection),)
                else:
                    clauses.append(
                        sql.SQL("{sql_field} {sql_operator} %s").format(
                            sql_field=sql.Identifier(condition[0]),
                            sql_operator=sql.SQL(operator),
                        )
                    )
                    vals += (condition[2],)

        where_script += sql.SQL(" AND ").join(clauses)
        return where_script, vals

    def get_table_columns(self, table: str) -> list:
        """Return ordered column names for *table* from INFORMATION_SCHEMA."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME = %s ORDER BY ordinal_position;",
                (table,),
            )
            return [row[0] for row in cur.fetchall()]
