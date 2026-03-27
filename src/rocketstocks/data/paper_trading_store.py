"""Repository for paper trading tables (portfolios, positions, transactions, snapshots, pending orders)."""
import datetime
import logging

logger = logging.getLogger(__name__)

_PORTFOLIO_FIELDS = ['guild_id', 'user_id', 'cash', 'created_at']
_POSITION_FIELDS = ['guild_id', 'user_id', 'ticker', 'shares', 'avg_cost_basis']
_TRANSACTION_FIELDS = ['id', 'guild_id', 'user_id', 'ticker', 'side', 'shares', 'price', 'total', 'executed_at']
_SNAPSHOT_FIELDS = ['guild_id', 'user_id', 'snapshot_date', 'portfolio_value', 'cash', 'positions_value']
_PENDING_ORDER_FIELDS = [
    'id', 'guild_id', 'user_id', 'ticker', 'side', 'shares',
    'quoted_price', 'status', 'queued_at', 'executed_at', 'execution_price',
]

_STARTING_CASH = 10000.0


class PaperTradingRepository:
    """Async repository for paper trading portfolios, positions, transactions, snapshots, and orders."""

    def __init__(self, db=None):
        self._db = db

    # ------------------------------------------------------------------
    # Portfolio
    # ------------------------------------------------------------------

    async def get_portfolio(self, guild_id: int, user_id: int) -> dict | None:
        """Return portfolio row for (guild_id, user_id), or None."""
        row = await self._db.execute(
            "SELECT guild_id, user_id, cash, created_at FROM paper_portfolios "
            "WHERE guild_id = %s AND user_id = %s",
            [guild_id, user_id],
            fetchone=True,
        )
        return dict(zip(_PORTFOLIO_FIELDS, row)) if row else None

    async def create_portfolio(self, guild_id: int, user_id: int, starting_cash: float = _STARTING_CASH) -> None:
        """Create a new portfolio (no-op if it already exists)."""
        await self._db.execute(
            "INSERT INTO paper_portfolios (guild_id, user_id, cash) VALUES (%s, %s, %s) "
            "ON CONFLICT (guild_id, user_id) DO NOTHING",
            [guild_id, user_id, starting_cash],
        )
        logger.debug(f"Created portfolio for guild={guild_id} user={user_id}")

    async def reset_portfolio(self, guild_id: int, user_id: int, starting_cash: float = _STARTING_CASH) -> None:
        """Reset portfolio: delete positions/transactions/snapshots/pending orders, restore cash and created_at."""
        async with self._db.transaction() as conn:
            await conn.execute(
                "DELETE FROM paper_positions WHERE guild_id = %s AND user_id = %s",
                [guild_id, user_id],
            )
            await conn.execute(
                "DELETE FROM paper_transactions WHERE guild_id = %s AND user_id = %s",
                [guild_id, user_id],
            )
            await conn.execute(
                "DELETE FROM paper_snapshots WHERE guild_id = %s AND user_id = %s",
                [guild_id, user_id],
            )
            await conn.execute(
                "DELETE FROM paper_pending_orders WHERE guild_id = %s AND user_id = %s AND status = 'pending'",
                [guild_id, user_id],
            )
            await conn.execute(
                "UPDATE paper_portfolios SET cash = %s, created_at = NOW() "
                "WHERE guild_id = %s AND user_id = %s",
                [starting_cash, guild_id, user_id],
            )
        logger.debug(f"Reset portfolio for guild={guild_id} user={user_id}")

    async def get_all_portfolios(self, guild_id: int) -> list[dict]:
        """Return all portfolios for a guild."""
        rows = await self._db.execute(
            "SELECT guild_id, user_id, cash, created_at FROM paper_portfolios WHERE guild_id = %s",
            [guild_id],
        )
        return [dict(zip(_PORTFOLIO_FIELDS, row)) for row in (rows or [])]

    async def get_distinct_guild_ids(self) -> list[int]:
        """Return all guild IDs that have at least one portfolio."""
        rows = await self._db.execute(
            "SELECT DISTINCT guild_id FROM paper_portfolios",
        )
        return [row[0] for row in (rows or [])]

    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------

    async def get_positions(self, guild_id: int, user_id: int) -> list[dict]:
        """Return all positions for (guild_id, user_id)."""
        rows = await self._db.execute(
            "SELECT guild_id, user_id, ticker, shares, avg_cost_basis FROM paper_positions "
            "WHERE guild_id = %s AND user_id = %s ORDER BY ticker",
            [guild_id, user_id],
        )
        return [dict(zip(_POSITION_FIELDS, row)) for row in (rows or [])]

    async def get_position(self, guild_id: int, user_id: int, ticker: str) -> dict | None:
        """Return a single position row or None."""
        row = await self._db.execute(
            "SELECT guild_id, user_id, ticker, shares, avg_cost_basis FROM paper_positions "
            "WHERE guild_id = %s AND user_id = %s AND ticker = %s",
            [guild_id, user_id, ticker],
            fetchone=True,
        )
        return dict(zip(_POSITION_FIELDS, row)) if row else None

    async def upsert_position(
        self, guild_id: int, user_id: int, ticker: str, shares: int, avg_cost_basis: float
    ) -> None:
        """Insert or update a position row."""
        await self._db.execute(
            """
            INSERT INTO paper_positions (guild_id, user_id, ticker, shares, avg_cost_basis)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id, ticker)
            DO UPDATE SET shares = EXCLUDED.shares, avg_cost_basis = EXCLUDED.avg_cost_basis
            """,
            [guild_id, user_id, ticker, shares, avg_cost_basis],
        )

    async def delete_position(self, guild_id: int, user_id: int, ticker: str) -> None:
        """Remove a position row."""
        await self._db.execute(
            "DELETE FROM paper_positions WHERE guild_id = %s AND user_id = %s AND ticker = %s",
            [guild_id, user_id, ticker],
        )

    # ------------------------------------------------------------------
    # Trade execution (atomic)
    # ------------------------------------------------------------------

    async def execute_buy(
        self, guild_id: int, user_id: int, ticker: str, shares: int, price: float
    ) -> None:
        """Atomically: deduct cash, upsert position (weighted avg), insert transaction."""
        total = shares * price
        async with self._db.transaction() as conn:
            # Get existing position for weighted avg calc
            row = await conn.execute(
                "SELECT shares, avg_cost_basis FROM paper_positions "
                "WHERE guild_id = %s AND user_id = %s AND ticker = %s",
                [guild_id, user_id, ticker],
                fetchone=True,
            )
            if row:
                existing_shares, existing_avg = row[0], row[1]
                new_shares = existing_shares + shares
                new_avg = (existing_shares * existing_avg + shares * price) / new_shares
            else:
                new_shares = shares
                new_avg = price

            await conn.execute(
                "UPDATE paper_portfolios SET cash = cash - %s WHERE guild_id = %s AND user_id = %s",
                [total, guild_id, user_id],
            )
            await conn.execute(
                """
                INSERT INTO paper_positions (guild_id, user_id, ticker, shares, avg_cost_basis)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (guild_id, user_id, ticker)
                DO UPDATE SET shares = EXCLUDED.shares, avg_cost_basis = EXCLUDED.avg_cost_basis
                """,
                [guild_id, user_id, ticker, new_shares, new_avg],
            )
            await conn.execute(
                "INSERT INTO paper_transactions (guild_id, user_id, ticker, side, shares, price, total) "
                "VALUES (%s, %s, %s, 'BUY', %s, %s, %s)",
                [guild_id, user_id, ticker, shares, price, total],
            )
        logger.debug(f"Executed BUY {shares}x{ticker}@{price} for guild={guild_id} user={user_id}")

    async def execute_sell(
        self, guild_id: int, user_id: int, ticker: str, shares: int, price: float
    ) -> None:
        """Atomically: add cash, reduce/delete position, insert transaction."""
        total = shares * price
        async with self._db.transaction() as conn:
            row = await conn.execute(
                "SELECT shares FROM paper_positions "
                "WHERE guild_id = %s AND user_id = %s AND ticker = %s",
                [guild_id, user_id, ticker],
                fetchone=True,
            )
            existing_shares = row[0] if row else 0
            remaining = existing_shares - shares

            await conn.execute(
                "UPDATE paper_portfolios SET cash = cash + %s WHERE guild_id = %s AND user_id = %s",
                [total, guild_id, user_id],
            )
            if remaining <= 0:
                await conn.execute(
                    "DELETE FROM paper_positions WHERE guild_id = %s AND user_id = %s AND ticker = %s",
                    [guild_id, user_id, ticker],
                )
            else:
                await conn.execute(
                    "UPDATE paper_positions SET shares = %s "
                    "WHERE guild_id = %s AND user_id = %s AND ticker = %s",
                    [remaining, guild_id, user_id, ticker],
                )
            await conn.execute(
                "INSERT INTO paper_transactions (guild_id, user_id, ticker, side, shares, price, total) "
                "VALUES (%s, %s, %s, 'SELL', %s, %s, %s)",
                [guild_id, user_id, ticker, shares, price, total],
            )
        logger.debug(f"Executed SELL {shares}x{ticker}@{price} for guild={guild_id} user={user_id}")

    # ------------------------------------------------------------------
    # Order queue (atomic)
    # ------------------------------------------------------------------

    async def queue_buy_order(
        self, guild_id: int, user_id: int, ticker: str, shares: int, quoted_price: float
    ) -> None:
        """Atomically: deduct cash + insert pending buy order."""
        total = shares * quoted_price
        async with self._db.transaction() as conn:
            await conn.execute(
                "UPDATE paper_portfolios SET cash = cash - %s WHERE guild_id = %s AND user_id = %s",
                [total, guild_id, user_id],
            )
            await conn.execute(
                "INSERT INTO paper_pending_orders (guild_id, user_id, ticker, side, shares, quoted_price) "
                "VALUES (%s, %s, %s, 'BUY', %s, %s)",
                [guild_id, user_id, ticker, shares, quoted_price],
            )
        logger.debug(f"Queued BUY {shares}x{ticker}@{quoted_price} for guild={guild_id} user={user_id}")

    async def queue_sell_order(
        self, guild_id: int, user_id: int, ticker: str, shares: int, quoted_price: float
    ) -> None:
        """Atomically: reduce shares + insert pending sell order."""
        async with self._db.transaction() as conn:
            row = await conn.execute(
                "SELECT shares FROM paper_positions "
                "WHERE guild_id = %s AND user_id = %s AND ticker = %s",
                [guild_id, user_id, ticker],
                fetchone=True,
            )
            existing_shares = row[0] if row else 0
            remaining = existing_shares - shares
            if remaining <= 0:
                await conn.execute(
                    "DELETE FROM paper_positions WHERE guild_id = %s AND user_id = %s AND ticker = %s",
                    [guild_id, user_id, ticker],
                )
            else:
                await conn.execute(
                    "UPDATE paper_positions SET shares = %s "
                    "WHERE guild_id = %s AND user_id = %s AND ticker = %s",
                    [remaining, guild_id, user_id, ticker],
                )
            await conn.execute(
                "INSERT INTO paper_pending_orders (guild_id, user_id, ticker, side, shares, quoted_price) "
                "VALUES (%s, %s, %s, 'SELL', %s, %s)",
                [guild_id, user_id, ticker, shares, quoted_price],
            )
        logger.debug(f"Queued SELL {shares}x{ticker}@{quoted_price} for guild={guild_id} user={user_id}")

    async def cancel_buy_order(self, order_id: int, guild_id: int, user_id: int) -> bool:
        """Atomically: refund cash + cancel order. Returns True if cancelled."""
        row = await self._db.execute(
            "SELECT shares, quoted_price FROM paper_pending_orders "
            "WHERE id = %s AND guild_id = %s AND user_id = %s AND side = 'BUY' AND status = 'pending'",
            [order_id, guild_id, user_id],
            fetchone=True,
        )
        if not row:
            return False
        shares, quoted_price = row[0], row[1]
        refund = shares * quoted_price
        async with self._db.transaction() as conn:
            await conn.execute(
                "UPDATE paper_portfolios SET cash = cash + %s WHERE guild_id = %s AND user_id = %s",
                [refund, guild_id, user_id],
            )
            await conn.execute(
                "UPDATE paper_pending_orders SET status = 'cancelled' WHERE id = %s",
                [order_id],
            )
        logger.debug(f"Cancelled buy order {order_id}, refunded {refund}")
        return True

    async def cancel_sell_order(self, order_id: int, guild_id: int, user_id: int) -> bool:
        """Atomically: restore shares + cancel order. Returns True if cancelled."""
        row = await self._db.execute(
            "SELECT ticker, shares FROM paper_pending_orders "
            "WHERE id = %s AND guild_id = %s AND user_id = %s AND side = 'SELL' AND status = 'pending'",
            [order_id, guild_id, user_id],
            fetchone=True,
        )
        if not row:
            return False
        ticker, shares = row[0], row[1]
        async with self._db.transaction() as conn:
            await conn.execute(
                """
                INSERT INTO paper_positions (guild_id, user_id, ticker, shares, avg_cost_basis)
                VALUES (%s, %s, %s, %s, 0.0)
                ON CONFLICT (guild_id, user_id, ticker)
                DO UPDATE SET shares = paper_positions.shares + EXCLUDED.shares
                """,
                [guild_id, user_id, ticker, shares],
            )
            await conn.execute(
                "UPDATE paper_pending_orders SET status = 'cancelled' WHERE id = %s",
                [order_id],
            )
        logger.debug(f"Cancelled sell order {order_id}, restored {shares}x{ticker}")
        return True

    async def get_pending_orders(self, guild_id: int, user_id: int) -> list[dict]:
        """Return pending orders for (guild_id, user_id)."""
        rows = await self._db.execute(
            "SELECT id, guild_id, user_id, ticker, side, shares, quoted_price, status, queued_at, "
            "executed_at, execution_price FROM paper_pending_orders "
            "WHERE guild_id = %s AND user_id = %s AND status = 'pending' ORDER BY queued_at DESC",
            [guild_id, user_id],
        )
        return [dict(zip(_PENDING_ORDER_FIELDS, row)) for row in (rows or [])]

    async def get_all_pending_orders(self) -> list[dict]:
        """Return all pending orders across all guilds (for execution loop)."""
        rows = await self._db.execute(
            "SELECT id, guild_id, user_id, ticker, side, shares, quoted_price, status, queued_at, "
            "executed_at, execution_price FROM paper_pending_orders "
            "WHERE status = 'pending' ORDER BY queued_at ASC",
        )
        return [dict(zip(_PENDING_ORDER_FIELDS, row)) for row in (rows or [])]

    async def mark_order_executed(self, order_id: int, execution_price: float) -> None:
        """Mark an order as executed with the actual execution price."""
        await self._db.execute(
            "UPDATE paper_pending_orders SET status = 'executed', executed_at = NOW(), "
            "execution_price = %s WHERE id = %s",
            [execution_price, order_id],
        )
        logger.debug(f"Marked order {order_id} as executed at {execution_price}")

    # ------------------------------------------------------------------
    # Transactions
    # ------------------------------------------------------------------

    async def get_transactions(self, guild_id: int, user_id: int, limit: int = 20) -> list[dict]:
        """Return the most recent transactions for a user."""
        rows = await self._db.execute(
            "SELECT id, guild_id, user_id, ticker, side, shares, price, total, executed_at "
            "FROM paper_transactions WHERE guild_id = %s AND user_id = %s "
            "ORDER BY executed_at DESC LIMIT %s",
            [guild_id, user_id, limit],
        )
        return [dict(zip(_TRANSACTION_FIELDS, row)) for row in (rows or [])]

    async def get_guild_transactions(
        self, guild_id: int, since: datetime.datetime
    ) -> list[dict]:
        """Return all transactions for a guild since a given datetime."""
        rows = await self._db.execute(
            "SELECT id, guild_id, user_id, ticker, side, shares, price, total, executed_at "
            "FROM paper_transactions WHERE guild_id = %s AND executed_at >= %s "
            "ORDER BY executed_at DESC",
            [guild_id, since],
        )
        return [dict(zip(_TRANSACTION_FIELDS, row)) for row in (rows or [])]

    async def get_user_transactions_in_range(
        self,
        guild_id: int,
        user_id: int,
        start: datetime.datetime,
        end: datetime.datetime,
    ) -> list[dict]:
        """Return transactions for a user within a date range."""
        rows = await self._db.execute(
            "SELECT id, guild_id, user_id, ticker, side, shares, price, total, executed_at "
            "FROM paper_transactions WHERE guild_id = %s AND user_id = %s "
            "AND executed_at >= %s AND executed_at <= %s ORDER BY executed_at ASC",
            [guild_id, user_id, start, end],
        )
        return [dict(zip(_TRANSACTION_FIELDS, row)) for row in (rows or [])]

    # ------------------------------------------------------------------
    # Snapshots
    # ------------------------------------------------------------------

    async def insert_snapshot(
        self,
        guild_id: int,
        user_id: int,
        snapshot_date: datetime.date,
        portfolio_value: float,
        cash: float,
        positions_value: float,
    ) -> None:
        """Insert or update a daily snapshot."""
        await self._db.execute(
            """
            INSERT INTO paper_snapshots (guild_id, user_id, snapshot_date, portfolio_value, cash, positions_value)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id, snapshot_date)
            DO UPDATE SET portfolio_value = EXCLUDED.portfolio_value,
                          cash = EXCLUDED.cash,
                          positions_value = EXCLUDED.positions_value
            """,
            [guild_id, user_id, snapshot_date, portfolio_value, cash, positions_value],
        )

    async def get_snapshots(
        self,
        guild_id: int,
        user_id: int,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list[dict]:
        """Return daily snapshots for a user in a date range."""
        rows = await self._db.execute(
            "SELECT guild_id, user_id, snapshot_date, portfolio_value, cash, positions_value "
            "FROM paper_snapshots WHERE guild_id = %s AND user_id = %s "
            "AND snapshot_date >= %s AND snapshot_date <= %s ORDER BY snapshot_date ASC",
            [guild_id, user_id, start_date, end_date],
        )
        return [dict(zip(_SNAPSHOT_FIELDS, row)) for row in (rows or [])]

    async def get_all_snapshots_for_date(
        self, guild_id: int, snapshot_date: datetime.date
    ) -> list[dict]:
        """Return snapshots for all users in a guild on a given date."""
        rows = await self._db.execute(
            "SELECT guild_id, user_id, snapshot_date, portfolio_value, cash, positions_value "
            "FROM paper_snapshots WHERE guild_id = %s AND snapshot_date = %s",
            [guild_id, snapshot_date],
        )
        return [dict(zip(_SNAPSHOT_FIELDS, row)) for row in (rows or [])]

    async def get_all_snapshots_in_range(
        self,
        guild_id: int,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list[dict]:
        """Return snapshots for all users in a guild within a date range."""
        rows = await self._db.execute(
            "SELECT guild_id, user_id, snapshot_date, portfolio_value, cash, positions_value "
            "FROM paper_snapshots WHERE guild_id = %s "
            "AND snapshot_date >= %s AND snapshot_date <= %s ORDER BY snapshot_date ASC",
            [guild_id, start_date, end_date],
        )
        return [dict(zip(_SNAPSHOT_FIELDS, row)) for row in (rows or [])]
