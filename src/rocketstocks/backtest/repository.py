"""Repository for backtest_runs, backtest_results, and strategy_stats tables."""
import logging

from psycopg.types.json import Json

logger = logging.getLogger(__name__)

_RUN_FIELDS = [
    'run_id', 'strategy_name', 'timeframe', 'parameters', 'filters',
    'ticker_count', 'start_date', 'end_date', 'cash', 'commission', 'created_at',
]
_RESULT_FIELDS = [
    'result_id', 'run_id', 'ticker', 'classification', 'sector',
    'return_pct', 'sharpe_ratio', 'max_drawdown', 'win_rate', 'num_trades',
    'avg_trade_pct', 'profit_factor', 'exposure_pct', 'equity_final',
    'buy_hold_pct', 'error', 'created_at',
]
_STAT_FIELDS = [
    'stat_id', 'run_id', 'group_key', 'group_value', 'ticker_count',
    'mean_return', 'median_return', 'std_return', 'mean_sharpe', 'mean_win_rate',
    'total_trades', 'mean_max_dd', 'mean_profit_factor', 't_stat', 'p_value',
    'significant', 'created_at',
]


class BacktestRepository:
    """CRUD access for backtest_runs, backtest_results, and strategy_stats tables."""

    def __init__(self, db):
        self._db = db

    # ------------------------------------------------------------------ runs --

    async def insert_run(
        self,
        strategy_name: str,
        timeframe: str,
        parameters: dict,
        filters: dict,
        ticker_count: int,
        start_date,
        end_date,
        cash: float = 10_000,
        commission: float = 0.002,
    ) -> int:
        """Insert a new backtest run and return its generated run_id."""
        row = await self._db.execute(
            """
            INSERT INTO backtest_runs
            (strategy_name, timeframe, parameters, filters, ticker_count,
             start_date, end_date, cash, commission)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING run_id
            """,
            [
                strategy_name, timeframe,
                Json(parameters), Json(filters),
                ticker_count, start_date, end_date,
                cash, commission,
            ],
            fetchone=True,
        )
        run_id = row[0]
        logger.debug(f"Inserted backtest run {run_id} for strategy '{strategy_name}'")
        return run_id

    async def get_run(self, run_id: int) -> dict | None:
        """Return a single run as a dict, or None if not found."""
        row = await self._db.execute(
            f"SELECT {', '.join(_RUN_FIELDS)} FROM backtest_runs WHERE run_id = %s",
            [run_id],
            fetchone=True,
        )
        return dict(zip(_RUN_FIELDS, row)) if row else None

    async def get_recent_runs(self, limit: int = 20) -> list[dict]:
        """Return the most recent runs, newest first."""
        rows = await self._db.execute(
            f"SELECT {', '.join(_RUN_FIELDS)} FROM backtest_runs "
            "ORDER BY created_at DESC LIMIT %s",
            [limit],
        )
        return [dict(zip(_RUN_FIELDS, row)) for row in (rows or [])]

    # --------------------------------------------------------------- results --

    async def insert_results_batch(self, run_id: int, results: list[dict]) -> None:
        """Bulk insert per-ticker results for a run."""
        cols = [
            'run_id', 'ticker', 'classification', 'sector',
            'return_pct', 'sharpe_ratio', 'max_drawdown', 'win_rate',
            'num_trades', 'avg_trade_pct', 'profit_factor', 'exposure_pct',
            'equity_final', 'buy_hold_pct', 'error',
        ]
        placeholders = ', '.join(['%s'] * len(cols))
        col_list = ', '.join(cols)
        values = [
            (
                run_id,
                r.get('ticker'),
                r.get('classification'),
                r.get('sector'),
                r.get('return_pct'),
                r.get('sharpe_ratio'),
                r.get('max_drawdown'),
                r.get('win_rate'),
                r.get('num_trades'),
                r.get('avg_trade_pct'),
                r.get('profit_factor'),
                r.get('exposure_pct'),
                r.get('equity_final'),
                r.get('buy_hold_pct'),
                r.get('error'),
            )
            for r in results
        ]
        await self._db.execute_batch(
            f"INSERT INTO backtest_results ({col_list}) VALUES ({placeholders}) "
            "ON CONFLICT (run_id, ticker) DO NOTHING",
            values,
        )
        logger.debug(f"Inserted {len(results)} results for run {run_id}")

    async def get_results_by_run(self, run_id: int) -> list[dict]:
        """Return all per-ticker results for a run."""
        rows = await self._db.execute(
            f"SELECT {', '.join(_RESULT_FIELDS)} FROM backtest_results "
            "WHERE run_id = %s ORDER BY ticker",
            [run_id],
        )
        return [dict(zip(_RESULT_FIELDS, row)) for row in (rows or [])]

    async def get_successful_results_by_run(self, run_id: int) -> list[dict]:
        """Return only successful results (error IS NULL) for a run."""
        rows = await self._db.execute(
            f"SELECT {', '.join(_RESULT_FIELDS)} FROM backtest_results "
            "WHERE run_id = %s AND error IS NULL ORDER BY ticker",
            [run_id],
        )
        return [dict(zip(_RESULT_FIELDS, row)) for row in (rows or [])]

    # -------------------------------------------------------------- stats --

    async def insert_stats_batch(self, run_id: int, stats_list: list[dict]) -> None:
        """Bulk insert aggregate strategy stats rows for a run."""
        cols = [
            'run_id', 'group_key', 'group_value', 'ticker_count',
            'mean_return', 'median_return', 'std_return', 'mean_sharpe',
            'mean_win_rate', 'total_trades', 'mean_max_dd', 'mean_profit_factor',
            't_stat', 'p_value', 'significant',
        ]
        placeholders = ', '.join(['%s'] * len(cols))
        col_list = ', '.join(cols)
        values = [
            (
                run_id,
                s.get('group_key'),
                s.get('group_value'),
                s.get('ticker_count'),
                s.get('mean_return'),
                s.get('median_return'),
                s.get('std_return'),
                s.get('mean_sharpe'),
                s.get('mean_win_rate'),
                s.get('total_trades'),
                s.get('mean_max_dd'),
                s.get('mean_profit_factor'),
                s.get('t_stat'),
                s.get('p_value'),
                s.get('significant'),
            )
            for s in stats_list
        ]
        await self._db.execute_batch(
            f"INSERT INTO strategy_stats ({col_list}) VALUES ({placeholders}) "
            "ON CONFLICT (run_id, group_key) DO NOTHING",
            values,
        )
        logger.debug(f"Inserted {len(stats_list)} stat groups for run {run_id}")

    async def get_stats_by_run(self, run_id: int) -> list[dict]:
        """Return all aggregate stat rows for a run."""
        rows = await self._db.execute(
            f"SELECT {', '.join(_STAT_FIELDS)} FROM strategy_stats "
            "WHERE run_id = %s ORDER BY group_key",
            [run_id],
        )
        return [dict(zip(_STAT_FIELDS, row)) for row in (rows or [])]

    async def get_stats_across_runs(
        self,
        strategy_name: str,
        group_key: str = 'all',
    ) -> list[dict]:
        """Return aggregate stats for a strategy across all runs.

        Joins strategy_stats with backtest_runs to filter by strategy name.
        Useful for comparing the same strategy's performance over time.
        """
        rows = await self._db.execute(
            f"SELECT ss.{', ss.'.join(_STAT_FIELDS)} "
            "FROM strategy_stats ss "
            "JOIN backtest_runs br ON ss.run_id = br.run_id "
            "WHERE br.strategy_name = %s AND ss.group_key = %s "
            "ORDER BY ss.created_at DESC",
            [strategy_name, group_key],
        )
        return [dict(zip(_STAT_FIELDS, row)) for row in (rows or [])]
