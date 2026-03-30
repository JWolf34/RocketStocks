"""BacktestRunner — orchestrates strategy runs across filtered tickers."""
import datetime
import logging

from backtesting import Backtest

from rocketstocks.backtest.data_prep import (
    enrich_5m_with_daily_context,
    filter_regular_hours,
    mark_regular_hours,
    merge_popularity,
    prep_5m,
    prep_daily,
)
from rocketstocks.backtest.filters import TickerFilter
from rocketstocks.backtest.registry import get_strategy
from rocketstocks.backtest.repository import BacktestRepository
from rocketstocks.backtest.stats import compute_all_group_stats

logger = logging.getLogger(__name__)

# Mapping from backtesting.py stat keys to our DB column names
_STAT_MAP = {
    'Return [%]': 'return_pct',
    'Sharpe Ratio': 'sharpe_ratio',
    'Max. Drawdown [%]': 'max_drawdown',
    'Win Rate [%]': 'win_rate',
    '# Trades': 'num_trades',
    'Avg. Trade [%]': 'avg_trade_pct',
    'Profit Factor': 'profit_factor',
    'Exposure Time [%]': 'exposure_pct',
    'Equity Final [$]': 'equity_final',
    'Buy & Hold Return [%]': 'buy_hold_pct',
}

_MIN_BARS = 30


class BacktestRunner:
    """Orchestrates running a strategy across filtered tickers and persisting results.

    Args:
        stock_data: StockData singleton providing access to all repositories.
        repo: BacktestRepository for persisting runs, results, and stats.
    """

    def __init__(self, stock_data, repo: BacktestRepository):
        self._stock_data = stock_data
        self._repo = repo

    async def run(
        self,
        strategy_name: str,
        ticker_filter: TickerFilter,
        timeframe: str = 'daily',
        cash: float = 10_000,
        commission: float = 0.002,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
        strategy_params: dict | None = None,
    ) -> int:
        """Run a strategy across all tickers matched by the filter.

        Steps:
        1. Resolve the strategy class from the registry.
        2. Apply the ticker filter to get a ticker list.
        3. Insert the backtest_runs record.
        4. Run backtesting.py per ticker, collect results.
        5. Bulk-insert results and aggregate stats.

        Args:
            strategy_name: Registered strategy name (see ``list_strategies()``).
            ticker_filter: TickerFilter describing which tickers to include.
            timeframe: ``'daily'`` or ``'5m'``.
            cash: Starting cash per ticker (default 10 000).
            commission: Per-trade commission fraction (default 0.2%).
            start_date: Earliest price bar to include.
            end_date: Latest price bar to include.
            strategy_params: Optional parameter overrides passed to ``bt.run()``.

        Returns:
            The run_id of the created backtest run, or -1 if no tickers matched.
        """
        strategy_cls = get_strategy(strategy_name)

        tickers = await ticker_filter.apply(self._stock_data)
        if not tickers:
            logger.warning('BacktestRunner.run: no tickers matched the filter — aborting')
            return -1

        logger.info(
            f"Backtest '{strategy_name}' ({timeframe}): "
            f"{len(tickers)} tickers, "
            f"{start_date or 'all'} → {end_date or 'all'}"
        )

        # Fetch metadata for result annotations
        classifications = await self._stock_data.ticker_stats.get_all_classifications()
        mcap_map = await self._stock_data.ticker_stats.get_all_market_caps()
        ticker_info_df = await self._stock_data.tickers.get_all_ticker_info()
        sector_map = dict(zip(ticker_info_df['ticker'], ticker_info_df['sector']))
        exchange_col = ticker_info_df['exchange'] if 'exchange' in ticker_info_df.columns else None
        exchange_map = dict(zip(ticker_info_df['ticker'], exchange_col)) if exchange_col is not None else {}
        watchlist_map = await self._stock_data.watchlists.get_ticker_to_watchlist_map()

        run_id = await self._repo.insert_run(
            strategy_name=strategy_name,
            timeframe=timeframe,
            parameters=strategy_params or {},
            filters=ticker_filter.to_dict(),
            ticker_count=len(tickers),
            start_date=start_date,
            end_date=end_date,
            cash=cash,
            commission=commission,
        )
        logger.info(f'Created backtest run {run_id}')

        results: list[dict] = []
        for i, ticker in enumerate(tickers, 1):
            logger.info(f'[{i}/{len(tickers)}] {strategy_name} on {ticker}')
            result = await self._run_single(
                ticker=ticker,
                strategy_cls=strategy_cls,
                timeframe=timeframe,
                cash=cash,
                commission=commission,
                start_date=start_date,
                end_date=end_date,
                strategy_params=strategy_params,
                classification=classifications.get(ticker, 'standard'),
                sector=sector_map.get(ticker),
                exchange=exchange_map.get(ticker),
                watchlist=watchlist_map.get(ticker),
            )
            result['run_id'] = run_id
            results.append(result)

        await self._repo.insert_results_batch(run_id, results)

        group_stats = compute_all_group_stats(results, mcap_map=mcap_map)
        if group_stats:
            await self._repo.insert_stats_batch(run_id, [gs.to_dict() for gs in group_stats])

        successful = [r for r in results if r.get('error') is None]
        failed = [r for r in results if r.get('error') is not None]
        logger.info(
            f"Run {run_id} complete: {len(successful)} succeeded, {len(failed)} failed"
        )

        return run_id

    async def _run_single(
        self,
        ticker: str,
        strategy_cls: type,
        timeframe: str,
        cash: float,
        commission: float,
        start_date: datetime.date | None,
        end_date: datetime.date | None,
        strategy_params: dict | None,
        classification: str,
        sector: str | None,
        exchange: str | None = None,
        watchlist: str | None = None,
    ) -> dict:
        """Run the backtest on one ticker and return a result dict."""
        result: dict = {
            'ticker': ticker,
            'classification': classification,
            'sector': sector,
            'exchange': exchange,
            'watchlist': watchlist,
        }

        try:
            requires_daily = getattr(strategy_cls, 'requires_daily', False)
            requires_popularity = getattr(strategy_cls, 'requires_popularity', False)

            if timeframe == 'daily':
                raw_df = await self._stock_data.price_history.fetch_daily_price_history(
                    ticker, start_date=start_date, end_date=end_date,
                )
                df = prep_daily(raw_df)

                if requires_popularity:
                    pop_raw = await self._stock_data.popularity.fetch_popularity(ticker)
                    df = merge_popularity(df, pop_raw)
            else:
                start_dt = (
                    datetime.datetime.combine(start_date, datetime.time.min)
                    if start_date else None
                )
                end_dt = (
                    datetime.datetime.combine(end_date, datetime.time.max)
                    if end_date else None
                )
                raw_df = await self._stock_data.price_history.fetch_5m_price_history(
                    ticker, start_datetime=start_dt, end_datetime=end_dt,
                )
                df = prep_5m(raw_df)
                df = mark_regular_hours(df)

                if requires_daily:
                    daily_start = self._extend_start(start_date, extra_trading_days=25)
                    raw_daily = await self._stock_data.price_history.fetch_daily_price_history(
                        ticker, start_date=daily_start, end_date=end_date,
                    )
                    daily_df = prep_daily(raw_daily)
                    df = enrich_5m_with_daily_context(df, daily_df)

                if requires_popularity:
                    pop_raw = await self._stock_data.popularity.fetch_popularity(ticker)
                    df = merge_popularity(df, pop_raw)

            if df.empty or len(df) < _MIN_BARS:
                result['error'] = f'insufficient_data ({len(df)} bars)'
                return result

            bt = Backtest(
                df,
                strategy_cls,
                cash=cash,
                commission=commission,
                exclusive_orders=True,
                finalize_trades=True,
            )

            stats = bt.run(**(strategy_params or {}))

            for bt_key, our_key in _STAT_MAP.items():
                val = stats.get(bt_key)
                try:
                    result[our_key] = float(val) if val is not None else None
                except (TypeError, ValueError):
                    result[our_key] = None

            # num_trades must be int
            if result.get('num_trades') is not None:
                result['num_trades'] = int(result['num_trades'])

            result['error'] = None

        except Exception as exc:
            logger.warning(f"Backtest failed for '{ticker}': {exc}")
            result['error'] = str(exc)

        return result

    async def run_benchmark(
        self,
        ticker: str,
        timeframe: str = 'daily',
        cash: float = 10_000,
        commission: float = 0.002,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
    ) -> float:
        """Run buy-and-hold on a single ticker and return the return_pct.

        Used to establish a passive benchmark for excess-return calculations.

        Args:
            ticker: Benchmark ticker (e.g. 'SPY').
            timeframe: Price data timeframe.
            cash: Starting cash.
            commission: Per-trade commission fraction.
            start_date: Start of benchmark period.
            end_date: End of benchmark period.

        Returns:
            Return [%] for the buy-and-hold run, or NaN if data is unavailable.
        """
        result = await self._run_single(
            ticker=ticker,
            strategy_cls=get_strategy('buy_hold'),
            timeframe=timeframe,
            cash=cash,
            commission=commission,
            start_date=start_date,
            end_date=end_date,
            strategy_params=None,
            classification='standard',
            sector=None,
        )
        val = result.get('return_pct')
        return float(val) if val is not None else float('nan')

    def _extend_start(
        self,
        start_date: datetime.date | None,
        extra_trading_days: int,
    ) -> datetime.date | None:
        """Return a start date shifted earlier by approximately extra_trading_days.

        Uses a 1.5x calendar-day multiplier to account for weekends and holidays.
        """
        if start_date is None:
            return None
        extra_calendar = int(extra_trading_days * 1.5) + 10
        return start_date - datetime.timedelta(days=extra_calendar)

    async def optimize(
        self,
        strategy_name: str,
        ticker: str,
        param_grid: dict,
        maximize: str = 'Sharpe Ratio',
        timeframe: str = 'daily',
        cash: float = 10_000,
        commission: float = 0.002,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
    ) -> dict:
        """Sweep strategy parameters on a single ticker.

        Uses backtesting.py's built-in optimize() method. When exactly two
        parameters are provided, a heatmap is returned as well.

        Args:
            strategy_name: Registered strategy name.
            ticker: Single ticker to optimize against.
            param_grid: Dict mapping parameter names to lists of candidate values,
                e.g. ``{'hold_bars': [1, 3, 5, 10], 'zscore_threshold': [2.0, 2.5]}``.
            maximize: Metric name to maximize (default ``'Sharpe Ratio'``).
            timeframe: ``'daily'`` or ``'5m'``.
            cash: Starting cash.
            commission: Per-trade commission fraction.
            start_date: Optional start date.
            end_date: Optional end date.

        Returns:
            Dict with ``best_stats`` and, when two params are given, ``heatmap``.
        """
        strategy_cls = get_strategy(strategy_name)

        if timeframe == 'daily':
            raw_df = await self._stock_data.price_history.fetch_daily_price_history(
                ticker, start_date=start_date, end_date=end_date,
            )
            df = prep_daily(raw_df)
        else:
            start_dt = (
                datetime.datetime.combine(start_date, datetime.time.min)
                if start_date else None
            )
            end_dt = (
                datetime.datetime.combine(end_date, datetime.time.max)
                if end_date else None
            )
            raw_df = await self._stock_data.price_history.fetch_5m_price_history(
                ticker, start_datetime=start_dt, end_datetime=end_dt,
            )
            df = prep_5m(raw_df)
            df = mark_regular_hours(df)

        if df.empty or len(df) < _MIN_BARS:
            return {'error': f'insufficient_data ({len(df)} bars)'}

        bt = Backtest(df, strategy_cls, cash=cash, commission=commission,
                      exclusive_orders=True, finalize_trades=True)

        return_heatmap = len(param_grid) == 2
        opt_result = bt.optimize(
            maximize=maximize,
            return_heatmap=return_heatmap,
            **param_grid,
        )

        if return_heatmap:
            best_stats, heatmap = opt_result
        else:
            best_stats = opt_result
            heatmap = None

        extracted = {}
        for bt_key, our_key in _STAT_MAP.items():
            val = best_stats.get(bt_key)
            try:
                extracted[our_key] = float(val) if val is not None else None
            except (TypeError, ValueError):
                extracted[our_key] = None

        out: dict = {'best_stats': extracted}
        if heatmap is not None:
            try:
                out['heatmap'] = heatmap.to_dict()
            except AttributeError:
                out['heatmap'] = str(heatmap)

        return out
