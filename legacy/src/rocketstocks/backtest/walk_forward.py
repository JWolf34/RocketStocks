"""Walk-forward validation for backtesting strategies.

Splits a date range into N anchored folds. For each fold, optimizes strategy
parameters on the train period and validates on the out-of-sample test period.
Only test-period results are reported — they reflect true out-of-sample performance.

Fold structure (anchored / expanding train window):
    Fold 1: train=[start, split_1)  test=[split_1, split_2)
    Fold 2: train=[start, split_2)  test=[split_2, split_3)
    ...
    Fold N: train=[start, split_N)  test=[split_N, end]

The expanding train window ensures later folds have more data for optimization.
"""
import datetime
import logging
import math

from rocketstocks.backtest.filters import TickerFilter
from rocketstocks.backtest.registry import get_strategy
from rocketstocks.backtest.runner import BacktestRunner
from rocketstocks.backtest.stats import compute_group_stats

logger = logging.getLogger(__name__)


def _split_date_range(
    start_date: datetime.date,
    end_date: datetime.date,
    folds: int,
    train_pct: float,
) -> list[tuple[datetime.date, datetime.date, datetime.date, datetime.date]]:
    """Compute anchored fold boundaries.

    Args:
        start_date: First date of the full range.
        end_date: Last date of the full range.
        folds: Number of train/test folds.
        train_pct: Fraction of each fold allocated to training (0 < train_pct < 1).

    Returns:
        List of (train_start, train_end, test_start, test_end) tuples.
        train_start is always the global start_date (anchored/expanding window).
    """
    total_days = (end_date - start_date).days
    if total_days <= 0 or folds < 1:
        return []

    # Each fold is total_days / folds calendar days wide
    fold_days = total_days / folds
    splits = []
    for i in range(folds):
        fold_end = start_date + datetime.timedelta(days=int(fold_days * (i + 1)))
        fold_end = min(fold_end, end_date)

        fold_size = (fold_end - start_date).days
        train_end = start_date + datetime.timedelta(days=int(fold_size * train_pct))
        test_start = train_end + datetime.timedelta(days=1)

        if test_start >= fold_end:
            continue

        splits.append((start_date, train_end, test_start, fold_end))

    return splits


class WalkForwardRunner:
    """Run walk-forward validation for a strategy.

    For each fold, optimizes parameters on the train period using the runner's
    existing optimize() method, then validates on the test period with the best
    parameters fixed.

    Args:
        runner: BacktestRunner with DB and price-history access.
    """

    def __init__(self, runner: BacktestRunner):
        self._runner = runner

    async def run(
        self,
        strategy_name: str,
        ticker_filter: TickerFilter,
        param_grid: dict,
        folds: int = 5,
        train_pct: float = 0.7,
        maximize: str = 'Sharpe Ratio',
        timeframe: str = 'daily',
        cash: float = 10_000,
        commission: float = 0.002,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
        optimize_on: str | None = None,
    ) -> dict:
        """Run walk-forward validation across a ticker subset.

        Args:
            strategy_name: Registered strategy name.
            ticker_filter: Tickers to test.
            param_grid: Parameter grid for per-fold optimization,
                e.g. ``{'vol_threshold': [1.5, 2.0, 2.5], 'hold_bars': [5, 10]}``.
            folds: Number of train/test folds (default 5).
            train_pct: Fraction of each fold used for training (default 0.7).
            maximize: Metric to maximize during optimization (default 'Sharpe Ratio').
            timeframe: ``'daily'`` or ``'5m'``.
            cash: Starting cash per ticker.
            commission: Commission fraction per trade.
            start_date: Start of the full date range.
            end_date: End of the full date range.
            optimize_on: Single ticker to use for per-fold optimization. When None,
                picks the first ticker that resolves from the filter.

        Returns:
            Dict with keys:
                ``folds``: list of per-fold result dicts
                ``oos_stats``: aggregate GroupStats dict for all out-of-sample results
                ``param_stability``: dict of param → list of values chosen per fold
        """
        if start_date is None or end_date is None:
            return {'error': 'start_date and end_date are required for walk-forward'}

        # Resolve tickers once
        tickers = await ticker_filter.apply(self._runner._stock_data)
        if not tickers:
            return {'error': 'No tickers matched the filter'}

        # Pick the optimization ticker
        opt_ticker = optimize_on or tickers[0]

        fold_splits = _split_date_range(start_date, end_date, folds, train_pct)
        if not fold_splits:
            return {'error': 'Could not compute fold splits — check date range and folds'}

        strategy_cls = get_strategy(strategy_name)  # validate early

        fold_results = []
        all_oos_results: list[dict] = []
        param_stability: dict[str, list] = {k: [] for k in param_grid}

        for fold_idx, (train_start, train_end, test_start, test_end) in enumerate(fold_splits, 1):
            logger.info(
                f"Walk-forward fold {fold_idx}/{len(fold_splits)}: "
                f"train={train_start}→{train_end}  test={test_start}→{test_end}"
            )

            # Step 1: optimize on train period using one representative ticker
            opt_result = await self._runner.optimize(
                strategy_name=strategy_name,
                ticker=opt_ticker,
                param_grid=param_grid,
                maximize=maximize,
                timeframe=timeframe,
                cash=cash,
                commission=commission,
                start_date=train_start,
                end_date=train_end,
            )

            if 'error' in opt_result:
                logger.warning(f"Fold {fold_idx}: optimization failed — {opt_result['error']}")
                fold_results.append({
                    'fold': fold_idx,
                    'train_start': str(train_start),
                    'train_end': str(train_end),
                    'test_start': str(test_start),
                    'test_end': str(test_end),
                    'best_params': None,
                    'oos_run_id': None,
                    'error': opt_result['error'],
                })
                continue

            best_params = opt_result.get('best_stats', {})
            # Extract only the params from param_grid that appear in best_stats
            chosen_params = _extract_chosen_params(param_grid, opt_result)
            for param_name, chosen_val in chosen_params.items():
                param_stability[param_name].append(chosen_val)

            # Step 2: run test period with fixed best params
            test_run_id = await self._runner.run(
                strategy_name=strategy_name,
                ticker_filter=TickerFilter(tickers=tickers),
                timeframe=timeframe,
                cash=cash,
                commission=commission,
                start_date=test_start,
                end_date=test_end,
                strategy_params=chosen_params or None,
            )

            fold_info: dict = {
                'fold': fold_idx,
                'train_start': str(train_start),
                'train_end': str(train_end),
                'test_start': str(test_start),
                'test_end': str(test_end),
                'best_params': chosen_params,
                'oos_run_id': test_run_id,
            }

            if test_run_id >= 0:
                oos_results = await self._runner._repo.get_successful_results_by_run(test_run_id)
                all_oos_results.extend(oos_results)

                oos_group = compute_group_stats(
                    oos_results,
                    group_key=f'fold:{fold_idx}',
                    group_value=f'Fold {fold_idx}',
                )
                if oos_group:
                    fold_info['oos_mean_return'] = oos_group.mean_return
                    fold_info['oos_sharpe'] = oos_group.mean_sharpe
                    fold_info['oos_num_trades'] = oos_group.total_trades
                    fold_info['oos_p_value'] = oos_group.p_value
                    fold_info['oos_significant'] = oos_group.significant

            fold_results.append(fold_info)

        # Aggregate all out-of-sample results
        oos_agg = compute_group_stats(
            all_oos_results,
            group_key='walk_forward_oos',
            group_value='Out-of-Sample (All Folds)',
        )

        return {
            'folds': fold_results,
            'oos_stats': oos_agg.to_dict() if oos_agg else None,
            'param_stability': param_stability,
            'strategy_name': strategy_name,
            'n_folds': len(fold_splits),
            'n_tickers': len(tickers),
        }


def _extract_chosen_params(param_grid: dict, opt_result: dict) -> dict:
    """Extract the best parameter values from an optimize() result.

    backtesting.py's optimize() returns best_stats with the chosen parameter
    values embedded as keys matching the strategy class attributes. This function
    pulls those values from the best_stats dict, falling back to the first value
    in each param list if a param is missing.

    Args:
        param_grid: Original parameter grid dict.
        opt_result: Dict returned by BacktestRunner.optimize().

    Returns:
        Dict of param_name → chosen_value.
    """
    best_stats = opt_result.get('best_stats') or {}
    chosen = {}
    for param_name, candidates in param_grid.items():
        if param_name in best_stats and best_stats[param_name] is not None:
            val = best_stats[param_name]
            # Match type to candidates (int vs float)
            if candidates and isinstance(candidates[0], int):
                try:
                    val = int(round(float(val)))
                except (TypeError, ValueError):
                    pass
            elif candidates and isinstance(candidates[0], float):
                try:
                    val = float(val)
                except (TypeError, ValueError):
                    pass
            chosen[param_name] = val
        elif candidates:
            chosen[param_name] = candidates[0]
    return chosen
