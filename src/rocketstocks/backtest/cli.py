"""CLI argument parsing and subcommand dispatch for the backtest system."""
import argparse
import datetime
import json
import logging

from rocketstocks.backtest.filters import TickerFilter
from rocketstocks.backtest.registry import list_strategies
from rocketstocks.backtest.repository import BacktestRepository
from rocketstocks.backtest.runner import BacktestRunner
from rocketstocks.backtest.stats import compare_against_benchmark, compare_strategies

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='python -m rocketstocks.backtest',
        description='RocketStocks Backtesting CLI',
    )
    sub = parser.add_subparsers(dest='command', required=True)

    # ------------------------------------------------------------------ run --
    run_p = sub.add_parser('run', help='Run a strategy across filtered tickers')
    run_p.add_argument('strategy', help='Strategy name (see: list)')
    run_p.add_argument('--timeframe', choices=['daily', '5m'], default='daily',
                       help='Price bar resolution (default: daily)')
    run_p.add_argument('--cash', type=float, default=10_000,
                       help='Starting cash per ticker (default: 10000)')
    run_p.add_argument('--commission', type=float, default=0.002,
                       help='Commission per trade as a fraction (default: 0.002)')
    run_p.add_argument('--start-date', type=str, default=None,
                       metavar='YYYY-MM-DD', help='Earliest bar date')
    run_p.add_argument('--end-date', type=str, default=None,
                       metavar='YYYY-MM-DD', help='Latest bar date')
    run_p.add_argument('--params', type=str, default=None,
                       metavar='JSON',
                       help='Strategy parameter overrides as JSON object')
    # filter flags
    run_p.add_argument('--tickers', nargs='+', default=None,
                       metavar='TICKER', help='Explicit ticker list (bypasses other filters)')
    run_p.add_argument('--classification', nargs='+', default=None,
                       metavar='CLASS',
                       help='StockClass values: volatile, meme, standard, blue_chip')
    run_p.add_argument('--sector', nargs='+', default=None, metavar='SECTOR')
    run_p.add_argument('--industry', nargs='+', default=None, metavar='INDUSTRY')
    run_p.add_argument('--min-market-cap', type=float, default=None,
                       metavar='DOLLARS', help='Minimum market cap in dollars')
    run_p.add_argument('--max-market-cap', type=float, default=None,
                       metavar='DOLLARS', help='Maximum market cap in dollars')
    run_p.add_argument('--min-popularity-rank', type=int, default=None,
                       metavar='RANK', help='Most-popular end of rank range (1 = #1)')
    run_p.add_argument('--max-popularity-rank', type=int, default=None,
                       metavar='RANK', help='Least-popular end of rank range')
    run_p.add_argument('--exchange', nargs='+', default=None, metavar='EXCHANGE',
                       help='Exchange names to include (e.g. NYSE NASDAQ)')
    run_p.add_argument('--watchlist', nargs='+', default=None, metavar='WATCHLIST',
                       help='Named watchlist IDs (e.g. mag7 semiconductors). Multiple are OR-combined.')
    run_p.add_argument('--min-volatility', type=float, default=None,
                       metavar='VOL', help='Minimum 20-day volatility in percent (inclusive)')
    run_p.add_argument('--max-volatility', type=float, default=None,
                       metavar='VOL', help='Maximum 20-day volatility in percent (inclusive)')
    run_p.add_argument('--benchmark', type=str, default=None,
                       metavar='TICKER',
                       help='Run buy-and-hold on TICKER as a passive benchmark (e.g. SPY)')
    run_p.add_argument('--include-delisted', action='store_true', default=False,
                       help='Include tickers with a delist_date (reduces survivorship bias)')
    run_p.add_argument('--slippage', type=float, default=0.0,
                       metavar='BPS',
                       help='Extra slippage in basis points added to commission per trade (default: 0)')
    run_p.add_argument('--spread-model', choices=['none', 'fixed', 'volatility'],
                       default='none',
                       help='Spread cost model: none (default), fixed=10bps per trade, '
                            'volatility=spread scales with 20d volatility')

    # --------------------------------------------------------------- stats --
    stats_p = sub.add_parser('stats', help='View aggregate stats for a backtest run')
    stats_p.add_argument('run_id', type=int, help='Run ID to display')
    stats_p.add_argument('--group', type=str, default=None,
                         metavar='GROUP_KEY',
                         help='Filter to a specific group (e.g. class:volatile, sector:Technology)')

    # ------------------------------------------------------------- compare --
    compare_p = sub.add_parser('compare', help='Compare two backtest runs via t-test')
    compare_p.add_argument('run_id_a', type=int, help='First run ID')
    compare_p.add_argument('run_id_b', type=int, help='Second run ID')

    # ------------------------------------------------------------ optimize --
    opt_p = sub.add_parser('optimize', help='Sweep strategy parameters on a single ticker')
    opt_p.add_argument('strategy', help='Strategy name')
    opt_p.add_argument('ticker', help='Single ticker to optimize against')
    opt_p.add_argument('--params', type=str, required=True,
                       metavar='JSON',
                       help='Parameter grid as JSON: {"hold_bars":[1,3,5],"zscore":[2.0,2.5]}')
    opt_p.add_argument('--maximize', type=str, default='Sharpe Ratio',
                       help='Metric to maximize (default: "Sharpe Ratio")')
    opt_p.add_argument('--timeframe', choices=['daily', '5m'], default='daily')
    opt_p.add_argument('--cash', type=float, default=10_000)
    opt_p.add_argument('--commission', type=float, default=0.002)
    opt_p.add_argument('--start-date', type=str, default=None, metavar='YYYY-MM-DD')
    opt_p.add_argument('--end-date', type=str, default=None, metavar='YYYY-MM-DD')

    # ------------------------------------------------------------- results --
    results_p = sub.add_parser('results', help='Show per-ticker results for a run')
    results_p.add_argument('run_id', type=int, help='Run ID to display')
    results_p.add_argument('--sort', type=str, default='return_pct',
                           choices=['return_pct', 'sharpe_ratio', 'num_trades'],
                           help='Sort key (default: return_pct)')
    results_p.add_argument('--limit', type=int, default=20,
                           help='Number of results to show (default: 20)')

    # ---------------------------------------------------------------- list --
    sub.add_parser('list', help='List all available strategy names')

    return parser


async def main(argv: list[str] | None = None) -> int:
    """Parse CLI arguments and dispatch to the appropriate handler.

    Returns:
        Integer exit code (0 = success, 1 = error).
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == 'list':
        return _handle_list()

    # All other commands require DB access
    from rocketstocks.data.stockdata import StockData
    from rocketstocks.data.schema import create_tables

    stock_data = StockData()
    await stock_data.db.open()
    try:
        await create_tables(stock_data.db)
        repo = BacktestRepository(db=stock_data.db)
        runner = BacktestRunner(stock_data=stock_data, repo=repo)

        if args.command == 'run':
            return await _handle_run(args, runner, repo)
        elif args.command == 'stats':
            return await _handle_stats(args, repo)
        elif args.command == 'compare':
            return await _handle_compare(args, repo)
        elif args.command == 'optimize':
            return await _handle_optimize(args, runner)
        elif args.command == 'results':
            return await _handle_results(args, repo)
    finally:
        await stock_data.db.close()

    return 0


def _handle_list() -> int:
    strategies = list_strategies()
    if not strategies:
        print('No strategies registered.')
    else:
        print('Available strategies:')
        for name in strategies:
            print(f'  {name}')
    return 0


async def _handle_run(args, runner: BacktestRunner, repo: BacktestRepository) -> int:
    ticker_filter = TickerFilter(
        tickers=args.tickers,
        classifications=args.classification,
        sectors=args.sector,
        industries=args.industry,
        exchanges=args.exchange,
        watchlists=args.watchlist,
        min_market_cap=args.min_market_cap,
        max_market_cap=args.max_market_cap,
        min_volatility=args.min_volatility,
        max_volatility=args.max_volatility,
        min_popularity_rank=args.min_popularity_rank,
        max_popularity_rank=args.max_popularity_rank,
    )

    start_date = datetime.date.fromisoformat(args.start_date) if args.start_date else None
    end_date = datetime.date.fromisoformat(args.end_date) if args.end_date else None
    strategy_params = json.loads(args.params) if args.params else None

    run_id = await runner.run(
        strategy_name=args.strategy,
        ticker_filter=ticker_filter,
        timeframe=args.timeframe,
        cash=args.cash,
        commission=args.commission,
        start_date=start_date,
        end_date=end_date,
        strategy_params=strategy_params,
    )

    if run_id < 0:
        print('No tickers matched the filter. Aborting.')
        return 1

    print(f'\nBacktest run {run_id} complete.')
    print(f'View results:  python -m rocketstocks.backtest stats {run_id}')
    print(f'Per-ticker:    python -m rocketstocks.backtest results {run_id}')

    if args.benchmark:
        benchmark_return = await runner.run_benchmark(
            ticker=args.benchmark,
            timeframe=args.timeframe,
            cash=args.cash,
            commission=args.commission,
            start_date=start_date,
            end_date=end_date,
        )
        results = await repo.get_successful_results_by_run(run_id)
        bench = compare_against_benchmark(results, benchmark_return, label=args.benchmark)
        _print_benchmark_comparison(bench)

    return 0


async def _handle_stats(args, repo: BacktestRepository) -> int:
    run = await repo.get_run(args.run_id)
    if not run:
        print(f'Run {args.run_id} not found.')
        return 1

    stats_rows = await repo.get_stats_by_run(args.run_id)
    if args.group:
        stats_rows = [s for s in stats_rows if s['group_key'] == args.group]

    print(f'\n=== Backtest Run {args.run_id} ===')
    print(f"Strategy:   {run['strategy_name']}")
    print(f"Timeframe:  {run['timeframe']}")
    print(f"Tickers:    {run['ticker_count']}")
    print(f"Date range: {run['start_date'] or 'all'} → {run['end_date'] or 'all'}")
    print(f"Created:    {run['created_at']}")
    if run.get('parameters'):
        print(f"Parameters: {run['parameters']}")
    print()

    if not stats_rows:
        print('No stats found for this run.')
        return 0

    for s in stats_rows:
        _print_group_stats(s)

    return 0


async def _handle_compare(args, repo: BacktestRepository) -> int:
    results_a = await repo.get_successful_results_by_run(args.run_id_a)
    results_b = await repo.get_successful_results_by_run(args.run_id_b)
    run_a = await repo.get_run(args.run_id_a)
    run_b = await repo.get_run(args.run_id_b)

    if not run_a or not run_b:
        print('One or both run IDs not found.')
        return 1

    label_a = f"Run {args.run_id_a} ({run_a['strategy_name']})"
    label_b = f"Run {args.run_id_b} ({run_b['strategy_name']})"
    comparison = compare_strategies(results_a, results_b, label_a=label_a, label_b=label_b)

    print(f'\n=== Strategy Comparison ===')
    mean_a = comparison.get('mean_a')
    mean_b = comparison.get('mean_b')
    print(f"  {label_a}: mean return = {mean_a:.2f}%" if mean_a is not None else f"  {label_a}: no data")
    print(f"  {label_b}: mean return = {mean_b:.2f}%" if mean_b is not None else f"  {label_b}: no data")

    if 'error' in comparison:
        print(f"  (Insufficient data for significance test)")
    else:
        sig = 'Yes' if comparison['significant'] else 'No'
        print(f"  n:                {comparison['n_a']} vs {comparison['n_b']}")
        print(f"  Mean return:      {_fmt(comparison.get('mean_a'))}%"
              f" vs {_fmt(comparison.get('mean_b'))}%")
        print(f"  Mean Sharpe:      {_fmt(comparison.get('mean_sharpe_a'), 3)}"
              f" vs {_fmt(comparison.get('mean_sharpe_b'), 3)}")
        print(f"  Mean Max DD:      {_fmt(comparison.get('mean_max_dd_a'))}%"
              f" vs {_fmt(comparison.get('mean_max_dd_b'))}%")
        print(f"  Mean Win Rate:    {_fmt(comparison.get('mean_win_rate_a'))}%"
              f" vs {_fmt(comparison.get('mean_win_rate_b'))}%")
        print(f"  t-statistic:      {comparison['t_stat']:.4f}")
        print(f"  p-value:          {comparison['p_value']:.4f}")
        print(f"  Significant:      {sig} (p < 0.05)")
        print(f"  Better:           {comparison['better']}")

    return 0


async def _handle_optimize(args, runner: BacktestRunner) -> int:
    param_grid = json.loads(args.params)
    start_date = datetime.date.fromisoformat(args.start_date) if args.start_date else None
    end_date = datetime.date.fromisoformat(args.end_date) if args.end_date else None

    result = await runner.optimize(
        strategy_name=args.strategy,
        ticker=args.ticker,
        param_grid=param_grid,
        maximize=args.maximize,
        timeframe=args.timeframe,
        cash=args.cash,
        commission=args.commission,
        start_date=start_date,
        end_date=end_date,
    )

    if 'error' in result:
        print(f"Optimization failed: {result['error']}")
        return 1

    print(f'\n=== Optimization Results ===')
    print(f'Strategy:  {args.strategy}')
    print(f'Ticker:    {args.ticker}')
    print(f'Maximize:  {args.maximize}')
    print('\nBest stats:')
    for k, v in (result.get('best_stats') or {}).items():
        print(f'  {k:<20} {v:.4f}' if isinstance(v, float) else f'  {k:<20} {v}')

    return 0


async def _handle_results(args, repo: BacktestRepository) -> int:
    run = await repo.get_run(args.run_id)
    if not run:
        print(f'Run {args.run_id} not found.')
        return 1

    results = await repo.get_results_sorted_by(args.run_id, args.sort, args.limit)
    print(f'\n=== Per-Ticker Results: Run {args.run_id} ({run["strategy_name"]}) ===')
    print(f'Sorted by: {args.sort} | Showing top {args.limit}')
    print()

    for r in results:
        excess = ''
        if r.get('buy_hold_pct') is not None and r.get('return_pct') is not None:
            excess_val = r['return_pct'] - r['buy_hold_pct']
            excess = f'  excess: {_fmt(excess_val)}%'
        err = f'  [ERROR: {r["error"]}]' if r.get('error') else ''
        print(
            f"  {r['ticker']:<8}"
            f"  return: {_fmt(r.get('return_pct'))}%"
            f"  sharpe: {_fmt(r.get('sharpe_ratio'), 3)}"
            f"  trades: {r.get('num_trades') or 0}"
            f"{excess}{err}"
        )

    return 0


def _print_benchmark_comparison(bench: dict) -> None:
    print(f'\n=== Benchmark Comparison ({bench.get("label", "")}) ===')
    if 'error' in bench:
        print(f'  Error: {bench["error"]}')
        return
    sig = 'Yes' if bench.get('significant') else 'No'
    print(f"  Benchmark return:     {_fmt(bench.get('benchmark_return_pct'))}%")
    print(f"  Mean excess return:   {_fmt(bench.get('mean_excess_return'))}%")
    print(f"  Median excess return: {_fmt(bench.get('median_excess_return'))}%")
    print(f"  % beating benchmark:  {_fmt(bench.get('pct_beating_benchmark'))}%")
    print(f"  n:                    {bench.get('n', 0)}")
    print(f"  t-statistic:          {_fmt(bench.get('t_stat'), 4)}")
    print(f"  p-value:              {_fmt(bench.get('p_value'), 4)}")
    print(f"  Significant:          {sig} (p < 0.05)")
    print()


def _print_group_stats(s: dict) -> None:
    pv = s.get('p_value')
    t_stat = s.get('t_stat')
    sig = 'Yes' if s.get('significant') else 'No'
    print(f"--- {s.get('group_key', '?')} ({s.get('group_value', '')}) ---")
    print(f"  Tickers:          {s.get('ticker_count', 0)}")
    print(f"  Mean Return:      {_fmt(s.get('mean_return'))}%")
    print(f"  Median Return:    {_fmt(s.get('median_return'))}%")
    print(f"  Std Return:       {_fmt(s.get('std_return'))}%")
    print(f"  Mean Sharpe:      {_fmt(s.get('mean_sharpe'), 3)}")
    print(f"  Mean Win Rate:    {_fmt(s.get('mean_win_rate'))}%")
    print(f"  Total Trades:     {s.get('total_trades', 0)}")
    print(f"  Mean Max DD:      {_fmt(s.get('mean_max_dd'))}%")
    print(f"  t-stat:           {_fmt(t_stat, 4)}")
    print(f"  p-value:          {_fmt(pv, 4)}")
    print(f"  Significant:      {sig}")
    _print_if_set('  Mean Exposure:   ', s.get('mean_exposure_pct'), '%')
    _print_if_set('  Mean Excess Ret: ', s.get('mean_excess_return'), '%')
    _print_if_set('  % Beat Buy&Hold: ', s.get('pct_beating_buy_hold'), '%')
    print()


def _print_if_set(label: str, value, suffix: str = '') -> None:
    """Print a stat line only if the value is set (not None or NaN)."""
    import math
    if value is None:
        return
    try:
        if math.isnan(float(value)):
            return
    except (TypeError, ValueError):
        return
    print(f'{label}{_fmt(value)}{suffix}')


def _fmt(value, decimals: int = 2) -> str:
    if value is None:
        return 'N/A'
    try:
        import math
        if math.isnan(float(value)):
            return 'NaN'
        return f'{float(value):.{decimals}f}'
    except (TypeError, ValueError):
        return str(value)
