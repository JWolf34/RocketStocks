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

    # --------------------------------------------------------- walk-forward --
    wf_p = sub.add_parser('walk-forward', help='Run walk-forward (out-of-sample) validation')
    wf_p.add_argument('strategy', help='Strategy name')
    wf_p.add_argument('--params', type=str, required=True, metavar='JSON',
                      help='Parameter grid for per-fold optimization: {"hold_bars":[5,10]}')
    wf_p.add_argument('--folds', type=int, default=5,
                      help='Number of train/test folds (default: 5)')
    wf_p.add_argument('--train-pct', type=float, default=0.7,
                      help='Fraction of each fold used for training (default: 0.7)')
    wf_p.add_argument('--maximize', type=str, default='Sharpe Ratio',
                      help='Metric to maximize during optimization (default: "Sharpe Ratio")')
    wf_p.add_argument('--optimize-on', type=str, default=None, metavar='TICKER',
                      help='Ticker to use for per-fold optimization (default: first matched ticker)')
    wf_p.add_argument('--timeframe', choices=['daily', '5m'], default='daily')
    wf_p.add_argument('--cash', type=float, default=10_000)
    wf_p.add_argument('--commission', type=float, default=0.002)
    wf_p.add_argument('--start-date', type=str, required=True, metavar='YYYY-MM-DD')
    wf_p.add_argument('--end-date', type=str, required=True, metavar='YYYY-MM-DD')
    # filter flags (same as run)
    wf_p.add_argument('--tickers', nargs='+', default=None, metavar='TICKER')
    wf_p.add_argument('--classification', nargs='+', default=None, metavar='CLASS')
    wf_p.add_argument('--sector', nargs='+', default=None, metavar='SECTOR')
    wf_p.add_argument('--industry', nargs='+', default=None, metavar='INDUSTRY')
    wf_p.add_argument('--min-market-cap', type=float, default=None, metavar='DOLLARS')
    wf_p.add_argument('--max-market-cap', type=float, default=None, metavar='DOLLARS')
    wf_p.add_argument('--exchange', nargs='+', default=None, metavar='EXCHANGE')
    wf_p.add_argument('--watchlist', nargs='+', default=None, metavar='WATCHLIST')

    # --------------------------------------------------------- correlation --
    corr_p = sub.add_parser('correlation', help='Signal overlap and redundancy analysis')
    corr_p.add_argument('run_id_a', type=int, help='First run ID')
    corr_p.add_argument('run_id_b', type=int, help='Second run ID')
    corr_p.add_argument('--window', type=int, default=3,
                        help='Calendar days within which two entries on the same ticker '
                             'are considered overlapping (default: 3)')

    # --------------------------------------------------------- monte-carlo --
    mc_p = sub.add_parser('monte-carlo', help='Monte Carlo drawdown distribution')
    mc_p.add_argument('run_id', type=int, help='Run ID to simulate')
    mc_p.add_argument('--simulations', type=int, default=1000,
                      help='Number of random trade-order shuffles (default: 1000)')
    mc_p.add_argument('--ruin-threshold', type=float, default=0.5, metavar='FRACTION',
                      help='Equity fraction below which counts as ruin (default: 0.5 = 50%%)')
    mc_p.add_argument('--ticker', type=str, default=None,
                      help='Restrict simulation to trades from a single ticker')

    # ---------------------------------------------------------------- decay --
    decay_p = sub.add_parser('decay', help='Signal decay — forward return curves at multiple horizons')
    decay_p.add_argument('run_id', type=int, help='Run ID to analyse')
    decay_p.add_argument('--horizons', type=str, default='1,2,3,5,10,20',
                         metavar='N,N,...',
                         help='Comma-separated forward bar horizons (default: 1,2,3,5,10,20)')
    decay_p.add_argument('--timeframe', choices=['daily', '5m'], default='daily',
                         help='Price bar resolution for fetching forward prices (default: daily)')

    # --------------------------------------------------------------- trades --
    trades_p = sub.add_parser('trades', help='Show individual trade records for a run')
    trades_p.add_argument('run_id', type=int, help='Run ID to display')
    trades_p.add_argument('--ticker', type=str, default=None,
                          help='Filter to a single ticker')
    trades_p.add_argument('--limit', type=int, default=50,
                          help='Number of trades to show (default: 50, 0 = all)')

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
        elif args.command == 'trades':
            return await _handle_trades(args, repo)
        elif args.command == 'walk-forward':
            return await _handle_walk_forward(args, runner)
        elif args.command == 'decay':
            return await _handle_decay(args, runner, repo)
        elif args.command == 'monte-carlo':
            return await _handle_monte_carlo(args, repo)
        elif args.command == 'correlation':
            return await _handle_correlation(args, repo)
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


async def _handle_correlation(args, repo: BacktestRepository) -> int:
    from rocketstocks.backtest.correlation import compute_signal_correlation

    run_a = await repo.get_run(args.run_id_a)
    run_b = await repo.get_run(args.run_id_b)
    if not run_a or not run_b:
        print('One or both run IDs not found.')
        return 1

    trades_a = await repo.get_trades_by_run(args.run_id_a)
    trades_b = await repo.get_trades_by_run(args.run_id_b)

    if not trades_a or not trades_b:
        print('One or both runs have no trades stored. Re-run the backtests to generate trade data.')
        return 1

    label_a = f"Run {args.run_id_a} ({run_a['strategy_name']})"
    label_b = f"Run {args.run_id_b} ({run_b['strategy_name']})"

    result = compute_signal_correlation(
        trades_a, trades_b,
        label_a=label_a, label_b=label_b,
        overlap_window=args.window,
    )

    print(f'\n=== Signal Correlation ===')
    print(f'  {label_a}: {result.n_trades_a} trades')
    print(f'  {label_b}: {result.n_trades_b} trades')
    print(f'  Overlap window: ±{args.window} days, same ticker')
    print()
    print(f'Overlap:')
    print(f'  Overlapping trades:      {result.n_overlap}')
    print(f'  % of {run_a["strategy_name"]} trades:  {_fmt(result.overlap_pct_a)}%')
    print(f'  % of {run_b["strategy_name"]} trades:  {_fmt(result.overlap_pct_b)}%')
    print(f'  Jaccard index:           {_fmt(result.jaccard_index, 3)}  '
          f'(0=no overlap, 1=identical)')
    print()
    print('Returns by group:')
    print(f'  Both fire  ({result.n_overlap:3d} trades):  '
          f'mean {_fmt(result.mean_return_overlap)}%  '
          f'p={_fmt(result.p_value_overlap, 3)}  '
          f'{"***" if result.significant_overlap else "n.s."}')
    print(f'  {run_a["strategy_name"]:<20} only ({result.n_a_only:3d}):  '
          f'mean {_fmt(result.mean_return_a_only)}%  '
          f'p={_fmt(result.p_value_a_only, 3)}  '
          f'{"***" if result.significant_a_only else "n.s."}')
    print(f'  {run_b["strategy_name"]:<20} only ({result.n_b_only:3d}):  '
          f'mean {_fmt(result.mean_return_b_only)}%  '
          f'p={_fmt(result.p_value_b_only, 3)}  '
          f'{"***" if result.significant_b_only else "n.s."}')
    print()
    print(f'Conclusion:')
    for sentence in result.conclusion.split('. '):
        if sentence:
            print(f'  {sentence.strip()}.')

    return 0


async def _handle_monte_carlo(args, repo: BacktestRepository) -> int:
    from rocketstocks.backtest.monte_carlo import run_monte_carlo

    run = await repo.get_run(args.run_id)
    if not run:
        print(f'Run {args.run_id} not found.')
        return 1

    trades = await repo.get_trades_by_run(args.run_id, ticker=getattr(args, 'ticker', None))
    if not trades:
        print(f'No trades found for run {args.run_id}.')
        return 1

    trade_returns = [
        float(t['return_pct'])
        for t in trades
        if t.get('return_pct') is not None
    ]

    if len(trade_returns) < 2:
        print(f'Need at least 2 trades for Monte Carlo. Found {len(trade_returns)}.')
        return 1

    starting_cash = run.get('cash', 10_000) or 10_000

    print(f'\nRunning {args.simulations:,} simulations... ', end='', flush=True)
    result = run_monte_carlo(
        trade_returns=trade_returns,
        starting_cash=starting_cash,
        n_simulations=args.simulations,
        ruin_threshold=args.ruin_threshold,
    )
    print('done.')

    ticker_label = f' ({args.ticker})' if getattr(args, 'ticker', None) else ''
    print(f'\n=== Monte Carlo: Run {args.run_id} ({run["strategy_name"]}){ticker_label} ===')
    print(f'Trades: {result.n_trades:,}  |  Simulations: {result.n_simulations:,}  |  Starting cash: ${result.starting_cash:,.0f}')
    print()
    print('Max Drawdown Distribution:')
    print(f'  Historical (actual):  {_fmt(result.historical_max_dd)}%')
    print(f'  Mean:                 {_fmt(result.mean_max_dd)}%')
    print(f'  5th pctile (worst):   {_fmt(result.p5_max_dd)}%')
    print(f'  25th pctile:          {_fmt(result.p25_max_dd)}%')
    print(f'  Median:               {_fmt(result.p50_max_dd)}%')
    print(f'  75th pctile:          {_fmt(result.p75_max_dd)}%')
    print(f'  95th pctile (best):   {_fmt(result.p95_max_dd)}%')
    print()
    print('Final Equity Distribution:')
    print(f'  5th pctile (worst):   ${result.p5_final_equity:,.0f}')
    print(f'  25th pctile:          ${result.p25_final_equity:,.0f}')
    print(f'  Median:               ${result.p50_final_equity:,.0f}')
    print(f'  75th pctile:          ${result.p75_final_equity:,.0f}')
    print(f'  95th pctile (best):   ${result.p95_final_equity:,.0f}')
    print()
    ruin_pct_display = int(result.ruin_threshold * 100)
    print(f'Probability of Ruin (<{ruin_pct_display}% equity): {_fmt(result.ruin_probability)}%')

    return 0


async def _handle_decay(args, runner: BacktestRunner, repo: BacktestRepository) -> int:
    from rocketstocks.backtest.data_prep import prep_daily
    from rocketstocks.backtest.signal_decay import compute_signal_decay, find_peak_horizon

    run = await repo.get_run(args.run_id)
    if not run:
        print(f'Run {args.run_id} not found.')
        return 1

    trades = await repo.get_trades_by_run(args.run_id)
    if not trades:
        print(f'No trades found for run {args.run_id}. Run a backtest first.')
        return 1

    horizons = [int(h.strip()) for h in args.horizons.split(',') if h.strip().isdigit()]
    if not horizons:
        print('Invalid --horizons value. Use comma-separated integers e.g. 1,3,5,10,20')
        return 1

    # Fetch daily price data for all unique tickers in trades
    unique_tickers = list({t['ticker'] for t in trades})
    start_date = run.get('start_date')
    end_date = run.get('end_date')

    price_data = {}
    for ticker in unique_tickers:
        try:
            raw = await runner._stock_data.price_history.fetch_daily_price_history(
                ticker, start_date=start_date, end_date=None,
            )
            if not raw.empty:
                price_data[ticker] = prep_daily(raw)
        except Exception as exc:
            logger.debug(f'decay: could not fetch {ticker}: {exc}')

    if not price_data:
        print('Could not fetch price data for any ticker in this run.')
        return 1

    points = compute_signal_decay(trades, price_data, horizons=horizons)

    print(f'\n=== Signal Decay Analysis: Run {args.run_id} ({run["strategy_name"]}) ===')
    print(f'Signals: {len(trades)}  |  Tickers: {len(price_data)}  |  Timeframe: daily')
    print()
    print(f'  {"Horizon":<10} {"Mean Ret":>9} {"Median":>9} {"Std":>7} {"Win%":>7} {"n":>6} {"p-val":>8} {"Sig":>4}')
    print('  ' + '-' * 65)
    for p in points:
        sig = '***' if p.significant else ''
        print(
            f"  {p.horizon:<10}"
            f"  {_fmt(p.mean_return):>8}%"
            f"  {_fmt(p.median_return):>8}%"
            f"  {_fmt(p.std_return):>6}%"
            f"  {_fmt(p.win_rate):>6}%"
            f"  {p.n_signals:>5}"
            f"  {_fmt(p.p_value, 3):>7}"
            f"  {sig}"
        )

    peak = find_peak_horizon(points)
    if peak is not None:
        sig_points = [p for p in points if p.significant]
        if sig_points:
            last_sig = max(sig_points, key=lambda p: p.horizon).horizon
            print(f'\nPeak edge at: {peak} bars | Edge significant up to: {last_sig} bars')
            print(f'Suggested hold_bars: {last_sig}')
        else:
            print(f'\nNo horizons showed statistical significance (p < 0.05).')
            print(f'Best mean return at {peak} bars, but not reliable.')

    return 0


async def _handle_walk_forward(args, runner: BacktestRunner) -> int:
    from rocketstocks.backtest.walk_forward import WalkForwardRunner

    param_grid = json.loads(args.params)
    start_date = datetime.date.fromisoformat(args.start_date)
    end_date = datetime.date.fromisoformat(args.end_date)

    ticker_filter = TickerFilter(
        tickers=args.tickers,
        classifications=args.classification,
        sectors=args.sector,
        industries=args.industry,
        exchanges=args.exchange,
        watchlists=args.watchlist,
        min_market_cap=args.min_market_cap,
        max_market_cap=args.max_market_cap,
    )

    wf = WalkForwardRunner(runner)
    result = await wf.run(
        strategy_name=args.strategy,
        ticker_filter=ticker_filter,
        param_grid=param_grid,
        folds=args.folds,
        train_pct=args.train_pct,
        maximize=args.maximize,
        timeframe=args.timeframe,
        cash=args.cash,
        commission=args.commission,
        start_date=start_date,
        end_date=end_date,
        optimize_on=args.optimize_on,
    )

    if 'error' in result:
        print(f'Walk-forward failed: {result["error"]}')
        return 1

    print(f'\n=== Walk-Forward Validation: {args.strategy} ===')
    print(f'Folds: {result["n_folds"]}  |  Tickers: {result["n_tickers"]}')
    print(f'Date range: {args.start_date} → {args.end_date}')
    print()

    for fold in result.get('folds', []):
        print(f"Fold {fold['fold']}: train={fold['train_start']}→{fold['train_end']}  "
              f"test={fold['test_start']}→{fold['test_end']}")
        if fold.get('error'):
            print(f"  ERROR: {fold['error']}")
            continue
        if fold.get('best_params'):
            params_str = ', '.join(f'{k}={v}' for k, v in fold['best_params'].items())
            print(f"  Best params: {params_str}")
        if fold.get('oos_run_id') is not None:
            print(
                f"  OOS run_id: {fold['oos_run_id']}"
                f"  mean_return: {_fmt(fold.get('oos_mean_return'))}%"
                f"  sharpe: {_fmt(fold.get('oos_sharpe'), 3)}"
                f"  trades: {fold.get('oos_num_trades', 0)}"
                f"  p={_fmt(fold.get('oos_p_value'), 3)}"
            )
        print()

    oos = result.get('oos_stats')
    if oos:
        sig = 'Yes' if oos.get('significant') else 'No'
        print('=== Out-of-Sample Aggregate (All Folds) ===')
        print(f"  Mean Return:    {_fmt(oos.get('mean_return'))}%")
        print(f"  Median Return:  {_fmt(oos.get('median_return'))}%")
        print(f"  Mean Sharpe:    {_fmt(oos.get('mean_sharpe'), 3)}")
        print(f"  Total Trades:   {oos.get('total_trades', 0)}")
        print(f"  t-stat:         {_fmt(oos.get('t_stat'), 4)}")
        print(f"  p-value:        {_fmt(oos.get('p_value'), 4)}")
        print(f"  Significant:    {sig} (p < 0.05)")

    stability = result.get('param_stability', {})
    if stability:
        print()
        print('=== Parameter Stability ===')
        for param, values in stability.items():
            if values:
                unique = sorted(set(values))
                print(f"  {param}: chose {values}  (range: {unique[0]}–{unique[-1]})")

    return 0


async def _handle_trades(args, repo: BacktestRepository) -> int:
    run = await repo.get_run(args.run_id)
    if not run:
        print(f'Run {args.run_id} not found.')
        return 1

    trades = await repo.get_trades_by_run(args.run_id, ticker=args.ticker)
    limit = args.limit if args.limit > 0 else len(trades)
    trades = trades[:limit]

    ticker_label = f' ({args.ticker})' if args.ticker else ''
    print(f'\n=== Trade Records: Run {args.run_id} ({run["strategy_name"]}){ticker_label} ===')
    print(f'Showing {len(trades)} trade(s)')
    print()

    if not trades:
        print('No trades found.')
        return 0

    for t in trades:
        regime_str = f'  regime:{t["regime"]}' if t.get('regime') else ''
        print(
            f"  {t['ticker']:<8}"
            f"  entry:{str(t['entry_time'])[:16]}"
            f"  exit:{str(t['exit_time'])[:16]}"
            f"  ret:{_fmt(t.get('return_pct'))}%"
            f"  pnl:{_fmt(t.get('pnl'))}"
            f"  bars:{t.get('duration_bars', 0)}"
            f"{regime_str}"
        )
    return 0


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
