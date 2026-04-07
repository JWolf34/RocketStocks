"""CLI argument parsing and subcommand dispatch for the EDA framework."""
import argparse
import datetime
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='python -m rocketstocks.eda',
        description='RocketStocks Exploratory Data Analysis',
    )
    sub = parser.add_subparsers(dest='command', required=True)

    # Shared arguments added to each subcommand
    def _add_common(p):
        p.add_argument(
            '--source',
            default='sentiment',
            metavar='SOURCE',
            help=(
                'Event source(s). Single: sentiment, volume. '
                'Combined (AND): sentiment+volume. Default: sentiment'
            ),
        )
        p.add_argument(
            '--timeframe', choices=['daily', '5m'], default='daily',
            help='Price bar resolution (default: daily)',
        )
        p.add_argument(
            '--start-date', type=str, default=None, metavar='YYYY-MM-DD',
            help='Earliest date to include in analysis',
        )
        p.add_argument(
            '--end-date', type=str, default=None, metavar='YYYY-MM-DD',
            help='Latest date to include in analysis',
        )
        p.add_argument(
            '--tickers', nargs='+', default=None, metavar='TICKER',
            help='Restrict analysis to these tickers',
        )

    # -------------------------------------------------- forward-returns --
    fwd_p = sub.add_parser('forward-returns', help='Event study: forward returns after events')
    _add_common(fwd_p)
    fwd_p.add_argument(
        '--min-mentions', type=int, default=10, metavar='N',
        help='Minimum mention count for sentiment events (default: 10)',
    )
    fwd_p.add_argument(
        '--mention-thresholds', type=str, default='2.0,3.0,5.0', metavar='LIST',
        help='Comma-separated mention_ratio thresholds (default: 2.0,3.0,5.0)',
    )
    fwd_p.add_argument(
        '--rank-thresholds', type=str, default='50,100,200', metavar='LIST',
        help='Comma-separated rank_change thresholds (default: 50,100,200)',
    )
    fwd_p.add_argument(
        '--sentiment-mode', choices=['mention_ratio', 'rank_change', 'both'],
        default='both',
        help='Which sentiment condition(s) to detect (default: both)',
    )
    fwd_p.add_argument(
        '--volume-threshold', type=float, default=2.0, metavar='Z',
        help='Volume z-score threshold for volume events (default: 2.0)',
    )
    fwd_p.add_argument(
        '--dedup-window', type=int, default=3, metavar='DAYS',
        help='Deduplication window in days: min gap between events per ticker (default: 3)',
    )
    fwd_p.add_argument(
        '--no-stratify', action='store_true',
        help='Skip signal_value stratification (faster)',
    )
    fwd_p.add_argument(
        '--horizons', type=str, default=None, metavar='LIST',
        help='Comma-separated custom horizon list (bars)',
    )

    # -------------------------------------------------- lead-lag --
    ll_p = sub.add_parser('lead-lag', help='Cross-correlation: does signal lead price?')
    _add_common(ll_p)
    ll_p.add_argument(
        '--max-lag', type=int, default=10, metavar='N',
        help='Maximum lag in bars (both directions, default: 10)',
    )
    ll_p.add_argument(
        '--min-periods', type=int, default=30, metavar='N',
        help='Minimum observations per ticker (default: 30)',
    )
    ll_p.add_argument(
        '--signal-col', type=str, default=None, metavar='COL',
        help=(
            'Panel column to use as signal (default: mention_ratio for sentiment, '
            'volume_zscore for volume). Use mention_delta for denser datasets.'
        ),
    )

    # -------------------------------------------------- regime --
    reg_p = sub.add_parser('regime', help='Regime-conditional signal effectiveness')
    _add_common(reg_p)
    reg_p.add_argument(
        '--min-mentions', type=int, default=10, metavar='N',
        help='Minimum mention count for sentiment events (default: 10)',
    )
    reg_p.add_argument(
        '--mention-thresholds', type=str, default='3.0', metavar='LIST',
        help='Comma-separated mention_ratio thresholds (default: 3.0)',
    )
    reg_p.add_argument(
        '--sentiment-mode', choices=['mention_ratio', 'rank_change', 'both'],
        default='mention_ratio',
        help='Which sentiment condition(s) to detect (default: mention_ratio)',
    )
    reg_p.add_argument(
        '--horizons', type=str, default=None, metavar='LIST',
        help='Comma-separated custom horizon list (bars)',
    )
    reg_p.add_argument(
        '--breadth-horizon', type=int, default=5, metavar='DAYS',
        help='Trading days for breadth vs. SPY return analysis (default: 5)',
    )

    return parser


async def main(argv: list[str] | None = None) -> int:
    """Parse CLI arguments and dispatch to the appropriate handler."""
    parser = build_parser()
    args = parser.parse_args(argv)

    from rocketstocks.data.stockdata import StockData
    from rocketstocks.data.schema import create_tables

    stock_data = StockData()
    await stock_data.db.open()
    try:
        await create_tables(stock_data.db)

        if args.command == 'forward-returns':
            return await _handle_forward_returns(args, stock_data)
        elif args.command == 'lead-lag':
            return await _handle_lead_lag(args, stock_data)
        elif args.command == 'regime':
            return await _handle_regime(args, stock_data)
    finally:
        await stock_data.db.close()

    return 0


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

async def _handle_forward_returns(args, stock_data) -> int:
    from rocketstocks.eda.data_loader import load_daily_panel, load_intraday_panel
    from rocketstocks.eda.events.base import deduplicate_events
    from rocketstocks.eda.engines.forward_returns import run_forward_returns, print_results

    start_date, end_date = _parse_dates(args)
    detector = _build_detector(args)

    events = await detector.detect(
        stock_data,
        timeframe=args.timeframe,
        start_date=start_date,
        end_date=end_date,
        tickers=args.tickers,
    )

    if events.empty:
        print("No events detected. Try lowering --mention-thresholds or --volume-threshold.")
        return 1

    events = deduplicate_events(events, window_days=args.dedup_window)
    print(f"\nDetected {len(events)} events across {events['ticker'].nunique()} tickers")

    # Load price data for forward return lookups
    if args.timeframe == 'daily':
        _, close_dict = await load_daily_panel(
            stock_data, start_date, end_date, args.tickers
        )
    else:
        _, close_dict = await load_intraday_panel(
            stock_data, start_date, end_date, args.tickers
        )

    custom_horizons = _parse_int_list(args.horizons) if args.horizons else None
    results = run_forward_returns(
        events=events,
        close_dict=close_dict,
        timeframe=args.timeframe,
        custom_horizons=custom_horizons,
        stratify=not args.no_stratify,
    )
    print_results(results, timeframe=args.timeframe)
    return 0


async def _handle_lead_lag(args, stock_data) -> int:
    from rocketstocks.eda.data_loader import load_daily_panel, load_intraday_panel
    from rocketstocks.eda.engines.cross_correlation import run_cross_correlation, print_results

    start_date, end_date = _parse_dates(args)

    if args.timeframe == 'daily':
        panel, _ = await load_daily_panel(stock_data, start_date, end_date, args.tickers)
    else:
        panel, _ = await load_intraday_panel(stock_data, start_date, end_date, args.tickers)

    if panel.empty:
        print("No panel data loaded. Check your date range and that price/popularity data exists.")
        return 1

    # Determine signal column
    signal_col = args.signal_col
    if signal_col is None:
        source = args.source.split('+')[0].strip()
        if source == 'sentiment':
            # mention_ratio: non-NaN whenever the ticker appears in popularity (sparse-friendly).
            # mention_delta: non-NaN only on consecutive-day appearances — needs dense coverage.
            signal_col = 'mention_ratio'
        elif source == 'volume':
            signal_col = '_volume_zscore'
            # Compute rolling volume z-score on the panel if not present
            if '_volume_zscore' not in panel.columns and 'volume' in panel.columns:
                panel = _add_volume_zscore(panel, args.timeframe)
        else:
            print(f"Could not determine signal column for source '{source}'. Use --signal-col.")
            return 1

    if signal_col not in panel.columns:
        print(f"Signal column '{signal_col}' not found in panel. Available: {list(panel.columns)}")
        return 1

    return_col = 'daily_return' if args.timeframe == 'daily' else 'bar_return'
    if return_col not in panel.columns:
        print(f"Return column '{return_col}' not found in panel.")
        return 1

    result = run_cross_correlation(
        panel=panel,
        signal_col=signal_col,
        return_col=return_col,
        timeframe=args.timeframe,
        max_lag=args.max_lag,
        min_periods=args.min_periods,
    )
    print_results(result)
    return 0


async def _handle_regime(args, stock_data) -> int:
    from rocketstocks.eda.data_loader import (
        load_daily_panel, load_intraday_panel, load_spy_daily
    )
    from rocketstocks.eda.events.base import deduplicate_events
    from rocketstocks.eda.engines.regime_analysis import run_regime_analysis, print_results

    start_date, end_date = _parse_dates(args)
    detector = _build_detector(args)

    events = await detector.detect(
        stock_data,
        timeframe=args.timeframe,
        start_date=start_date,
        end_date=end_date,
        tickers=args.tickers,
    )

    if events.empty:
        print("No events detected. Try lowering --mention-thresholds.")
        return 1

    events = deduplicate_events(events, window_days=3)
    print(f"\nDetected {len(events)} events across {events['ticker'].nunique()} tickers")

    if args.timeframe == 'daily':
        _, close_dict = await load_daily_panel(
            stock_data, start_date, end_date, args.tickers
        )
    else:
        _, close_dict = await load_intraday_panel(
            stock_data, start_date, end_date, args.tickers
        )

    spy_df = await load_spy_daily(stock_data, start_date, end_date)

    custom_horizons = _parse_int_list(args.horizons) if args.horizons else None
    results = run_regime_analysis(
        events=events,
        close_dict=close_dict,
        spy_df=spy_df,
        timeframe=args.timeframe,
        custom_horizons=custom_horizons,
        breadth_horizon_days=args.breadth_horizon,
    )
    print_results(results)
    return 0


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _parse_dates(args) -> tuple[datetime.date | None, datetime.date | None]:
    start = datetime.date.fromisoformat(args.start_date) if args.start_date else None
    end = datetime.date.fromisoformat(args.end_date) if args.end_date else None
    return start, end


def _parse_float_list(s: str) -> list[float]:
    return [float(x.strip()) for x in s.split(',') if x.strip()]


def _parse_int_list(s: str) -> list[int]:
    return [int(x.strip()) for x in s.split(',') if x.strip()]


def _build_detector(args):
    """Instantiate the appropriate event detector from --source."""
    from rocketstocks.eda.events.sentiment import SentimentDetector
    from rocketstocks.eda.events.volume import VolumeDetector
    from rocketstocks.eda.events.composite import CompositeDetector

    source = args.source.lower()

    def _make_sentiment():
        mention_thresh = _parse_float_list(args.mention_thresholds) if hasattr(args, 'mention_thresholds') else [3.0]
        rank_thresh = _parse_int_list(args.rank_thresholds) if hasattr(args, 'rank_thresholds') else [100]
        min_mentions = args.min_mentions if hasattr(args, 'min_mentions') else 10
        mode = args.sentiment_mode if hasattr(args, 'sentiment_mode') else 'both'
        return SentimentDetector(
            mention_thresholds=mention_thresh,
            rank_change_thresholds=rank_thresh,
            min_mentions=min_mentions,
            mode=mode,
        )

    def _make_volume():
        zscore = args.volume_threshold if hasattr(args, 'volume_threshold') else 2.0
        return VolumeDetector(zscore_threshold=zscore)

    if source == 'sentiment':
        return _make_sentiment()
    elif source == 'volume':
        return _make_volume()
    elif '+' in source:
        parts = [p.strip() for p in source.split('+')]
        detectors = []
        for part in parts:
            if part == 'sentiment':
                detectors.append(_make_sentiment())
            elif part == 'volume':
                detectors.append(_make_volume())
            else:
                raise ValueError(f"Unknown detector source: '{part}'")
        return CompositeDetector(detectors, mode='and')
    else:
        raise ValueError(
            f"Unknown --source '{args.source}'. "
            "Valid values: sentiment, volume, sentiment+volume"
        )


def _add_volume_zscore(panel: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Add a rolling volume z-score column to the panel."""
    import numpy as np
    panel = panel.copy()
    time_col = 'date' if timeframe == 'daily' else 'datetime'
    zscores = []

    for _ticker, grp in panel.groupby('ticker'):
        grp = grp.sort_values(time_col)
        vol = grp['volume'].astype(float)
        roll_mean = vol.shift(1).rolling(20, min_periods=3).mean()
        roll_std = vol.shift(1).rolling(20, min_periods=3).std()
        z = (vol - roll_mean) / roll_std.replace(0, np.nan)
        zscores.append(z)

    if zscores:
        import pandas as pd
        panel['_volume_zscore'] = pd.concat(zscores).reindex(panel.index)

    return panel
