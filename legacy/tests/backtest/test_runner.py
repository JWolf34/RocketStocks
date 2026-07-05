"""Tests for rocketstocks.backtest.runner.BacktestRunner."""
import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from backtesting import Strategy

from rocketstocks.backtest.filters import TickerFilter
from rocketstocks.backtest.repository import BacktestRepository
from rocketstocks.backtest.runner import BacktestRunner


def _make_daily_df(n: int = 100, seed: int = 42) -> pd.DataFrame:
    """DB-format daily OHLCV DataFrame."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    close = 100.0 + rng.standard_normal(n).cumsum()
    return pd.DataFrame({
        'ticker': 'AAPL',
        'open': close - 0.5,
        'high': close + 0.5,
        'low': close - 0.5,
        'close': close,
        'volume': rng.integers(500_000, 2_000_000, n).astype(float),
        'date': dates.date,
    })


def _make_mock_repo() -> MagicMock:
    repo = MagicMock(spec=BacktestRepository)
    repo.insert_run = AsyncMock(return_value=1)
    repo.insert_results_batch = AsyncMock()
    repo.insert_stats_batch = AsyncMock()
    repo.insert_trades_batch = AsyncMock()
    repo.get_run = AsyncMock(return_value={'run_id': 1, 'strategy_name': 'test'})
    repo.get_successful_results_by_run = AsyncMock(return_value=[])
    repo.get_stats_by_run = AsyncMock(return_value=[])
    return repo


def _make_mock_stock_data(daily_df: pd.DataFrame | None = None) -> MagicMock:
    if daily_df is None:
        daily_df = _make_daily_df()
    sd = MagicMock(name='StockData')
    sd.tickers = MagicMock()
    sd.tickers.get_all_tickers = AsyncMock(return_value=['AAPL'])
    sd.tickers.get_all_ticker_info = AsyncMock(return_value=pd.DataFrame({
        'ticker': ['AAPL'],
        'sector': ['Technology'],
        'exchange': ['NASDAQ'],
        'delist_date': [None],
    }))
    sd.ticker_stats = MagicMock()
    sd.ticker_stats.get_all_classifications = AsyncMock(return_value={'AAPL': 'blue_chip'})
    sd.ticker_stats.get_all_market_caps = AsyncMock(return_value={'AAPL': 3_000_000_000_000})
    sd.ticker_stats.get_all_stats = AsyncMock(return_value=[
        {'ticker': 'AAPL', 'classification': 'blue_chip', 'market_cap': 3_000_000_000_000, 'volatility_20d': 1.2},
    ])
    sd.popularity = MagicMock()
    sd.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame(
        columns=['datetime', 'rank', 'ticker', 'name', 'mentions', 'upvotes',
                 'rank_24h_ago', 'mentions_24h_ago']
    ))
    sd.watchlists = MagicMock()
    sd.watchlists.get_ticker_to_watchlist_map = AsyncMock(return_value={})
    sd.trading_view = MagicMock()
    sd.trading_view.get_market_caps = MagicMock(return_value=pd.DataFrame(
        columns=['ticker', 'market_cap']
    ))
    sd.price_history = MagicMock()
    sd.price_history.fetch_daily_price_history = AsyncMock(return_value=daily_df)
    sd.price_history.fetch_5m_price_history = AsyncMock(return_value=pd.DataFrame())
    return sd


class _AlwaysBuyStrategy(Strategy):
    """Minimal strategy that buys immediately and holds 1 bar."""
    hold_bars = 1
    def init(self): pass
    def next(self):
        if self.position:
            if len(self.data) - self.trades[-1].entry_bar >= self.hold_bars:
                self.position.close()
            return
        self.buy()


# ---------------------------------------------------------------------------
# run() — happy path
# ---------------------------------------------------------------------------

async def test_run_returns_run_id():
    sd = _make_mock_stock_data()
    repo = _make_mock_repo()
    runner = BacktestRunner(stock_data=sd, repo=repo)

    with patch('rocketstocks.backtest.runner.get_strategy', return_value=_AlwaysBuyStrategy):
        run_id = await runner.run(
            strategy_name='alert_signal',
            ticker_filter=TickerFilter(tickers=['AAPL']),
        )

    assert run_id == 1


async def test_run_inserts_run_record():
    sd = _make_mock_stock_data()
    repo = _make_mock_repo()
    runner = BacktestRunner(stock_data=sd, repo=repo)

    with patch('rocketstocks.backtest.runner.get_strategy', return_value=_AlwaysBuyStrategy):
        await runner.run(
            strategy_name='alert_signal',
            ticker_filter=TickerFilter(tickers=['AAPL']),
        )

    repo.insert_run.assert_called_once()
    kwargs = repo.insert_run.call_args.kwargs
    assert kwargs['strategy_name'] == 'alert_signal'
    assert kwargs['ticker_count'] == 1


async def test_run_persists_results():
    sd = _make_mock_stock_data()
    repo = _make_mock_repo()
    runner = BacktestRunner(stock_data=sd, repo=repo)

    with patch('rocketstocks.backtest.runner.get_strategy', return_value=_AlwaysBuyStrategy):
        await runner.run(
            strategy_name='alert_signal',
            ticker_filter=TickerFilter(tickers=['AAPL']),
        )

    repo.insert_results_batch.assert_called_once()
    run_id_arg, results_arg = repo.insert_results_batch.call_args.args
    assert run_id_arg == 1
    assert len(results_arg) == 1
    assert results_arg[0]['ticker'] == 'AAPL'


async def test_run_persists_aggregate_stats():
    sd = _make_mock_stock_data()
    repo = _make_mock_repo()
    runner = BacktestRunner(stock_data=sd, repo=repo)

    with patch('rocketstocks.backtest.runner.get_strategy', return_value=_AlwaysBuyStrategy):
        await runner.run(
            strategy_name='alert_signal',
            ticker_filter=TickerFilter(tickers=['AAPL']),
        )

    repo.insert_stats_batch.assert_called_once()


# ---------------------------------------------------------------------------
# run() — empty filter
# ---------------------------------------------------------------------------

async def test_run_empty_filter_returns_minus_one():
    sd = MagicMock()
    sd.tickers.get_all_tickers = AsyncMock(return_value=[])
    sd.tickers.get_all_ticker_info = AsyncMock(return_value=pd.DataFrame(
        columns=['ticker', 'sector', 'delist_date']
    ))
    sd.ticker_stats.get_all_stats = AsyncMock(return_value=[])
    sd.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame())
    repo = _make_mock_repo()
    runner = BacktestRunner(stock_data=sd, repo=repo)

    with patch('rocketstocks.backtest.runner.get_strategy', return_value=_AlwaysBuyStrategy):
        run_id = await runner.run(
            strategy_name='alert_signal',
            ticker_filter=TickerFilter(),
        )

    assert run_id == -1
    repo.insert_run.assert_not_called()


# ---------------------------------------------------------------------------
# _run_single() — insufficient data
# ---------------------------------------------------------------------------

async def test_run_single_insufficient_data_sets_error():
    tiny_df = _make_daily_df(n=10)  # below _MIN_BARS=30
    sd = _make_mock_stock_data(daily_df=tiny_df)
    repo = _make_mock_repo()
    runner = BacktestRunner(stock_data=sd, repo=repo)

    result, trades = await runner._run_single(
        ticker='AAPL',
        strategy_cls=_AlwaysBuyStrategy,
        timeframe='daily',
        cash=10_000,
        commission=0.002,
        start_date=None,
        end_date=None,
        strategy_params=None,
        classification='standard',
        sector='Technology',
    )

    assert result['error'] is not None
    assert 'insufficient_data' in result['error']
    assert trades == []


# ---------------------------------------------------------------------------
# _run_single() — extracts key metrics
# ---------------------------------------------------------------------------

async def test_run_single_success_extracts_return_pct():
    sd = _make_mock_stock_data()
    repo = _make_mock_repo()
    runner = BacktestRunner(stock_data=sd, repo=repo)

    result, trades = await runner._run_single(
        ticker='AAPL',
        strategy_cls=_AlwaysBuyStrategy,
        timeframe='daily',
        cash=10_000,
        commission=0.002,
        start_date=None,
        end_date=None,
        strategy_params=None,
        classification='blue_chip',
        sector='Technology',
    )

    assert result['error'] is None
    assert result['return_pct'] is not None
    assert result['num_trades'] is not None and isinstance(result['num_trades'], int)
    assert isinstance(trades, list)


# ---------------------------------------------------------------------------
# 5m timeframe applies mark_regular_hours (not filter_regular_hours)
# ---------------------------------------------------------------------------

async def test_run_5m_applies_mark_regular_hours():
    sd = _make_mock_stock_data()
    repo = _make_mock_repo()
    runner = BacktestRunner(stock_data=sd, repo=repo)

    with patch('rocketstocks.backtest.runner.mark_regular_hours') as mock_mark, \
         patch('rocketstocks.backtest.runner.get_strategy', return_value=_AlwaysBuyStrategy):
        mock_mark.return_value = pd.DataFrame()  # empty → insufficient_data
        await runner.run(
            strategy_name='test_strat',
            ticker_filter=TickerFilter(tickers=['AAPL']),
            timeframe='5m',
        )

    mock_mark.assert_called_once()


# ---------------------------------------------------------------------------
# requires_daily: fetches and enriches daily data for 5m strategies
# ---------------------------------------------------------------------------

class _RequiresDailyStrategy(Strategy):
    requires_daily = True
    def init(self): pass
    def next(self):
        if not self.position:
            self.buy()
        elif len(self.data) - self.trades[-1].entry_bar >= 2:
            self.position.close()


async def test_run_5m_requires_daily_fetches_daily_data():
    sd = _make_mock_stock_data()
    repo = _make_mock_repo()
    runner = BacktestRunner(stock_data=sd, repo=repo)

    with patch('rocketstocks.backtest.runner.enrich_5m_with_daily_context',
               return_value=pd.DataFrame()) as mock_enrich, \
         patch('rocketstocks.backtest.runner.mark_regular_hours',
               return_value=pd.DataFrame()) as mock_mark, \
         patch('rocketstocks.backtest.runner.get_strategy',
               return_value=_RequiresDailyStrategy):
        mock_mark.return_value = pd.DataFrame()
        await runner.run(
            strategy_name='test_strat',
            ticker_filter=TickerFilter(tickers=['AAPL']),
            timeframe='5m',
        )

    # Daily data must have been fetched when requires_daily=True
    assert sd.price_history.fetch_daily_price_history.await_count >= 1


# ---------------------------------------------------------------------------
# requires_popularity: fetches and merges popularity data
# ---------------------------------------------------------------------------

class _RequiresPopularityStrategy(Strategy):
    requires_popularity = True
    def init(self): pass
    def next(self):
        if not self.position:
            self.buy()
        elif len(self.data) - self.trades[-1].entry_bar >= 2:
            self.position.close()


async def test_run_daily_requires_popularity_fetches_data():
    sd = _make_mock_stock_data()
    repo = _make_mock_repo()
    runner = BacktestRunner(stock_data=sd, repo=repo)

    with patch('rocketstocks.backtest.runner.get_strategy',
               return_value=_RequiresPopularityStrategy):
        await runner.run(
            strategy_name='test_strat',
            ticker_filter=TickerFilter(tickers=['AAPL']),
            timeframe='daily',
        )

    sd.popularity.fetch_popularity.assert_called_once_with('AAPL')


# ---------------------------------------------------------------------------
# run_benchmark
# ---------------------------------------------------------------------------

async def test_run_benchmark_returns_float():
    sd = _make_mock_stock_data()
    repo = _make_mock_repo()
    runner = BacktestRunner(stock_data=sd, repo=repo)

    with patch('rocketstocks.backtest.runner.get_strategy') as mock_get:
        # Use a simple strategy that buys and holds
        mock_get.return_value = _AlwaysBuyStrategy
        result = await runner.run_benchmark(ticker='SPY', timeframe='daily')

    assert isinstance(result, float)


async def test_run_benchmark_nan_on_no_data():
    sd = _make_mock_stock_data(daily_df=_make_daily_df(n=5))  # insufficient data
    repo = _make_mock_repo()
    runner = BacktestRunner(stock_data=sd, repo=repo)

    with patch('rocketstocks.backtest.runner.get_strategy') as mock_get:
        mock_get.return_value = _AlwaysBuyStrategy
        result = await runner.run_benchmark(ticker='SPY', timeframe='daily')

    import math
    assert math.isnan(result)


# ---------------------------------------------------------------------------
# _extend_start
# ---------------------------------------------------------------------------

def test_extend_start_adds_days():
    runner = BacktestRunner(stock_data=MagicMock(), repo=MagicMock())
    start = datetime.date(2025, 3, 1)
    extended = runner._extend_start(start, extra_trading_days=25)
    assert extended < start
    # Should be roughly 25*1.5 + 10 = 47 calendar days earlier
    delta = (start - extended).days
    assert delta >= 40


def test_extend_start_none_returns_none():
    runner = BacktestRunner(stock_data=MagicMock(), repo=MagicMock())
    assert runner._extend_start(None, 25) is None
