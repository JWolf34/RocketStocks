# Backtesting System (`src/rocketstocks/backtest/`)

Separate package for signal validation and strategy research. CLI entry: `python -m rocketstocks.backtest`. Uses the **backtesting.py** framework (chosen for simplicity and direct integration with existing pure signal functions — no adapters needed). See `backtesting_guide.md` for operational usage (CLI commands, filter options, interpreting results).

---

## Architecture

```
CLI (cli.py)
  └─→ BacktestRunner (runner.py)
        ├─→ Strategy Registry (registry.py)  ──→  Strategy classes (strategies/)
        ├─→ data_prep.py                     ──→  backtesting.Backtest
        ├─→ StockData (price_history, popularity)
        ├─→ TickerFilter (filters.py)
        └─→ BacktestRepository (repository.py)  ──→  DB tables
```

---

## CLI (`backtest/cli.py`)

`backtest/__main__.py` calls `cli.py` which builds an `argparse` parser. Subcommands: `run`, `list`, `stats`, `results`, `compare`, `optimize`, `correlation`. All subcommands run async via `asyncio.run()` using a real `StockData` + `Postgres` connection.

---

## Strategy Registry (`backtest/registry.py`)

- `@register(name)` decorator at `registry.py:10-33` — registers a `backtesting.Strategy` subclass by name. Raises `TypeError` if not a Strategy subclass.
- `get_strategy(name)` at `:36-48` — lazy-loads all built-in strategy modules on first call via `_ensure_loaded()` at `:57-67`.
- `list_strategies()` at `:51-54` — sorted list of registered names.

Registered strategies: `buy_hold`, `volume_accumulation`, `popularity_surge`, `leading_indicator_combo`, `direction_prediction`.

---

## LeadingIndicatorStrategy Base Class (`backtest/strategies/base.py`)

`LeadingIndicatorStrategy` at `base.py:61`. Shared base for all active strategies. Subclasses must implement `_detect_signal() -> bool` at `:136-138`.

**Runner flags** (class attributes inspected by BacktestRunner):
- `requires_popularity: bool` — triggers `merge_popularity()` data enrichment
- `requires_daily: bool` — triggers `enrich_5m_with_daily_context()` for 5m strategies

**Configurable exit modes** (class-level attributes at `:83-87`):
- `breakout` (default) — exit when price z-score since entry hits `exit_zscore` (profit) or `-stop_zscore` (loss); `hold_bars` as max-hold failsafe
- `momentum` — trailing exit: tracks peak z-score, exits when it fades by `trail_zscore` while profitable
- `bar_hold` — simple hold for `hold_bars` bars (baseline comparison)

Exit logic implemented in `_check_exit()` at `:260-324`. Reuses `evaluate_confirmation()` from `core/analysis/alert_strategy.py` for z-scoring, matching production bot behavior exactly.

**Direction filter** (opt-in, `:89-94`):
- `use_direction_filter = True` enables logistic regression gating on each entry
- Model trained in `_train_direction_model()` at `:144-181` on the first `direction_train_fraction` of bars (no lookahead)
- Entry gated by `_direction_allows_entry()` at `:183-235` — blocked if `P(up) < direction_threshold` or `confidence < direction_min_confidence`
- Gracefully degrades (allows entry) if model fails to fit

**5m pending-signal pattern**: entry signal detected outside regular hours sets `_pending = True` at `:101`; position opens on the next regular-hours bar at `:131-133`.

---

## BacktestRunner (`backtest/runner.py`)

`BacktestRunner` class at `runner.py:40`. Orchestrates multi-ticker strategy runs.

`run()` at `:52`:
1. Resolves strategy class from registry
2. Applies `TickerFilter` to get ticker list
3. Inserts `backtest_runs` record
4. Fetches/preps price data per ticker, optionally merges popularity/daily context
5. Runs `backtesting.Backtest` per ticker, collects results
6. Tags trades with market regime (SPY-based)
7. Bulk-inserts results, trades, and aggregate stats

`_STAT_MAP` at `:24-35` maps backtesting.py stat keys (e.g., `'Sharpe Ratio'`) to DB column names (e.g., `'sharpe_ratio'`). Minimum 30 bars required per ticker (`_MIN_BARS` at `:37`).

`optimize()` at `:363` — parameter sweep on a single ticker using backtesting.py's built-in `optimize()`. Returns `best_stats` and optional `heatmap`.

---

## Data Preparation (`backtest/data_prep.py`)

Converts DB-format DataFrames (lowercase columns, `date`/`datetime` column) to backtesting.py format (capitalized columns, DatetimeIndex).

| Function | Purpose |
|----------|---------|
| `prep_daily(df)` at `:32` | Rename lowercase → capitalized, set DatetimeIndex from `date` column |
| `prep_5m(df)` at `:68` | Same for 5-minute data using `datetime` column |
| `mark_regular_hours(df)` at `:149` | Adds `Is_Regular_Hours` boolean column using NYSE calendar (with fixed-hours fallback) |
| `filter_regular_hours(df)` at `:103` | Returns subset of bars within regular market hours |
| `enrich_5m_with_daily_context(df_5m, df_daily)` at `:199` | Adds `Prev_Close`, `Daily_Vol_Mean/Std`, `Daily_Return_Mean/Std`, `Cumulative_Volume`, `Intraday_Pct_Change` — ensures z-score exits match production bot |
| `merge_popularity(price_df, popularity_df)` at `:272` | Forward-fills popularity readings (Rank, Mentions, Rank_24h_ago, Mentions_24h_ago) onto price bars |
| `prep_for_signals(df)` at `:374` | Reverses column rename (capitalized → lowercase) for passing to `core/analysis/` functions |

---

## Ticker Filtering (`backtest/filters.py`)

`TickerFilter` dataclass at `filters.py:9-55`. Composable AND-combined filters (all fields optional):

- `tickers` — explicit list, bypasses all other filters
- `classifications`, `sectors`, `industries`, `exchanges`, `watchlists`
- `min/max_market_cap`, `min/max_volatility` (20d)
- `min/max_popularity_rank`, `exclude_delisted` (default True)

`to_dict()` at `:57-59` serializes for JSONB storage in `backtest_runs` table.

---

## Statistical Analysis (`backtest/stats.py`)

- `GroupStats` dataclass — aggregate metrics (mean return, Sharpe, win rate, t-stat, p-value)
- `compute_group_stats(results, group_by)` — one-sample t-test for mean return > 0
- `compare_strategies(run_a_results, run_b_results)` — two-sample t-test between strategies
- `compare_against_benchmark(results, benchmark_return)` — excess return analysis
- `compute_regime_stats(trades)` — per-regime breakdowns (bull/bear/sideways)

---

## Persistence (`backtest/repository.py`)

`BacktestRepository` at `repository.py:31`. Tables:
- `backtest_runs` — one record per run (strategy, timeframe, params, filters, dates)
- `backtest_results` — one record per ticker per run (return, Sharpe, drawdown, win rate, etc.)
- `strategy_stats` — aggregate group stats per run
- `backtest_trades` — individual trade records (entry/exit time/price, PnL, duration, regime)

---

## Advanced Modules

| File | Purpose |
|------|---------|
| `walk_forward.py` | Walk-forward validation — rolling train/test windows to detect overfitting |
| `monte_carlo.py` | Monte Carlo simulation — bootstrapped confidence intervals for strategy metrics |
| `signal_decay.py` | Signal strength decay analysis — does the edge diminish over time? |
| `correlation.py` | Cross-strategy correlation — are two strategies betting on the same tickers? |
| `regime.py` | Market regime classification (bull/bear/sideways via SPY 200-day SMA) and trade tagging |

---

## Framework Decision

backtesting.py was chosen because all `core/analysis/` signal functions are pure (no DB, no discord) and can be called directly from `Strategy.next()` — no adapters or rewrites needed. Single-asset, per-ticker focus matches the production alert model. 10 frameworks were evaluated; others were eliminated for complexity (NautilusTrader, Lean), cost (VectorBT PRO), stagnation (VectorBT OSS, Backtrader), or paradigm mismatch (Zipline, bt).
