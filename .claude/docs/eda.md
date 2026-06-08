# EDA Framework (`src/rocketstocks/eda/`)

Standalone signal research tool for exploratory data analysis on stock events. CLI entry: `python -m rocketstocks.eda`. Decouples event detection (pluggable detectors) from statistical analysis (source-agnostic engines), enabling rapid hypothesis testing. Reads from the same DB as the production bot but has no dependency on `core/`, `bot/`, or `backtest/`.

---

## Architecture

```
CLI (__main__.py → cli.py)
  ├─→ EventDetector (events/)       ──→  events DataFrame (standard schema)
  │     ├── SentimentDetector
  │     ├── VolumeDetector
  │     └── CompositeDetector
  └─→ Analysis Engine (engines/)    ──→  consumes events DataFrame
        ├── ForwardReturnsEngine
        ├── CrossCorrelationEngine
        └── RegimeAnalysisEngine

Data Layer
  ├── data_loader.py  ──→  panel construction (daily + 5m)
  └── streaming.py    ──→  per-ticker async generator (O(1) peak memory)
```

---

## CLI (`eda/cli.py`)

`eda/__main__.py` sets up logging and calls `cli.main()`. Three subcommands, all async with a real `StockData` + `Postgres` connection:

| Subcommand | Purpose |
|---|---|
| `forward-returns` | Event study: computes return distributions at multiple horizons after events |
| `lead-lag` | Cross-correlation analysis: tests whether a signal leads or lags price |
| `regime` | Regime-conditional analysis: splits signal effectiveness by market regime |

**Shared flags**: `--source` (`sentiment`, `volume`, `sentiment+volume`), `--timeframe` (`daily`, `5m`), `--start-date`, `--end-date`, `--tickers`

---

## Event Detection Layer (`eda/events/`)

### Core Protocol (`events/base.py`)

- **`EventDetector`** — protocol any detector must satisfy: `async def detect(...) -> pd.DataFrame`
- **Standard columns** required in all events DataFrames: `ticker`, `datetime`, `signal_value`, `source`
- **`empty_events()`** at `base.py` — zero-row DataFrame with required columns; use as safe default
- **`validate_events()`** — raises `ValueError` if required columns are missing
- **`deduplicate_events(window_days=3)`** — keeps first event per ticker per N-day window; prevents clustering bias in return studies
- **`build_control_group(events, bar_counts)`** — samples random `(ticker, bar-offset)` pairs via cumsum+searchsorted, excluding event dates; used by `ForwardReturnsEngine` for baseline comparison

### SentimentDetector (`events/sentiment.py`)

Detects popularity spikes from the `popularity` table.

- **Mention spike**: `mentions / mentions_24h_ago >= threshold` (configurable, default 3.0)
- **Rank jump**: `rank_24h_ago - rank >= threshold` (configurable, default 50)
- Both modes active by default; either can be disabled
- Filters: `min_mentions` (ignores low-base tickers), optional ticker list
- Daily timeframe: reduces to one event per ticker per day (highest `mention_ratio` wins)
- Extra columns returned: `source_detail`, `mentions`, `rank`, `mention_ratio`, `rank_change`

### VolumeDetector (`events/volume.py`)

Detects volume spikes from price history tables using a rolling z-score.

- **Signal**: `zscore = (volume - rolling_mean) / rolling_std`, shifted by 1 bar to avoid lookahead
- Emits event when `zscore >= threshold AND volume >= min_volume`
- Streams data one ticker at a time (memory-efficient); supports `daily` and `5m` timeframes
- Extra columns returned: `source_detail`, `volume`

### CompositeDetector (`events/composite.py`)

Combines N detectors with AND or OR logic.

- **OR mode** — union of all events from all detectors
- **AND mode** — events where all detectors fired within a configurable time window (co-occurrence study)
- Useful for: studying combined signals (e.g., sentiment spike + volume spike on same day)

---

## Data Loading Layer (`eda/data_loader.py`)

Builds panel DataFrames and close-price dicts for analysis engines.

| Function | Returns | Notes |
|---|---|---|
| `load_daily_panel()` | `(panel_df, close_dict)` | Joins popularity snapshots with daily closes; computes `mention_ratio`, `rank_change`, `mention_delta` |
| `load_intraday_panel()` | `(panel_df, close_dict)` | Forward-fills 30-min popularity onto 5-min bars |
| `load_spy_daily()` | `pd.DataFrame` | SPY daily price history for regime classification |

`close_dict` is `{ticker: pd.Series(close, DatetimeIndex)}` — used by engines for forward return lookups.

---

## Streaming (`eda/streaming.py`)

**`stream_tickers(stock_data, timeframe, ...)`** — async generator that yields `(ticker, price_df, pop_df)` one at a time, deleting each DataFrame before advancing. Peak RSS stays O(1) regardless of universe size. Used by `VolumeDetector`, `ForwardReturnsEngine`, and `CrossCorrelationEngine`.

**`fetch_bar_counts(stock_data, timeframe, tickers)`** — single `COUNT(*) GROUP BY ticker` query; returns `{ticker: bar_count}` for control group sampling without loading price data.

**`fetch_distinct_tickers(stock_data)`** — queries distinct tickers from the popularity table; used when no explicit ticker list is given.

---

## Analysis Engines (`eda/engines/`)

### Forward Returns Engine (`engines/forward_returns.py`)

Event study: compute the distribution of price returns N bars after each event.

- Streams price data one ticker at a time; accumulates returns without materializing a full panel
- Computes returns at multiple horizons:
  - Daily: `[1, 2, 3, 5, 10, 20]` days
  - Intraday: `[6, 12, 39, 78, 390]` bars (~30 min, 1 h, half-day, 1 day, 5 days)
- Generates a control group via `build_control_group()` for baseline comparison
- Groups results by `source_detail` if multiple event subtypes are present
- Per-horizon output: `HorizonResult` with `mean_return`, `median_return`, `std_return`, `win_rate`, `t_stat`, `p_value`, `significant`

### Cross-Correlation Engine (`engines/cross_correlation.py`)

Lead-lag analysis: does the signal predict future returns?

- For each lag (`-max_lag` to `+max_lag`), correlates z-scored signal with future returns
- Aggregates across tickers: mean correlation + 95% CI
- **Peak lag detection**: identifies the lag with highest positive correlation → "signal leads by X bars"
- **Panel regression**: OLS model `return_{t+1} = b0 + b1·signal_t + b2·return_t`
  - Uses running sums (`X'X`, `X'y`) — never materializes a residual vector
  - Returns `b1`, `b1_stderr`, `b1_tstat`, `b1_pvalue`, `R²`
- Signal columns resolved from panel data: `mention_ratio`, `mention_delta`, `rank_change` (from popularity) or `_volume_zscore` (computed on-the-fly from price)
- Output: `CrossCorrelationResult` with `signal_name`, `n_tickers`, `ccf: list[CCFPoint]`, `peak_lag`, `regression: RegressionResult`

### Regime Analysis Engine (`engines/regime_analysis.py`)

Partition events by market regime and analyze effectiveness within each.

- Regime classification via `backtest/regime.py:classify_regimes()` using SPY daily data → `bull`, `bear`, `correction`, `recovery`, `unknown`
- Reuses `_compute_horizons()` from `forward_returns.py` for within-regime return stats
- **Breadth analysis**: correlates daily event count with SPY N-day forward return
  - Contrarian signature: high breadth → negative SPY returns (retail crowd top)
  - Momentum signature: high breadth → positive SPY returns
- Output: `RegimeAnalysisResult` with `regime_slices`, `breadth_corr`, `breadth_pvalue`

---

## Events DataFrame Schema

All detectors return a DataFrame with this standard schema (plus optional source-specific columns):

| Column | Type | Description |
|---|---|---|
| `ticker` | `str` | Stock symbol |
| `datetime` | `Timestamp` | Event timestamp |
| `signal_value` | `float` | Signal strength (mention_ratio, z-score, rank_change, etc.) |
| `source` | `str` | `'sentiment'`, `'volume'`, `'composite'`, or `'control'` |
| `source_detail` | `str` | Human-readable filter description, e.g. `'mention_ratio>=3.0'` |

---

## Utilities

**`formatting.py`**: `fmt_pct()`, `fmt_float()`, `fmt_pvalue()`, `significant_marker()`, `print_table()`, `print_kv()` — terminal output helpers for CLI results tables.

**`_memlog.py`**: `log_memory(stage)` — logs peak RSS in MB via `resource.RUSAGE_SELF`; sprinkled around the pipeline for memory profiling.

---

## Tests (`tests/eda/`)

| File | Covers |
|---|---|
| `test_events_base.py` | `empty_events`, `validate_events`, `deduplicate_events`, `build_control_group` |
| `test_events_sentiment.py` | Mention spike and rank jump detection, daily dedup, `min_mentions` filter |
| `test_events_volume.py` | Z-score thresholds, `min_volume` filter, multi-ticker, 5m timeframe, edge cases |
| `test_streaming.py` | `fetch_bar_counts`, `stream_tickers`, `fetch_distinct_tickers`, date filtering |
| `test_engines_forward_returns.py` | Horizon computation, control group, return sign, empty/missing ticker handling |
| `test_engines_cross_correlation.py` | CCF length, peak lag detection, regression results, missing data handling |
| `test_engines_regime_analysis.py` | Regime slices, breadth analysis fields, unknown-regime fallback |

---

## Usage Examples

```bash
# Forward return event study — sentiment spikes, daily bars
python -m rocketstocks.eda forward-returns --source sentiment --timeframe daily

# Lead-lag — does volume z-score predict next-day returns?
python -m rocketstocks.eda lead-lag --source volume --timeframe daily

# Regime-conditional — does sentiment work better in bull vs bear markets?
python -m rocketstocks.eda regime --source sentiment --timeframe daily

# Composite detector — co-occurring sentiment + volume spikes only
python -m rocketstocks.eda forward-returns --source sentiment+volume --timeframe 5m --start-date 2024-01-01
```

---

## Design Notes

**Memory efficiency**: `stream_tickers()` ensures peak RSS is bounded to roughly one ticker's history regardless of universe size. Running-sum accumulators in `CrossCorrelationEngine` avoid materializing per-row regression arrays.

**Separation of concerns**: All engines consume the standard events DataFrame and are agnostic to which detector produced it — swap `SentimentDetector` for `VolumeDetector` without changing any engine code.

**Statistical choices**: one-sample t-test (null: mean forward return = 0) for event studies; Pearson CCF with 95% CI for lead-lag; OLS from running sums for regression. Deduplication window (default 3 days) prevents event clustering from inflating sample sizes.

**No production dependency**: `eda/` imports only `data/` (via `StockData`) and `backtest/regime.py`. Safe to run against a live DB without touching the bot.
