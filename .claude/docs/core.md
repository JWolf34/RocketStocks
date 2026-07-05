# Core Layer (`legacy/src/rocketstocks/core/`)

Business logic. **Zero discord imports. Zero data-layer imports** — the one exception is `scheduler/jobs.py`, which imports `StockData` to wire up APScheduler jobs. All analysis functions are pure (no side effects, no DB).

---

## core/analysis/ — Pure Analysis Functions

Accept DataFrames/scalars, return dataclasses/scalars. Safe to call in tests with no mocking.

| File | Purpose |
|------|---------|
| `signals.py:9` | `signals` class — vectorized indicators (RSI, MACD, SMA, ADX, OBV, A/D, ROC, z-score, Bollinger Bands, technical_confluence) via pandas_ta |
| `indicators.py` | Point-in-time functions — `intraday_zscore()`, `return_percentile()`, RVOL, popularity velocity |
| `alert_strategy.py:30-42` | `AlertTriggerResult` dataclass; `evaluate_price_alert()` dispatches by StockClass; `evaluate_confirmation()` for price-since-flag z-score |
| `composite_score.py` | `compute_composite_score()` — weighted vol(50%) + price(35%) + confluence(15%) with dual-gate |
| `classification.py:18-22` | `StockClass` enum (VOLATILE, MEME, BLUE_CHIP, STANDARD); `classify_ticker()`; `compute_volatility()`; `compute_return_stats()` |
| `momentum.py` | Velocity/acceleration z-scores; `should_update_alert()`; `build_momentum_snapshot()` |
| `popularity_signals.py` | `evaluate_popularity_surge()` — MENTION_SURGE, RANK_JUMP, NEW_ENTRANT, VELOCITY_SPIKE detection |
| `volume_divergence.py` | `evaluate_volume_accumulation()` — institutional accumulation (high vol / no price move). Thresholds: `vol_threshold=2.5` (z-score), `price_ceiling=1.0` (abs z-score), `min_divergence=1.5` (`vol_z - abs(price_z)`). All three must pass. |
| `signal_confirmation.py` | `should_confirm_signal()` — multi-bar sustained / accelerating confirmation |
| `direction_model.py` | `DirectionModel` — logistic regression for price direction prediction; `build_features_for_backtest()` |
| `volume_pressure.py` | Volume pressure analysis helpers |
| `options.py` | Options chain analysis (IV, strikes, spreads) |
| `options_flow.py` | `evaluate_options_flow()` — unusual options activity detection |
| `alert_performance.py` | `compute_surge_confidence()`, `compute_signal_confidence()`, `compute_price_outcome()` for `/alert stats` |
| `paper_trading.py` | Portfolio value/gain calculations, weekly award logic |

---

## core/content/ — Content Classes

All classes accept a typed dataclass (from `models.py`) and return `EmbedSpec`. No discord imports anywhere in this package.

### models.py

Single file at `core/content/models.py` holding every typed data model. Key groups:

- **Discord-agnostic colors/EmbedSpec**: `EmbedField:36`, `EmbedSpec:43`
- **Shared base**: `TickerData:59` (ticker, ticker_info, quote)
- **Report models**: `StockReportData:71`, `FullStockReportData:82`, `ComparisonReportData:98`, `NewsReportData:111`, `EarningsSpotlightData:116`, `PopularityReportData:125`, `PoliticianReportData:131`, `AlertSummaryData:138`
- **Screener models**: `GainerScreenerData:149`, `VolumeScreenerData:155`, `PopularityScreenerData:160`, `WeeklyEarningsData:165`
- **Alert models**: `PopularitySurgeData:174`, `MomentumConfirmationData:182`, `VolumeAccumulationAlertData:194`, `BreakoutAlertData:204`, `EarningsMoverData:232`, `WatchlistMoverData:243`
- **Stats/history models**: `AlertStatsData:250`, `AlertHistoryData:260`
- **Data cog models**: `QuoteData:272`, `PriceSnapshotData:307`, `FundamentalsSnapshotData:329`, `OptionsSummaryData:334`, `EarningsForecastData:418`, `OnDemandScreenerData:425`, `TechnicalReportData:432`, `OptionsReportData:441`
- **Paper trading models**: `TradeQuoteData:453`, `PortfolioViewData:489`, `LeaderboardViewData:519`, `WeeklyRoundupData:559`

### alerts/ (6 subclasses)

Base class: `alerts/base.py:12` — `Alert`.

- `base.py:23-24` — `__init__` stores fields in `self.alert_data` dict
- `base.py:26-31` — `build() -> EmbedSpec` (subclasses override)
- `base.py:59-73` — `override_and_edit(prev_data) -> bool` — z-score based re-post decision
- `base.py:75-98` — `record_momentum(prev_data)` — appends velocity/acceleration snapshot

Subclasses: `watchlist_alert.py`, `earnings_alert.py`, `popularity_surge_alert.py`, `momentum_confirmation_alert.py`, `volume_accumulation_alert.py`, `breakout_alert.py`. Each sets `alert_type` and `role_key` class attributes.

### screeners/ (4 subclasses)

Base class: `screeners/base.py:11` — `Screener`.

- `base.py:19-23` — `__init__(screener_type, data: DataFrame, column_map)` filters/renames columns
- `base.py:34-35` — `build() -> EmbedSpec` (subclasses override)

Subclasses: `gainer_screener.py`, `volume_screener.py`, `popularity_screener.py`, `earnings_screener.py`.

### reports/ (21 classes)

No shared base class. Each is a standalone class with `build() -> EmbedSpec`. Notable reports: `stock_report.py` (StockReport + FullStockReport), `technical_report.py`, `comparison_report.py`, `options_report.py`, `earnings_report.py`, `earnings_result_report.py`, `alert_history_report.py`, `alert_stats_report.py`, `alert_summary.py` + paper trading reports (`portfolio_view.py`, `trade_history.py`, `leaderboard.py`, `weekly_roundup.py`, `trade_announcement.py`, `trade_confirmation.py`, `trade_quote.py`, `performance_view.py`).

### data/ (20 card classes)

Simple `build() -> EmbedSpec` classes for `/data` slash commands. No base class. One file per card type: `quote_card.py`, `price_snapshot.py`, `fundamentals_snapshot.py`, `options_summary.py`, `popularity_snapshot.py`, `earnings_card.py`, `sec_filing_card.py`, `stats_card.py`, `analyst_card.py`, `ownership_card.py`, `insider_card.py`, `short_interest_card.py`, `news_card.py`, `forecast_card.py`, `on_demand_screener.py`, `movers_card.py`, `tickers_summary.py`, `upcoming_earnings_card.py`, `financial_highlights.py`.

### Helpers
- `sections_card.py` — reusable embed section builder used by multiple report classes
- `formatting.py` — content-layer formatting helpers (price/change display, field builders)

---

## core/config/

- `settings.py:5-34` — pydantic-settings `Settings` class. Reads `.env`. Singleton `settings` at `:34`. Required fields: discord_token, schwab credentials, tiingo/news API keys, postgres connection. Optional: nasdaq_api_key, eodhd_api_token, tz.
- `paths.py:19-21` — `datapaths` class with `attachments_path = "discord/attachments"`.

---

## core/notifications/

- `emitter.py:14` — `EventEmitter` (thread-safe Queue-based bridge between scheduler/cog threads and the notifications cog). `emit()`, `drain()`, `job_wrapper()`, `task_wrapper()`.
- `event.py` — `NotificationEvent` dataclass (level, source, job_name, message, traceback, elapsed_seconds).
- `config.py` — `NotificationLevel` enum (SUCCESS, WARNING, FAILURE), `NotificationFilter` enum (ALL, FAILURES_ONLY, OFF), `NotificationConfig` dataclass.

See `architectural_patterns.md` for full EventEmitter usage pattern.

---

## core/scheduler/

- `jobs.py:16-243` — `register_jobs(aio_sched, stock_data, emitter)`. The one `core/` module that imports from `data/`. All jobs wrapped with `emitter.job_wrapper()` for automatic success/failure notifications.
- Scheduled jobs: update_tickers (mon-sun 05:00), insert_tickers (sun 06:00), upcoming_earnings (fri 06:00), remove_past_earnings (tue-sat 06:00), historical_earnings (tue-sat 07:00), daily_price_history (tue-sat 03:00), 5m_price_history (tue-sat 04:00), update_politicians (sun 07:00), check_schwab_token (every 6h), classify_tickers (tue-sat 05:30), collect_iv_history (tue-sat 21:30), enrich_tickers (every 1h), import_delisted (sun 09:00), load_delisted_prices (daily 10:00).

---

## core/charting/

- `chart.py` — `Chart` class using mplfinance for candlestick/line charts. Returns chart as file bytes for Discord attachment.
- `helpers.py` — chart helper functions (color maps, date ranges, indicator overlays).

---

## core/utils/

- `market.py` — `MarketUtils` class. NYSE calendar, market hours checks, schedule queries.
- `dates.py` — `timezone()`, `configure_tz()`, `format_date_mdy()`, `seconds_until_minute_interval()`, `format_duration_since()`.
- `formatting.py` — `change_emoji()`, `format_signed_pct()`, `finviz_url()`, `get_company_name()`.
- `logging_config.py` — `setup_logging()` called from `__main__.py:12`. Configures file + console handlers.

---

## core/auth/

- `token_manager.py` — `TokenStatus` enum (HEALTHY, EXPIRING_SOON, EXPIRED, INVALID, MISSING), `TokenInfo` dataclass, `get_token_info(token_path)`. Used by scheduler job and `/schwab-status` command.
