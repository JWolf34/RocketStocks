# Backtesting Guide: How to Use the RocketStocks Backtesting System

## Available Strategies

| Strategy | What It Tests | Timeframe Fit |
|----------|---------------|---------------|
| `volume_accumulation` | High volume + no price move yet = institutional accumulation | Both (daily for swing, 5m for intraday) |
| `popularity_surge` | Social media buzz spike = retail attention incoming | Daily (popularity data is ~30min granularity) |
| `leading_indicator_combo` | Both signals firing simultaneously = highest conviction | Daily primarily |
| `buy_hold` | Passive benchmark for comparison | Both |

All active strategies share configurable exit modes via the `LeadingIndicatorStrategy` base class:
- **breakout** (default): Exit when price z-score since entry hits `exit_zscore` (profit) or `-stop_zscore` (loss)
- **momentum**: Trailing exit — tracks peak z-score, exits when it fades by `trail_zscore` while profitable
- **bar_hold**: Simple hold for N bars, useful as a baseline

## Running Backtests

### Basic Usage

```bash
# Run a strategy across all non-delisted tickers
python -m rocketstocks.backtest run volume_accumulation --benchmark SPY

# Run with 5-minute bars for intraday analysis
python -m rocketstocks.backtest run volume_accumulation --timeframe 5m --benchmark SPY

# Restrict date range
python -m rocketstocks.backtest run volume_accumulation --start-date 2024-01-01 --end-date 2024-12-31

# Override strategy parameters
python -m rocketstocks.backtest run volume_accumulation --params '{"vol_threshold": 2.5, "hold_bars": 10}'

# List available strategies
python -m rocketstocks.backtest list
```

### Filtering Ticker Subsets

All filters are AND-combined (intersection). Use `--tickers` to bypass all filters with an explicit list.

```bash
# By sector
python -m rocketstocks.backtest run volume_accumulation --sector Technology --benchmark QQQ
python -m rocketstocks.backtest run volume_accumulation --sector "Health Care" --benchmark XLV
python -m rocketstocks.backtest run volume_accumulation --sector Financials --benchmark XLF

# By stock classification (from ticker_stats)
python -m rocketstocks.backtest run volume_accumulation --classification blue_chip --benchmark SPY
python -m rocketstocks.backtest run volume_accumulation --classification volatile --benchmark SPY
python -m rocketstocks.backtest run volume_accumulation --classification meme --benchmark SPY

# By watchlist
python -m rocketstocks.backtest run volume_accumulation --watchlist mag7 --benchmark QQQ
python -m rocketstocks.backtest run volume_accumulation --watchlist semiconductors --benchmark SMH

# By market cap tier
python -m rocketstocks.backtest run volume_accumulation --min-market-cap 10000000000 --benchmark SPY          # Large cap >$10B
python -m rocketstocks.backtest run volume_accumulation --min-market-cap 2000000000 --max-market-cap 10000000000 --benchmark MDY  # Mid cap
python -m rocketstocks.backtest run volume_accumulation --max-market-cap 2000000000 --benchmark IWM           # Small cap <$2B

# By exchange
python -m rocketstocks.backtest run volume_accumulation --exchange NYSE NASDAQ

# By volatility range
python -m rocketstocks.backtest run volume_accumulation --min-volatility 3.0 --max-volatility 10.0

# By popularity rank
python -m rocketstocks.backtest run volume_accumulation --min-popularity-rank 1 --max-popularity-rank 100

# Specific tickers (bypasses all other filters)
python -m rocketstocks.backtest run volume_accumulation --tickers AAPL MSFT NVDA GOOGL
```

### Viewing Results

```bash
# Aggregate stats (auto-grouped by all, sector, classification, exchange, watchlist)
python -m rocketstocks.backtest stats <run_id>

# Filter stats to a specific group
python -m rocketstocks.backtest stats <run_id> --group sector:Technology
python -m rocketstocks.backtest stats <run_id> --group class:volatile

# Per-ticker results sorted by different metrics
python -m rocketstocks.backtest results <run_id> --sort return_pct --limit 30
python -m rocketstocks.backtest results <run_id> --sort sharpe_ratio --limit 30
```

### Comparing Strategies

```bash
# Run two strategies on the same subset
python -m rocketstocks.backtest run volume_accumulation --sector Technology  # -> run_id 1
python -m rocketstocks.backtest run popularity_surge --sector Technology     # -> run_id 2

# Statistical comparison (two-sample t-test)
python -m rocketstocks.backtest compare 1 2
```

### Parameter Optimization

```bash
# Sweep parameters on a single ticker
python -m rocketstocks.backtest optimize volume_accumulation AAPL \
  --params '{"vol_threshold": [1.5, 2.0, 2.5, 3.0], "hold_bars": [3, 5, 10, 20]}'

# Maximize a specific metric
python -m rocketstocks.backtest optimize volume_accumulation NVDA \
  --params '{"exit_mode": ["breakout", "momentum", "bar_hold"], "exit_zscore": [1.5, 2.0, 2.5]}' \
  --maximize "Win Rate [%]"
```

---

## Systematic Evaluation Workflow

### Step 1: Establish Baselines

Before testing active strategies, know what passive buy-and-hold returns.

```bash
python -m rocketstocks.backtest run buy_hold --tickers SPY QQQ XLK XLV XLF XLE XLI --start-date 2024-01-01
```

### Step 2: Broad Sweep

Run each strategy across all tickers. The `stats` command auto-groups by sector, classification, exchange, and watchlist.

```bash
python -m rocketstocks.backtest run volume_accumulation --benchmark SPY --start-date 2024-01-01
python -m rocketstocks.backtest stats <run_id>
```

Look for group_key rows with p-value < 0.05 — those subsets show statistically significant results.

### Step 3: Drill Into Promising Subsets

Focus on subsets where the broad sweep showed positive results.

### Step 4: Compare Strategies Head-to-Head

For any subset where multiple strategies showed promise, use `compare`.

### Step 5: Optimize Parameters on Winners

Use `optimize` on individual tickers to find ideal parameter ranges.

### Step 6: Validate

Split date ranges manually: optimize on 2024 data, validate on 2025 data without changing parameters. If performance holds, you have confidence.

---

## Interpreting Backtest Statistics

### Per-Ticker Metrics

| Metric | What It Means | How to Act On It |
|--------|---------------|------------------|
| **Return [%]** | Total P&L over the test period | Compare to buy_hold_pct. If return < buy_hold, the strategy destroyed value on this ticker. |
| **Sharpe Ratio** | Risk-adjusted return (return / volatility of returns) | **Most important single metric.** >1.0 = decent, >1.5 = good, >2.0 = excellent. Prefer high Sharpe over high raw return. |
| **Win Rate [%]** | % of trades that were profitable | Context-dependent. A 40% win rate is fine if winners are 3x losers (high profit factor). Don't chase win rate alone. |
| **Profit Factor** | Gross profits / gross losses | >1.0 = profitable. >1.5 = solid. >2.0 = strong. <1.0 = losing money. |
| **Max Drawdown [%]** | Worst peak-to-trough equity decline | Measures worst-case pain. Rule of thumb: expect 2x historical max DD in live trading. |
| **# Trades** | Total trades completed | Statistical reliability. <10 = noise. 30+ = meaningful. 100+ = robust. |
| **Avg Trade [%]** | Mean return per trade | Your per-trade edge. Must exceed commission (0.4% round-trip at 0.2% each way). |
| **Exposure Time [%]** | % of time in a position | Capital efficiency. Low exposure = capital free for other strategies. |
| **Buy & Hold Return [%]** | Passive holding return | The baseline. Strategy must beat this to justify its complexity. |

### Group Statistics

| Metric | What It Tells You |
|--------|-------------------|
| **Mean Return** | Average profitability across the subset |
| **Median Return** | Less sensitive to outliers. If mean >> median, a few winners mask many losers. |
| **Std Return** | Consistency. Low = reliable. High = works on some tickers, fails on others. |
| **t-stat / p-value** | Statistical significance. p < 0.05 = unlikely to be random luck. p > 0.10 = can't distinguish from random. |
| **% Beating Buy & Hold** | Fraction of tickers where strategy outperformed. <50% = strategy hurts more than helps. |
| **Mean Excess Return** | Average alpha over buy-and-hold. Positive = strategy adds value. |

### Benchmark Comparison (--benchmark flag)

Computes mean excess return over the benchmark ETF, what % of tickers beat the benchmark, and whether the outperformance is statistically significant (t-test).

---

## Decision Framework: Keep, Optimize, or Kill

For each strategy + subset combination:

### KILL if:
- p-value > 0.10 (not distinguishable from random)
- Mean return < 0 (losing money on average)
- < 10 total trades (insufficient data to judge)
- Profit factor < 0.8 (consistently losing)

### OPTIMIZE if:
- p-value between 0.05 and 0.10 (trending toward significant)
- Mean return > 0 but % beating buy-and-hold < 50%
- Profit factor between 1.0 and 1.3 (barely profitable, might improve with tuning)
- Trade count is borderline (10-30 trades)

### KEEP / TRADE if:
- p-value < 0.05 (statistically significant)
- Mean return > 0 AND mean excess return > 0
- Profit factor > 1.5
- Sharpe > 1.0 across the subset
- 30+ trades providing statistical reliability
- % beating buy-and-hold > 50%

---

## Translating Stats Into Trading Decisions

Once you've identified a "KEEP" strategy + subset:

1. **Filter alerts by subset** — Only act on alerts for tickers in subsets where the strategy tested well (e.g., Technology sector large caps).

2. **Set profit targets from avg trade %** — If backtesting showed avg trade of 2.5% on this subset, target that range rather than expecting outsized moves.

3. **Set time stops from hold_bars** — If the optimal hold period was 5 days, exit after 5 days even without hitting your target. Edge diminishes after the optimal period.

4. **Set stop losses from max drawdown** — If backtested max drawdown per trade was -3%, use a -3% to -4% stop.

5. **Weight signal rarity** — If exposure time is 5% (strategy fires rarely), each alert is high-signal. If exposure is 80%, the strategy is nearly always "on" and individual alerts are less meaningful.

6. **Size positions by Sharpe** — Higher Sharpe ratio across the subset = more confidence = can allocate larger positions. Lower Sharpe = smaller positions.

---

## Caveats and Biases to Watch For

1. **In-sample bias** — All results are tested on historical data. Manually split date ranges (train on 2024, test on 2025) to validate.

2. **Survivorship bias** — By default, delisted stocks are excluded. Results only reflect stocks that survived to today, inflating historical returns.

3. **Transaction costs** — Flat 0.2% commission doesn't capture slippage, spread, or market impact. Stress-test with higher commission (e.g., `--commission 0.005`).

4. **Regime blindness** — Aggregate stats blend bull and bear markets. A strategy that appears profitable in aggregate may only work in one regime.

5. **Low trade counts** — Never draw conclusions from < 10 trades. Require 30+ for meaningful stats, 100+ for robust conclusions.

6. **Outlier dependence** — If mean >> median, the strategy depends on a few big winners. This is fragile and may not repeat.


# Guide for tetsing directional prediction

  # Baseline                                                                                                                                                                                                                                                                           
  backtest run volume_accumulation --start 2024-01-01                                                                                                                                                                                                                                  
   
  # With direction filter                                                                                                                                                                                                                                                              
  backtest run volume_accumulation --start 2024-01-01 \ 
    --params use_direction_filter=true direction_threshold=0.60                                                                                                                                                                                                                        
                                                        
  # Optimize the threshold                                                                                                                                                                                                                                                             
  backtest optimize volume_accumulation \
    --params use_direction_filter=true direction_threshold=0.55,0.60,0.65,0.70                                                                                                                                                                                                         
                                                                                                                                                                                                                                                                                       
  # Compare strategies with correlation analysis
  backtest correlation <run_id_without_filter> <run_id_with_filter>           