# Stock Classification

Every ticker tracked by RocketStocks is assigned a `StockClass` once per trading day. This classification drives **alert strategy** — which detection logic applies. A blue chip uses Bollinger Band analysis; all others use a dynamic z-score threshold that scales continuously with volatility.

---

## The Four Classes

| Class | Description | Alert Strategy |
|---|---|---|
| `meme` | Highly popular on social media AND historically volatile | Dynamic z-score threshold (see below) |
| `volatile` | Small cap with high daily volatility | Dynamic z-score threshold |
| `blue_chip` | Large cap with low daily volatility | Bollinger Band strategy + confluence fallback |
| `standard` | Everything else | Dynamic z-score threshold |

Blue chips use an entirely different trigger strategy because their normal moves rarely cross a z-score threshold — instead the system looks for Bollinger Band breaches with volume confirmation.

For meme, volatile, and standard classes, the numerical z-score threshold is not fixed per-class — it scales continuously with volatility via `dynamic_zscore_threshold()`.

---

## Dynamic Z-Score Threshold

Instead of discrete per-class thresholds that produce cliff effects (a stock at 3.9% volatility and one at 4.1% got different fixed thresholds), the system uses a continuous function:

```python
def dynamic_zscore_threshold(volatility_20d: float, max_volatility: float = 8.0) -> float:
    normalized = min(volatility_20d / max_volatility, 1.0)
    return 3.0 - (normalized * 1.5)
    # Range: 3.0 (near-zero vol) down to 1.5 (≥8% vol)
    # Fallback: 2.5 for unknown/negative volatility
```

**Illustrative thresholds:**

| volatility_20d | threshold |
|---|---|
| 0.5% | ~2.91 |
| 1.5% | ~2.72 |
| 2.5% | ~2.53 |
| 4.0% | ~2.25 |
| 6.0% | ~1.88 |
| 8.0%+ | 1.50 (floor) |
| Unknown/None | 2.50 (fallback) |

A highly volatile stock gets a lower threshold (easier to trigger because its moves are larger by nature), and a stable stock gets a higher one. Two stocks at similar volatility get similar thresholds — no sudden jumps.

The four StockClass categories still drive strategy dispatch (blue_chip → BB strategy, others → dynamic z-score), but the numerical threshold within the z-score strategy is now volatility-driven rather than class-driven.

---

## Classification Rules (Priority Order)

Rules are evaluated top-to-bottom. The first match wins.

### 1. Watchlist Override (highest priority)

If a watchlist is named with the prefix `class:`, its members are forced into that class regardless of any computed metrics. Valid watchlist names:

- `class:meme`
- `class:volatile`
- `class:blue_chip`
- `class:standard`

Example: adding TSLA to a watchlist named `class:volatile` forces TSLA to use the volatile classification (and its corresponding dynamic threshold) even if its market cap would normally qualify it as standard or blue chip.

### 2. Meme: High Popularity + High Volatility

```
popularity_rank <= 50  AND  volatility_20d > 4.0%
```

The ticker must currently appear in the top 50 most-mentioned stocks on WallStreetBets (as fetched during the classification job) **and** have a 20-day daily return standard deviation above 4%.

Example: **GME** — regularly appears in the top 50 WSB mentions, and its daily volatility is well above 4% on most trading days.

### 3. Volatile: Small Cap + High Volatility

```
market_cap < $2,000,000,000  AND  volatility_20d > 4.0%
```

Market cap below $2B with a daily volatility standard deviation above 4%.

Example: A small biotech with a $400M market cap that swings 6–8% per day on clinical trial news.

### 4. Blue Chip: Large Cap + Low Volatility

```
market_cap >= $10,000,000,000  AND  volatility_20d < 1.5%
```

Market cap at or above $10B with a daily volatility standard deviation below 1.5%.

Example: **AAPL** — market cap in the trillions, typical daily move under 1.5%.

### 5. Standard (Default)

Everything that does not match rules 2–4. This includes:

- Mid-caps with moderate volatility
- Any stock missing market cap or volatility data

Example: **MSFT** — large cap but if its 20-day volatility exceeds 1.5%, it falls through to standard.

---

## Volatility Formula

Volatility is the standard deviation of daily percentage returns over the most recent 20 trading days:

```
daily_return[i] = (close[i] - close[i-1]) / close[i-1] * 100
volatility_20d  = std(daily_return[-20:])   # pandas default ddof=1
```

The result is in percent. A value of 3.5 means the stock typically moves 3.5% per day (one standard deviation).

---

## When Classification Is Updated

The `classify_tickers` job runs at **05:30 UTC, Tuesday through Saturday** (covering Monday–Friday trading). It processes every ticker in the `tickers` table:

1. Fetches Schwab quotes in batches of 25 for market cap data
2. Reads the current top-100 popularity ranks from ApeWisdom for meme detection
3. Reads watchlist classification overrides
4. Computes 20-day volatility from the `daily_price_history` table
5. Calls `classify_ticker()` and upserts the result into the `ticker_stats` table

Bollinger Band values (upper, middle, lower) are also computed and stored for blue-chip tickers, since the blue-chip alert strategy needs them at runtime.

---

## Examples

| Ticker | Market Cap | Volatility 20d | Pop Rank | Class | Approx Threshold |
|---|---|---|---|---|---|
| GME | ~$7B | ~8% | ~30 | `meme` (rank ≤ 50, vol > 4%) | 1.5 |
| Small biotech (e.g., SAVA) | ~$300M | ~12% | Not top 50 | `volatile` (cap < $2B, vol > 4%) | 1.5 (floored) |
| AAPL | ~$3T | ~0.9% | Not top 50 | `blue_chip` (cap ≥ $10B, vol < 1.5%) | BB strategy |
| MSFT | ~$3T | ~1.8% | Not top 50 | `standard` (cap ≥ $10B but vol ≥ 1.5%) | ~2.66 |
