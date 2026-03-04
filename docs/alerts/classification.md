# Stock Classification

Every ticker tracked by RocketStocks is assigned a `StockClass` once per trading day. This classification drives alert sensitivity: a meme stock that swings 10% on a slow day is unremarkable, but a blue-chip moving 2% with elevated volume is a genuine event. Using a single threshold for all stocks would produce either too many false positives on volatile names or too few alerts on stable ones.

---

## The Four Classes

| Class | Description | Alert Approach |
|---|---|---|
| `meme` | Highly popular on social media AND historically volatile | Z-score threshold 2.0σ |
| `volatile` | Small cap with high daily volatility | Z-score threshold 2.0σ |
| `blue_chip` | Large cap with low daily volatility | Bollinger Band strategy + confluence fallback |
| `standard` | Everything else | Z-score threshold 2.5σ |

The lower threshold for meme and volatile stocks acknowledges that their moves are more frequent; the system still wants to catch the statistically unusual ones. The higher threshold for standard stocks reduces noise on mid-caps. Blue chips use an entirely different trigger strategy because their normal moves rarely cross a z-score threshold — instead the system looks for Bollinger Band breaches with volume confirmation.

---

## Classification Rules (Priority Order)

Rules are evaluated top-to-bottom. The first match wins.

### 1. Watchlist Override (highest priority)

If a watchlist is named with the prefix `class:`, its members are forced into that class regardless of any computed metrics. Valid watchlist names:

- `class:meme`
- `class:volatile`
- `class:blue_chip`
- `class:standard`

Example: adding TSLA to a watchlist named `class:volatile` forces TSLA to use the volatile thresholds even if its market cap would normally qualify it as standard or blue chip. This is useful when a stock's behavior has changed but the daily stats haven't caught up yet.

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

- Mid-caps with moderate volatility (e.g., **MSFT** if volatility is >= 1.5% but the stock is still large)
- Any stock missing market cap or volatility data

Example: **MSFT** — large cap but if its 20-day volatility exceeds 1.5%, it falls through to standard and uses the 2.5σ threshold.

---

## Volatility Formula

Volatility is the standard deviation of daily percentage returns over the most recent 20 trading days:

```
daily_return[i] = (close[i] - close[i-1]) / close[i-1] * 100
volatility_20d  = std(daily_return[-20:])   # population-style, pandas default ddof=1
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

| Ticker | Market Cap | Volatility 20d | Pop Rank | Class |
|---|---|---|---|---|
| GME | ~$7B | ~8% | ~30 | `meme` (rank ≤ 50, vol > 4%) |
| Small biotech (e.g., SAVA) | ~$300M | ~12% | Not top 50 | `volatile` (cap < $2B, vol > 4%) |
| AAPL | ~$3T | ~0.9% | Not top 50 | `blue_chip` (cap ≥ $10B, vol < 1.5%) |
| MSFT | ~$3T | ~1.8% | Not top 50 | `standard` (cap ≥ $10B but vol ≥ 1.5%) |
