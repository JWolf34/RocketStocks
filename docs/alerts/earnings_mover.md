# Earnings Mover Alert

An Earnings Mover alert fires when a stock that is scheduled to report earnings today moves by a statistically unusual amount. Earnings events are high-volatility moments — a company beats expectations and gaps up 15%, or misses and drops 20%. This pipeline isolates those tickers so their alerts include earnings context.

---

## Which Tickers Enter This Pipeline

Only tickers where today's date matches a record in the `upcoming_earnings` table. The Earnings Pipeline fetches these via `get_earnings_on_date(date=today)`. Earnings tickers are **not** excluded from the Watchlist or Volume Accumulation pipelines — a ticker reporting earnings that is also on a watchlist can generate both an Earnings Mover alert and a Watchlist Mover alert. Similarly, if unusual volume divergence is detected, a Volume Accumulation alert can fire alongside the Earnings Mover.

---

## Trigger Condition

`evaluate_price_alert()` must return `should_alert = True`. The threshold scales dynamically with the ticker's 20-day volatility via `dynamic_zscore_threshold()`.

**For standard, meme, and volatile tickers:**
```
abs(z-score) >= dynamic_zscore_threshold(volatility_20d)

  z-score    = (pct_change - mean_return_20d) / std_return_20d
  threshold  = 3.0 - (min(volatility_20d / 8.0, 1.0) × 1.5)
             = range 1.5 (at ≥8% daily volatility) to 3.0 (near-zero volatility)
             = fallback 2.5 for unknown/negative volatility
```

There is no composite score gate — if the z-score threshold is met, the alert fires immediately.

**For blue-chip tickers:** The Bollinger Band + volume confirmation strategy applies:

*Primary trigger:* `volume_z_score >= 2.0` AND:
- Below lower BB with RSI oversold or OBV trending up → `mean_reversion`
- Above upper BB with ADX strong or MACD bullish → `trend_breakout`

*Fallback trigger:* `confluence_count >= 3 AND volume_z_score >= 2.0 AND abs(z_score) >= threshold` → `unusual_move`

---

## What the Alert Shows

In addition to standard price/volume stats, the embed includes:

- **EPS Forecast** — the analyst consensus EPS estimate for this quarter (from `upcoming_earnings`)
- **Earnings Timing** — Pre-market or After Hours (parsed from the `time` field of the earnings record)
- **Recent Earnings** — a card showing the last few historical earnings results (EPS actual vs forecast, surprise percentage)
- Z-score, move percentile, classification

---

## Update Logic

Same momentum acceleration logic as other alerts. See [Overview — Deduplication](overview.md#alert-deduplication-and-update-in-place).

---

## Examples

**NVDA reporting after-hours, up 9% pre-announcement**

NVDA 20-day stats: mean = 0.5%, std = 2.8%, volatility_20d = 2.8%.

```
threshold = 3.0 - (min(2.8 / 8.0, 1.0) × 1.5) = 3.0 - 0.525 = 2.475

z-score = (9.0 - 0.5) / 2.8 = 3.04  ≥ 2.475 → alert fires
```

Alert shows: price, +9%, EPS Forecast (e.g., $0.89), Time: After Hours, recent earnings card.

**Small biotech reports pre-market, gaps up 25%**

Biotech 20-day stats: mean = 0.5%, std = 8.0%, volatility_20d = 8.0%.

```
threshold = 3.0 - (min(8.0 / 8.0, 1.0) × 1.5) = 1.5

z-score = (25 - 0.5) / 8.0 = 3.06  ≥ 1.5 → alert fires
```

Alert shows: price, +25%, EPS Forecast, Time: Pre-market, recent earnings card.

**Stable retailer reports, moves +1.2%**

Retailer 20-day stats: mean = 0.1%, std = 1.0%, volatility_20d = 1.0%.

```
threshold = 3.0 - (min(1.0 / 8.0, 1.0) × 1.5) = 3.0 - 0.1875 = 2.81

z-score = (1.2 - 0.1) / 1.0 = 1.1  < 2.81 → no alert
```

No alert fires even though the stock reported earnings. The move was not statistically unusual relative to its own history.

---

## Would / Would Not Trigger

| Scenario | Triggers? | Reason |
|---|---|---|
| NVDA (std=2.8%, threshold≈2.48): earnings day, up 9%, z-score 3.0 | Yes | z-score ≥ dynamic threshold |
| Small biotech (std=8.0%, threshold=1.5): earnings day, up 25%, z-score 3.1 | Yes | z-score ≥ 1.5 floor |
| Retailer (std=1.0%, threshold≈2.81): earnings day, up 1.2%, z-score 1.1 | No | z-score < dynamic threshold |
| Ticker not reporting earnings today | No | Only in pipeline when earnings date = today |
| Ticker on watchlist AND reporting earnings today | Both can fire | Watchlist and Earnings pipelines run in parallel; both can produce alerts |
