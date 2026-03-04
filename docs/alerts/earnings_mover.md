# Earnings Mover Alert

An Earnings Mover alert fires when a stock that is scheduled to report earnings today moves by a statistically unusual amount. Earnings events are high-volatility moments — a company beats expectations and gaps up 15%, or misses and drops 20%. This pipeline isolates those tickers so their alerts include earnings context.

---

## Which Tickers Enter This Pipeline

Only tickers where today's date matches a record in the `upcoming_earnings` table. The Earnings Pipeline fetches these via `get_earnings_on_date(date=today)`. Earnings tickers are excluded from the Market Pipeline, so they cannot generate both a Market Alert and an Earnings Mover Alert on the same day.

Earnings timing (pre-market or after-hours) is pulled from the `upcoming_earnings` record and shown in the alert.

---

## Trigger Condition

`evaluate_price_alert()` must return `should_alert = True` using the ticker's classification-based z-score threshold. There is no composite score gate (same approach as Watchlist Mover).

**For standard, meme, and volatile tickers:**
```
abs(z-score) >= threshold

  z-score   = (pct_change - mean_return_20d) / std_return_20d
  threshold = 2.0σ  for meme and volatile
              2.5σ  for standard
```

**For blue-chip tickers:** The Bollinger Band + volume confirmation strategy applies. See [Blue Chip Strategy in Market Alert](market_alert.md#blue-chip-strategy).

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

NVDA (blue chip or standard depending on current volatility): earnings date = today, time = after-hours. By 2:00 PM the stock is up 9% in anticipation.

NVDA 20-day stats (standard class): mean = 0.5%, std = 2.8%.

```
z-score = (9.0 - 0.5) / 2.8 = 3.04  ≥ 2.5 → alert fires
```

Alert shows: price, +9%, EPS Forecast (e.g., $0.89), Time: After Hours, recent earnings card.

**Small biotech reports pre-market, gaps up 25%**

Biotech (volatile class): earnings date = today, time = pre-market. Stock opens up 25%.

Biotech 20-day stats: mean = 0.5%, std = 8.0%.

```
z-score = (25 - 0.5) / 8.0 = 3.06  ≥ 2.0 → alert fires
```

Alert shows: price, +25%, EPS Forecast, Time: Pre-market, recent earnings card.

**Stable retailer reports, moves +1.2%**

Retailer (standard class): earnings today. Stock barely moves.

Retailer 20-day stats: mean = 0.1%, std = 1.0%.

```
z-score = (1.2 - 0.1) / 1.0 = 1.1  < 2.5 → no alert
```

No alert fires even though the stock reported earnings. The move was not statistically unusual relative to its own history.

---

## Would / Would Not Trigger

| Scenario | Triggers? | Reason |
|---|---|---|
| NVDA (standard): earnings day, up 9%, z-score 3.0 | Yes | z-score ≥ 2.5 |
| Small biotech (volatile): earnings day, up 25%, z-score 3.1 | Yes | z-score ≥ 2.0 |
| Retailer (standard): earnings day, up 1.2%, z-score 1.1 | No | z-score < 2.5 |
| Ticker not reporting earnings today | No | Only in pipeline when earnings date = today |
| Ticker on watchlist AND reporting earnings today | Earnings Mover only | Watchlist tickers are not excluded from Earnings Pipeline; the Market Pipeline excludes earnings tickers, but both Watchlist and Earnings pipelines run in parallel — both alerts can fire |
