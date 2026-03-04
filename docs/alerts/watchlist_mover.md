# Watchlist Mover Alert

A Watchlist Mover alert fires when a stock on a user watchlist moves by a statistically unusual amount. Unlike the Market Alert, there is no composite score gate — if the z-score threshold is met, the alert fires immediately.

---

## What It Monitors

Every ticker on any watchlist in the `watchlists` table is checked every 5 minutes by the Watchlist Pipeline. Watchlist tickers are processed **regardless** of whether they also appear in the screener list, and they are explicitly excluded from the Market Pipeline to avoid duplicate alerts.

---

## Trigger Condition

`evaluate_price_alert()` must return `should_alert = True` using the ticker's classification-based z-score threshold. There is no second stage (no composite score gate).

**For standard, meme, and volatile tickers:**
```
abs(z-score) >= threshold

  z-score   = (pct_change - mean_return_20d) / std_return_20d
  threshold = 2.0σ  for meme and volatile
              2.5σ  for standard
```

**For blue-chip tickers:** The blue-chip Bollinger Band + volume strategy applies (same as Market Alert). See [Blue Chip Strategy in Market Alert](market_alert.md#blue-chip-strategy).

---

## Watchlist Names and Classification Overrides

Watchlists named with the `class:` prefix serve a dual purpose: they define a user's watchlist AND they override the ticker's classification. For example, adding GME to a watchlist named `class:volatile` both:

1. Puts GME in the Watchlist Pipeline (so it gets Watchlist Mover alerts)
2. Forces GME's classification to `volatile` during the daily classification job

See [Classification — Watchlist Override](classification.md#1-watchlist-override-highest-priority) for details.

---

## What the Alert Shows

- Price and intraday % change
- Watchlist name the ticker belongs to
- Z-score
- Move percentile (rank among last 60 days of daily returns)
- Classification
- For blue chips: BB position, confluence count, signal type

---

## Update Logic

The same momentum acceleration logic applies as other alerts. See [Overview — Deduplication](overview.md#alert-deduplication-and-update-in-place). The alert edits in place unless acceleration is statistically unusual (accel z-score ≥ 2.0), at which point a new post is made.

---

## Examples

**TSLA on a watchlist (standard class)**

TSLA 20-day stats: mean daily return = 0.8%, std = 1.8%.

| Scenario | pct_change | z-score | Threshold | Alert? |
|---|---|---|---|---|
| TSLA up 5.3% | +5.3% | (5.3 - 0.8) / 1.8 = 2.50 | 2.5σ | Yes (exactly at threshold) |
| TSLA up 3.5% | +3.5% | (3.5 - 0.8) / 1.8 = 1.50 | 2.5σ | No |
| TSLA down 6.2% | -6.2% | abs((-6.2 - 0.8) / 1.8) = 3.89 | 2.5σ | Yes |

**GME on a watchlist (meme class)**

GME 20-day stats: mean daily return = 1.2%, std = 5.1%.

| Scenario | pct_change | z-score | Threshold | Alert? |
|---|---|---|---|---|
| GME up 12% | +12% | (12 - 1.2) / 5.1 = 2.12 | 2.0σ | Yes |
| GME up 10% | +10% | (10 - 1.2) / 5.1 = 1.73 | 2.0σ | No |
| GME up 11.3% | +11.3% | (11.3 - 1.2) / 5.1 = 1.98 | 2.0σ | No (just below 2.0) |

**Small biotech on a watchlist (volatile class)**

Biotech 20-day stats: mean = 0.5%, std = 8.0%.

| Scenario | pct_change | z-score | Threshold | Alert? |
|---|---|---|---|---|
| Up 18% | +18% | (18 - 0.5) / 8.0 = 2.19 | 2.0σ | Yes |
| Up 10% | +10% | (10 - 0.5) / 8.0 = 1.19 | 2.0σ | No |

---

## Would / Would Not Trigger

| Scenario | Triggers? | Reason |
|---|---|---|
| TSLA (standard): z-score 2.5 | Yes | Meets 2.5σ threshold exactly |
| TSLA (standard): z-score 2.4 | No | Below 2.5σ threshold |
| GME (meme): z-score 2.1 | Yes | Meets 2.0σ meme threshold |
| AAPL (blue chip) breaches upper BB with vol z-score 2.3 and ADX strong | Yes | Primary blue chip trigger |
| AAPL (blue chip) up 1.5% but within Bollinger Bands, low volume | No | Blue chip primary and fallback both require volume ≥ 2.0σ |
| Ticker not on any watchlist | No | Only watchlist tickers enter this pipeline |
