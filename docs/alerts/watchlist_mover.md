# Watchlist Mover Alert

A Watchlist Mover alert fires when a stock on a user watchlist moves by a statistically unusual amount. Unlike the predictive alerts, this is a reactive alert — traders explicitly want to know when a ticker they're watching is moving right now. There is no leading indicator pipeline for watchlist tickers.

---

## What It Monitors

Every ticker on any watchlist in the `watchlists` table is checked every 5 minutes by the Watchlist Pipeline. Watchlist tickers are processed regardless of whether they also appear in the screener list. Watchlist tickers are not excluded from the Volume Accumulation or Earnings pipelines — a watchlist ticker can generate both a Watchlist Mover alert and a Volume Accumulation alert simultaneously if volume divergence is also detected.

---

## Trigger Condition

`evaluate_price_alert()` must return `should_alert = True`. The threshold is determined dynamically by the ticker's 20-day volatility via `dynamic_zscore_threshold()`.

**For standard, meme, and volatile tickers:**
```
abs(z-score) >= dynamic_zscore_threshold(volatility_20d)

  z-score    = (pct_change - mean_return_20d) / std_return_20d
  threshold  = 3.0 - (min(volatility_20d / 8.0, 1.0) × 1.5)
             = range 1.5 (at ≥8% daily volatility) to 3.0 (near-zero volatility)
             = fallback 2.5 for unknown/negative volatility
```

The threshold scales continuously with volatility — a highly volatile stock gets a lower threshold (easier to trigger), and a stable stock gets a higher one. This eliminates the cliff effects of discrete per-class thresholds where a stock at 3.9% volatility and one at 4.1% got different thresholds.

There is no second-stage composite score gate — if the z-score threshold is met, the alert fires immediately.

**For blue-chip tickers:**
The Bollinger Band + volume confirmation strategy applies:

*Primary trigger:* `volume_z_score >= 2.0` AND:
- Below lower BB with RSI oversold or OBV trending up → `mean_reversion`
- Above upper BB with ADX strong or MACD bullish → `trend_breakout`

*Fallback trigger:* `confluence_count >= 3 AND volume_z_score >= 2.0 AND abs(z_score) >= threshold` → `unusual_move`

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

The same momentum acceleration logic applies as other alerts. See [Overview — Deduplication](overview.md#alert-deduplication-and-update-in-place). The alert re-posts if acceleration is statistically unusual (accel z-score ≥ 2.0), otherwise no update.

---

## Examples

**TSLA on a watchlist**

TSLA 20-day stats: mean daily return = 0.8%, std = 1.8%, volatility_20d = 1.8%.

```
threshold = 3.0 - (min(1.8 / 8.0, 1.0) × 1.5) = 3.0 - 0.338 = 2.66
```

| Scenario | pct_change | z-score | Threshold | Alert? |
|---|---|---|---|---|
| TSLA up 6.0% | +6.0% | (6.0 - 0.8) / 1.8 = 2.89 | 2.66σ | Yes |
| TSLA up 5.5% | +5.5% | (5.5 - 0.8) / 1.8 = 2.61 | 2.66σ | No |
| TSLA down 6.5% | -6.5% | abs((-6.5 - 0.8) / 1.8) = 4.06 | 2.66σ | Yes |

**GME on a watchlist (high volatility)**

GME 20-day stats: mean = 1.2%, std = 5.1%, volatility_20d = 8.0%.

```
threshold = 3.0 - (min(8.0 / 8.0, 1.0) × 1.5) = 3.0 - 1.5 = 1.5
```

| Scenario | pct_change | z-score | Alert? |
|---|---|---|---|
| GME up 12% | +12% | (12 - 1.2) / 5.1 = 2.12 | Yes (≥ 1.5) |
| GME up 8.5% | +8.5% | (8.5 - 1.2) / 5.1 = 1.43 | No |

**Small biotech on a watchlist (volatile)**

Biotech 20-day stats: mean = 0.5%, std = 8.0%, volatility_20d = 8.0%.

```
threshold = 1.5 (same calculation, at max volatility floor)
```

| Scenario | pct_change | z-score | Alert? |
|---|---|---|---|
| Up 18% | +18% | (18 - 0.5) / 8.0 = 2.19 | Yes |
| Up 10% | +10% | (10 - 0.5) / 8.0 = 1.19 | No |

---

## Would / Would Not Trigger

| Scenario | Triggers? | Reason |
|---|---|---|
| TSLA (std=1.8%, threshold≈2.66): z-score 2.7 | Yes | Meets dynamic threshold |
| TSLA (std=1.8%, threshold≈2.66): z-score 2.6 | No | Below dynamic threshold |
| GME (std=5.1%, threshold=1.5): z-score 1.6 | Yes | Meets lower threshold for high-volatility stock |
| AAPL (blue chip) breaches upper BB with vol z-score 2.3 and ADX strong | Yes | Primary blue chip trigger |
| AAPL (blue chip) up 1.5% but within Bollinger Bands, low volume | No | Blue chip requires BB breach or high-confluence fallback |
| Ticker not on any watchlist | No | Only watchlist tickers enter this pipeline |
