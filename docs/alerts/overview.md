# Alert System Overview

RocketStocks monitors thousands of stocks and fires Discord alerts when statistically unusual activity is detected. The system runs two tiers of detection: a social-media sentiment tier that scans popularity data every 30 minutes, and a price/volume tier that processes four parallel pipelines every 5 minutes during market hours.

---

## Two-Tier Architecture

```
╔══════════════════════════════════════════════════════════════════════╗
║  TIER 1 — Popularity Surge Detection  (every 30 min, market hours)  ║
║                                                                      ║
║  ApeWisdom top 1,000 stocks                                          ║
║       │                                                              ║
║       ▼                                                              ║
║  evaluate_popularity_surge()                                         ║
║       │ MENTION_SURGE / RANK_JUMP / NEW_ENTRANT / VELOCITY_SPIKE     ║
║       ▼                                                              ║
║  PopularitySurgeAlert ──► Discord  ──► insert into popularity_surges ║
╚══════════════════════════════════════════════════════════════════════╝
                  │
                  │  active (unconfirmed) surges feed into Tier 2
                  ▼
╔══════════════════════════════════════════════════════════════════════╗
║  TIER 2 — Alert Processing  (every 5 min, market hours)             ║
║                                                                      ║
║  ┌────────────────────┐  ┌─────────────────┐                        ║
║  │ Confirmation       │  │ Market          │  (run in parallel)      ║
║  │ Pipeline           │  │ Pipeline        │                        ║
║  │                    │  │                 │                        ║
║  │ surge tickers only │  │ screener tickers│                        ║
║  │ evaluate_price +   │  │ (excl. surge,   │                        ║
║  │ mark confirmed     │  │  watchlist,      │                        ║
║  │                    │  │  earnings)       │                        ║
║  │ → MomentumConfirm  │  │ evaluate_price + │                        ║
║  │   Alert            │  │ composite_score  │                        ║
║  │                    │  │ → MarketAlert    │                        ║
║  └────────────────────┘  └─────────────────┘                        ║
║                                                                      ║
║  ┌────────────────────┐  ┌─────────────────┐                        ║
║  │ Watchlist          │  │ Earnings        │  (run in parallel)      ║
║  │ Pipeline           │  │ Pipeline        │                        ║
║  │                    │  │                 │                        ║
║  │ user watchlist     │  │ tickers with    │                        ║
║  │ tickers            │  │ earnings today  │                        ║
║  │ evaluate_price     │  │ evaluate_price  │                        ║
║  │ → WatchlistMover   │  │ → EarningsMover │                        ║
║  │   Alert            │  │   Alert         │                        ║
║  └────────────────────┘  └─────────────────┘                        ║
╚══════════════════════════════════════════════════════════════════════╝
```

---

## Schedule Timeline

All times are UTC.

| Time (UTC) | Job | Runs On |
|---|---|---|
| 03:00 | Update daily price history | Tue–Sat |
| 04:00 | Update 5-minute price history | Tue–Sat |
| 05:00 | Update tickers in DB | Daily |
| 05:30 | Classify tickers (StockClass assignment) | Tue–Sat |
| 06:00 | Post date separator in alerts channels | Daily |
| 06:00 | Remove past earnings | Tue–Sat |
| 06:00 | Insert new tickers into DB | Sun |
| 06:00 | Update upcoming earnings | Fri |
| 07:00 | Update historical earnings | Tue–Sat |
| Market hours | Popularity surge detection | Every 30 min |
| Market hours | Alert processing (4 pipelines) | Every 5 min |

---

## How Stocks Enter Each Pipeline

**Popularity Surge (Tier 1)**
ApeWisdom returns the top 1,000 stocks by mention count. Every ticker in that list is evaluated by `evaluate_popularity_surge()`. Tickers with fewer than 5 mentions are skipped. If a surge is detected, the ticker is inserted into the `popularity_surges` table and a `PopularitySurgeAlert` is posted.

**Confirmation Pipeline**
All tickers currently in the `popularity_surges` table with `confirmed=FALSE` and `expired=FALSE` and `flagged_at` within the last 24 hours. These are the active unconfirmed surges from Tier 1.

**Market Pipeline**
All tickers from the bot's screener list (`stock_data.alert_tickers`) **minus** any ticker that is:
- currently in an active popularity surge, OR
- on a user watchlist, OR
- reporting earnings today.

**Watchlist Pipeline**
All tickers currently on any user watchlist (from the `watchlists` table), regardless of whether they appear in the screener list.

**Earnings Pipeline**
All tickers where today's date matches a record in the `upcoming_earnings` table.

---

## Alert Deduplication and Update-in-Place

When an alert fires for a ticker that already has an active alert in Discord, the system decides whether to edit the existing message or post a new one. This decision is made by `override_and_edit(prev_alert_data)` on the alert object.

**Base logic (all alert types):** Uses momentum acceleration z-scores. The system tracks a `momentum_history` list in `alert_data`. Each entry records `pct_change`, `velocity` (rate of change of pct_change), and `acceleration` (rate of change of velocity). When there are at least 2 historical acceleration readings, it computes `abs(accel_zscore) >= 2.0` to decide if the move is accelerating unusually. With fewer readings, it falls back to the simple heuristic: re-post if pct_change moved more than 100% relative to the previous reading (e.g., +3% → +6.1% triggers; +3% → +5.9% does not).

**Popularity Surge override:** Additionally re-posts if the new mention ratio is >= 1.5x the mention ratio at the time of the previous post.

The `record_momentum()` method is called by the alert sender after `override_and_edit()` and before persisting the updated alert data to the DB, so each update cycle appends a snapshot to the history.

---

## Popularity Surge → Momentum Confirmation Relationship

These two alert types form a cause-and-effect pair:

1. **Tier 1 detects social interest** — a stock appears in the top 1,000 by mentions/rank activity. A `PopularitySurgeAlert` is posted immediately to Discord and the surge is recorded in the DB with `confirmed=FALSE`.

2. **Tier 2 watches for price follow-through** — every 5 minutes, the Confirmation Pipeline checks each active unconfirmed surge. If `evaluate_price_alert()` fires for that ticker, a `MomentumConfirmationAlert` is posted and the surge is marked `confirmed=TRUE` in the DB. Once confirmed, no further confirmation can fire for that surge record.

3. **Expiry** — if 24 hours pass without confirmation, the surge record is marked `expired=TRUE` and removed from the active pool.

---

## Alert Type Reference

| Alert | Pipeline | Trigger |
|---|---|---|
| [Popularity Surge](popularity_surge.md) | Tier 1 (30 min) | Social sentiment spike |
| [Momentum Confirmation](momentum_confirmation.md) | Tier 2 – Confirmation | Price/volume follow-through on a surge |
| [Market Alert](market_alert.md) | Tier 2 – Market | Unusual price/volume, composite score ≥ 2.5 |
| [Watchlist Mover](watchlist_mover.md) | Tier 2 – Watchlist | Watched stock moves above z-score threshold |
| [Earnings Mover](earnings_mover.md) | Tier 2 – Earnings | Earnings-day ticker moves above z-score threshold |
