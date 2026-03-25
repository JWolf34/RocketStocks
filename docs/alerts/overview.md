# Alert System Overview

RocketStocks monitors stocks and fires Discord alerts when statistically unusual activity is detected. The system is designed around **leading indicators** — alerts that fire before price moves, not after. Two independent signal pipelines (social traction, volume divergence) each produce a leading indicator alert followed by a confirmation alert when price follows through.

---

## Design Philosophy

**Leading indicator alerts fire freely.** Popularity Surge and Volume Accumulation alerts fire on social and volume signals regardless of current price movement. The goal is to flag potential opportunities early and track how often they lead to price action.

**Follow-up alerts confirm with price-since-flag z-score.** Momentum Confirmation and Breakout alerts measure price change *since the leading indicator was flagged*, z-scored against the ticker's own historical return distribution. This avoids rewarding moves that already happened before the signal.

**Reactive alerts (Earnings Mover, Watchlist Mover) remain price-driven.** These fire on the current price move — traders explicitly want to know when tickers they're watching or tickers with earnings are moving right now.

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
║       │ tier damping + mention acceleration gate                     ║
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
║  ┌────────────────────┐  ┌──────────────────────────────────────┐   ║
║  │ Confirmation       │  │ Volume Accumulation                  │   ║
║  │ Pipeline           │  │ Pipeline                             │   ║
║  │                    │  │                                      │   ║
║  │ surge tickers only │  │ all quoted tickers                   │   ║
║  │ evaluate_          │  │ evaluate_volume_accumulation()        │   ║
║  │ confirmation()     │  │ → VolumeAccumulationAlert ──► Discord │   ║
║  │ (price since flag) │  │   insert into market_signals         │   ║
║  │                    │  └──────────────────────────────────────┘   ║
║  │ → Momentum         │                │                            ║
║  │   ConfirmAlert     │                │  pending signals           ║
║  └────────────────────┘                ▼  feed next cycle           ║
║                          ┌──────────────────────────────────────┐   ║
║                          │ Breakout Pipeline                    │   ║
║                          │                                      │   ║
║                          │ active signals from market_signals   │   ║
║                          │ evaluate_confirmation()              │   ║
║                          │ (price since flag, 10-min delay)     │   ║
║                          │ → BreakoutAlert ──► Discord          │   ║
║                          │   mark_confirmed in DB               │   ║
║                          └──────────────────────────────────────┘   ║
║                                                                      ║
║  ┌────────────────────┐  ┌──────────────────────────────────────┐   ║
║  │ Watchlist          │  │ Earnings                             │   ║
║  │ Pipeline           │  │ Pipeline                             │   ║
║  │                    │  │                                      │   ║
║  │ user watchlist     │  │ tickers with earnings today          │   ║
║  │ tickers            │  │ evaluate_price_alert()               │   ║
║  │ evaluate_price_    │  │ → EarningsMoverAlert                 │   ║
║  │ alert()            │  └──────────────────────────────────────┘   ║
║  │ → WatchlistMover   │                                             ║
║  │   Alert            │  (all 5 pipelines run in parallel)          ║
║  └────────────────────┘                                             ║
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
| Market hours | Alert processing (5 pipelines) | Every 5 min |

---

## How Stocks Enter Each Pipeline

**Popularity Surge (Tier 1)**
ApeWisdom returns the top 1,000 stocks by mention count. Every ticker in that list is evaluated by `evaluate_popularity_surge()`. Tickers with fewer than 15 mentions are skipped. If a surge is detected (subject to tier damping and the mention acceleration gate), the ticker is inserted into the `popularity_surges` table and a `PopularitySurgeAlert` is posted.

**Confirmation Pipeline**
All tickers currently in the `popularity_surges` table with `confirmed=FALSE`, `expired=FALSE`, and `flagged_at` within the last 24 hours, **and** at least 15 minutes have elapsed since flagging. Price change since flagging is z-scored against the ticker's own historical return distribution via `evaluate_confirmation()`.

**Volume Accumulation Pipeline**
All tickers in the current Schwab quote batch (screener tickers, active surge tickers, watchlist tickers, and earnings tickers). For each, the pipeline computes volume z-score and price z-score and calls `evaluate_volume_accumulation()`. Tickers already signaled today are skipped. When accumulation is detected, a signal is recorded in `market_signals` with `signal_source='volume_accumulation'` and a `VolumeAccumulationAlert` is posted.

**Breakout Pipeline**
All tickers with a `pending` record in `market_signals` from today with `signal_source='volume_accumulation'`, **and** at least 10 minutes have elapsed since the signal was detected. `evaluate_confirmation()` checks whether price has moved significantly since the signal time. When confirmed, a `BreakoutAlert` is posted and the signal is marked `confirmed`.

**Watchlist Pipeline**
All tickers currently on any user watchlist (from the `watchlists` table). Uses `evaluate_price_alert()` with dynamic z-score thresholds.

**Earnings Pipeline**
All tickers where today's date matches a record in the `upcoming_earnings` table. Uses `evaluate_price_alert()` with dynamic z-score thresholds.

---

## Alert Deduplication and Update-in-Place

When an alert fires for a ticker that already has an active alert in Discord, the system decides whether to post a new message. This decision is made by `override_and_edit(prev_alert_data)` on the alert object.

**Base logic (all alert types):** Uses momentum acceleration z-scores. The system tracks a `momentum_history` list in `alert_data`. Each entry records `pct_change`, `velocity` (rate of change of pct_change), and `acceleration` (rate of change of velocity). When there are at least 2 historical acceleration readings, it computes `abs(accel_zscore) >= 2.0` to decide if the move is accelerating unusually. With fewer readings, it falls back to the simple heuristic: re-post if pct_change moved more than 100% relative to the previous reading (e.g., +3% → +6.1% triggers; +3% → +5.9% does not).

**Popularity Surge override:** Additionally re-posts if the new mention ratio is >= 1.5x the mention ratio at the time of the previous post.

The `record_momentum()` method is called by the alert sender after `override_and_edit()` and before persisting the updated alert data to the DB, so each update cycle appends a snapshot to the history.

---

## Popularity Surge → Momentum Confirmation Relationship

These two alert types form a cause-and-effect pair:

1. **Tier 1 detects social interest** — a stock appears in the top 1,000 by mentions/rank activity with accelerating mention growth. A `PopularitySurgeAlert` is posted immediately to Discord and the surge is recorded in the DB with `confirmed=FALSE`.

2. **Tier 2 watches for price follow-through** — every 5 minutes, the Confirmation Pipeline checks each active unconfirmed surge (after a 15-minute delay). `evaluate_confirmation()` z-scores the price change since the surge was flagged against the ticker's own return distribution. When `abs(zscore_since_flag) >= 1.5`, a `MomentumConfirmationAlert` is posted and the surge is marked `confirmed=TRUE`.

3. **Expiry** — if 24 hours pass without confirmation, the surge record is marked `expired=TRUE` and removed from the active pool.

---

## Volume Accumulation → Breakout Relationship

These two alert types form a cause-and-effect pair for the volume-divergence pipeline:

1. **Volume Accumulation detects institutional activity** — when a ticker shows vol_z >= 2.0 with abs(price_z) < 1.0, unusual volume is present without a corresponding price move. A `VolumeAccumulationAlert` is posted and a signal is recorded in `market_signals` with `status='pending'`.

2. **Breakout pipeline confirms price follow-through** — every 5 minutes, `evaluate_confirmation()` checks whether price has moved significantly since the signal time (after a 10-minute delay). When `abs(zscore_since_flag) >= 1.5`, a `BreakoutAlert` is posted linking back to the original Volume Accumulation alert, and the signal is marked `confirmed`.

3. **Expiry** — pending signals older than 8 hours are marked `expired` and removed from the active pool.

---

## Signal Confluence

All tickers flow through all applicable pipelines. A ticker can trigger both a Popularity Surge AND a Volume Accumulation alert. Traders who see both alerts for the same ticker know the signal is coming from independent sources — social traction and institutional volume — which is a stronger combined signal.

---

## Alert Type Reference

| Alert | Pipeline | Category | Trigger |
|---|---|---|---|
| [Popularity Surge](popularity_surge.md) | Tier 1 (30 min) | Predictive (leading) | Early-phase social traction with acceleration gate |
| [Momentum Confirmation](momentum_confirmation.md) | Tier 2 – Confirmation | Predictive (follow-up) | Price z-score since surge flagged ≥ 1.5σ (15-min delay) |
| [Volume Accumulation](volume_accumulation.md) | Tier 2 – Volume | Predictive (leading) | High volume without price movement (vol_z ≥ 2.0, price_z < 1.0) |
| [Breakout](breakout.md) | Tier 2 – Breakout | Predictive (follow-up) | Price z-score since signal ≥ 1.5σ (10-min delay) |
| [Watchlist Mover](watchlist_mover.md) | Tier 2 – Watchlist | Reactive | Watched stock moves above dynamic z-score threshold |
| [Earnings Mover](earnings_mover.md) | Tier 2 – Earnings | Reactive | Earnings-day ticker moves above dynamic z-score threshold |
