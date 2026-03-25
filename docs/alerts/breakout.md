# Breakout Alert

A Breakout alert fires when a stock that previously showed a [Volume Accumulation](volume_accumulation.md) signal has now moved significantly in price. It answers the question: "That stock had unusual volume — did price follow through?"

This alert can only fire for tickers that already have an active, unconfirmed volume accumulation signal recorded in `market_signals`. It will not fire for tickers that are not in the volume signal pipeline.

---

## Relationship to Volume Accumulation

```
Volume Accumulation (leading)          Breakout (follow-up)
──────────────────────────────         ─────────────────────────────────────
VolumeAccumulationAlert fires  ──────► Breakout Pipeline checks each active
signal saved to market_signals          signal every 5 min (after 10-min delay)
status = 'pending'
                                        evaluate_confirmation() for the ticker
                                             │
                                             ├── should_confirm = True
                                             │     → BreakoutAlert
                                             │     → signal marked status = 'confirmed'
                                             │
                                             └── should_confirm = False
                                                   → nothing fires, check again
                                                     in 5 min (up to 8h)
```

After a signal is confirmed, the signal record's `status='confirmed'` is set and no further Breakout alerts can fire for that specific signal event.

---

## 10-Minute Delay

The Breakout pipeline waits at least **10 minutes** after the volume signal was first detected before checking for price confirmation. This ensures the confirmation measures subsequent price action rather than noise from the same bar that triggered the volume signal.

---

## Trigger Condition

The pipeline calls `evaluate_confirmation()` from `core/analysis/alert_strategy.py` using the **price change since the volume signal was detected** — not the overall intraday move:

```
pct_change_since_flag = (current_price - price_at_flag) / price_at_flag * 100
zscore_since_flag     = (pct_change_since_flag - mean_return_20d) / std_return_20d

Confirms when: abs(zscore_since_flag) >= 1.5
```

The z-score is computed against the **ticker's own return distribution** (mean and std from the last 20 trading days). A z-score threshold of 1.5 means the move since the volume signal is statistically significant for that specific stock — not a generic absolute percentage threshold.

**Sustained direction check:** When 2+ prior observations exist (the Breakout pipeline has checked this signal multiple times), all previous `pct_change_since_flag` readings must be moving in the same direction as the current reading. This prevents alerts when price spiked briefly and reversed.

---

## What the Alert Shows

**Narrative section:**
- Time elapsed since volume accumulation signal was detected
- Price change since the signal was flagged
- Link to the original Volume Accumulation alert message

**Fields:**
- Price now
- Change Since Flag (% from price at signal time)
- Time Since Signal
- RVOL (current relative volume)
- Vol Z-Score at Signal (what the volume looked like when originally detected)
- Price Z-Score Since Flag
- Signal Strength (carried from original signal: `volume_only` or `volume_plus_options`)
- Signal Confidence (30d) — percentage of past volume accumulation signals confirmed

**Options Flow section** (when present, carried from the original signal):
- Unusual contracts, put/call ratio, IV skew, max pain, flow score

**Color:**
- Green for positive `price_change_since_flag` (upward breakout)
- Red for negative `price_change_since_flag` (downward breakdown/distribution)

---

## Signal Lifecycle

```
market_signals table:

  status = 'pending'    → signal recorded, waiting for price confirmation
  status = 'confirmed'  → BreakoutAlert posted to Discord
  status = 'expired'    → 8 hours passed without confirmation
```

Signals expire after **8 hours**. Unusual volume at 9:30 AM that never sees price follow-through will expire without producing a Breakout alert, and it will appear as an "unconfirmed" data point in the signal confidence statistics.

---

## Examples

**Upward breakout confirmed**

Stock: volume_accumulation detected at 9:45 AM, price_at_flag = $42.50. At 10:20 AM (35 min later), price = $44.10.

Ticker 20-day stats: mean_return = 0.3%, std_return = 1.8%.

```
pct_change_since_flag = (44.10 - 42.50) / 42.50 * 100 = +3.76%
zscore_since_flag = (3.76 - 0.3) / 1.8 = 1.92 >= 1.5  → TRIGGER
```

BreakoutAlert fires, showing "+3.76% since volume detected at 9:45 AM · 35 minutes ago."

**Downward breakdown (distribution) confirmed**

Stock: volume_accumulation detected at 11:00 AM (institutional distribution suspected). Price drifts down.

Ticker stats: mean_return = 0.1%, std_return = 2.0%.

```
pct_change_since_flag = -3.8%
zscore_since_flag = (-3.8 - 0.1) / 2.0 = -1.95  abs() = 1.95 >= 1.5  → TRIGGER
```

BreakoutAlert fires in red (negative change since flag), showing "-3.8% since volume detected."

**No confirmation — signal expires**

Stock: volume_accumulation detected but price drifts ±0.5% for 8 hours, never reaching z-score 1.5. Signal marked `expired`. The event is logged as an unconfirmed signal in the confidence statistics.

**10-minute delay not yet elapsed — skipped**

Volume accumulation detected at 2:10 PM. At 2:15 PM (5 min later), the Breakout pipeline checks but finds only 5 minutes have elapsed → skips without calling `evaluate_confirmation()`.

---

## Would / Would Not Trigger

| Scenario | Triggers? | Reason |
|---|---|---|
| zscore_since_flag = 1.6 | Yes | ≥ 1.5 threshold |
| zscore_since_flag = 1.4 | No | Below 1.5 threshold |
| 8 minutes since signal | No | 10-minute minimum delay |
| Signal confirmed earlier | No | `status='confirmed'` — excluded from active signal list |
| Signal expired (8h) | No | `status='expired'` — excluded from active signal list |
| Price spiked then reversed (2+ obs, mixed direction) | No | Sustained direction check fails |
| Ticker not in `market_signals` | No | Only tickers with active volume signals enter this pipeline |
