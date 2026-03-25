# Volume Accumulation Alert

A Volume Accumulation alert fires when a stock shows unusually high trading volume without a corresponding price move. This is a leading indicator — it signals that institutional accumulation or distribution may be underway before price has reacted. The [Breakout](breakout.md) alert is the follow-through signal that fires when price confirms the move.

---

## What It Detects

The "volume without price" pattern is characteristic of institutional activity. Large players rarely move a stock's price aggressively all at once — they accumulate or distribute over time, generating elevated volume while price stays relatively flat. When price eventually moves, the prior volume is a leading indicator that the move has institutional support.

---

## Trigger Condition

`evaluate_volume_accumulation()` from `core/analysis/volume_divergence.py` evaluates each ticker:

```
is_accumulating = vol_zscore >= 2.0  AND  abs(price_zscore) < 1.0
```

- **Volume Z-Score** — `(current_volume - mean_volume_20d) / std_volume_20d`. A z-score of 2.0 means volume is 2 standard deviations above its 20-day average.
- **Price Z-Score** — intraday z-score of the current pct_change relative to the ticker's 20-day return distribution. Must be below 1.0 in absolute terms — the price should not already be moving significantly.

Both conditions must hold simultaneously. High volume alone is interesting; high volume without price movement is the signal.

---

## Divergence Score

```
divergence_score = vol_zscore - abs(price_zscore)
```

Higher values indicate a more extreme gap between volume and price activity. A divergence score of 3.5 (vol_z=4.0, price_z=0.5) is more significant than a score of 1.5 (vol_z=2.0, price_z=0.5).

---

## Signal Strength

| Value | Meaning |
|---|---|
| `volume_only` | Volume divergence detected; no unusual options activity |
| `volume_plus_options` | Volume divergence AND unusual options flow detected |

When the volume accumulation pipeline identifies a ticker, it optionally fetches the options chain and runs `evaluate_options_flow()`. If unusual options activity is detected (volume/OI ratio ≥ 3x on any contract), the signal strength is upgraded to `volume_plus_options`.

---

## What the Alert Shows

**Narrative section:**
- RVOL and price z-score at detection time
- Signal strength label

**Fields:**
- Price and intraday % change
- RVOL (relative volume — current volume vs 10-period average)
- Volume Z-Score
- Price Z-Score
- Divergence Score
- Signal Strength

**Options Flow section** (shown when present):
- Unusual contracts (ticker, strike, expiry, vol/OI ratio)
- Put/Call ratio
- IV Skew direction
- Max Pain vs current price
- Flow Score (0–10 composite)

---

## Relationship to Breakout Alert

Volume Accumulation and [Breakout](breakout.md) form a cause-and-effect pair:

1. Volume Accumulation fires as a leading indicator — "unusual volume detected, no price move yet."
2. The signal is recorded in `market_signals` with `signal_source='volume_accumulation'` and `status='pending'`.
3. The Breakout pipeline checks active signals every 5 minutes. When price moves significantly since the signal was detected, a Breakout alert fires and the signal is marked `confirmed`.
4. If price never moves, the signal is marked `expired` after 8 hours.

Traders who see a Volume Accumulation alert can watch for the Breakout follow-up or act immediately on the volume signal alone.

---

## Deduplication

A ticker is only signaled once per day via `is_already_signaled()`. If a ticker already has an active signal in `market_signals`, subsequent cycles skip it. This prevents duplicate Volume Accumulation alerts for the same event.

The standard momentum acceleration update logic applies to the alert embed itself (see [Overview — Deduplication](overview.md#alert-deduplication-and-update-in-place)).

---

## Examples

**Volume-only signal — triggers**

Standard ticker: up 0.4%, price_z = 0.3, vol_z = 3.8, RVOL = 4.2x.

```
vol_z=3.8 >= 2.0  ✓
abs(price_z)=0.3 < 1.0  ✓
→ is_accumulating = True
divergence_score = 3.8 - 0.3 = 3.5
signal_strength = 'volume_only'
```

**Volume + options signal — triggers**

Blue chip: up 0.2%, price_z = 0.1, vol_z = 2.4, RVOL = 2.8x. Options chain shows 3 contracts with vol/OI ≥ 3x.

```
vol_z=2.4 >= 2.0  ✓
abs(price_z)=0.1 < 1.0  ✓
→ is_accumulating = True
options flow detected → signal_strength = 'volume_plus_options'
```

**Both dimensions elevated — does NOT trigger**

Ticker: up 3.5%, price_z = 2.2, vol_z = 3.1.

```
vol_z=3.1 >= 2.0  ✓
abs(price_z)=2.2 < 1.0  ✗  (price already moving)
→ is_accumulating = False  — this is a price move with volume, not accumulation before the move
```

**Volume too low — does NOT trigger**

Ticker: up 0.1%, price_z = 0.05, vol_z = 1.6.

```
vol_z=1.6 >= 2.0  ✗
→ is_accumulating = False
```

---

## Would / Would Not Trigger

| Scenario | Triggers? | Reason |
|---|---|---|
| vol_z=3.5, price_z=0.4 | Yes | Both conditions met; divergence_score=3.1 |
| vol_z=2.0, price_z=0.9 | Yes | Exactly at thresholds |
| vol_z=2.0, price_z=1.0 | No | price_z not strictly below 1.0 |
| vol_z=1.9, price_z=0.5 | No | vol_z below 2.0 threshold |
| vol_z=4.0, price_z=2.5 | No | Price already moving significantly |
| Ticker already signaled today | No | `is_already_signaled()` check skips it |
