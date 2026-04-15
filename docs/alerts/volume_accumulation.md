# Volume Accumulation Alert

A Volume Accumulation alert fires when a stock shows unusually high trading volume without a corresponding price move. This is a leading indicator — it signals that institutional accumulation or distribution may be underway before price has reacted. The [Breakout](breakout.md) alert is the follow-through signal that fires when price confirms the move.

---

## What It Detects

The "volume without price" pattern is characteristic of institutional activity. Large players rarely move a stock's price aggressively all at once — they accumulate or distribute over time, generating elevated volume while price stays relatively flat. When price eventually moves, the prior volume is a leading indicator that the move has institutional support.

---

## Trigger Condition

`evaluate_volume_accumulation()` from `core/analysis/volume_divergence.py` evaluates each ticker:

```
divergence_score = vol_zscore - abs(price_zscore)

is_accumulating = vol_zscore >= 2.5
             AND abs(price_zscore) < 1.0
             AND divergence_score >= 1.5
```

- **Volume Z-Score** — `(current_volume - mean_volume_20d) / std_volume_20d`. A z-score of 2.5 means volume is 2.5 standard deviations above its 20-day average.
- **Price Z-Score** — intraday z-score of the current pct_change relative to the ticker's 20-day return distribution. Must be below 1.0 in absolute terms — the price should not already be moving significantly.
- **Divergence Score** — `vol_z - abs(price_z)`. Must be at least 1.5, ensuring the gap between volume activity and price activity is meaningful, not marginal.

All three conditions must hold simultaneously. A borderline volume spike (e.g., vol_z=2.5, price_z=0.9) produces divergence_score=1.6 and passes; a weaker case (vol_z=2.5, price_z=0.95) produces 1.55 and also passes, but (vol_z=2.5, price_z=0.99) is blocked (1.51 rounds to a borderline pass — still passes). Only cases where the divergence is genuinely weak are filtered.

---

## Divergence Score

```
divergence_score = vol_zscore - abs(price_zscore)
```

Higher values indicate a more extreme gap between volume and price activity. A divergence score of 3.5 (vol_z=4.0, price_z=0.5) is more significant than a score of 1.6 (vol_z=2.5, price_z=0.9). Minimum required to trigger: 1.5.

---

## Options Flow Gate

After `evaluate_volume_accumulation()` passes, the pipeline fetches the options chain and evaluates flow via `evaluate_options_flow()`. **If no unusual options activity is found, the ticker is skipped — no alert fires.** Unusual activity is defined as at least one contract with a volume/OI ratio ≥ 3.0.

This requirement ensures Volume Accumulation alerts reflect genuine institutional positioning rather than routine high-volume days. Tickers without liquid options markets will not produce VA alerts.

All VA alerts have `signal_strength = 'volume_plus_options'` — the `volume_only` value is no longer used in production.

---

## Signal Strength

| Value | Meaning |
|---|---|
| `volume_plus_options` | Volume divergence AND unusual options activity confirmed — the only value used in production |

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

**Classic accumulation — triggers**

Ticker: up 0.4%, price_z = 0.3, vol_z = 3.8, RVOL = 4.2x. Options chain shows 2 contracts with vol/OI = 4.5x.

```
vol_z=3.8 >= 2.5              ✓
abs(price_z)=0.3 < 1.0        ✓
divergence_score=3.5 >= 1.5   ✓
unusual options activity       ✓
→ fires, signal_strength = 'volume_plus_options'
```

**Volume divergent but no unusual options — does NOT trigger**

Ticker: up 0.2%, price_z = 0.1, vol_z = 2.8, RVOL = 3.1x. Options chain has no contracts with vol/OI ≥ 3.0.

```
vol_z=2.8 >= 2.5              ✓
abs(price_z)=0.1 < 1.0        ✓
divergence_score=2.7 >= 1.5   ✓
unusual options activity       ✗  ← skipped, no alert
```

**Weak divergence — does NOT trigger**

Ticker: up 0.4%, price_z = 0.8, vol_z = 2.6.

```
vol_z=2.6 >= 2.5              ✓
abs(price_z)=0.8 < 1.0        ✓
divergence_score=1.8 >= 1.5   ✓  (passes)
→ would continue to options check
```

vs. tighter case: price_z = 0.95, vol_z = 2.6 → divergence=1.65 ✓ still passes; but vol_z=2.5, price_z=0.95 → divergence=1.55 ✓ just passes.

**Both dimensions elevated — does NOT trigger**

Ticker: up 3.5%, price_z = 2.2, vol_z = 3.1.

```
vol_z=3.1 >= 2.5              ✓
abs(price_z)=2.2 < 1.0        ✗  (price already moving)
→ is_accumulating = False — this is a price move with volume, not accumulation before the move
```

**Volume below threshold — does NOT trigger**

Ticker: up 0.1%, price_z = 0.05, vol_z = 2.2.

```
vol_z=2.2 >= 2.5  ✗
→ is_accumulating = False
```

---

## Would / Would Not Trigger

| Scenario | Triggers? | Reason |
|---|---|---|
| vol_z=3.5, price_z=0.4, unusual options | Yes | All gates pass; divergence=3.1 |
| vol_z=2.5, price_z=0.0, unusual options | Yes | Exactly at vol threshold; divergence=2.5 |
| vol_z=2.5, price_z=0.0, no unusual options | No | Options hard gate blocks |
| vol_z=2.4, price_z=0.5, unusual options | No | vol_z below 2.5 threshold |
| vol_z=2.5, price_z=1.0, unusual options | No | price_z not strictly below 1.0 |
| vol_z=2.6, price_z=0.95 (div=1.65), options | Yes | All gates pass |
| vol_z=4.0, price_z=2.5, unusual options | No | Price already moving; price_z ≥ 1.0 |
| Ticker already signaled today | No | `is_already_signaled()` check skips it |
