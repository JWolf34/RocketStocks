# Market Signal Pipeline

> **Note:** The "Market Alert" has been replaced by a two-stage system — the **Market Signal Pipeline** (silent detection) feeds into the **Market Mover** alert (visible confirmation). See [Market Mover](market_mover.md) for the current alert behavior. This page describes the underlying composite scoring and detection logic shared by both stages.

---

## Exclusion Logic

Before any ticker enters the Market Signal Pipeline, it is excluded if it is currently:

- In an active (unconfirmed, unexpired) popularity surge → handled by the [Confirmation Pipeline](momentum_confirmation.md)
- On any user watchlist → handled by the [Watchlist Pipeline](watchlist_mover.md)
- Reporting earnings today → handled by the [Earnings Pipeline](earnings_mover.md)

This ensures each ticker is processed by exactly one pipeline per cycle and prevents duplicate alerts.

---

## Two-Stage Detection Gate

A market signal requires **both** conditions to pass:

### Stage 1: evaluate_price_alert()

`evaluate_price_alert()` must return `should_alert = True`. This uses classification-based thresholds:

**Standard, Meme, Volatile:**
```
abs(z-score) >= threshold

  z-score   = (pct_change - mean_return_20d) / std_return_20d
  threshold = 2.0σ  for meme and volatile
              2.5σ  for standard
```

**Blue Chip:** Bollinger Band breach + volume confirmation strategy. See [Blue Chip Strategy](#blue-chip-strategy) below.

### Stage 2: compute_composite_score() — dual-gate + weighted score ≥ 2.5

Even if Stage 1 passes, a signal is only recorded if the composite score reaches **2.5** AND the dual-gate pre-check passes.

**Dual-gate pre-check (new):**
```
(|price_z| >= 1.5  AND  |vol_z| >= 1.5)   ← both dimensions must move
   OR
|vol_z| >= 4.0                             ← extreme volume alone passes
```

If neither condition is met, the composite score is set to 0.0 and no signal is recorded, regardless of Stage 1 result. This eliminates cases where only one dimension (e.g., a high price z-score with flat volume) triggered a false positive.

---

## Composite Score Formula

The composite score combines three weighted components. Classification is no longer a factor.

```
composite_score = 0.50 × volume_component
                + 0.35 × price_component
                + 0.15 × cross_signal_component
```

### Volume Component
```
volume_component = abs(volume_z_score)

  volume_z_score = (current_volume - mean_volume_20d) / std_volume_20d
```

### Price Component
```
price_component = abs(price_z_score)   (the same z-score from Stage 1)
```

### Cross-Signal Component
```
cross_signal_component = (confluence_count / confluence_total) × 4.0
```

`confluence_count` is the number of technical indicators that agree with the direction of the move (RSI, MACD, ADX, OBV). `confluence_total` is the total number of indicators evaluated. A perfect confluence score (all indicators agree) produces `1.0 × 4.0 = 4.0`. This component is 0.0 for non-blue-chip tickers (confluence is only computed for blue chips in `evaluate_price_alert()`).

### Classification

The classification component has been **removed**. Previously, meme and volatile stocks received a score bonus that could push borderline signals over the threshold even with modest price/volume activity. All stock classes are now scored identically — a meme stock and a blue chip with the same price_z and vol_z produce the same composite score.

---

## Dominant Signal

The alert embed identifies whether the move is primarily **volume-driven**, **price-driven**, or **mixed**. This is determined by comparing the weighted contributions:

```
vol_weighted   = 0.50 × volume_component
price_weighted = 0.35 × price_component

ratio = vol_weighted / price_weighted

  ratio >= 1.5        → dominant = 'volume'
  ratio <= 1/1.5      → dominant = 'price'
  otherwise           → dominant = 'mixed'
```

If price_weighted is 0, the signal is forced to 'volume'. If both are 0, it is 'mixed'.

---

## Blue Chip Strategy

Blue-chip tickers use a two-step strategy inside `evaluate_price_alert()`:

**Primary trigger: Bollinger Band breach + volume confirmation**

Volume confirmation requires: `volume_z_score >= 2.0`

- **Mean reversion** (price below lower BB with volume): `bb_position == 'below_lower'` AND (`RSI oversold` OR `OBV trending up`)
- **Trend breakout** (price above upper BB with volume): `bb_position == 'above_upper'` AND (`ADX strong` OR `MACD bullish`)

**Fallback trigger: Confluence + volume + z-score**

If the primary trigger does not fire:
```
confluence_count >= 3  AND  volume_z_score >= 2.0  AND  abs(z_score) >= 2.0
```

The fallback catches blue-chip moves that are unusual across multiple dimensions but haven't cleanly breached a Bollinger Band.

---

## What Happens After a Signal Is Recorded

When both gates pass, a row is written to `market_signals` with `status='pending'`. No Discord message is sent. Each subsequent 5-minute cycle where the same ticker still qualifies appends an observation snapshot `{ts, pct_change, vol_z, price_z, composite}` to the row's `signal_data` JSON array.

The [Market Confirmation Pipeline](market_mover.md) then reads these pending signals and applies `should_confirm_signal()` to decide if a visible alert should be posted.

---

## Examples (Signal Recording, No Alert Yet)

**Dual-gate passes, composite passes → signal recorded silently**

Standard ticker: up 3.5%, z-score 2.6, volume z-score 3.0.
```
Dual-gate: |price_z|=2.6 >= 1.5  AND  |vol_z|=3.0 >= 1.5  ✓

composite_score = 0.50 × 3.0 + 0.35 × 2.6 + 0.15 × 0.0
               = 1.50 + 0.91 + 0.00 = 2.41  < 2.5 → no signal
```

With confluence (e.g., 2/5 indicators → cross = 1.6):
```
               = 1.50 + 0.91 + 0.15 × 1.6 = 1.50 + 0.91 + 0.24 = 2.65  ≥ 2.5 → signal recorded
```

**Dual-gate fails (price high, volume low) → no signal**

Standard ticker: z-score 4.0, volume z-score 0.8.
```
Dual-gate: |price_z|=4.0 >= 1.5 ✓  BUT  |vol_z|=0.8 < 1.5 ✗
           extreme vol: 0.8 < 4.0 ✗
→ gate fails → composite_score = 0.0 → no signal
```

**Extreme volume gate passes alone**

Any ticker: volume z-score 5.0, price z-score 0.4 (almost no price move yet).
```
Dual-gate: extreme vol: 5.0 >= 4.0  ✓

composite_score = 0.50 × 5.0 + 0.35 × 0.4 = 2.50 + 0.14 = 2.64  ≥ 2.5 → signal recorded
```
This catches accumulation/distribution events where volume spikes before price moves.

---

## Would / Would Not Record a Signal

| Scenario | Records Signal? | Reason |
|---|---|---|
| z-score 2.6, vol z-score 3.2 | Yes (likely) | Gate passes; 0.50×3.2+0.35×2.6 = 1.60+0.91 = 2.51 ≥ 2.5 |
| z-score 2.6, vol z-score 0.8 | No | Gate fails (vol < 1.5, not extreme) |
| z-score 0.4, vol z-score 5.0 | Yes | Extreme vol gate (5.0 ≥ 4.0); composite 2.64 ≥ 2.5 |
| Meme stock, z-score 2.0, vol z-score 2.0 | Depends on score | No classification bonus; same threshold as standard |
| Ticker on user watchlist | No | Excluded from Market Signal Pipeline |
| Ticker with active surge | No | Excluded from Market Signal Pipeline |
| Ticker reporting earnings today | No | Excluded from Market Signal Pipeline |
