# Market Alert

A Market Alert fires when a screener ticker shows statistically unusual price or volume activity and is not being tracked by another pipeline. It is the broadest alert type — covering the general market rather than a specific social, watchlist, or earnings event.

---

## Exclusion Logic

Before any ticker enters the Market Pipeline, it is excluded if it is currently:

- In an active (unconfirmed, unexpired) popularity surge → handled by the [Confirmation Pipeline](momentum_confirmation.md)
- On any user watchlist → handled by the [Watchlist Pipeline](watchlist_mover.md)
- Reporting earnings today → handled by the [Earnings Pipeline](earnings_mover.md)

This ensures each ticker is processed by exactly one pipeline per cycle and prevents duplicate alerts.

---

## Two-Stage Trigger

A Market Alert requires **both** conditions to pass:

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

### Stage 2: compute_composite_score() >= 2.5

Even if Stage 1 passes, the alert only fires if the composite score reaches **2.5**. This second gate filters out moves that are statistically significant but lack multi-factor support.

---

## Composite Score Formula

The composite score combines four weighted components:

```
composite_score = 0.40 × volume_component
                + 0.30 × price_component
                + 0.15 × cross_signal_component
                + 0.15 × classification_component
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

### Classification Component

| Class | Score |
|---|---|
| `meme` | 2.5 |
| `volatile` | 2.0 |
| `blue_chip` | 1.5 |
| `standard` | 1.0 |

Meme and volatile stocks receive a higher classification score because their moves, while frequent, can be more significant when combined with unusual volume.

---

## Dominant Signal

The alert embed identifies whether the move is primarily **volume-driven**, **price-driven**, or **mixed**. This is determined by comparing the weighted contributions:

```
vol_weighted   = 0.40 × volume_component
price_weighted = 0.30 × price_component

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

## RVOL

When available, Relative Volume (RVOL) is displayed in the alert embed. RVOL is current volume divided by the average volume over the last 10 periods. It gives an intuitive "X times normal volume" reading alongside the volume z-score.

---

## What the Alert Shows

- Price and intraday % change
- Composite score
- Score breakdown (Vol, Price, Cross, Class components)
- Dominant signal label
- RVOL (if available)
- Z-score, move percentile, classification
- For blue chips: BB position, confluence count, signal type

---

## Examples

**Volume-dominant: AAPL moderate price move, 3x normal volume**

AAPL (blue chip): up 1.8%, 20d mean = 0.2%, std = 0.8%, volume = 3x normal.

```
z-score = (1.8 - 0.2) / 0.8 = 2.0   → Stage 1: blue chip fallback check
volume_z_score = 3.0

Primary: price is within Bollinger Bands → no BB breach → primary fails
Fallback: need 3+ confluence, vol ≥ 2.0, abs(z) ≥ 2.0
  → if 3 indicators agree → fallback fires

composite_score = 0.40 × 3.0 + 0.30 × 2.0 + 0.15 × confluence + 0.15 × 1.5
               = 1.20 + 0.60 + confluence_part + 0.225
               (assuming confluence = 3/5 signals → 0.15 × 2.4 = 0.36)
               = 1.20 + 0.60 + 0.36 + 0.225 = 2.385  < 2.5 → no alert

  If confluence = 4/5 signals → 0.15 × 3.2 = 0.48
               = 1.20 + 0.60 + 0.48 + 0.225 = 2.505  ≥ 2.5 → ALERT (volume-dominant)
```

**Price-dominant: Small volatile cap up 15%, volume normal**

Stock (volatile class): up 15%, 20d mean = 0.5%, std = 5.0%, volume normal (z-score = 0.8).

```
z-score = (15 - 0.5) / 5.0 = 2.9  ≥ 2.0 → Stage 1 passes

composite_score = 0.40 × 0.8 + 0.30 × 2.9 + 0.15 × 0.0 + 0.15 × 2.0
               = 0.32 + 0.87 + 0.00 + 0.30 = 1.49  < 2.5 → no alert

  Even though Stage 1 passed, composite score is below 2.5.
  This small cap would need elevated volume to reach the composite threshold.
```

**Mixed: Both price and volume elevated simultaneously**

Stock (standard class): up 4.5%, 20d mean = 0.3%, std = 1.5%, volume z-score = 3.2.

```
z-score = (4.5 - 0.3) / 1.5 = 2.8  ≥ 2.5 → Stage 1 passes

composite_score = 0.40 × 3.2 + 0.30 × 2.8 + 0.15 × 0.0 + 0.15 × 1.0
               = 1.28 + 0.84 + 0.00 + 0.15 = 2.27  < 2.5 → no alert

  With confluence:  (e.g., 2/5 indicators agree → 0.15 × 1.6 = 0.24)
               = 1.28 + 0.84 + 0.24 + 0.15 = 2.51  ≥ 2.5 → ALERT (mixed)
  vol_weighted = 1.28, price_weighted = 0.84, ratio = 1.28/0.84 = 1.52 ≥ 1.5 → volume-dominant
```

**Would not trigger: Composite score below threshold**

Stock (standard class): up 3.5%, z-score 2.6 (Stage 1 passes), volume z-score = 1.0.

```
composite_score = 0.40 × 1.0 + 0.30 × 2.6 + 0.15 × 0.0 + 0.15 × 1.0
               = 0.40 + 0.78 + 0.00 + 0.15 = 1.33  < 2.5 → no alert
```

---

## Would / Would Not Trigger

| Scenario | Triggers? | Reason |
|---|---|---|
| Standard ticker: z-score 2.6, vol z-score 4.5, no confluence | Yes (likely) | 0.40×4.5 + 0.30×2.6 + 0.15×1.0 = 1.80+0.78+0.15 = 2.73 ≥ 2.5 |
| Standard ticker: z-score 2.6, vol z-score 0.8, no confluence | No | 0.40×0.8 + 0.30×2.6 + 0.15×1.0 = 0.32+0.78+0.15 = 1.25 < 2.5 |
| Ticker on user watchlist | No | Excluded from Market Pipeline, handled by Watchlist Pipeline |
| Ticker with active surge | No | Excluded from Market Pipeline, handled by Confirmation Pipeline |
| Ticker reporting earnings today | No | Excluded from Market Pipeline, handled by Earnings Pipeline |
