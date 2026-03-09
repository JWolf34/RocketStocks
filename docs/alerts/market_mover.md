# Market Mover

A Market Mover alert fires when a screener ticker's unusual price/volume activity has been **confirmed** — the move has sustained or accelerated across multiple 5-minute observations rather than appearing as a single transient spike.

This is the visible output of the two-stage market detection system. The [Market Signal Pipeline](market_alert.md) silently records candidates; this pipeline promotes them to visible Discord alerts.

---

## Why Two Stages?

The original single-pass market alert fired on every qualifying composite score, producing ~119 alerts on a busy Friday. Most of those were transient blips — a single bar of unusual activity followed by nothing. The two-stage design requires a move to *persist* before a user sees it:

```
Cycle N    → composite passes → record pending signal (silent)
Cycle N+1  → same ticker still moving → append observation
Cycle N+2  → should_confirm_signal() passes → post MarketMoverAlert
```

Target rate: 15–25 confirmed alerts per day.

---

## Confirmation Criteria

`should_confirm_signal()` checks four conditions, evaluated in priority order. The **first** condition that passes determines the `confirmation_reason` shown in the alert.

### 1. Volume Extreme (immediate)

```
|vol_z| >= 4.0  at any check
```

No waiting required. Extreme volume (more than 4 standard deviations above the mean) is independently significant enough to confirm immediately. This catches pre-market accumulation, halts, and news-driven volume before the price has moved.

### 2. Sustained

```
n_observations >= 2
AND
|current_pct_change| >= |original_pct_change|
```

The move hasn't faded. If the stock was up 3% when first detected, it must still be up at least 3% (in absolute terms) after two or more observation cycles. Works symmetrically for downward moves.

### 3. Price Accelerating

```
n_observations >= 3
AND
price acceleration z-score >= 1.5
```

The rate of change of the price move is increasing. Computed from the velocity series (first differences of pct_change across observations). The last velocity reading must be at least 1.5 standard deviations above the baseline of prior velocities.

### 4. Volume Accelerating

```
n_observations >= 3
AND
volume acceleration z-score >= 1.5
```

Same logic applied to the vol_z series. Catches situations where volume is ramping up even if the price hasn't moved significantly yet — often a leading indicator of an imminent price swing.

---

## Confirmation Reason Labels

| `confirmation_reason` | Meaning |
|---|---|
| `volume_extreme` | vol_z ≥ 4.0 at current check — immediate confirmation |
| `sustained` | Move has not faded across ≥ 2 observations |
| `price_accelerating` | Price velocity is accelerating (z-score ≥ 1.5) |
| `volume_accelerating` | Volume velocity is accelerating (z-score ≥ 1.5) |

---

## Signal Lifecycle

```
market_signals table:

  status = 'pending'    → signal recorded, waiting for confirmation
  status = 'confirmed'  → MarketMoverAlert posted to Discord
  status = 'expired'    → 8 hours passed without confirmation
```

Signals expire after **8 hours**. An intraday move that looks unusual at 9:30 AM but fades by 10 AM will expire without ever producing a visible alert.

Once a signal is confirmed, `mark_confirmed()` is called immediately so no duplicate alert fires in the next cycle.

---

## What the Alert Shows

- **Company name and ticker**
- **Driver label** — `Volume-driven`, `Price-driven`, or `Mixed signals`
- **Confirmation reason** — how the signal qualified
- **Number of observations** — how many 5-minute cycles the signal persisted through
- **Price and intraday % change**
- **Composite score** — the weighted activity score at time of confirmation
- **RVOL** — relative volume (current / 10-period average), if available
- **Z-score, move percentile, classification** — from the underlying trigger result
- **Momentum fields** — price velocity, price acceleration, volume velocity, volume acceleration (when present)

---

## Examples

**Volume Extreme — immediate confirmation**

Stock: flat price (+0.4%), but volume z-score = 5.2 at 9:35 AM.

```
Cycle 1: composite passes → signal recorded (pct=0.4%, vol_z=5.2)
Cycle 1: should_confirm_signal() → |vol_z|=5.2 >= 4.0 → volume_extreme ✓
→ MarketMoverAlert posted immediately (1 observation)
```

Useful for detecting accumulation, institutional buying, or pre-halt activity before any visible price move.

**Sustained — move holds across cycles**

Stock: up 3.2% initially, still up 3.5% two cycles later.

```
Cycle 1: composite passes → signal recorded (pct=3.2%)
Cycle 2: pct=3.5%  → append observation (2 obs total)
         |3.5| >= |3.2| AND n_obs=2 >= 2 → sustained ✓
→ MarketMoverAlert posted (2 observations)
```

**Price Accelerating — move is speeding up**

Stock: moves +1.0%, +1.3%, +1.8% across cycles, then jumps to +5.0%.

```
pct_series = [1.0, 1.3, 1.8, 5.0]
velocities = [0.3, 0.5, 3.2]
accel z-score of 3.2 vs baseline [0.3, 0.5] → very large
→ price_accelerating ✓  (after 3 observations)
```

**Volume Accelerating — volume ramp precedes price**

Stock: price barely moves (+0.5%), but vol_z climbs 1.0 → 1.2 → 1.4 → 5.8.

```
vol_series = [1.0, 1.2, 1.4, 5.8]
velocities = [0.2, 0.2, 4.4]
accel z-score of 4.4 vs baseline [0.2, 0.2] → very large
→ volume_accelerating ✓  (after 3 observations)
```

**Would not confirm — move fades**

Stock: initially up 4% (signal recorded), then 3%, then 2%.

```
Cycle 2: pct=3.0%  → |3.0| < |4.0| → sustained fails (n_obs=1, not enough yet anyway)
Cycle 3: pct=2.0%, n_obs=2 → |2.0| < |4.0| → sustained fails
         n_obs < 3 → no accel checks
→ no confirmation; signal expires after 8 hours
```

---

## Relationship to Other Alerts

| Scenario | Which Alert Fires |
|---|---|
| Stock has active popularity surge | [Momentum Confirmation](momentum_confirmation.md) (not Market Mover) |
| Stock is on a user watchlist | [Watchlist Mover](watchlist_mover.md) (not Market Mover) |
| Stock reports earnings today | [Earnings Mover](earnings_mover.md) (not Market Mover) |
| Screener stock, composite passes, move sustains | **Market Mover** |
| Screener stock, composite passes, move fades in 8h | No alert (expired signal) |
