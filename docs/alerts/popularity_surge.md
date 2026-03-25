# Popularity Surge Alert

A Popularity Surge alert fires when a stock shows an unusual spike in social media discussion on Reddit, primarily r/WallStreetBets. This is a leading indicator — it tells you that retail traders are talking about a stock before any significant price move has necessarily occurred. The [Momentum Confirmation](momentum_confirmation.md) alert is the follow-through signal that fires when price and volume validate the social interest.

---

## Data Source

ApeWisdom aggregates mention counts and popularity rankings across WallStreetBets and other subreddits. The bot fetches the **top 1,000 stocks** by popularity rank every 30 minutes during market hours.

Each row in the popularity data contains:
- `ticker` — stock symbol
- `rank` — current popularity rank (1 = most mentioned)
- `rank_24h_ago` — rank 24 hours earlier
- `mentions` — mention count in the current period
- `mentions_24h_ago` — mention count 24 hours earlier

---

## Minimum Mentions Filter

Any ticker with fewer than **15 mentions** is skipped entirely. Low-mention stocks produce noisy rank fluctuations (jumping from rank 950 to rank 700 might mean 1 mention instead of 0) that are not meaningful signals.

Note: this global filter is separate from the MENTION_SURGE base-volume requirement described below.

---

## Acceleration Gate

Before any surge type is evaluated, the system checks whether social interest is actually **growing**. When 3 or more data points of popularity history are available, `mention_acceleration` is computed:

```
First differences of mentions over recent intervals → rate of change
Acceleration = rate of change of that rate of change (positive = growth phase)
```

If `mention_acceleration <= 0` and the ticker has 3 or more history points, the surge is suppressed entirely — the stock is at peak or declining social traction, not the early phase of a surge.

**Fallback:** If fewer than 3 data points are available (e.g., a new entrant), the gate is skipped and the surge is evaluated normally.

This gate eliminates alerts for stocks whose Reddit discussion is already fading, which is when the opportunity has typically already passed.

---

## Tier Damping

The surge thresholds are adjusted based on the ticker's current rank tier to reduce noise at the extremes:

| Tier | Rank Range | Adjustments |
|---|---|---|
| **Tier 1** | 1–25 | VELOCITY_SPIKE suppressed entirely; MENTION_SURGE threshold 5.0x (from 3.0x). Top-25 stocks have saturated attention; small rank shuffles are noise. |
| **Tier 2** | 26–100 | VELOCITY_SPIKE tightened to -3.5σ (from -2.5σ). Still a high-attention zone. |
| **Tier 3** | 101–500 | Default thresholds. This is the predictive sweet spot. |
| **Tier 4** | 501+ | NEW_ENTRANT cutoff lowered to rank ≤ 150 (from 200); MENTION_SURGE min_base lowered to 10 (from 15). Lower-ranked stocks need lower mention counts to be meaningful. |

---

## Surge Types

Multiple surge types can fire simultaneously for the same ticker (subject to tier damping). Each type checks a different dimension of popularity movement.

### MENTION_SURGE

```
mentions / mentions_24h_ago >= 3.0  AND  mentions_24h_ago >= 15
```

(Tier 1 overrides threshold to 5.0x; Tier 4 lowers min_base to 10.)

The current mention count is at least 3x the count from 24 hours ago, **and** the 24h-ago baseline meets the minimum. The base-volume requirement prevents low-signal noise from triggering the alert.

- **Example (triggers):** NVDA had 200 mentions yesterday; today it has 750. Ratio = 3.75x, base = 200 → MENTION_SURGE.
- **Example (triggers, Tier 4):** Low-rank stock: 10 mentions yesterday, 35 today. Ratio = 3.5x, Tier 4 min_base = 10 → MENTION_SURGE.
- **Example (does not trigger):** Stock had 6 mentions yesterday; today it has 36. Ratio = 6x, but base = 6 < 15 (Tier 3 threshold) → no MENTION_SURGE.

### RANK_JUMP

```
rank_change / current_rank >= 1.5  AND  rank_change >= 50
```

The ticker gained popularity at a rate that is significant **relative to its current rank**. The ratio condition weights entry into high-attention zones more heavily (750→120 is far more significant than 230→120, even though both jump over 100 spots). The minimum-spots guard prevents micro-fluctuations near the top from triggering.

- **Example (triggers):** 750 → 120: gain = 630, ratio = 5.25 → RANK_JUMP.
- **Example (triggers):** 120 → 40: gain = 80, ratio = 2.0 → RANK_JUMP.
- **Example (does not trigger):** 230 → 120: gain = 110, ratio = 0.92 → no RANK_JUMP (ratio below 1.5).
- **Example (does not trigger):** 50 → 10: gain = 40, ratio = 4.0 → no RANK_JUMP (below 50-spot minimum).

### NEW_ENTRANT

```
current_rank <= 200  AND  rank_24h_ago is None
```

(Tier 4 lowers cutoff to rank ≤ 150.)

The ticker just entered the top 200 for the first time — it had no rank 24 hours ago (meaning it was not in the top 1,000 or had zero mentions). Combined with MENTION_SURGE when mention count also jumped.

- **Example (triggers):** GME was not in the ApeWisdom top 1,000 yesterday; today it appears at rank #120. No prior rank → NEW_ENTRANT.
- **Example (does not trigger):** A ticker enters at rank #350 having not been ranked yesterday → no NEW_ENTRANT (above the 200 cutoff for Tier 3).

### VELOCITY_SPIKE

```
rank_velocity_zscore <= -2.5
```

(Suppressed for Tier 1; tightened to -3.5 for Tier 2.)

The rank improvement velocity (rate of rank-number decrease per 5-period window) is more than 2.5 standard deviations below its 30-day mean. A negative z-score means the rank number is dropping faster than usual — i.e., the stock is gaining popularity at an accelerating rate.

- **Example (triggers, Tier 3):** A stock has been slowly climbing in rank. Over the last few data points its rank-drop rate suddenly accelerates to 2.8σ below the mean → VELOCITY_SPIKE.
- **Example (does not trigger, Tier 1):** AAPL rank velocity spikes — suppressed because Tier 1 stocks always have volatility in their rank.

---

## What the Alert Shows

The Discord embed includes:
- Mention acceleration phase ("Social traction accelerating" / "Social traction decelerating")
- Current price and intraday % change
- Current rank and rank 24 hours ago
- Rank change (direction and number of spots)
- Current mention count
- Mention surge ratio (if MENTION_SURGE fired)
- Bullet-point reasons for each surge type that fired
  - Velocity spike shown as positive: "Gaining popularity at **+2.80σ** above normal pace"
- **Rank Trend (last 8 data points):** rank history displayed as `450 → 380 → 290 → 210 → 140 → 80 → ...`
- **Signal Confidence (30d):** percentage of past surges confirmed by a Momentum Confirmation alert

---

## Update Logic

If a Popularity Surge alert already exists in Discord for this ticker, the system re-posts (updates the message) if **either** of these conditions is true:

1. The new mention ratio is >= 1.5x the mention ratio at the time of the previous post. (e.g., previous post had ratio 3.2x; current reading is 4.9x = 1.53x higher → re-post.)
2. The momentum acceleration logic triggers (see [Overview — Deduplication](overview.md#alert-deduplication-and-update-in-place)).

---

## 24-Hour Expiry

Once a surge is recorded, it remains active for up to 24 hours. If the [Momentum Confirmation](momentum_confirmation.md) alert does not fire within that window, the surge is marked `expired=TRUE` and removed from the active pool.

---

## Deduplication

Once a ticker is flagged as an active surge (`confirmed=FALSE`, `expired=FALSE`), it will not be flagged again from Tier 1. The `is_already_flagged()` check in the surge detection loop skips tickers that already have an active surge record.

---

## Would / Would Not Trigger

| Scenario | Triggers? | Reason |
|---|---|---|
| GME: rank 750 → rank 120 overnight | Yes | RANK_JUMP (gain=630, ratio=5.25) |
| Stock: rank 230 → rank 120 | No | RANK_JUMP ratio=0.92 < 1.5 |
| Stock: rank 120 → rank 40 | Yes | RANK_JUMP (gain=80, ratio=2.0) |
| NVDA (Tier 1): 200 mentions → 750 mentions | Yes | MENTION_SURGE (3.75x, base=200 ≥ 15, and 3.75x ≥ 5.0x threshold? No → not for Tier 1 unless ≥5x) |
| NVDA (Tier 1): 200 mentions → 1100 mentions | Yes | MENTION_SURGE (5.5x ≥ 5.0x Tier 1 threshold, base=200) |
| Stock (Tier 3): 6 mentions → 36 mentions (6x) | No | MENTION_SURGE base=6 < 15 min_base |
| Stock (Tier 3): 15 mentions → 50 mentions | Yes | MENTION_SURGE (3.33x, base=15) |
| Stock enters top 200 for first time, 15 mentions | Yes | NEW_ENTRANT |
| Stock enters at rank 350 for first time | No | NEW_ENTRANT requires rank ≤ 200 |
| Mention acceleration ≤ 0 (3+ history points) | No | Acceleration gate suppresses surge |
| Fewer than 3 history points | Evaluated normally | Gate skipped (insufficient history) |
| AAPL (Tier 1): velocity spike at 2.8σ | No | VELOCITY_SPIKE suppressed for Tier 1 |
