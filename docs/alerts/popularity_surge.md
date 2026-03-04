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

Any ticker with fewer than **5 mentions** is skipped. Low-mention stocks produce noisy rank fluctuations (jumping from rank 950 to rank 700 might mean 1 mention instead of 0) that are not meaningful signals.

---

## Surge Types

Multiple surge types can fire simultaneously for the same ticker. Each type checks a different dimension of popularity movement.

### MENTION_SURGE

```
mentions / mentions_24h_ago >= 3.0
```

The current mention count is at least 3x the count from 24 hours ago. This is the clearest signal of a sudden spike in discussion.

- **Example (triggers):** NVDA had 200 mentions yesterday; today it has 750. Ratio = 3.75x → MENTION_SURGE.
- **Example (does not trigger):** NVDA had 200 mentions yesterday; today it has 540. Ratio = 2.7x → no surge.

### RANK_JUMP

```
rank_24h_ago - current_rank >= 100
```

The ticker gained at least 100 spots in the ranking (rank number dropped by 100 or more, meaning it became more popular). This captures stocks climbing quickly through the rankings even if their raw mention count is moderate.

- **Example (triggers):** A stock was ranked #620 yesterday and is now ranked #480. Gained 140 spots → RANK_JUMP.
- **Example (does not trigger):** A stock moved from #350 to #310. Gained 40 spots → no surge.

### NEW_ENTRANT

```
current_rank <= 200  AND  rank_24h_ago is None
```

The ticker just entered the top 200 for the first time — it had no rank 24 hours ago (meaning it was not in the top 1,000 or had zero mentions). Combined with MENTION_SURGE when mention count also jumped.

- **Example (triggers):** GME was not in the ApeWisdom top 1,000 yesterday; today it appears at rank #120. No prior rank → NEW_ENTRANT. If it also has 3x more mentions → MENTION_SURGE fires too.
- **Example (does not trigger):** A ticker enters at rank #350 having not been ranked yesterday → no NEW_ENTRANT (above the 200 cutoff). However, if it satisfies MENTION_SURGE or RANK_JUMP criteria, those can still fire.

### VELOCITY_SPIKE

```
rank_velocity_zscore <= -2.5
```

The rank improvement velocity (rate of rank-number decrease per 5-period window) is more than 2.5 standard deviations below its 30-day mean. A negative z-score means the rank number is dropping faster than usual — i.e., the stock is gaining popularity at an accelerating rate. This catches sustained climbers that haven't yet triggered one of the threshold-based checks.

The rank velocity is computed over a 5-period window; the z-score baseline uses the last 30 days of popularity history.

- **Example (triggers):** A stock has been slowly climbing in rank for weeks. Over the last few data points its rank-drop rate suddenly accelerates to 2.8σ below the mean → VELOCITY_SPIKE.

---

## What the Alert Shows

The Discord embed includes:
- Current price and intraday % change
- Current rank and rank 24 hours ago
- Rank change (direction and number of spots)
- Current mention count
- Mention surge ratio (if MENTION_SURGE fired)
- Bullet-point reasons for each surge type that fired

---

## Update Logic

If a Popularity Surge alert already exists in Discord for this ticker, the system re-posts (updates the message) if **either** of these conditions is true:

1. The new mention ratio is >= 1.5x the mention ratio at the time of the previous post. (e.g., previous post had ratio 3.2x; current reading is 4.9x = 1.53x higher → re-post.)
2. The momentum acceleration logic triggers (see [Overview — Deduplication](overview.md#alert-deduplication-and-update-in-place)).

---

## 24-Hour Expiry

Once a surge is recorded, it remains active for up to 24 hours. If the [Momentum Confirmation](momentum_confirmation.md) alert does not fire within that window, the surge is marked `expired=TRUE` and removed from the active pool. Expired surges are no longer evaluated by the Confirmation Pipeline.

---

## Deduplication

Once a ticker is flagged as an active surge (`confirmed=FALSE`, `expired=FALSE`), it will not be flagged again from Tier 1. The `is_already_flagged()` check in the surge detection loop skips tickers that already have an active surge record. This prevents duplicate surge posts for the same event.

---

## Would / Would Not Trigger

| Scenario | Triggers? | Reason |
|---|---|---|
| GME: rank 850 → rank 120 overnight | Yes | RANK_JUMP (730 spots) + possibly NEW_ENTRANT if previously unranked |
| NVDA: 200 mentions → 750 mentions | Yes | MENTION_SURGE (3.75x) |
| Stock: rank 300 → rank 260 | No | Only 40 spots gained, below 100-spot threshold |
| Stock: 8 mentions → 20 mentions | No | Ratio 2.5x, below 3.0x threshold |
| Stock: 3 mentions → 15 mentions | No | Only 3 mentions (below min 5 mentions filter) |
| Stock enters top 200 for first time, 10 mentions | Yes | NEW_ENTRANT |
| Stock enters at rank 350 for first time | No | NEW_ENTRANT requires rank ≤ 200 |
