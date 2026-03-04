# Momentum Confirmation Alert

A Momentum Confirmation alert is the price/volume follow-through signal for a [Popularity Surge](popularity_surge.md). It answers the question: "That stock was lighting up Reddit — did the market actually respond?"

This alert can only fire for tickers that already have an active, unconfirmed popularity surge recorded in the database. It will not fire for tickers that are not in the surge pipeline.

---

## Relationship to Popularity Surge

```
Tier 1 (30 min)                        Tier 2 (5 min)
─────────────────                      ─────────────────────────────────
PopularitySurgeAlert fires    ─────►   Confirmation Pipeline checks each
surge saved to DB with                 active surge every 5 min
confirmed = FALSE
                                       evaluate_price_alert() for the ticker
                                            │
                                            ├── should_alert = True
                                            │     → MomentumConfirmationAlert
                                            │     → surge marked confirmed = TRUE
                                            │
                                            └── should_alert = False
                                                  → nothing fires, check again
                                                    in 5 min (up to 24h)
```

After a surge is confirmed, the surge record's `confirmed=TRUE` is set and no further Momentum Confirmation alerts can fire for that specific surge event. If the same ticker surges again days later, a new surge record will be created.

---

## Trigger Condition

The Confirmation Pipeline calls `evaluate_price_alert()` with the surge ticker's current classification and price data. This is **the same function used by all other pipelines** — the trigger logic is identical to Watchlist and Earnings alerts.

For non-blue-chip tickers, the trigger is:

```
abs(z-score) >= threshold

  where:
    z-score   = (pct_change - mean_return_20d) / std_return_20d
    threshold = 2.0σ  for meme and volatile
                2.5σ  for standard
```

For blue-chip tickers, see the [Blue Chip strategy in Market Alert](market_alert.md#blue-chip-strategy).

There is no additional composite score gate for Momentum Confirmation. If `evaluate_price_alert()` fires, the confirmation alert fires.

---

## What the Alert Shows

In addition to the standard price/volume stats, the Momentum Confirmation embed includes:

- **Price change since surge flagged** — the percentage gain or loss since the Popularity Surge was first detected. Example: "up 4.2% since the surge was detected 2 hours ago." Computed as `(current_price - price_at_flag) / price_at_flag * 100`.
- **Original surge types** — the MENTION_SURGE / RANK_JUMP / NEW_ENTRANT / VELOCITY_SPIKE labels from when the Tier 1 alert fired.
- **Z-score** — how many standard deviations the current price move is from the 20-day mean.
- **Move percentile** — where today's move ranks among the last 60 days of daily returns.
- **Classification** — the ticker's assigned StockClass.
- **Signal type** — `mean_reversion`, `trend_breakout`, or `unusual_move` (blue chips only show the first two).
- **BB position / Confluence** (blue chips only) — Bollinger Band position and technical signal count.

---

## Examples

**Example 1 — Confirmation fires**

GME surges in popularity at 10:00 AM with a RANK_JUMP. Price at that moment: $18.50. By 1:30 PM, GME is up 8.6% on the day. Its 20-day mean return is 1.2%, std is 5.1%.

```
z-score = (8.6 - 1.2) / 5.1 = 1.45  (GME is meme class, threshold 2.0)
```

Not enough — z-score 1.45 < 2.0. No confirmation yet.

By 2:15 PM, GME is up 11.8%.

```
z-score = (11.8 - 1.2) / 5.1 = 2.08 >= 2.0  → TRIGGER
price_change_since_flag = (current_price - 18.50) / 18.50 * 100 ≈ +11.2%
```

MomentumConfirmationAlert fires, showing "+11.2% since surge flagged at 10:00 AM · Original surge: Rank Jump."

**Example 2 — No confirmation, surge expires**

A stock (standard class) enters the top 200 at 9:30 AM and gets a NEW_ENTRANT alert. The stock drifts +0.8% through the day. Its 20-day mean return is 0.3%, std is 1.4%.

```
z-score = (0.8 - 0.3) / 1.4 = 0.36  (below 2.5 standard threshold)
```

No confirmation fires. After 24 hours the surge record is marked `expired=TRUE`.

---

## Would / Would Not Trigger

| Scenario | Triggers? | Reason |
|---|---|---|
| MEME ticker surged 4h ago; now up 12% with z-score 2.3 | Yes | z-score ≥ 2.0 (meme threshold) |
| STANDARD ticker surged; now up 4% with z-score 2.1 | No | z-score < 2.5 (standard threshold) |
| STANDARD ticker surged; now up 5.5% with z-score 2.7 | Yes | z-score ≥ 2.5 |
| Ticker surged 25h ago, still unconfirmed | No | Surge expired after 24h, removed from active pool |
| Ticker already confirmed earlier today | No | `confirmed=TRUE` — excluded from active surge list |
