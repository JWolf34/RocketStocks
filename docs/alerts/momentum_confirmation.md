# Momentum Confirmation Alert

A Momentum Confirmation alert is the price follow-through signal for a [Popularity Surge](popularity_surge.md). It answers the question: "That stock was lighting up Reddit — did the market actually respond after the surge was detected?"

This alert can only fire for tickers that already have an active, unconfirmed popularity surge recorded in the database. It will not fire for tickers that are not in the surge pipeline.

---

## Relationship to Popularity Surge

```
Tier 1 (30 min)                        Tier 2 (5 min)
─────────────────                      ─────────────────────────────────
PopularitySurgeAlert fires    ─────►   Confirmation Pipeline checks each
surge saved to DB with                 active surge every 5 min
confirmed = FALSE                      (after 15-minute minimum delay)

                                       evaluate_confirmation() for the ticker
                                            │
                                            ├── should_confirm = True
                                            │     → MomentumConfirmationAlert
                                            │     → surge marked confirmed = TRUE
                                            │
                                            └── should_confirm = False
                                                  → nothing fires, check again
                                                    in 5 min (up to 24h)
```

After a surge is confirmed, the surge record's `confirmed=TRUE` is set and no further Momentum Confirmation alerts can fire for that specific surge event. If the same ticker surges again later, a new surge record will be created.

---

## 15-Minute Delay

The Confirmation Pipeline waits at least **15 minutes** after a surge is flagged before checking for price confirmation. This ensures the confirmation measures *subsequent* price action — not the same move that was happening when the surge was detected.

---

## Trigger Condition

The Confirmation Pipeline calls `evaluate_confirmation()` from `core/analysis/alert_strategy.py`. Unlike the Watchlist and Earnings pipelines, which use `evaluate_price_alert()` on the current intraday move, this function measures only the price change **since the surge was flagged**:

```
pct_change_since_flag = (current_price - price_at_flag) / price_at_flag * 100
zscore_since_flag     = (pct_change_since_flag - mean_return_20d) / std_return_20d

Confirms when: abs(zscore_since_flag) >= 1.5
```

The z-score uses the **ticker's own 20-day return distribution** (mean and standard deviation), not a generic class-based threshold. A z-score of 1.5 means the price move since flagging is statistically significant for that specific stock.

**Sustained direction check:** When 2 or more prior observations exist (the pipeline has already evaluated this surge multiple times), all previous `pct_change_since_flag` readings must be moving in the same direction as the current reading. This prevents confirmation when price spiked briefly after the surge and then reversed.

---

## What the Alert Shows

- **Link to original Popularity Surge alert** — clickable hyperlink to the Discord message that started the event
- **Time since surge flagged** — "Popularity surge detected 2h 15m ago"
- **Price change since flag** — "up X.X% since flagged at $XX.XX"
- **Z-Score Since Flag** — how statistically significant the move is relative to the ticker's own distribution
- **Sustained** — whether the direction has held across multiple observations
- **Original surge types** — the MENTION_SURGE / RANK_JUMP / NEW_ENTRANT / VELOCITY_SPIKE labels from when the Tier 1 alert fired
- **Signal Confidence (30d)** — percentage of past popularity surges that were confirmed by price action

---

## Examples

**Example 1 — Confirmation fires**

GME surges in popularity at 10:00 AM with a RANK_JUMP. Price at that moment: $18.50. The surge is recorded. By 12:10 PM (130 min later — well past the 15-min delay), GME is at $20.57.

GME 20-day stats: mean_return = 1.2%, std_return = 5.1%.

```
pct_change_since_flag = (20.57 - 18.50) / 18.50 * 100 = +11.2%
zscore_since_flag = (11.2 - 1.2) / 5.1 = 1.96  ≥ 1.5  → TRIGGER
```

MomentumConfirmationAlert fires, showing "+11.2% since surge flagged at 10:00 AM · 2 hours 10 min ago · Original surge: Rank Jump."

**Example 2 — 15-minute delay prevents early fire**

At 10:10 AM (10 min after surge), the pipeline checks. Price is up 3%.

```
Time elapsed: 10 min < 15 min minimum → skipped
```

At 10:20 AM (20 min after surge), the pipeline checks again and evaluates normally.

**Example 3 — No confirmation, surge expires**

A stock (standard class) enters the top 200 at 9:30 AM and gets a NEW_ENTRANT alert. The stock drifts +0.8% through the day. Its 20-day stats: mean_return = 0.3%, std = 1.4%.

```
pct_change_since_flag ≈ 0.8%
zscore_since_flag = (0.8 - 0.3) / 1.4 = 0.36  < 1.5
```

No confirmation fires. After 24 hours the surge record is marked `expired=TRUE`.

---

## Would / Would Not Trigger

| Scenario | Triggers? | Reason |
|---|---|---|
| zscore_since_flag = 1.6, 20 min after surge | Yes | ≥ 1.5 threshold, past 15-min delay |
| zscore_since_flag = 1.6, 10 min after surge | No | 15-minute delay not yet elapsed |
| zscore_since_flag = 1.3 | No | Below 1.5 threshold |
| Price spiked then reversed (2+ obs, mixed direction) | No | Sustained direction check fails |
| Surge 25h ago, still unconfirmed | No | Surge expired after 24h |
| Ticker already confirmed earlier today | No | `confirmed=TRUE` — excluded from active surge list |
