# RocketStocks

RocketStocks scans the whole market for stocks worth your time that you would not otherwise hear about, and posts a small number of them per day in a private Discord server.
This glossary is the shared vocabulary; `docs/vision.md` says what is built.

## Language

**Popularity**:
How much a stock is discussed on Reddit (rank, mentions, upvotes from ApeWisdom), as a history over time.
_Avoid_: attention, buzz

**Popularity snapshot**:
One poll's reading of Popularity for one stock at one time.

**Universe**:
Every listed stock RocketStocks tracks on a given date, each marked **optionable** or not.
_Avoid_: symbol master, market

**Baseline**:
A stock's own recent normal for a measure (its typical call volume, its typical Popularity rank).
_Avoid_: threshold, average

**Unusual**:
Far from the stock's own Baseline; never a fixed cutoff shared by all stocks.
_Avoid_: high, big

**Trigger**:
A named, versioned rule that decides a stock is worth attention now.
_Avoid_: signal, alert type, detector

**Alert**:
A short Discord post that one stock is worth attention now, with its Triggers as the reasons, a talking point, the Track record and the next Catalyst.
_Avoid_: notification, signal

**Report** (final name to be decided):
The one per-stock view of RocketStocks' own data, attached to Alerts and available on demand.
_Avoid_: dossier, stock report, data card

**Outcome**:
What a stock did after an Alert, at 1, 5 and 20 trading days.
_Avoid_: result, return

**Track record**:
Outcomes summarised for one Trigger (and later common Trigger combinations), saying "too few cases" when that is true.
_Avoid_: base rate, confidence

**Catalyst**:
A scheduled event that could move a stock; today only the earnings date.
_Avoid_: event, news

**Earnings preview**:
The weekly post ranking the week's earnings reporters by RocketStocks' data.
_Avoid_: digest, screener

**Paper trade / Leaderboard**:
Pretend-money buys and sells, and the ranking of players.
_Avoid_: portfolio sim

**As-of time**:
When a fact was true; every number shown carries one.
_Avoid_: timestamp

**Ops ping**:
A message to the operator that something needs a human.
_Avoid_: alert (reserved for stocks)

**Tombstone**:
A stored record that a fetch was attempted and failed; it means "no observation", never zero.

## Relationships

- A **Popularity** history is made of **Popularity snapshots**.
- A stock is **Unusual** when it is far from its own **Baseline**.
- A **Trigger** decides a stock is worth attention now; an **Alert** carries its **Triggers** as the reasons.
- An **Alert** carries a talking point, the **Track record** and the next **Catalyst**, and comes with a **Report**.
- An **Outcome** is what a stock did after an **Alert**; a **Track record** summarises **Outcomes** for one **Trigger**.
- An **Alert** is only ever about a stock; a message to the operator is an **Ops ping**.
