# Triggers and Alert selection run in ingest; delivery is NOTIFY plus sweep

Deciding that a stock is worth attention must keep running when Discord or the bot is down, and the bot must be restartable at will without losing Alerts.
We decided that ingest evaluates every Trigger and selects Alerts, and the bot only delivers them.

- **ingest** evaluates Triggers, turns hits into candidates, and applies the selector (one Alert per stock per day, the daily budget, ranking by how Unusual a candidate is, and the stale rule).
  It writes a row for every candidate, whether it is selected for posting or not, and marks the ones it selected.
- **Delivery:** ingest writes the Alert row and issues NOTIFY in one transaction, so the write and the signal commit together.
  The bot LISTENs, but NOTIFY is only latency.
  The correctness mechanism is the bot's sweep of selected Alerts that have not been posted yet (no `posted_at`), run on every reconnect and periodically while connected.
  A lost NOTIFY, a bot outage or a mid-post crash are all covered by the next sweep.
- **Nothing posts twice:** an Alert is marked posted once it is posted, and the sweep skips posted Alerts.
- **The bot never calls a vendor and never evaluates Triggers.**
  It renders and posts what the Alert row and the stored data already say.
- **Stale rule:** after an outage, Alerts older than about an hour are recorded but not posted, because a stale Alert is worse than none.

## Consequences

- Worst-case latency when a NOTIFY is lost is one sweep interval: bounded and visible.
- A bot crash or redeploy never stops collection.
- The Alert table is the contract between ingest and any later consumer, which brings its own completion marker and sweep.
- Logic a reader expects "in the bot" lives in ingest.

## Considered options

- Evaluating Triggers in the bot: couples deciding to Discord latency and uptime, and stops detection when the bot is down.
- Posting every unposted Alert on sweep: rejected, it floods the channel with stale Alerts after an outage.
- A separate outbox table or message broker: rejected, the Alert row is already the durable record.
