# Milestone 1: Popularity Alerts in the friends' server

Friends receive a few Popularity Alerts a day, each carrying a Track record seeded from history, and v1 is retired.
`docs/vision.md` is the authority; this spec holds only what a builder cannot decide alone, and is rewritten in place when reality differs.

## Selection

- Every Trigger hit is stored as a candidate with the selector's verdict (posted, reply, below bar, over budget, repeat, stale); `alert_type` becomes `trigger` throughout.
- Strength is a candidate's percentile among its own Trigger's hits of the 30 Eastern days before its day, replayed from stored Popularity; the raw score measures distance from the stock's own Popularity Baseline and is defined and tested at build time on the imported history.
- A candidate posts at once when its strength clears a bar set from those 30 days so a typical day yields about the budget (5 to start), and the day's budget is not used up.
- A day is the US Eastern calendar day, for the budget and for one Alert per stock per day.
- A later candidate on a stock already alerted that day replies in the Alert's thread only when it escalates: a sub-rule the Alert did not carry, or strength up by a margin tuned at build time, at most a few replies per stock per day.
- Replies never ping roles and never use the budget; a stock below the bar earlier can still get its first Alert later that day, and one over budget cannot, since the budget never frees within a day.

## Delivery

- The bot commits a sending mark before each send, and every send carries the Alert's or reply's id as its Discord nonce.
- A row found still sending is reconciled against the channel's or thread's recent history: the bot's own message with that id in its embed footer found means record it, not found means send, and a failed history read means do not post and send an Ops ping.
- The posted message id is stored, and the Alert's thread is created from it; a row more than an hour past its hit time when it would be sent is recorded stale and not posted.
- The Alert shows a templated one-line talking point, the reasons, the Track record, the next Catalyst and the price; Volume moves to the Report and the stored SPY benchmark fact is removed.
- Every time shown uses Discord timestamp markup, so each reader sees their own zone.

## Outcomes and Track records

- A case is a Trigger version's first candidate on a stock on an Eastern day, timed at the post of the stock's Alert that day if one posted, otherwise at that first hit.
- Entry is the open of the first session that begins after that time; the h-day exit is the close of the h-th session counting the entry session, for h of 1, 5 and 20; SPY is measured over the same sessions.
- Direction is excess return over SPY.
- Move size is the absolute excess move over the stock's typical h-day move: 1.4826 times the median absolute deviation of its daily excess moves over the 250 sessions before entry, times the square root of h; under about 120 sessions there is no move size and the case is counted as such.
- Per horizon the Track record shows the share of cases at 2x typical or more, the same stocks' measured share over their ordinary windows that year, and the median multiple.
- Seeding is a replay of the Trigger over all stored Popularity; a nightly job adds new cases and matured Outcomes; the reconstructed and logged split is removed.
- The Track record shows how many of its cases are live, and "too few cases" below 30 Outcomes at a horizon.
- Daily bars must be confirmed split-adjusted, or adjusted, before Outcomes are trusted.

## Discord, Ops pings, v1

- `/pings` gives every member a private menu to toggle their Trigger roles; `/setup roles add|remove|show` maps a Trigger to an existing role; `/setup` is hidden from members.
- The bot needs View Channels, Send Messages, Embed Links, Read Message History, Create Public Threads, Send Messages in Threads and Manage Roles, and its role must sit above every role it manages (`docs/operations.md`).
- Ops pings fire per source on first failure and on recovery, replacing the three-failure threshold.
- After the import passes, the operator stops and removes v1's containers; deleting the public ghcr image is the operator's choice.
- Then tag `v1-final` and delete `legacy/`, the v1 Dockerfile and compose files, `docker-publish.yml`, the legacy CI job and the v1 parts of `CLAUDE.md` and `.claude/docs/`; the v2 compose file and Dockerfile take the plain names.
- Rewritten in place: the vision's Outcomes line and glossary (an Outcome follows a Trigger hit, posted or not), ADR 0004's four commands, and `docs/operations.md`.

## Done

Friends receive Popularity Alerts carrying a seeded Track record, and the token-expiry and ingest-restart rehearsals pass on portainer.
