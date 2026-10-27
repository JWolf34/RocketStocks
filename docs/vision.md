# RocketStocks vision and build plan

Status: drafted 2026-10-20 from a first-principles audit; approved 2026-10-20.
This document is the only authority for what RocketStocks is and how it gets built.
It replaced `docs/foundation/` (charter, roadmap, architecture, specs) and the ADRs in `docs/v2/adr/`, both archived under tag `v2-foundation-archive-2026-10`.
When reality disagrees with it, rewrite the affected lines; never append amendments.

## Purpose

RocketStocks scans the whole market for stocks worth your time that you would not otherwise hear about.
For each one it shows its own data, including how similar alerts played out before, and it hands an AI research pass trusted collected data on any stock, so a full picture comes fast.
The goal is an educated decision, not a guaranteed edge.

**The one job:** post a small number of stocks per day that are worth talking about, in a private Discord server.
Each Alert is short, with a talking point and an honest Track record, and comes with a Report of RocketStocks' own data: Popularity, options, price and volume, Catalysts and Track record.
Its collected data is available to John's own AI research for any stock; its judgments (Alerts, Outcomes, Track records) stay in Discord, and the analysis belongs to the research agent.

It is working when:

- it regularly flags stocks before Reddit or news does;
- every Alert carries a Track record its readers trust;
- going from "this caught my eye" to a full research picture takes minutes, not an afternoon.

## Users

John and his friends are both primary users, in one private Discord server per installation.
Discord is the main surface; friends use Alerts, Reports and (later) paper trading there.
John's research runs in his own Claude, on RocketStocks' collected data, at no cost to the app.
RocketStocks is installable by another small private group (nothing hard-wired to one server), but it is never a hosted multi-server bot.

## Glossary

| Term | Meaning | Avoid |
|---|---|---|
| **Popularity** | How much a stock is discussed on Reddit (rank, mentions, upvotes from ApeWisdom), as a history over time | attention, buzz |
| **Popularity snapshot** | One poll's reading for one stock at one time | |
| **Universe** | Every listed stock RocketStocks tracks on a given date, each marked **optionable** or not | symbol master, market |
| **Baseline** | A stock's own recent normal for a measure (its typical call volume, its typical Popularity rank) | threshold, average |
| **Unusual** | Far from the stock's own Baseline; never a fixed cutoff shared by all stocks | high, big |
| **Trigger** | A named, versioned rule that decides a stock is worth attention now | signal, alert type, detector |
| **Alert** | A short Discord post that one stock is worth attention now, with its Triggers as the reasons, a talking point, the Track record and the next Catalyst | notification, signal |
| **Report** (final name to be decided) | The one per-stock view of RocketStocks' own data, attached to Alerts and available on demand | dossier, stock report, data card |
| **Outcome** | What a stock did after a Trigger hit, posted or not, at 1, 5 and 20 trading days | result, return |
| **Track record** | Outcomes summarised for one Trigger (and later common Trigger combinations), saying "too few cases" when that is true | base rate, confidence |
| **Catalyst** | A scheduled event that could move a stock; today only the earnings date | event, news |
| **Earnings preview** | The weekly post ranking the week's earnings reporters by RocketStocks' data | digest, screener |
| **Paper trade / Leaderboard** | Pretend-money buys and sells, and the ranking of players | portfolio sim |
| **As-of time** | When a fact was true; every number shown carries one | timestamp |
| **Ops ping** | A message to the operator that something needs a human | alert (reserved for stocks) |
| **Tombstone** | A stored record that a fetch was attempted and failed; it means "no observation", never zero | |

## Scope

**In:**

- Triggers on Popularity, options activity, and Unusual price and volume.
- Alerts: at most one per stock per day; later Triggers on the same stock become replies in the Alert's thread; a hard daily budget (about 5 to start, tuned in dev); the strongest candidates win, ranked by how Unusual they are; every candidate is recorded, posted or not.
- Role pings by Trigger, so people opt in to what pings them.
- The Report, attached to every Alert and on demand with `/report TICKER`, including a Popularity-plus-price chart.
- Track records and the rule for cutting Triggers.
- The weekly Earnings preview, ranked by options-implied move, Popularity and past earnings reactions.
- Export of all collected data (Popularity, the Universe, daily bars, options chains, earnings calendar with EPS estimates and actuals), never RocketStocks' judgments, to John's Claude through a read-only MCP server.
- The daily Universe snapshot, daily bars for every in-scope listing seeded back to 1990, the nightly full options chain sweep, and intraday polling.
- `/setup` (channels, roles, Schwab sign-in) and Ops pings.
- Paper trading, kept small (buy, sell, positions, Leaderboard), built after the core loop and treated as an experiment: it stays only if people use it.

**Follow-up after v2:** a backtesting framework over v2's stored history (v1's backtesting is retired).

**Out:** watchlists and watchlist alerts; gainers, losers, movers and other screeners or digests; on-demand public-data commands (quote, financials, fundamentals, analyst, ownership, insider, short interest, news, forecast, SEC filings); politician trades; technical breakout and momentum alerts; earnings reaction as an Alert type; max pain and IV rank as named features; EDGAR filing facts; fetching data on request for untracked stocks; delisted-ticker import; v1's research, scoring and classification tools; paper trading extras (pending orders, snapshots, roundups); AI-written reports inside the app; streaming; an auto-trader; a hosted multi-server bot.

**Kill test.** A capability stays only if it feeds a Trigger, feeds the Report, holds Alerts to account, or is on the keep-on-purpose list (paper trading; backtesting as a follow-up).
It must also answer: who uses it, how often, which decision it changes, and what breaks without it.

**Deferral test.** A question that can be answered at build time is not decided in a plan.

## Rules

- **Informational only.** RocketStocks describes what happened, why it is Unusual and what followed historically; descriptive direction ("call-heavy activity") is allowed, buy or sell calls, targets and sizes never are.
- **Wrong is worse than missing, and missing is worse than labelled.** Every number carries its As-of time; a fact that cannot be computed is shown as missing with the reason; a Trigger never fires on missing inputs; an Alert with missing supporting facts posts with them labelled and pings the operator.
- **Outcomes** follow a Trigger hit, posted or not, and start at the next tradable price after the Alert is posted (or after the hit, when it was not posted).
  Each records excess return over SPY (direction) and move size against the stock's own typical move (was it worth attention), at 1, 5 and 20 trading days.
  A Track record shows "too few cases" below about 30 Outcomes at a horizon.
- **Cutting Triggers.** A Trigger is a candidate for cutting when its stocks do not move unusually more often than the same stocks do on ordinary days.
- **Discord display.** Anything may be shown, raw Schwab data included; everything passes through one rendering layer so a stricter posture is one switch.
  Raw vendor data is never committed to git.

## Constraints

- **Money:** free tier only (Schwab with John's brokerage account, ApeWisdom, Nasdaq calendar and symbol files, Cboe symbol directory, SEC EDGAR for the earnings-date fallback, `exchange_calendars`), on the home-lab VM.
  Revisit a paid options feed only if a probe shows Schwab's chains cannot support a defined options Trigger.
- **Operator effort:** a weekly Schwab browser sign-in (the refresh token lives 7 days) plus answering Ops pings; everything else is automated or designed out.
- **Backups, disk monitoring and host health** are the operator's job, done with their own tools; the app documents what is irreplaceable and how to restore it.

## Architecture

- **Storage:** one Postgres with Timescale; compressed hypertables for options snapshots, Popularity and daily bars; plain tables for the rest; no Redis.
- **ingest** (long-running): the only Schwab caller and token holder, with an in-process schedule and one shared Schwab rate budget.
  - Every 30 minutes: Popularity snapshot.
  - Every 15 minutes in market hours: a batch quote of the whole Universe for Unusual price and volume, and chains for the stocks that currently stand out.
  - Nightly: the Universe snapshot, daily bars, the full chain sweep, earnings dates, Baselines, Outcomes and Track records.
  - Trigger hits become candidates; a selector applies one-per-stock-per-day, the daily budget, ranking and the stale rule, then writes Alerts.
  - Open-interest Triggers fire in a morning post, because open interest settles once a day.
- **bot** (long-running): owns Discord; delivers Alerts with their Report in a thread, `/report`, `/setup`, role pings, the Earnings preview and later `/trade`.
  It never calls a vendor and never evaluates Triggers.
  Kept separate from ingest so a bot crash or redeploy never stops irreplaceable collection.
- **Delivery:** ingest writes the Alert row and NOTIFYs in one transaction; the bot's sweep of unposted Alerts is the correctness mechanism; nothing posts twice.
- **Reads:** one library (`rs_query`) shared by the bot, the Track record job and the read-only MCP server; no REST API.
- **Per-service database roles** keep Schwab tokens away from the Discord-facing bot and give the MCP server a read-only role.

**Reliability** is defined by whether data can be fetched again:

| Data | Refetchable | Commitment |
|---|---|---|
| Popularity snapshots | No | Gaps tolerated, never silent |
| Options chain snapshots | No | Gaps tolerated as Tombstones a backtester can see |
| User state (paper trades, settings, roles) | No | Must survive; the operator backs it up |
| Daily bars | Yes | Repaired automatically; split check by overlap with stored bars |
| Intraday quotes | Not kept | Only the inputs that fired a Trigger are kept, with the Alert |
| Alerts | Re-derivable | At most one polling interval late; after an outage, Alerts older than about an hour are recorded but not posted |
| Baselines, Track records, Reports | Recomputable | Rebuilt at will |

Every source failure sends an Ops ping when it starts and another when it clears; a failure is a source failing (or more than about 5% of a sweep failing), not a single stock.
A dead Schwab token pauses Schwab jobs and the rest keep running; signing in again through `/setup schwab` resumes them.
Everything restarts unattended; there is no high availability, and hours of downtime are acceptable when the operator is told.

**Scale.** Only history grows: keep every chain contract compressed, indefinitely, and measure real compressed growth on the first nightly sweep (the old 20-40 MB a night estimate assumed unmeasured compression; at the only measured 4.7x, chains are about 12-21 GB a year).
If the measurement projects the disk past 80% within about two years, grow the disk; keep only contracts with open interest or volume only if the host has no room.
Triggers must be cheap to add and cut without disturbing Track records.
Stocks covered (about 13,000 listings, about 5,480 optionable), users, installations, request load and latency do not need to scale.

## Audit of the existing plan

The 2026-10-19 plan got its biggest calls right and those are kept: informational alerts with a measured track record, no paid data, weekly Schwab sign-in, raw display in a private server, no in-app AI write-ups, operator-owned backups, and a well-tested ingest core.
It diverges where it was built for its own charter instead of this one.
Evidence paths below refer to tag `v2-foundation-archive-2026-10`.

| Item | Plan says | Verdict | Reason | Evidence |
|---|---|---|---|---|
| Purpose | Push, pull and Data API research assistant | Re-decided | One job is discovery plus the Report; pull is one Report, not three | `docs/foundation/charter.md` |
| Alert shape | One alert per rule type, 24h cooldown per rule | Change | One Alert per stock per day with Triggers as reasons; v1 averaged 92 alerts a day in May 2026 | `architecture.md` "Alert payload"; v1 `alerts` by month |
| Daily budget | None | Add | Alert fatigue drove friends away | v1 `alerts` 911/1929/1295 in Apr/May/Jun 2026 |
| Stale alerts | Sweep posts every unposted alert | Change | Stale Alerts are worse than none | `architecture.md` "Delivery" |
| Track record measure | Direction only, hit rate and median excess, 1/5/10 days | Change | Add move size against the stock's own typical move; horizons 1/5/20 | `architecture.md` "Base rate" |
| Options alerts | After 250 sessions and a passed event study | Change | That gate was for a directional signal; an informational Trigger ships once Baselines exist and is held to account by the cut rule | `charter.md` follow-ups; `research/signal-evidence.md` |
| Popularity surge | Attention surge, the first alert | Keep, rename | Core Trigger; its Track record must be able to show the measured mild negative drift | `research/signal-evidence.md` results table |
| Earnings reaction, large move alert types | In at launch | Out as types | Widely visible; large move survives as the Unusual price and volume Trigger | `charter.md` scope |
| Reports | Stock, Options, Watchlist | Replace | One Report of RocketStocks' own data | `charter.md` scope |
| Watchlists, daily digest | In | Out | Yahoo covers stocks already followed; one personal watchlist exists in v1 prod | v1 `watchlists` |
| Earnings preview | Monday digest | Keep, reshape | Ranked by implied move, Popularity and past reactions | `roadmap.md` slice 5 |
| Stream band and 1-minute bars | Slice 4 | Out | 15-minute polling keeps lead time without a connection to babysit | `architecture.md` "Data flow" |
| Unwired stream and chain-scanner code | Kept for slice 4 | Delete | About 1,700 lines reachable only from tests and replay tools | `services/ingest/src/rs_ingest/{pipeline,aggregator,detector,chain_scanner,...}.py` |
| Collection requests | Slice 3 pull path | Out | Whole-market collection means the Report never needs untracked data | `architecture.md` "Data flow" |
| Data API | REST plus MCP in `serve` | Change | MCP only, read-only; `GET /v1/bars` has no consumer | `services/serve/src/rs_serve/bars.py` |
| EDGAR 8-K facts | In the facts block | Out | Your Claude fetches filings; one fewer vendor per Alert | `architecture.md` "Alert payload" |
| Symbol master spans | Listing spans with recycled-ticker identity | Change | A daily Universe snapshot records membership by date with no span logic | `specs/02-symbol-master-eod-scan.md` |
| Daily bar seed | From 1990 for every in-scope listing | Keep | John wants full depth for backtesting | spec 02 D-C |
| Index underlyings | `$SPX` and others in the sweep | Out | Their only reader was the Options Report | spec 02 D-I |
| Nightly chain sweep | Ladder, seven statuses, deadline, resume | Keep | Chains are irreplaceable; measured 502s and stale quotes justify it | `research/vendor-stack.md` "Measured Schwab facts" |
| Ops notices | After 3 consecutive failures | Change | First failure plus recovery, per source | `architecture.md` "The ingest daemon" |
| Ingest daemon, token gate, `/setup schwab` | Built | Keep | Exactly what unattended restarts and weekly sign-in need; rehearsals passed | issue #57 comments |
| Per-service DB roles | Built | Keep | Protects tokens from the internet-facing bot; read-only MCP | `tools/migrate/src/rs_migrate/roles.py` |
| Backup restore rehearsal | In every definition of done | Out of done | Backups are the operator's job | `docs/foundation/README.md` |
| Auto-trader | Post-v2 follow-up | Out | Not in the vision | `charter.md` follow-ups |
| v1 Popularity import | All history since 2025-06 | Narrow | 204-day hole in 2025; 2026-03-03 onward is near-complete | gap probe below |
| Domain language | Attention, base rate, liquid band, alert scope, signal | Replace | Glossary above; 100 uses of attention/base rate in v2 source | `CONTEXT.md`; grep of `libs services tools` |
| Process | Pre-grilled slice specs, ADR amendments, 8-slice roadmap | Change | Slice 2 was grilled through ten decisions while slice 1 had posted no alert (0 of 10 locally) and stored no base rate; spec 01's "half page" is 122 lines | `specs/01-attention-surge.md`; local `alerts`, `base_rate` |

## Build path

**Decision: continue v2 from `feat/v2-rewrite`, re-scoped to this document, starting with a context reset.**

- Restarting from v1 was rejected: about 80% of its 32k source lines are out of scope, its data has structural defects (non-session bars, epoch-zero rows, swallowed insert errors), and its alert engine produced 92 alerts a day.
- Starting from scratch was rejected: it would rebuild the daemon, token gate, sign-in flow, adapters and delivery that v2 already has, tested (930 passing) and rehearsed, and lifted code would arrive with the same comments.
- The real risk in continuing is that agents follow the old plan, which today is the authority in `CLAUDE.md`, the foundation docs, `CONTEXT.md`, memories, and 182 plan citations in 56 of 127 source files.
  Milestone 0 removes that authority before any feature work, and a cold-read check proves it.
  If the cold read still drifts after the reset, switch to a fresh tree; the cost of switching is small at that point.

## What happens to existing material

- **Archive tag** `v2-foundation-archive-2026-10` on `feat/v2-rewrite` before anything is removed.
- **`docs/foundation/`:** removed from the tree.
  `runbooks/slice-1.md` becomes `docs/operations.md` (import window, reduced rehearsals, a "what to back up and how to restore" section).
  The "Measured Schwab facts" section of `research/vendor-stack.md` becomes `docs/reference/schwab-facts.md`.
- **ADRs:** every file in `docs/v2/adr/` is archived.
  Six are rewritten fresh in `docs/adr/`: storage (from 0003); Triggers and selection in ingest with sweep delivery (from 0005 and 0008); Discord-ID identity (0013); Discord surface, mobile-first, never-edit with thread follow-ups (0014); failure semantics and Tombstones (0017); testing against a real database with an injected clock (0023).
  0001, 0004, 0010, 0015, 0027 and 0031 are superseded by this document and the code.
- **Specs:** spec 01 is archived.
  Spec 02 is superseded; its evidenced decisions (1990 seed, bar scope, rate budget and job order, split check, chain-size probe, resume, dead token mid-sweep, deadline, statuses, storage measurement, host-level disk alarm) feed milestone 3.
- **`CLAUDE.md`, `CONTEXT.md`, memories:** the v2 section of `CLAUDE.md` shrinks to a pointer here; `CONTEXT.md` becomes the glossary above; project memories about the old plan and build process are rewritten or deleted.
- **Code:** delete the unwired stream, chain-scanner and replay code; remove the `serve` service (REST route, API keys, client provisioning), since milestone 3 builds the MCP server; drop the EDGAR filing fact, keeping the EDGAR adapter only as the earnings-date fallback; rename to the glossary; strip plan citations; add a CI grep gate that fails on `ADR-NNNN`, `grilling #`, `slice N`, `attention` and `base_rate` in v2 source.
- **Branches and worktrees:** `slice-2/symbol-master-eod-scan`, `plan/v2-foundation`, `slice-1/attention-surge` and the `s1/*` branches are deleted locally and remotely once the tag exists; the 15 agent worktrees and `worktree-agent-*` branches are removed; `.claude/handoffs/` is cleared.
- **Tracker:** slice-1 tracker #57 and the foundation map tickets are closed as superseded, linking this document.
- **v1:** stays muted, collecting Popularity, until v2's Popularity poll runs on portainer; then it is stopped and its containers removed, freeing about 35 GB.
  Taking a final dump first is the operator's choice; v1 data beyond the Popularity import is disposable.

## Process rules

- One milestone in flight; the next is specified only after the current one is in use.
- A milestone spec is at most about 40 lines, lands with the code, and is rewritten in place when reality differs; there is no deviations log.
- No new ADR while building, unless a decision is contested and hard to reverse.
- Done means observed use by the people the milestone serves, plus green CI and the rehearsals the milestone touches (token expiry, ingest restart).
- At every milestone close, run the kill test over what shipped; unused capabilities are removed.
- This document stays under about 300 lines; it is rewritten, never appended.

## Milestones

Each ends in something used; there are no dates.

0. **Context reset.**
   Tag, archive and remove the old docs; write the six ADRs, the glossary, `docs/operations.md` and the Schwab facts reference; rewrite `CLAUDE.md` and memories; delete the dead code; rename and scrub the source; add the grep gate; clean up branches, worktrees and tickets.
   Used by every later agent session; done when the grep gate passes and a cold agent asked "what is RocketStocks and what gets built next?" answers with this document.
1. **Popularity Alerts in the friends' server.**
   Deploy v2 on portainer with the ops webhook set; import v1 Popularity from 2026-03-03; ship the Alert shape (one per stock per day, thread replies, daily budget, stale rule, role pings by Trigger) and Track records with both Outcome measures at 1/5/20 days; then retire v1.
   Done when friends receive Popularity Alerts carrying a seeded Track record, and the token-expiry and ingest-restart rehearsals pass on portainer.
2. **Nightly sweep.**
   Daily Universe snapshot with the optionable flag; daily bars for every in-scope listing from 1990 with the split check; the full chain sweep with probes, ladder, Tombstones, deadline and resume; the earnings calendar (dates, EPS estimates and actuals) stored as collected history; real compressed growth measured against the disk.
   Done when a recorded evening and the next morning's open-interest pass finish inside their deadlines and compressed growth is measured against the disk; its use is observed in milestone 3.
3. **John's research through MCP.**
   A read-only MCP server over `rs_query` serving collected data for any stock: Popularity history, the Universe, daily bars, options chains and earnings dates; no Alerts, Outcomes or Track records.
   Done when John's research agent, working from its own instructions, builds an analysis of a stock he picks from data pulled through MCP.
4. **The Report.**
   `/report TICKER` and a Report in every Alert's thread: Popularity-plus-price chart, price and volume against Baseline and against the market, next earnings, past Alerts on the stock and the Track record.
   Done when friends open Reports from Alerts and on demand.
5. **Intraday Triggers and options.**
   15-minute Universe quotes with the Unusual price and volume Trigger; the options activity Trigger (intraday volume against open interest on stand-out chains, and a morning open-interest change Trigger once the settle time is probed); the options section of the Report; ranking by how Unusual a candidate is.
   Done when Alerts with price, volume or options reasons post within the budget and get discussed.
6. **Earnings preview.**
   A weekly post ranking the coming week's reporters by implied move, Popularity and past earnings reactions.
   Done when friends use it to pick what to watch that week.
7. **Paper trading experiment.**
   `/trade buy|sell`, positions and a Leaderboard, priced from the 15-minute Universe quotes.
   Done when friends place trades; removed at a later kill-test review if they stop.

Follow-up after milestone 7: the backtesting framework over stored history.

## Evidence

- Gates on `feat/v2-rewrite` (2026-10-20): `ruff check libs services tools` "All checks passed!"; `mypy` "Success: no issues found in 127 source files"; `lint-imports` "Contracts: 7 kept, 0 broken."; `scripts/check_clock_discipline.sh` OK; `pytest libs services tools` with `RS_TEST_DB_URL` "930 passed"; `pytest legacy/tests` "2804 passed".
- Local trial stack (2026-10-20): 10 attention-surge alerts stored, 0 posted, 0 `base_rate` rows; Schwab token dead and every price fact "unavailable"; ops webhook unset.
- v1 production (probe run by John, 2026-10-20): Popularity 2025-06-02 to 2026-10-20, 11.2M rows, with a 204-day gap from 2025-07-31 to 2026-02-21 and near-complete 30-minute polls since 2026-03-03; 4 paper trades by one user; one personal watchlist; alert roles in two servers; options chains for May and June 2026 only (31M rows, about 8.3 GB); daily prices end 2026-07-02 and include non-session dates.
- Schwab measured facts (July and August 2026): 120 requests per minute documented, 240 per minute burst clean; unfiltered SPY and QQQ chains return 502; about 4% sporadic 502s on bounded chains; open interest does not change intraday; about 5,480 optionable names; a full chain pass is about 68 minutes (`docs/reference/schwab-facts.md`).
- Signal study (2026-10-19): no v1 alert type shows an edge; posted Popularity surges drifted negative (adjusted 10-day excess -1.44%, interval -2.61 to -0.21) (`docs/foundation/research/signal-evidence.md` at tag `v2-foundation-archive-2026-10`).
