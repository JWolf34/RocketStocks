# RocketStocks v2 plan audit

Scope: the PLAN (docs), not the code. Read-only. Audited 2026-10-19 against branch `phase-2-spec-010` (HEAD 986feb7), plus `git show` of `phase-2-spec-009`, `phase-2-spec-011`, `feat/phase-2-universemanager-design`, `docs/phase1-close-phase2-plan`, `feat/v2-rewrite`, the two worktrees under `.claude/worktrees/`, two stashes, and the GitHub issue tracker (34 issues, read with `gh`).

Conventions: ADR-NNNN = `docs/v2/adr/NNNN-*.md`; UM = `docs/v2/universe-manager.md` (decisions D1-D11); P2 = `docs/v2/phase2-plan.md`; RM = `docs/v2/roadmap.md`; BRIEF = `docs/v2/architecture-brief.md`; #N = GitHub issue. Quotes have em dashes normalized to plain hyphens.

Caveat on visibility: the tracker (#6, #13, #25) says the S009, S010 and S011 spec drafts were completed around 2026-10-03/04 and an "ADR-0033" exists. None of those files exist in any local ref, stash, or worktree I could see (`find / -name 'S010-*'` finds nothing; no ADR-0033 file). Everything said below about Phase 2 spec state for S009 to S011 therefore comes from issue text, not from repo files.

---

## 0. Bottom line

1. The plan has been de-scoped well in places (auto-trader, UNLOGGED cache, charting, EDA, news sources, six alert types as parity), but what remains is still sized for a multi-user platform: three services, a whole-listed-universe symbol master, a two-band universe manager, three datasets (one of them 10k symbols x 1-minute bars nightly with no v2 consumer), a public API for one external consumer, nine cutover gates and a 3-4 week burn-in.
2. The product bet (ADR-0030/0032: retire all six working v1 alerts, ship ONE new alert inferred from 5-minute REST chain snapshots) has no predictive evidence behind it. The spike proved the data EXISTS and is MEASURABLE; it did not test whether the signal PREDICTS anything. No gate in G1-G9 tests usefulness. G7 can pass on "silence justified by a replay fixture".
3. Implementation has been fast when a spec existed (S003-S007 in about 10 days; S008 in about 2). The bottleneck is spec design: since 2026-07-17 (13 weeks) the repo has zero v2 code commits and zero merged specs; 3 spec "maps" with 34 tickets and about 29.7k words of tracker text exist. Phase 2's own appetite (6-8 weeks, "cut scope, never extend time", midpoint re-spec) expired around 2026-09-09 with no recorded cut or re-spec.
4. Nothing user-visible exists in v2 and the plan puts all user value at the very end (cutover after Phase 3, a 3-4 week private burn-in and nine gates). The one input that only time can supply (per-subject baseline history, "15-20 sessions") is not accumulating, because no unattended multi-session capture daemon exists yet (the daemon loop was moved into S010).
5. Roughly 20-25% of remaining weighted work sits on the shortest path to a first options-flow alert in the dev channel; about 60-65% is needed at cutover but sits beside that path (serve, Cowork, reports, paper trading, /setup, symbol master, EOD scan, freshness, gates); about 15% has no v2 consumer at all (whole-universe 1-minute history, stream-vs-REST gauge, bar-type rename, etc).
6. The cheapest, highest-value missing step: test the thesis with an event study on data that may already exist. v1 (on `main` since about 2026-05-20) schedules a nightly per-contract options-chain snapshot for ALL tickers (`legacy/.../core/scheduler/jobs.py:197-224`, Tue-Sat 21:30 CT). If that job has been running, there are about 5 months of daily OI/volume/IV for the v1 universe in production Postgres, which is enough to test "day-over-day OI build predicts 1/3/5/10-day returns" before building the Phase-2 detector stack. The plan treats all pre-cutover data as "disposable" and options history as "starts empty at cutover" and never mentions it.

---

## 1. Product definition

### What the plan says the product IS

- ADR-0029 (Accepted 2026-06-11), the only thesis statement: "RocketStocks v2 performs intraday detection of swing-relevant events for retail traders." Two clocks: detection is intraday (5-minute bar closes and 5-minute options-chain scans); the claim is swing ("the intraday observation predicts a multi-day-to-multi-week move in the underlying"), validated by event studies at 1d/3d/5d/10d. Boundaries: "Options-flow-first ... the underlying equity is the tradeable"; "Not HFT / not intraday execution guidance"; "Advisory, human-filtered".
- ADR-0030 (2026-06-12): "v2 ships exactly ONE alert class: the options-flow alert." "All six v1 alert types are retired: popularity surge, momentum confirmation, volume accumulation, breakout, watchlist mover, earnings mover." North star is amended: "parity with v1 applies to platform surfaces - watchlists, manual paper trading, reports, screeners, admin/config - not to detection."
- BRIEF "Scope discipline": "v2's north star is platform parity with v1 (watchlists, paper trading, reports, screeners, admin) plus the Cowork consumer, plus ONE novel options-flow alert class - before any further net-new features."
- ADR-0032 D0 defines the alert: "a burst: a concentrated, abnormal run of opening-consistent options volume on one side of an underlying's near-dated chain, caught while it is happening." Its own context: "'Someone just bought X' is outside this dataset; 'abnormal activity is accumulating' is inside it." D1: "Side, not direction: at REST cadence there is no aggressor side, so call volume != bullish."
- Stated #1 goal is reliability/data capture, not alert usefulness: ADR-0028 "'reliably store future data' - the project's #1 goal"; ADR-0023 "Reliability is v2's #1 goal"; ADR-0018 "reliability as #1 goal".

### Who the users are

Never quantified anywhere in `docs/v2`. The only characterizations: "retail traders" (ADR-0029), "majority-mobile Discord audience" (ADR-0014, ADR-0026), "single operator, one box, no HA requirement" (ADR-0031), "self-hostable"/"self-hosters" (ADR-0012, ADR-0016, ADR-0031), and a single named external consumer, "Cowork.ai ... an LLM agent platform that pulls stock/industry data to build reports. It is the reason serve is a first-class, authenticated, versioned contract" (BRIEF). Cowork's stated profile is "off-hours operation, DB-history-mostly, read-through for <=5-10 tickers per session" (ADR-0024), marked as an assumption. There is no audience size, no alert-volume target, no statement of how many users or guilds exist, and no statement of who runs Cowork. Several designs (public ingress, API-key tiers with an OAuth path, `users`-table retrofit playbook, self-hoster-friendly routing config, Apprise backends) are justified by hypothetical self-hosters or additional consumers.

### Trade horizon

Detection: intraday, RTH only (09:30-16:00 ET, ADR-0021), 5-minute cadence, "sub-bar latency was deliberately given up". Claimed edge: multi-day to multi-week. Next-day day-2 OI "verdict" (ADR-0032 D6) is the only directional evidence the system ever produces.

### Exactly which user-visible features exist at cutover (per RM Phase 3 + ADR-0026/0027/0014/0016)

1. Options-flow alerts only. Per subject = (underlying, call-side|put-side): T1 post, then T2/T3 as parent-linked NEW posts as cumulative session flow crosses tiers (messages never edited), then a day-2 verdict post that either confirms or denies (ADR-0032 D4, D6). Up to 4 posts per subject per day. Alerts are non-directional ("Side" not "direction").
2. Alert embed with four buttons: `Trade`, `Watchlist`, `Stock Report`, `Options Report` (ADR-0026). Field-level layout is not yet written (an "alert-presentation spec" is pending).
3. Manual paper trading: `/trade buy|sell <ticker> <qty> [limit]` and the Trade button; `holdings`/`transactions`/`portfolio_snapshots` with a `source` column; live marks via serve read-through (ADR-0027).
4. Watchlists (users recreate them; ADR-0010).
5. Stock Report (Schwab fundamentals, technicals, earnings date; no chart) and Options Report (top calls/puts, greeks, IV, put/call ratio, OI, flow context) (ADR-0026).
6. A "v2 screener": working recommendation is a Schwab movers/`screener_equity` passthrough view, no stored state, not user-configurable (RM Phase 3). Definition is still open.
7. `/setup notifications|channels|roles|schwab` admin umbrella; notification routes in a DB table (ADR-0014, ADR-0016).
8. Non-Discord: `serve` HTTP API for Cowork (keys, scopes, per-consumer rate limits, TLS proxy), `tools/backtest` (operator CLI).

What users LOSE at cutover: all six v1 alert types, charts, clean slate (watchlists, history). ADR-0030 Consequences: "Users lose the v1 alert types at cutover - a larger user-facing change than the clean slate alone (ADR-0010); communicate it at cutover."

---

## 2. Commitment catalogue (everything committed before cutover)

Legend. Status: done / in progress / not started. Cx: S (<1 day), M (1-3 days), L (about a week), XL (multi-week) using repo precedent (S003 appetite 5 days = L). Category: UF user-facing feature, DP data plane, OPS ops-reliability, PG process-gate. Path: A = on the shortest path to the first options-flow alert in the dev channel; B = required at cutover but beside that path; C = no v2 consumer / process-only / hypothetical.

### 2a. Done

| # | Item | Source | Phase | Status | Cx | Cat | Path |
|---|---|---|---|---|---|---|---|
| D1 | Monorepo restructure: uv workspace, `libs/services/tools`, v1 moved to `legacy/` | ADR-0001, S001 | 0 | done | M | OPS | - |
| D2 | Domain seed: `BarClosed`, `Clock`, `EventSource`, `TradingCalendar`, Pydantic models, Alembic rev-001 (hypertable `stream_bars_5m`, `alerts`, `outbox`) | ADR-0003/4/18/21/23, S002 | 0 | done | M | DP | - |
| D3 | Clock-discipline grep gate + UTC-everywhere convention | ADR-0023, S002 | 0 | done | S | PG | - |
| D4 | Phase-0 spikes: L1 stream session, 10-ticker chain capture, OI day-over-day diff, REST burst probe, history depth | RM Phase 0, `specs/fixtures/*` | 0 | done | M | PG | - |
| D5 | ingest skeleton: Schwab L1 stream -> 5-min bars -> `BarClosed` -> persist -> threshold detector -> alert+outbox+NOTIFY | S003, ADR-0005/8/21 | 1 | done | L | DP | - |
| D6 | Postgres single-row Schwab token store + login tool | S003 Q2 | 1 | done | S | OPS | - |
| D7 | bot skeleton: LISTEN, 60s sweep on `posted_at IS NULL`, generic embed, poison-skip | S004, ADR-0005 | 1 | done | M | UF | - |
| D8 | serve skeleton: API-key middleware, `/v1/bars`, `/metrics`, provision CLI, `service_clients` rev-004 | S005, ADR-0012 | 1 | done | M | DP | - |
| D9 | Spine: `Dockerfile.v2`, compose with migrate gate, structlog + `correlation_id`, excepthooks, metrics exposition, CI | S006, ADR-0015 | 1 | done | M | OPS | - |
| D10 | Replay harness v0 + golden CI test | S007, ADR-0023 | 1 | done | M | PG | - |
| D11 | Phase-1 acceptance plan + exit ceremony (9-row exit table) | `phase1-acceptance-plan.md` | 1 | done | M | PG | - |
| D12 | Private dev channel + dev bot token | operator checklist s4 | 0 | done | S | OPS | - |

### 2b. Phase 2 (data plane) - mostly not started

| # | Item | Source | Phase | Status | Cx | Cat | Path |
|---|---|---|---|---|---|---|---|
| P1 | `options_chain_snapshot` hypertable (30 columns, 1-day chunks, compression after 3 days) + curated 5-min `ChainScanner` (cycle model, tombstones, stale guard, 4,000-row cap, 502 retry-once) | S008, ADR-0020/32 D5-D8 | 2 | in progress: code merged on `feat/v2-rewrite`; all 5 acceptance boxes unticked, incl. the supervised RTH session | L | DP | A |
| P2 | Options EOD full scan: nightly, whole optionable universe (about 6.3k requests, about 68 min), full chain, split ladder, frozen roster + K-session cooling tail, 03:00 CT hard deadline, seven `source_status` values, coverage floor, divergence cross-check | S009 (map #6, 11 tickets), ADR-0020 | 2 | in progress (spec stage; probes #37/#39 rescheduled twice; harness "lost (never pushed)") | L | DP | B |
| P3 | Symbol master: nightly `nasdaqlisted`+`otherlisted` -> listing-span `symbols` table, mass-delist guard, 9-class `instrument_class` pipeline (about 15 rules), dot-strip normalization, optionability = Nasdaq `options.txt` (94 MB) union Cboe CSV | UM D1-D6, ADR-0025, S010 inc.1 | 2 | in progress (spec stage; draft claimed in tracker) | L | DP | B |
| P4 | 4th live source: Cboe options symbol directory | ADR-0025 (amended 07-16) | 2 | in progress | S | DP | B |
| P5 | UniverseManager: curated (about 400, ceiling 480) and stream (about 480, ceiling 500) bands, 6 priority sources (4 are empty seams in Phase 2), verdict-conditioned stickiness (K=3), discovery by median chain-premium-volume rank (M=20) with hysteresis (E/X/S), nightly recompute vs intraday add-only, headroom, forced-eviction total order, ADD-rejection-as-signal | UM D7-D11, ADR-0019, S010 inc.2 | 2 | not started (design note "Draft - pending review") | XL | DP | A (or static list) |
| P6 | Daemon outer loop + nightly train (`run_train`) + refresh-token-expiry "park, poll, alarm" + daily streamed-roster table for G2(a) | #19, #38, #21, S010 | 2 | spec stage | L | OPS | B |
| P7 | Multi-symbol `LiveEventSource` (about 480 symbols) + stream-ceiling probe (480 -> +50/30s -> cap 750) | S010, UM D7 | 2 | not started | M | DP | A |
| P8 | Equity research history: nightly 1-minute backfill for about 10k symbols with extended hours (780 buckets/day), `bars_1m`, `session_segment`, ET-day-shaped adapter port, per-symbol-day coverage table, run-level empty alarm, SPY canary outage detector, 30-day retry window | S011a (map #25), ADR-0021/22 | 2 | spec stage | L | DP | C |
| P9 | 5m/1d continuous aggregates + compression policies (compress_after 10d with late-retry upserts into compressed chunks + pinned-Timescale CI contract test) | S011b, ADR-0022 | 2 | spec stage | M | DP | C |
| P10 | Corporate-actions table + adjustment-at-read (`adjusted_through` instant) | S011b, ADR-0022/25 | 2 | spec stage | M | DP | B |
| P11 | `tools/backfill` deep daily seed CLI (`bars_1d_seed` + view) | S011b, ADR-0022 | 2 | spec stage | M | DP | B |
| P12 | Stream-vs-REST nightly gauge | S011b, ADR-0021 | 2 | spec stage | S | OPS | C |
| P13 | `Bar` retired -> `StreamBar` + `OfficialBar` rename (70 refs, 20 files, 4 members) | #28 | 2 | spec stage | S | DP | C |
| P14 | `FreshnessPolicy` enum + `max_age` config table + staleness columns + stream liveness budget + `source_last_success_seconds` | S012, ADR-0017 | 2 | not started (no spec) | M | OPS | B |
| P15 | Generic `coverage_gaps` table | #32, "ADR-0033 D5" (ADR not in repo) | 2 | spec stage | S | OPS | C |
| P16 | Notifications layer: Discord-webhook/ntfy/Apprise backends, 5 categories, `notification_routes` table seeded by migration | S013, ADR-0016/15 | 2 | not started (no spec) | M | OPS | B |
| P17 | Pollers: ApeWisdom + popularity history, Nasdaq earnings calendar, Nasdaq corporate actions (feeds only bands/reports/serve; "not alert sources") | S014, ADR-0025/30 | 2 | not started | M | DP | B |
| P18 | Warm-up from durable log (4 steps, incl. baseline replay and ladder state; detectors disabled until done) | S015, ADR-0018/32 D9 | 2 | not started | L | DP | A |
| P19 | Detector-framework seams: detection port, generic alert taxonomy migration (`detector, subject, tier, parent_alert_id, config_version, session, verdict, evidence`), per-detector config, pluggable rendering, "trivial second detector registers in tests" | S016, ADR-0030/32 D10 | 2 | not started | M | DP | A |
| P20 | Canary detector (metrics-only) | RM Phase 2, P2 s4 | 2 | superseded in effect: ADR-0032 D2 demotes it to a test fixture; roadmap/plan text not updated | S | OPS | C |
| P21 | Options-flow detector: per-subject time-of-day-bucketed median/MAD baselines, burst components (abnormal, concentrated, opening-consistent, material, context-graded), equity-bar fusion, cumulative T1/T2/T3 ladder, day-2 confirm/deny verdict, config-version stamp, golden fixtures | S017, ADR-0030/32 | 2 | not started | XL | UF | A |
| P22 | Alert-presentation spec (field-level embed layout) | ADR-0026 (amended) | 2/3 | not started | S | PG | A |
| P23 | Phase-2 midpoint re-spec with explicit cut-first order | RM Phase 2, P2 s5 | 2 | not recorded (due about week 3-4 of 6-8) | S | PG | C |
| P24 | S008/S009/S010/S011 specs: wayfinder map + tickets per slice (3 maps, 34 issues, 23 "grilling", 3 research, 4 task, 1 prototype) | tracker #6/#13/#25 | 2 | in progress, 13 weeks | L | PG | C |

### 2c. Phase 3 (surfaces) - nothing started

| # | Item | Source | Status | Cx | Cat | Path |
|---|---|---|---|---|---|---|
| F1 | serve contract-first: OpenAPI authored + reviewed; endpoint groups (bars/quotes/movers, popularity, earnings, alerts, portfolio); row cap + pagination | RM Phase 3, ADR-0024 | not started | L | DP | B |
| F2 | serve Schwab read-through (quote, fundamentals, current chain, movers) + a Schwab client inside serve | ADR-0024 | not started | M | DP | B |
| F3 | Scopes + per-consumer rate limits + metrics-scoped key | ADR-0012 | not started | M | OPS | B |
| F4 | TLS reverse proxy as public ingress (operator-run) | ADR-0012, checklist s3 | not started | S | OPS | B |
| F5 | Cowork-shaped consumer test (G8) with 429 demonstrated through the real proxy URL | RM G8 | not started | S | PG | B |
| F6 | Report-design session (field sets, top-3 ranking basis, screener definition, Cowork exposure) | ADR-0026, RM Phase 3 | not started | S | PG | B |
| F7 | bot: alert embed, 4 buttons, persistent `custom_id` views | ADR-0014/26 | not started | M | UF | A |
| F8 | bot: `/trade buy|sell`, paper store with `source`, marks via serve | ADR-0027 | not started | L | UF | B |
| F9 | bot: `/setup notifications|channels|roles|schwab` | ADR-0014/16 | not started | L | UF | B |
| F10 | bot: watchlists (modals/selects) | RM Phase 3 | not started | M | UF | B |
| F11 | bot: Stock Report | ADR-0026 | not started | M | UF | B |
| F12 | bot: Options Report | ADR-0026 | not started | M | UF | B |
| F13 | bot: screener passthrough + endpoint | RM Phase 3 | not started | M | UF | B |
| F14 | `tools/backtest` as a serve consumer + event studies at 1d/3d/5d/10d | ADR-0011/20 | not started | L | PG (validation) | B (should be earlier, see s5) |
| F15 | Bot numeric latency budgets | ADR-0029 | not started | S | PG | C |
| F16 | DB roles/GRANTs (token row ingest-only) | S003 Q2, S006 non-goals | no owner in any slice table | S | OPS | C |

### 2d. Phase 4/5 and operator items

| # | Item | Source | Status | Cx | Cat | Path |
|---|---|---|---|---|---|---|
| G0 | Box capacity check | checklist s1 | open | S | OPS | B |
| G00 | Backup conditions check (block-level atomic, off-host, daily+weekly) | ADR-0031, checklist s2 | open | S | OPS | B |
| G000 | Ingress check | ADR-0012, checklist s3 | open | S | OPS | B |
| H1 | Terminal burn-in: 3-4 market weeks unattended, alerts to private channel | ADR-0028 | not started | XL (calendar) | PG | B |
| H2 | Rehearsal #1 token expiry; #2 deliberate ingest restart; #3 bot kill; #4 backup restore | RM Phase 4 | not started | M | PG | B |
| H3 | Freshness budget finalization from observed p99 | ADR-0017, RM Phase 4 | not started | S | OPS | C |
| H4 | Gates G1-G9 (unattended, gap audits x2, token, warm-up, reconcile, Timescale jobs, detection liveness, serve contract, backup restore) | RM Phase 5 | not started | M | PG | B |
| H5 | Cutover: user communication that six alert types are gone, rename `docker-compose.v2.yml`, ghcr publish, retag `:pre-s001`, 2-week dormant v1 rollback, delete `legacy/` | ADR-0010/28, S001 | not started | M | OPS | B |
| H6 | Repo hygiene: replace committed real Schwab/Nasdaq/Cboe fixtures with synthetic ones before the repo can go public again | #21 ("tracked outside the tracker") | not started | M | PG | C |
| H7 | Pending ADR/doc amendments: ADR-0022 (7 -> 47 days), ADR-0021 clarification, UM D4 (when-issued), D5 (cooling tail), D9 addendum, UM line 233 multiplier bug, BRIEF ADR index (stops at 0031), status lines | #8, #9, #11, #29 | not started | S | PG | C |
| H8 | Documented-not-built migration paths (11 triggers: Redis Streams, OAuth, `users` table, circuit breakers, ErrorAggregator, pgBackRest, UNLOGGED cache, writes-behind-serve, 1-min detection, auto-trader service) | BRIEF "Migration-path notes" | docs only | S | PG | C |

Roll-up (S=1, M=2, L=4, XL=8; my weights, order-of-magnitude only): remaining weighted effort about 95 units. Path A about 19 (about 27 if the full UniverseManager is built instead of a configured list). Path B about 62. Path C about 15.

---

## 3. Path to first user value

### The chain as the plan is written

```
S008 live acceptance (unticked)
  -> S010 inc.1 symbol master + optionability (spec stage)
       -> S009 EOD full scan (needs the roster; needs OI-settle probe + eod pair capture, rescheduled to week of 2026-10-05)
            -> 20 sessions of eod_full  OR  S011b daily seed (discovery cold-start, UM D9)
                 -> S010 inc.2 UniverseManager bands
                      -> S015 warm-up (needs S010)
                           -> S016 taxonomy migration
                                -> S017 detector (needs min-history gate: "~15-20-session baseline ramp", #13)
                                     -> alert-presentation spec -> bot embed + buttons
                                          -> Phase 3 (serve, reports, /trade, /setup, watchlists, backtest)
                                               -> Phase 4 burn-in (3-4 market weeks, private channel)
                                                    -> G1-G9 -> cutover  <== first thing a Discord USER notices
```

First thing an end user notices is at the END of the chain. Nothing in v2 is user-visible before cutover. The first alert the OWNER sees from the real detector is after S017 plus baseline maturity (about 3-4 weeks of continuous curated capture per subject) and is only in a private dev channel.

### Points that make the chain longer than it looks

- The baseline clock is not running. ADR-0032 D3 requires a "minimum-history gate" (forward-only dataset), P2 s1 (07-16) argues capture should start "right-shaped" from go-live, yet the daemon is still "single-session-per-process" (S003 interface, S006 pin 5: "operator starts the stack on market mornings meanwhile"); the multi-session loop was moved into S010 (#19). Every week that passes without unattended capture of the real curated list is a week added to baseline maturity.
- The discovery cold-start (UM D9) needs "the seeded daily history", i.e. S011b's `tools/backfill`, which P2 s5 lists as FOURTH in cut-first order. The cut-first order and the dependency graph disagree.
- S011a hard-depends on S010 inc.1 (roster), S009 depends on S010 inc.1, S015 on S010, S017 on S015/S016/S009: S010 is the hub. S010 alone carries 12 tickets (map #13: 2 research, 2 task, 8 grilling) on top of an 11-decision design note that is still marked Draft.
- Phase 3 can start only for pieces that do not need ADR-0032 outputs, but the alert-presentation spec (needed for the embed) does not exist and ADR-0026 still lists "the report-design session" as open.

### On the path vs beside it

- On the path (A, about 19-27 units): S008 acceptance, a curated-list source, multi-symbol stream, warm-up, framework seams + taxonomy migration, the detector, presentation spec, bot embed + buttons.
- Beside it but required at cutover (B, about 62): EOD scan + symbol master + daemon train, freshness + notifications, pollers, all of serve/Cowork/ingress, `/trade`, `/setup`, watchlists, reports, screener, backtest, burn-in, rehearsals, gates, cutover mechanics.
- No consumer (C, about 15): whole-universe 1-minute history + its compression/aggregates + gauge, bar-type rename, canary, `coverage_gaps`, spec-process work itself.

A minimum path (for discussion in s9): one configured list, existing S008 scan running unattended, a T1-only detector with a nightly one-line verdict, existing S004 bot embed. That path reuses already-accepted code and needs roughly P1 acceptance, an unattended session loop, P19-lite, P21-lite and a field layout.

---

## 4. Bloat and over-engineering candidates

Verdicts are for a single-operator hobby bot with a small Discord audience. "Justified" parts are called out explicitly.

| # | Candidate | Problem it solves | Cost it adds | Verdict |
|---|---|---|---|---|
| 1 | Three-service topology ingest/serve/bot (ADR-0002) | Capture must survive Discord I/O and bot restarts; Cowork needs a contract | 3 containers, shared image, cross-service signaling, serve on the bot's read path (ADR-0024 matrix), a Schwab client in serve, token coordination hazards | KEEP ingest vs bot split (ADR-0008 reasoning is sound and already built). DEFER serve entirely until Cowork actually calls it; let bot read market tables through a repository module. |
| 2 | Outbox + NOTIFY + reconcile (ADR-0005) | Atomic write+signal; lost-NOTIFY recovery | NOTIFY + `posted_at` sweep: small, proven (S004 drill, Finding 3). `outbox` table: written on every alert, "the outbox has no v2 reader" (S003 pin 6), no relay ships | KEEP NOTIFY + sweep. CUT the outbox table (alerts table with `posted_at` is already the durable ledger). Do not build relay/consumer-contract abstractions. |
| 3 | serve API keys + rate limits + scopes + OAuth path + TLS proxy + G8 429 demo (ADR-0012/24) | One external consumer with bursty agent fan-out; protects Schwab budget | Phase-3 remainder: contract-first OpenAPI, row caps, scopes, limits, read-through, proxy, G8 gate. Cowork profile is an assumption ("stated 2026-07-03") | KEEP the S005 key check as built (cheap). DEFER everything else to "when Cowork calls it". If Cowork is the owner's own tool, a static key and no rate limiter suffice. |
| 4 | Two-band universe with about 10-13k symbol master + UniverseManager (ADR-0019/25, UM D1-D11) | Choose about 400 scan names inside the REST budget; stay reconstructible; protect baselines from churn | Largest design artifact (3.5k-word note, about 4.0k words of listing research across three docs, 12 S010 tickets: 2 research, 2 task, 8 grilling). Machinery: listing spans, recycled-ticker handling, 9-class heuristic classifier over 13k rows, dot-strip alias table (normalization worth "exactly +2 optionable names", `normalization-mapping` doc), 4th data source, premium-volume discovery rank with hysteresis, verdict-conditioned stickiness, headroom/eviction total order, daily streamed-roster table | SIMPLIFY hard. A configured list (e.g. top N liquid optionables, refreshed weekly by a script from daily bars) satisfies curated-subset-of-streamed, baseline stability and budget. Defer symbol master, classifier, normalization, Cboe, discovery, stickiness unless the whole-universe EOD scan is kept. Justified kernel: "membership changes only at session boundary" and "curated subset of streamed" (trivial with a list). |
| 5 | Freshness policies (ADR-0017) | Degrade gracefully, detect silent staleness | 3 policy enums per dataset x source, `max_age` config table finalized at burn-in, stream liveness budget, serve envelope times, status enum drifting 3 -> 5 -> 7 values (ADR-0017, S008, map #6) | SIMPLIFY to `source_last_success_seconds` + 2-3 hard-coded alarm thresholds. KEEP per-row `fetched_at`/`quote_time`/tombstones on option rows (cheap, S008 already pays). |
| 6 | Nine gates G1-G9 + 15-day burn-in + 4 rehearsals (RM Phase 4/5) | "Cutover on measurable criteria, never on vibes" | 3-4 market weeks calendar; G2 needs two gap audits; G6 observes Timescale jobs fire; G8 needs the serve stack public; G9 a restore; G7 is weak (below) | SIMPLIFY. Keep: G1 shortened (10 clean market days), G3 token rehearsal (weekly reality), G9 one restore rehearsal (genuinely protective). Drop G5 (done in S004 drills), G4 (covered by warm-up test), G6, G8 until their components exist. Replace G7 with a quality gate (s5). |
| 7 | Replay harness (ADR-0023, S007) | Detection is temporal; refactor safety; same seam as warm-up/backtest | Built and small (about 110 LOC comparator + tool); clock discipline is nearly free | KEEP, but cap scope: golden fixtures for the one detector, no "replay state == live state" program beyond a warm-up test. |
| 8 | Three datasets (ADR-0020) | Live alerts, bias-free research, equity history | `curated_5m` is the product (keep). `eod_full` whole-universe: 6.3k req/night, roster freeze, cooling tail, divergence cross-check, 7 statuses, deadline/coverage floor, 4 wayfinder tickets + 2 probes. Equity 1-minute for 10k symbols: about 10k req/night + extended hours + CAGGs + gap/coverage table + alarm + SPY canary + 30-day retry | KEEP `curated_5m`. SHRINK `eod_full` to the curated band + alerted subjects (about 400 requests; enough for D6 verdicts) until a committed research consumer exists. CUT whole-universe 1-minute history: no v2 consumer; ADR-0022's own logic dropped historical 5m "with no v2 consumer" and the window slides (47 days measured), so it is not a one-way door until someone needs it. Keep daily bars for reports. |
| 9 | Timescale compression/retention | Row volume of options snapshots | Options hypertable compression is already in S008 and is justified (30 columns x tens of millions of rows/day; ADR-0032 D8 quotes 5-8M rows/day, which looks low vs S008's measured SPY window of about 2,650 rows/snapshot x 78 cycles). Bar CAGGs/compress_after/late-upsert contract tests exist only for the 1-minute family | KEEP options compression. DEFER bar CAGG/compression machinery with item 8. Reconcile the rows/day estimate. |
| 10 | Options flow inferred from 5-min chain deltas, no prints (ADR-0032) | The product thesis | Per-subject (about 800 subjects) time-of-day baselines, fusion, tiering, verdict logic, warm-up of all of it | See s5. KEEP as an experiment gated on an evidence step; build the smallest detector first. |
| 11 | Escalation ladders T1/T2/T3 + parent-linked re-posts + confirm/deny verdict posts (ADR-0026 amended, ADR-0032 D4/D6/D10) | Avoid message edits; escalate significance; confirm via OI | `parent_alert_id`, `tier`, `session`, `verdict`, running accumulator state, ladder warm-up, up to 4 posts/subject/day to a small audience | DEFER. Ship T1 + one end-of-day verdict line; add tiers only after real alert logs show repeated bursts worth escalating. |
| 12 | Canary detector (RM Phase 2, P2 s4, S003 decision 1) | Keep streaming->detection seam hot | ADR-0032 D2 already says it "demotes to a test fixture" | CUT. Fix the stale references (see s6). |
| 13 | Backtest as a serve consumer (ADR-0011) | "Forcing function" for ingestion shape; three-consumer contract | Requires serve history endpoints before any validation can run; sequenced AFTER detector in Phase 3 | SIMPLIFY and MOVE EARLIER: an offline event-study script that reads the DB directly. It is the only instrument that can falsify the thesis. |
| 14 | Notifications layer (ADR-0016): Discord/ntfy/Apprise, 5 categories, `notification_routes` table, `/setup notifications` | Critical events must not drown in alert channel | DB table + slash commands + 3 backends for "self-hosters" | SIMPLIFY: one `notify_critical()` -> ntfy topic from env + log; defer routes table and Apprise. |
| 15 | Manual paper trading + Trade button + portfolio store + priority-1 streaming of open positions + serve portfolio endpoints (ADR-0027) | v1 parity | Touches bot, serve, UniverseManager source 1, DB schema, Trade button | QUESTION. No usage data in the docs. Check v1 `holdings`/`transactions` row counts and distinct user ids; cut if unused. |
| 16 | Weekly Schwab refresh-token expiry handling (S003 Q2, #38: "park + poll + alarm" daemon, bounded re-alarm, `schwab_token_valid` gauge) | The 7-day hard expiry has no unattended workaround | A designed state machine whose only job is to wait for a human | SIMPLIFY: on expiry alarm via ntfy and exit; `docker restart` after login. The human re-auth every 7 days is a permanent operating cost the plan never counts against "unattended". |
| 17 | Wayfinder map/ticket spec process (maps #6/#13/#25) | One-question-at-a-time design with recommendations | 34 issues, about 29.7k words, 13 weeks, no merged spec; cross-map reconciliation needed (nightly-slot conflict #10/#19/#27); "probe harness lost (never pushed)" | CUT for slices that fit the half-page template. Resolve open questions inline during implementation against live data; keep tickets only for real unknowns (probes). |
| 18 | Documentation volume (32 ADRs, 8 specs, plans, design note, findings) | Decision traceability | See s7: about 55k words in `docs/v2`, 15 of 32 ADRs amended, 3 superseded, 52 change-log lines; docs about 17 words per production line | SIMPLIFY: stop amending ADRs for implementation detail; one living `ARCHITECTURE.md` of current truth; keep ADRs for decisions that were actually contested. |
| 19 | Bot reads all market data through serve (ADR-0024 rule) | Dogfood the contract; two-party token coordination | Network hop and a serve dependency on every report; bot has no Schwab client | DEFER with item 1; weak rationale for one operator. |
| 20 | "Four live sources" accounting | Minimal external dependencies (ADR-0025) | Actually 7 surfaces: Schwab stream, Schwab REST (chains, history, quotes), ApeWisdom, Nasdaq calendar API, Nasdaq Trader `nasdaqlisted`/`otherlisted`/`options.txt` (94 MB), Cboe CSV | Reduce with item 4. |

### Where complexity IS justified (keep)

- Splitting capture (ingest) from Discord (ADR-0008) and the `posted_at` sweep with NOTIFY as latency hint (ADR-0005): proven by a live failure (psql `\watch` swallowed notifications, `acceptance-2026-07-09.md` Finding 3).
- Per-row `quote_time` event-time rule, stale-snapshot guard and tombstones on 502: grounded in real observations (a 14-minute wholesale-stale chain response at 17:49 UTC, 796 false negative deltas; 39/900 = 4.3% sporadic 502s on bounded requests).
- Postgres-only (no Redis), Postgres token store with single writer, UTC-everywhere convention, injected clock gate.
- Compression on `options_chain_snapshot`.
- Operator-level VM snapshots instead of in-repo backup tooling (ADR-0031), with one rehearsed restore.
- Spike-first discipline in Phase 0: the spikes caught changed-fields-only streaming, stream volume +9-15% vs official candles, SPY/QQQ unbounded chain 502, ET-day flooring of history bounds.
- The scope cuts already made (see s8).

---

## 5. Is the core product bet sound? (ADR-0030 / ADR-0032)

### The bet

Retire all six v1 alert types (which already run for users) and ship one new alert class inferred from 5-minute Schwab REST chain snapshots, defended only by a "philosophy review" (ADR-0030 Context: "is that philosophy the product, or v1 baggage?"). ADR-0030 admits: "Risk: the product rides on one signal's quality."

### Evidence cited for it

- `specs/fixtures/chains/findings-2026-07-06.md`: 10 mega-cap tickers, one day of 5-minute snapshots (SPY at 1-minute), per-contract cumulative volume deltas measurable, OI static intraday (0 changes), negative deltas explained by stale responses.
- `specs/fixtures/chains/oi-change-2026-07-09-to-2026-07-10.md`: one consecutive-pair OI diff; 1,100-2,700 contracts moved per book, top moves +5k to +28k; day D's settled OI reflects D-1's trading; shared-roster netting matters (SPY shared net +603k vs book net +267k).
- Budget math (`schwab-rest-budget.md`): 400-480 chain calls per 5-minute cycle fit under 80% of the documented 120/min; the probe found no 429 up to 240/min.

### What the evidence does NOT show

1. No predictive evidence at all. Nothing tests whether volume bursts or OI builds precede 1/3/5/10-day equity moves, or at what hit rate. The spike data answers "is the signal measurable", not "does it predict". No false-positive-rate estimate exists. Validation (event studies) is scheduled for Phase 3, after the detector is built, on forward-only data that starts accumulating at go-live (ADR-0020, ADR-0011).
2. The one worked example argues against the thesis. The OI fixture says NVDA gained 3.6% that day and calls at/above the new price built massively ("new opened interest chasing the move"); ADR-0032 D2 repeats that "the NVDA 7/10 call build was exactly this" (chasing/hedging). Flow that follows price is not a leading indicator; the design grades it DOWN but has no data on how often flow leads vs lags.
3. Direction is absent by construction. No aggressor side, no prints, no sweeps/blocks (ADR-0032 Context). A call-side burst is "Side, not direction". The only directional information (OI build vs unwind) arrives the NEXT day (D6), at the same cadence as a daily-only detector. For a swing trader the intraday part of the alert is non-directional.
4. Clock mismatch. ADR-0029 says the claim is "multi-day-to-multi-week"; the whole 5-minute stack (curated scan, stream band, bar fusion, time-of-day baselines) buys hours of latency whose value to a swing thesis is never quantified. ADR-0029 Consequences: "The 5-minute cadences are now product-aligned by statement, not by accident of the Schwab budget." That is circular.
5. Baseline thinness. D3 baselines are per subject, per time-of-day bucket, median/MAD over a trailing window with a min-history gate ("about 15-20 sessions" per #13). That is about 15-20 samples per (subject, bucket) of heavy-tailed volume deltas (many contracts have zero deltas). Illustrative alert-rate arithmetic: 400 names x 2 sides x 78 cycles = 62,400 evaluations/day; a 0.01% spurious rate is about 6 false alerts/day, 0.1% about 62. Thresholds are all "config tuned via replay fixtures and burn-in" (D3), i.e. unknown.
6. The test gate is weak. G7 (RM Phase 5): "fired >=1 real alert during burn-in, OR its silence is justified by a replay fixture demonstrating correct no-fire". With a 15-20-session silent ramp (D3) inside a 3-4 week burn-in, the detector can legitimately produce no live alert and still pass. The first real exposure to users is then cutover day, at the moment all six working v1 alerts are switched off.
7. Sample is narrow: 10 mega-cap tickers (AAPL, AMD, AMZN, GOOGL, META, MSFT, NVDA, QQQ, SPY, TSLA), 1 intraday day, 2 EOD days. The curated band the product will actually run on is about 400 mixed-liquidity names.
8. Vendor risk concentration: the whole product sits on Schwab's stream + REST (stale responses, 4% 502s, 7-day token expiry, one streamer connection per account). Data licensing is a live concern (repo made private 2026-08-01 per #21; committed vendor fixtures still in the tree).
9. v1 evidence was ignored. v1 already has an options-flow module (`legacy/.../core/analysis/options_flow.py`), a volume-accumulation alert that upgrades on unusual options activity, `alert_performance.py`, and an EDA forward-returns engine with t-stats (`.claude/docs/eda.md`). The docs cite no v1 hit-rate or user-feedback data when retiring the six types.

### Verdict

Plausible research hypothesis, planned as a certainty. The plan commits roughly all remaining Phase 2/3 effort (about 80% of the weighted work) to infrastructure serving the intraday alert before any falsification test. The cheapest test has not been run.

### Cheapest next step (recommended)

Run an event study first, before S009/S010/S015-S017:

1. Data: v1 `options_chain_history` (per-contract daily snapshots for all tickers, Tue-Sat 21:30 CT, on `main` since about 2026-05-20, up to about 5 months if the production job ran) plus v2's own spike captures (10 tickers).
2. Signal: per name per day, signed net OI build in near-dated calls vs puts over the shared roster (the exact day-2 verdict statistic from D6), and the volume/OI ratio proxy for "opening".
3. Outcome: forward equity returns at 1/3/5/10 days (v1 EDA forward-returns engine already computes mean, win rate, t-stat, p-value).
4. Decision rule: if the signal has no out-of-sample edge, the intraday stack is unjustified; if it does, build the EOD version first (about 1/50th of the infrastructure), and only then ask whether 5-minute timing adds measurable value.

Also keep at least one proven v1 alert type alive at cutover until the new class has live hit-rate data.

---

## 6. Contradictions, stale text, open questions, weak rationales

### 6a. Contradictions and stale text

1. Canary: RM Phase 2 ("stays in production as a metrics-only canary") and P2 s3 S016 row and S003 "Decisions surfaced 1" still describe a promoted production canary; ADR-0032 D2 (accepted 2026-07-16) says "the S003 threshold detector demotes to a test fixture". P2 s4 is conditional but unreconciled.
2. ADR index in BRIEF stops at ADR-0031; ADR-0032 is missing from BRIEF's index, from its "Detection + alerts" section (no equity-bar fusion, no ladder) and from its status line (last event 2026-06-12/07-03). README has no index.
3. "Whole tracked universe": ADR-0019 and BRIEF say the equity band is "<= ~500 underlyings (the whole tracked universe)"; UM D7 says tracked in-policy about 11.5k -> optionable 5-6k -> <=500 streamed -> about 400 curated. Findings correct the glossary: "sizing ... should use ~13k" listed rows.
4. Capture width: ADR-0029 and BRIEF still say "ATM+-5"; ADR-0032 D7 amended ADR-0020 to a +-10% moneyness window.
5. 1-minute depth: ADR-0022 (decision body, recovery bound, consequences), ADR-0020, RM G2 and the budget doc use "7 days"; measured 47 days (`schwab-rest-budget.md`, `history/findings-2026-08-01.md` on `phase-2-spec-011`); amendment not made; S011 plans a 30-day retry window which flips `compress_after` from about 10 to about 35 days.
6. UM status is "Draft - pending review" but #13 and P2 treat it as "binding"; it carries a known error (UM line 233 hardcodes 100 where #11 decided the stored `multiplier`; per `HANDOFF-S009.md`), and its pending amendments (D4 when-issued dropped, D5 cooling tail, D9 addendum and coverage floor) live only in tracker comments.
7. `source_status` enum: ADR-0017 (3 values) -> editorial 5 (S008) -> 7 (map #6 adds `rejected`, `empty`); each spec re-defines it.
8. Phase 0 is "closed" yet three of its pre-flight boxes are open (capacity, backup, ingress), deadline moved to "Phase 3->4 boundary" (`phase1-acceptance-plan.md`, checklist).
9. RM G6 expects "retention/rollup jobs observed firing"; ADR-0021/0022 and S008 say "no retention policy" for the forward-only datasets.
10. Operator checklist RAM line includes "Prometheus/Grafana - ADR-0015"; ADR-0015 says Prometheus is "not in docker-compose" and the Grafana template is deferred.
11. ADR-0012 says "rate limits from day one"; S005 redefines "day one" as "day one of external exposure" and enforces nothing; ADR text not amended.
12. Vendor-data fixtures: `docs/v2/specs/fixtures/chains/excerpt-spy-chain-*.json`, `streaming/excerpt-*.jsonl` and the four verbatim `listing/excerpt-*` Nasdaq/Cboe files are committed, but #21 (2026-08-01) decided "Fixtures are synthetic, never vendor corpora" because Schwab/Nasdaq/Cboe terms "bar redistribution contractually". The cleanup is "tracked outside the tracker" and gates re-publicizing the repo; S003/S008 acceptance text still says tests replay "the real SPY 17:48/17:49 pair".
13. Tracker references ADR-0033 (D2 `sequence` derivation, D5 `coverage_gaps`) that is not in any ref; specs S009/S010/S011 referenced as drafted are absent from the repo (the roadmap says specs live in `specs/`).
14. P2 s5 cut-first order vs UM D8/D9 dependencies: S014 pollers (cut first) feed the popularity priority source; `tools/backfill` seed (cut fourth) feeds discovery cold-start.
15. Roadmap status log is out of chronological order and stops at 2026-07-15; no entries for ADR-0032 acceptance, S008 implementation or the Phase 2 maps. `phase1-acceptance-plan.md` keeps a "read this first - what's left" banner that says nothing is left.
16. `libs/analysis` (ADR-0001's "pure math" member) is a one-line empty package after 4 months; `libs/sources` holds only a 71-line calendar adapter.

### 6b. Just-in-time design sessions still pending

- Alert-presentation spec (after ADR-0032) - not written; blocks the embed.
- Report-design session (field sets, top-3 ranking basis, screener definition, Cowork exposure) - Phase 3.
- Bot numeric latency budgets - Phase 3.
- Phase-2 midpoint re-spec with a cut-first order - due about week 3-4, nothing recorded at week 13.
- Detector spec: tier statistic "exact form pinned in the detector spec", thresholds, premium floor, trailing window N, bucket width, verdict scheduling (day-2 vs day-3 depends on the unobserved OI settle time, #37).

### 6c. Unresolved unknowns that gate the design

- OI settle time has "never been observed" (map #6, #10); probes planned 2026-08-03, rescheduled twice (token expired about 07-25), harness "lost (never pushed)", rebuilt for week of 2026-10-05; outcome not visible.
- Stream ceiling "about 500" is a legacy soft figure never probed (UM D7); S010 acceptance would probe it.
- Half-day behavior of the history endpoint is unmeasurable until Nov 2026.
- Alert volume, audience size, per-user alert subscription/role design (`/setup roles`, `alerts.user` category in ADR-0016) are never specified.
- Who operates Cowork and what it needs beyond a self-authored "Cowork-shaped consumer test" (G8).
- Row-volume estimate: ADR-0032 D8 "about 5-8M rows/day" vs S008's measured SPY in-window book of about 2,650 rows/snapshot; 400 names x 78 cycles x even 800 rows is about 25M rows/day. Storage sizing (operator checklist "20-40 GB/month") is unreconciled.

### 6d. Decisions with weak rationale

1. "Reliable storage of future data is the #1 goal" (ADR-0028) for a product whose users see alerts. Storage reliability matters only through baseline integrity; the plan optimizes storage guarantees (gap audits, tombstone accounting, compression jobs, restore drills) well beyond that.
2. "All pre-cutover data is disposable" (ADR-0028/0010) vs a detector that needs about 3-4 weeks of history before its first alert and a roadmap that waits for forward-only options history to validate anything. The decision removes the shadow-run option (ADR-0028 option 4) but the real cost is paid as delayed evidence.
3. Retiring all six alert types simultaneously on the basis of a philosophy review; no v1 performance data is cited even though v1 has `alert_performance.py` and an EDA engine.
4. Whole-universe EOD scan justified as "selection-bias-free research" with no committed v2 research consumer; the verdict consumer (D6) needs only alerted subjects; the discovery consumer (D9) is a ranking that has an equity-dollar-volume bootstrap the note itself defines.
5. Whole-universe 1-minute history called "the sharpest one-way door" (ADR-0022) while its window slides (47 days) and no consumer exists. The same ADR dropped historical 5-minute seeding for exactly "no v2 consumer". Inconsistent test.
6. Bot-via-serve rule (ADR-0024) for one operator: hygiene argument ("dogfood Cowork's contract") over simplicity.
7. Tracker-held design: a 30k-word design record in GitHub issue comments, outside the repo, with the repo's own convention saying specs live in `docs/v2/specs/`.
8. Time-of-day, per-subject median/MAD baselines on 15-20 sessions (statistically thin) fixed as the shape in an Accepted ADR before any replay on real bursts.
9. Appetite discipline: RM states "when it runs out, cut scope - never extend time." Phase 2 appetite (6-8 weeks from 2026-07-15) ended about 2026-09-09; no cut recorded; S011 instead SPLIT into S011a/b on 2026-10-03.

---

## 7. Process overhead

### Counts

- `docs/v2`: 55,144 words: 32 ADRs 18,577; 8 specs 11,554; top-level docs (brief, roadmap, phase0 checklist, phase1 acceptance plan, phase2 plan, REST budget, universe note, spec template, README) 16,171; fixture findings 8,842. Plus `CLAUDE.md` + `CONTEXT.md` about 1,571 words.
- GitHub tracker: 34 issues (3 maps, 23 grilling, 3 research, 4 task, 1 prototype), 37 comments, about 29,654 words. Plus uncommitted `HANDOFF-S009.md`, `S009-probe-runbook.md` (1,156 words), two handoff stashes.
- Total design text about 86k words.
- v2 production code: 4,864 lines (`ingest` 2,154, `persistence` 1,004, `bot` 519, `serve` 412, `observability` 345, `domain` 306, `sources` 71, `migrate` 52, `analysis` 1). Tests 5,038 lines (292 test functions). Spikes/scripts 1,419 lines.
- Docs-to-production-code ratio: about 17 words of design text per line of production code (11 for `docs/v2` alone).
- Design churn: of 32 ADRs, 15 carry "amended" status, 3 are superseded (0006, 0007, 0009, all within 9 days of being written), 52 change-log lines, 35 doc commits on 2026-07-03 alone; reversals include the UNLOGGED cache (designed 05-27, cut 07-03), auto-trader (05-27 -> cut 06-05), options streaming tier (05-27 -> removed 06-05), OI-confirmation update-in-place (06-05 -> dropped 07-03 -> re-added as verdict posts 07-16).

### Timeline

- 2026-05-27 to 2026-07-03: design only (about 5 weeks, zero v2 code).
- 2026-07-05 to 2026-07-17: S001-S008 built and accepted (about 2 weeks; S003-S007 in about 10 days).
- 2026-07-17 to 2026-10-19: no v2 code commits; last commit of any kind 2026-08-01; spec design continues in the tracker (issues closed through 2026-10-04).

### Ceremonies per slice (as practiced)

1. Spec from `spec-template.md` (7 sections; "half a page" target, S003 is 2,555 words).
2. Pre-implementation grilling session with pinned decisions, folded into the spec.
3. ADR promotion/amendment for anything architectural, then BRIEF/ROADMAP/CLAUDE/README alignment.
4. TDD, then two-axis code review (standards + spec) per CLAUDE.md.
5. Live market-gated acceptance run (needs a specific market morning, a dedicated 5498 DB, operator-run drills).
6. Acceptance evidence file in `fixtures/` and ticked boxes in the spec.
7. Roadmap status-log entry (136-line roadmap, 16 dated entries of 100-250 words each).
8. Phase-level exit checklist (Phase 1: 9-row table plus Parts A to G).
For Phase-2 slices add: wayfinder map, 8-12 child tickets (grilling, research, probe), resolution comments, cross-map reconciliation, then "survived John's review".

Process gates in the plan: 9 cutover gates, 4 rehearsals, 3 operator pre-flight checks, 1 phase exit table per phase, 1 clock-discipline gate, 1 midpoint re-spec rule, 1 per-slice acceptance section.

### Observation

The process found real things (ET-day flooring that would silently rewrite yesterday's basis, stale chain responses, 47-day depth). But most of those were found by running against the live API, which implementation does anyway. The marginal value of the ticket machinery for S009/S010/S011 is hard to see: it produced about 30k words and no merged spec in 13 weeks, while S008, specced in an afternoon, was implemented in about 2 days.

---

## 8. What is solid and worth keeping regardless of direction

- The engineering core already built and accepted: stream -> 5-minute bars -> persist -> detect -> alert row -> NOTIFY -> bot sweep, with idempotent `posted_at` cursor, poison-skip, replay-equals-live proof (12 = 12), and compose spine. About 5k LOC, well tested, small.
- Scope discipline decisions: auto-trader cut, UNLOGGED cache cut, charting cut, EDA deferred, news/SEC/Tiingo/etc. cut, relay not built, ErrorAggregator not built, OAuth/`users`-table deferred behind triggers.
- Data-quality instincts backed by observation: per-row `quote_time` + stale guard, tombstones, retry-once on 502, bounded chain requests, ET-day-shaped history adapter, naive-datetime rejection.
- Postgres-only with LISTEN/NOTIFY; token in Postgres; UTC convention; injected `Clock` and the grep gate.
- Budget math and probes (`schwab-rest-budget.md`): 80/min steady state at 400 names is comfortably inside measured capacity.
- Backup posture (ADR-0031): operator VM snapshots, off-host, one rehearsed restore; honest about RPO.
- Spike-before-spec as a method (Phase 0 spikes, the OI diff).
- Per-detector config + config-version stamp on alert rows (ADR-0030 PIT mandate) is cheap and is what makes honest hit-rate accounting possible later.
- Golden replay fixtures for the one detector (ADR-0023 scoped down).
- Appetite/cut-first rule itself: the roadmap's discipline is right; it is simply not being applied.

---

## 9. Suggested re-scope (for discussion; not part of the audit scope but follows from it)

1. Evidence gate (1-2 sessions): event study on v1 `options_chain_history` + v2 spike captures; decide whether the signal exists, and whether day-level or 5-minute timing is what carries it.
2. Start unattended multi-session capture of the real curated list NOW (small loop around the existing `run_session`/`ChainScanner`, restart policy, ntfy on token expiry). This starts the only clock that cannot be bought later.
3. Minimum detector: configured list (about 100-400 names), T1-only burst alert, nightly verdict line, existing S004 embed with at most one button; replay-fixture tested; to a dev channel for 2-3 weeks.
4. Keep v1 alert types alive until the new class has live hit-rate data; cutover = adding the new class, not removing six.
5. Defer: serve (past what bot needs), Cowork gates, symbol master/UniverseManager/discovery, EOD whole-universe, whole-universe 1-minute history, CAGGs, freshness tables, routes table/Apprise, outbox, canary, ladders, G2/G4/G5/G6/G8.
6. Process: write specs at half a page inside the PR that implements them; use tickets only for live probes; fold ADR amendments into one current-state architecture doc; enforce the appetite rule (record the cut when a phase timebox expires).
7. Verify product facts that the docs never state: number of active Discord users/guilds, v1 command usage (watchlists, paper trading, reports), who runs Cowork. These determine whether items 1, 3, 15 in s4 should exist at all.
