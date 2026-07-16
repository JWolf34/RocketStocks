# Phase 2 — Data plane: work plan

> **Purpose:** the operational work breakdown for Phase 2
> ([roadmap.md](./roadmap.md) §Phase 2 — appetite **6–8 weeks, re-spec at midpoint**):
> what gets designed, what gets built, in what order, and what gets cut first if the
> appetite runs short. Sibling to [phase1-acceptance-plan.md](./phase1-acceptance-plan.md)
> (which closed Phase 1 on 2026-07-15). This is a plan, not a spec — **every slice below
> still gets its own spec per [spec-template.md](./spec-template.md), reviewed before
> implementation.**
>
> **Method carried from the roadmap:** spec-driven slices · appetites not estimates ·
> replay-harness-first (every detection slice ships a golden fixture) · per-slice
> branches merging into the `feat/v2-rewrite` trunk.
>
> Written **2026-07-15** at Phase-2 open; amended **2026-07-16** (see status log). The
> slice table is a proposal: the two design artifacts (§1, §2) may reshape the
> detection-side slices (S015–S017) and S008's capture parameters — re-confirm the
> table when the detection-model ADR closes, and again at the midpoint re-spec.

## 0. Where Phase 2 starts from

Phase 1 delivered the walking skeleton, accepted on the trunk 2026-07-15: ingest
(stream → 5-min bars → `BarClosed` → persist → detector → alert+outbox+NOTIFY), bot
(LISTEN + reconcile sweep), serve (authenticated bars read), replay harness v0
(semantic-decision multiset diff, first golden fixture in CI), compose spine + CI.
Alembic head is **rev 0004** (`stream_bars_5m`, `alerts`, `outbox`, `schwab_tokens`,
`ix_alerts_unposted`, `service_clients`).

> **Migration discipline (S004/S005 collision lesson):** the next revision is **0005**
> and every new migration must be authored on a branch that already contains 0004 —
> merge the trunk in before numbering.

Phase 2 turns this one-ticker skeleton into the real data plane: the full two-band
universe, all three datasets writing, warm-up, freshness, notifications, pollers — and
v2's single alert class, the **options-flow detector** (ADR-0030).

## 1. Opening design session — the detection model *(gates the vertical)*

Per [ADR-0030](./adr/0030-detection-rederived-single-alert-class.md), the novel
options-flow detection model is designed **just-in-time at Phase-2 open**, against real
payloads, and **closes as its own ADR (next number: 0032) before any detector slice
starts**. Horizontal slices (§3) do not wait on it — with one exception: the S008 spec
waits for the session to pin the curated scan's **capture width** and **per-contract
field schema** (see §3's sequencing driver).

Agenda — the four named inputs from the roadmap, with their evidence:

1. **Equity-bar fusion.** Does the model consume `BarClosed` context from the equity
   stream (e.g., unusual options flow against a *flat* underlying) alongside the 5-min
   chain scan? If yes, the streaming→detection seam becomes load-bearing for the real
   alert class and the canary (§4) demotes to a test fixture; if no, the canary stays
   in production as the seam's liveness proof.
2. **Dedup / cooldown / escalation semantics.** Re-fire suppression grain (per-contract
   vs per-underlying) · cooldown windows · the tiered threshold ladder of the
   escalation lifecycle ([ADR-0026](./adr/0026-charting-deprecation-and-reports.md) as
   amended: **messages are never edited** — crossing a further tier posts a *new* alert
   referencing its parent, so the taxonomy needs a parent-linkage seam, e.g. nullable
   `parent_alert_id` + tier in the config stamp) · whether day-2 OI
   confirmation/denial participates (incl. downgrade posts) — a session decision.
   Warm-up ([ADR-0018](./adr/0018-temporal-hot-cold-model-and-warmup.md)) loads
   per-subject last-tier state, **so the session must define exactly what that state is**.
3. **The chain-spike empirical findings**
   ([findings-2026-07-06.md](./specs/fixtures/chains/findings-2026-07-06.md),
   [oi-change-2026-07-09-to-2026-07-10.md](./specs/fixtures/chains/oi-change-2026-07-09-to-2026-07-10.md)):
   OI is static intraday → confirmation is inherently next-day; day D's settled OI
   reflects D−1's trading (the PIT lag the confirmation design must encode); OI-delta
   *sign* gives the build-vs-unwind split intraday volume can't; nets must be computed
   over the **shared** roster, not book totals (ETF expiry churn is material); snapshot
   deltas are not monotonic — reject rows/snapshots whose `quote_time` regresses
   (chain-wide stale-response event observed live).
4. **The scan-cycle reality.** The curated scan is a rolling **~4-minute call train**,
   not an instantaneous snapshot; per-row `fetched_at`/`quote_time` is the event-time
   rule — never the cycle's nominal T.

**Session outputs:** the detection-model ADR · the alert-taxonomy fields (parent
linkage, tier, detector-config-version stamp) as migration input for S016 · the warm-up
state definition for S015 · the canary disposition (§4) · the curated scan's **capture
width** (confirming or amending ADR-0020's ATM±5 / ≤30 DTE) and **per-contract field
schema** (the "Stage-2 domain-modeling artifact" ADR-0020 defers) as spec input for
S008. Conducted as a dedicated working session — one design question at a time,
decisions recorded as they close.

## 2. Second design artifact — `UniverseManager` design note

[ADR-0019](./adr/0019-two-band-universe-and-priority.md) requires a **full design note,
not a half-page spec**, covering: the symbol master (Nasdaq Trader listing files —
[ADR-0025](./adr/0025-live-source-set-schwab-apewisdom-nasdaq.md)) · instrument filter
rules · the diff-based listing/delisting/rename lifecycle · the locked priority order
(paper-trade positions → active alert subjects → watchlists → screener top-10 →
top-100 popularity → discovery) with eviction · the sizes glossary (~500 streamed /
~5–6k optionable / ~10k listed). Independent of §1 — can run in parallel with it or
with the early substrate slices. Gates S010.

## 3. Slice breakdown *(proposal — each slice gets a spec before build)*

**Sequencing driver** *(amended 2026-07-16 — supersedes the data-loss driver)*: both
options datasets are **forward-only**
([ADR-0020](./adr/0020-three-datasets-and-backtest-constraints.md)), but pre-cutover
capture has no product value — what matters is that the scan captures the **right**
data from go-live onward. Two S008 parameters are detection-model outputs: the
**capture width** (ADR-0020's ATM±5 / ≤30 DTE, which the §1 session confirms or
amends) and the **per-contract field schema** (which fields the detector consumes —
IV, greeks, bid/ask). So the §1 session goes first and the S008 spec follows it,
written once, right-shaped. S008's *cycle model* (scan order, `cycle_id`, event-time
rule, overrun/retry/stale policies) stays pre-pinned by the roadmap and is
§1-independent.

| Slice | Scope | ADRs | Depends on |
|---|---|---|---|
| **S008** | `options_chain_snapshot` hypertable (`dataset` discriminator — ADR-0032 D5) + the **curated 5-min REST scan**. The spec must pin the **cycle model**: deterministic scan order, `cycle_id` on rows, per-row `fetched_at`/`quote_time` event-time rule, overrun policy (BEST_EFFORT skip, tombstone-visible), 502 retry-once-then-tombstone, stale-response guard (reject on `quote_time` regression). Bootstrap with a **static curated list** (the spike's 10 tickers or similar) until S010 supplies the real one. | 0020, 0017 | §1 ADR (0032) — capture width + field schema only; cycle model pre-pinned |
| **S009** | **Options EOD full scan**: nightly, whole optionable universe (~5–6k), `dataset=eod_full`, sequenced inside the REST budget ([schwab-rest-budget.md](./schwab-rest-budget.md)); establishes the day-over-day OI-diff basis (shared-roster netting). | 0020, 0025 | S008 (table) |
| **S010** | **Symbol master + `UniverseManager`**: nightly Nasdaq Trader listing ingest, filter rules, listing/delisting/rename lifecycle, two-band desired-set computation (stream ≤ ~500 / curated ~400), diff → ADD/UNSUB, priority eviction, fully reconstructible from Postgres. Replaces S008's static list and the skeleton's one-ticker subscription. | 0019, 0025 | §2 design note |
| **S011** | **Equity research history**: nightly 1-min EOD backfill (whole universe), 5-min + daily **continuous aggregates**, columnar **compression policies** (>~7d chunks), corporate-actions table + adjustment-at-read (`adjusted_through`), missed-night alarm + 7-day retry window; `tools/backfill` equity seed CLI (adjustment-basis stamped). | 0021, 0022, 0025 | — |
| **S012** | **`FreshnessPolicy` + staleness**: per-dataset policy enum, `fetched_at`/`source_status` columns + tombstone rows as the scan-membership record, `max_age` budget **config table** (provisional cadence×2 values), `source_last_success_seconds` metrics, stream-level liveness budget. S008/S009 rows are the first consumers. | 0017 | S008 |
| **S013** | **Notifications layer**: `NotificationBackend` protocol + Discord-webhook/ntfy backends, category routing (`alerts.market` … `system.critical`), `notification_routes` table seeded by migration; joins `rs_observability.hooks`' SYSTEM_CRITICAL half (token expiry → ntfy) and S011's missed-backfill alarm. | 0016, 0015 | — |
| **S014** | **Pollers**: ApeWisdom popularity + history table · Nasdaq earnings calendar · Nasdaq corporate-actions (forward) feeding S011's adjustment table. Universe/report/serve inputs only — **not alert sources**. | 0025, 0030 | — (feeds S010 bands) |
| **S015** | **Warm-up from durable log**: ADR-0018's four steps (rebuild subscription universe from Postgres → replay recent hypertable rows into rolling windows → bounded REST reseed → load dedup/last-tier lifecycle state); detectors disabled + stream unsubscribed until complete. | 0018 | §1 (state definition), S010 |
| **S016** | **Detector-framework seams + canary promotion**: detection port, generic alert-type taxonomy migration (`parent_alert_id`, tier, **config-version stamp** per ADR-0030), per-detector config, pluggable per-type rendering; S003's `ThresholdDetector` promoted to a **metrics-only canary** (§4). Extensibility proven by a trivial second detector registering in tests without pipeline changes. | 0030, 0026, 0008 | §1 |
| **S017** | **The options-flow detector** — the vertical. Co-developed with the replay harness; ships golden fixtures + semantic assertions, the escalation lifecycle (tier re-posts, parent-linked), config-stamped alert rows. | 0030, the §1 ADR (0032), 0026 | S008/S009, S015, S016 |

Suggested wave order (dependencies permit overlap; amended 2026-07-16): **wave 1**
§1 session + §2 note (in parallel) → **wave 2** S008 first, then S009, S011, S012 →
**wave 3** S010, S013, S014 → **wave 4** S015, S016 → **wave 5** S017. The midpoint
re-spec (~week 3–4) falls around wave 3.

## 4. Canary detector (roadmap clause, carried verbatim)

S003's skeleton threshold detector is **promoted, not deleted**: it stays in production
as a **metrics-only canary on `BarClosed`** — consumes the real detection port,
increments `canary_decisions_total`, **writes no alert rows, touches no user surface**.
This keeps the streaming→detection seam production-hot and burn-in-observable until a
real equity-bar detector ships. If the §1 session fuses bar context into the
options-flow model, the canary demotes to a test fixture instead. Implemented in S016.

## 5. Midpoint cut-first order *(roadmap-required; proposal, session may amend)*

**Never cut:** the detector vertical and its substrate — options scans (S008/S009),
warm-up (S015), freshness (S012), framework seams (S016).

Cut first → last if the appetite runs short:

1. **S014 pollers** — popularity/earnings feed universe bands and Phase-3 surfaces,
   not detection; S010 can run on watchlist + static discovery bands meanwhile.
2. **S013 full routing table** — fall back to hardcoded dev-channel + ntfy for
   SYSTEM_CRITICAL; the `/setup` surface is Phase 3 anyway.
3. **S011 aggregates/compression polish** — continuous aggregates + compression can
   land at burn-in start (they are gate **G6** inputs, not Phase-2 exit clauses); the
   nightly 1-min backfill itself stays (it is a dataset, cuttable last within S011).
4. **`tools/backfill` cutover seed** — needed by cutover, not by Phase-2 exit.

## 6. Exit criteria → slice mapping

From [roadmap.md](./roadmap.md) §Phase 2 exit:

| Exit clause | Proven by |
|---|---|
| Full two-band universe streamed/scanned per ADR-0019 | S010 (+S008 scan on the real curated list) |
| All three datasets writing per ADR-0020 | S008 (curated 5-min) · S009 (EOD full) · S011 (equity research history) |
| Warm-up proven by a mid-day restart on the dev box | S015 live drill (rehearsal-#2 prototype) |
| Options-flow detector has green replay fixtures | S017 golden fixtures in CI |
| Extensibility proven in tests (trivial second detector registers without pipeline changes) | S016 acceptance test |

## Status log

- 2026-07-15 — Plan written at Phase-2 open (Phase 1 closed same day). Next action:
  schedule the detection-model design session (§1); §2 design note and S008 spec can
  start in parallel.
- 2026-07-16 — **Wave 1 resequenced: §1 session first; S008 defers behind it** (moved
  to the front of wave 2). The data-loss sequencing driver is retired by decision:
  pre-cutover capture has no product value; the real driver is capturing the *right*
  data from go-live, and S008's capture width + per-contract field schema are §1
  outputs (added to §1's session-output list; S008's cycle model stays pre-pinned).
  Next action: run the §1 session.
