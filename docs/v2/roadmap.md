# RocketStocks v2 — Build Roadmap

> How the architecture in [architecture-brief.md](./architecture-brief.md) gets built. Strategy locked in [ADR-0028](./adr/0028-execution-strategy-greenfield-terminal-burn-in.md): **all-in greenfield, linear build, terminal burn-in gate, clean-slate cutover.** v1 is in code freeze and serves users untouched until cutover.

## Operating method

- **Spec-driven slices.** Every work item gets a half-page spec ([spec-template.md](./spec-template.md), instances in [specs/](./specs/)) *before* implementation: ADR links, interface, acceptance criteria, exact verification commands. Review happens at the spec and at the verification output, not line-by-line. Specs are updated in the same PR when reality disagrees; decisions that surface mid-build go to the ADR ledger.
- **Appetites, not estimates** (Shape-Up style). Each phase has a fixed timebox; when it runs out, cut scope — never extend time. Slices that miss get re-specced smaller.
- **Replay-harness-first.** The deterministic replay engine (ADR-0023) is built with detector #1 and every detection slice thereafter ships with a golden fixture + semantic assertions. Warm-up and backtest reuse the same seam.
- **Contract-first for `serve` only.** The OpenAPI surface is authored and reviewed before endpoint implementation, driven by a Cowork-shaped consumer test. Internal bus events stay cheap to change until replay fixtures pin them.
- **Hard rules carried from the ADRs:** no ambient `datetime.now()` in domain code (injected `Clock`, enforced by lint/grep gate); validation at the edges (Pydantic); mutations fail loud; hypertable DDL lives inside Alembic revisions.

## Phase 0 — Pre-flight + Stage 2 domain pass *(appetite: 2 weeks)*

Pre-flight checklist (each item is a yes/no with evidence, not a vibe):

- [ ] **v1 code freeze confirmed** — already in effect (2026-06-11). Bugfixes only; the parity target below is now fixed.
- [ ] **Schwab API budget check** — enumerate v1's REST call cadence (poll jobs) vs. v2's planned stream + 5-min options scans + nightly EOD scan; confirm the shared per-account rate limit absorbs both stacks during the burn-in window. Output: a one-page budget table.
- [ ] **Box capacity check** — RAM/disk for two Postgres instances (v1 + TimescaleDB) plus six containers for the burn-in window.
- [ ] **Private dev channel** created and its ID configured for burn-in alert routing.
- [ ] **Schwab streaming sanity spike** (timeboxed: 1 day) — authenticate, subscribe `level_one_equity` for one ticker, log raw payloads for a market session. Pure throwaway; output is a payload sample checked into `specs/fixtures/` and a list of surprises for S003.

Stage 2 domain pass — **expressed as code, not prose** (timeboxed; the skeleton is allowed to revise it):

- `libs/domain`: domain events (`BarClosed`, …) as serializable plain data with `(event_time, sequence)` total ordering; `Clock` protocol; event-source port (live + recorded adapters share it); core Pydantic models (Bar, Quote, Alert, OptionSnapshot).
- `libs/persistence`: SQLAlchemy 2.x async models + Alembic rev-001 (bars, alerts, outbox; hypertable DDL via `op.execute`).
- Spec: [S002](./specs/S002-domain-seed-and-persistence-rev-001.md).

**Exit:** domain + rev-001 land with ephemeral-Postgres integration tests green; clock-discipline grep gate wired into CI.

## Phase 1 — Walking skeleton *(appetite: 3 weeks)*

Thinnest end-to-end path through all three services, deliberately routed through the riskiest seams (streamer reconnect, Timescale-DDL-in-Alembic, outbox + NOTIFY + reconcile). The skeleton is allowed to be ugly; it exists to kill integration risk and give the data model its first live-feed contact while change is cheapest.

| Slice | One-liner | Spec |
|---|---|---|
| S001 | Monorepo restructure: `libs/`/`services/`/`tools/` roots; v1 → `legacy/` still deployable, tests green | [spec](./specs/S001-monorepo-restructure.md) |
| S002 | Domain seed + persistence rev-001 (Phase-0 deliverable, listed for ordering) | [spec](./specs/S002-domain-seed-and-persistence-rev-001.md) |
| S003 | `ingest` skeleton: Schwab L1 stream (1 ticker) → 5-min bar build → `BarClosed` → ordered fan-out (persist → cache stub → hardcoded detector) → alert+outbox row → NOTIFY | [spec](./specs/S003-ingest-skeleton-one-ticker.md) |
| S004 | `bot` skeleton: LISTEN, reconcile `posted_at IS NULL` on reconnect, render one minimal embed to the dev channel | TBD |
| S005 | `serve` skeleton: FastAPI, one read endpoint (`/v1/bars/{ticker}`), API-key middleware stub, `/metrics` | TBD |
| S006 | Spine: shared Dockerfile + compose entries for all three services; structlog JSON + `correlation_id`; CI (tests + lint + clock-grep) | TBD |
| S007 | Replay harness v0: record S003's event stream to a fixture; replay through the real pipeline; assert identical semantic alert decisions | TBD |

**Exit:** the skeleton runs a full live market day on the dev box; S007 replays that day and reproduces identical alert decisions; all three services run under compose; CI green; skeleton merged to `main` (per ADR-0001).

## Phase 2 — Data plane *(appetite: 6–8 weeks; re-spec at midpoint)*

**Opens with the detection-model design session** (just-in-time, ADR-0030): the novel options-flow detection model is designed against real payloads from the Phase-0 spike + Phase-1 skeleton, and closes as its own ADR/design note *before* detector slices start. v1's alert types are retired — nothing here is parity-checked against `docs/alerts/` (v1-historical). **Named session input:** whether the model fuses **equity-bar context** from `BarClosed` (underlying price/volume — e.g., unusual flow against a flat price) with the options scan; if yes, the single alert class makes the streaming→detection seam load-bearing for real.

- **Horizontals:** `UniverseManager` (two-band, priority/eviction — ADR-0019; **requires a full design note, not a half-page spec**) · warm-up from durable log (ADR-0018) · nightly 1-min EOD backfill + 5-min/daily rollups + compression policies (ADR-0021/0022) · per-source `FreshnessPolicy` + staleness columns (ADR-0017) · notifications layer + routes table (ADR-0016) · ApeWisdom poller + popularity history · Nasdaq earnings poller · options 5-min curated REST scan + options EOD full scan (ADR-0020) · `tools/backfill` (equity history seed).
- **Vertical:** the **options-flow detector** — v2's single alert class (ADR-0030) — co-developed with the replay harness, shipping with golden fixtures, the **config-version stamp** on alert rows, and the OI-confirmation update-in-place lifecycle (ADR-0026). Plus the **detector-framework extensibility seams**: detection port, generic alert taxonomy, per-detector config, pluggable rendering.
- **Canary detector:** S003's skeleton threshold detector is **promoted, not deleted** — it stays in production as a **metrics-only canary on `BarClosed`** (consumes the real detection port, increments `canary_decisions_total`, writes no alert rows, touches no user surface). This keeps the streaming→detection seam production-hot and burn-in-observable until a real equity-bar detector ships; if the design session fuses bar context into the options-flow model, the canary demotes to a test fixture.

**Exit:** full two-band universe streamed/scanned per ADR-0019; all three datasets writing per ADR-0020; warm-up proven by a mid-day restart on the dev box; the options-flow detector has green replay fixtures; extensibility proven in tests (a trivial second detector registers without pipeline changes).

## Phase 3 — Surfaces *(appetite: 4–6 weeks)*

- **`serve` (contract-first):** OpenAPI authored + reviewed → endpoints by group (bars/quotes/movers, popularity, earnings, alerts, portfolio) → `service_clients` auth + per-consumer rate limits (ADR-0012) → the Cowork-shaped consumer test runs green against a live instance.
- **Report-design session** (just-in-time, deferred by ADR-0026): field sets, the top-3 contract ranking basis, and serve exposure for Cowork — closes before the report slices below.
- **`bot`:** slash + components per ADR-0014 (mobile-first; v1 designs do not anchor) — alert posting with `💵 Trade` button · `/trade buy|sell` manual paper trading (ADR-0027) · `/setup notifications|channels|roles|schwab` · watchlists · report surfaces (no charting — ADR-0026). Bot code is performance-first (defer early; hot reads from the cache); **numeric latency budgets are deliberately deferred to this phase's planning** (ADR-0029).
- **`tools/backtest`:** rebuilt on the new datasets as a **`serve` API consumer** (the three-consumer contract — ADR-0011), reusing the replay machinery; validates the options-flow signal via event studies (forward equity returns at 1d/3d/5d/10d — ADR-0020); options strategies forward-only.

**Exit:** parity matrix (below) fully green or explicitly cut; bot dogfoods `serve` where the brief requires it.

## Phase 4 — Terminal burn-in *(fixed window: 3–4 market weeks)*

The complete v2 stack runs unattended against the live market; alerts route to the **private dev channel**; v1 continues serving users untouched. Scheduled rehearsals during the window:

1. One Schwab refresh-token expiry ridden end-to-end: expiry → `SYSTEM_CRITICAL` ntfy push → manual re-auth → stream resumes (gap bounded and *detected*).
2. One deliberate `ingest` restart mid-market-day: warm-up rebuilds state; replay-vs-live state assertion passes; no duplicate or missing alerts.
3. One deliberate `bot` kill during market hours: on reconnect, the `posted_at IS NULL` reconcile posts exactly the missed alerts.

Operator involvement otherwise limited to reading dashboards/notifications. Every unscheduled manual intervention is logged — they are gate inputs.

## Phase 5 — Cutover gate + cutover

Cutover proceeds only when **all** criteria hold:

| # | Criterion | Measure |
|---|---|---|
| G1 | Unattended operation | ≥15 consecutive market days full-stack; **0 unscheduled manual interventions in the final 10** |
| G2 | No silent gaps | 0 *undetected* bar gaps: a gap-audit query over 1-min bars finds no gap lacking a corresponding `source_status`/staleness record + notification |
| G3 | Token lifecycle | Rehearsal #1 passed; downtime within the bound set in the burn-in log |
| G4 | Warm-up | Rehearsal #2 passed (replay state == live state) |
| G5 | Bot reconcile | Rehearsal #3 passed (missed alerts = posted alerts, no dupes) |
| G6 | Timescale jobs | Compression observed having compressed chunks; retention/rollup jobs observed firing (or proven by narrowing a policy window) |
| G7 | Detection liveness | The options-flow detector fired ≥1 real alert during burn-in, or its silence is justified by a replay fixture demonstrating correct no-fire |
| G8 | Serve contract | Cowork-shaped consumer test green against the burn-in instance; rate limiting demonstrated (429 on burst past configured limit) |

**Cutover:** point users at v2 per ADR-0010 clean slate (watchlists recreated; popularity cold-start accepted; burn-in data carries over by default — no preservation value either way). v1 stops but stays **dormant-restartable for 2 weeks** as the rollback hatch; then `legacy/` is deleted and the brief is updated to mark v2 live.

## Parity reference (v1 capabilities → v2 disposition)

| v1 capability | v2 disposition |
|---|---|
| Alerts: all six v1 types (volume-accumulation, breakout, momentum-confirmation, watchlist-mover, earnings-mover, popularity-surge) | **Retired** (ADR-0030) — replaced by ONE novel options-flow alert class on an extensible detector framework |
| Screeners / reports | Report surfaces in `bot` (Phase 3); charting **cut** (ADR-0026) |
| Watchlists | Rebuilt (Phase 3); data not migrated (ADR-0010) |
| Manual paper trading (`holdings`/`transactions`/`portfolio_snapshots`) | Rebuilt with `source` discriminator (ADR-0027, Phase 3) |
| Auto-trader | **Cut** — post-v2 service; extraction seam preserved (ADR-0027) |
| Charting | **Cut** (ADR-0026) |
| Backtesting CLI | Rebuilt on new datasets (Phase 3; ADR-0011/0020) |
| EDA | **Deferred** post-v2 (ADR-0011) |
| Admin/config commands | `/setup` umbrella (Phase 3; ADR-0014) |
| Data API / CLI exporter (`api/`) | Superseded by `serve` (ADR-0024) |

## Status log

- 2026-06-11 — Roadmap created; strategy locked (ADR-0028); v1 code freeze in effect; Phase 0 ready to start.
- 2026-06-12 — Philosophy review closed: product thesis stated (ADR-0029); detection re-derived — one options-flow alert class, extensible framework, PIT lean mandate (ADR-0030). Phase 2/3, gate G7, and the parity table updated accordingly.
