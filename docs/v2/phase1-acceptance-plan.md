# Phase 1 — Acceptance & exit plan

> **Purpose:** the concrete, ordered steps to confirm acceptance of all Phase-1
> slices (S001–S007) and satisfy the roadmap's Phase-1 exit criteria so work can
> open on Phase 2. This is an operational checklist, not a slice spec — it drives
> existing merged code to accepted state; it introduces no new features.
>
> **Exit criteria being satisfied** ([roadmap.md](./roadmap.md) Phase 1 exit):
> the skeleton runs a full live market day on the dev box · S007 replays that day
> and reproduces identical alert decisions · all three services run under compose ·
> CI green · **walking skeleton accepted on the `feat/v2-rewrite` trunk** (merge to `main`
> deferred to cutover — trunk → `dev` for burn-in → `main`, per ADR-0001/0028).
>
> Snapshot taken **2026-07-13**; progress updated **2026-07-14 (market run complete)**.

## ⏭️ Market run complete — what's left (read this first)

The 2026-07-14 `scripts/acceptance/phase1_market_run.sh run` closed every market-gated item:
**D6** (one full XNYS session under compose: +77 5-min bars, 15 alerts committed by the
compose bot), **D5 ingest half** (`:9090` counter metrics served during the session), and
**D7** (S007 replay under the compose stack IDENTICAL, exit 0). All seven S006 criteria are
now met. Report: `data/phase1-acceptance-2026-07-13T23-23-40.md`.

No market-gated work remains. Off-hours: **E** (doc reconciliation — roadmap Status-log
entry + dev-channel box) → **G** (sign-off). Merge to `main` is **deferred to cutover** under
the trunk model (Part F), not a Phase-1 step.

## Current state (evidence, 2026-07-14)

- **All seven slice branches merged into `feat/v2-rewrite`** (S001–S007 verified
  ancestors of HEAD). Phase 1 is *code-complete*.
- **Part A fully green** (suite/lint/clock + CI on HEAD). **S004 acceptance independently
  validated by operator (2026-07-13)** — all five bot drills pass. **B provisioning
  proven working** (bot resolved the dev channel + logged in). S006's full spine
  (structured logs, correlation, excepthooks, bot + ingest metrics, live session under
  compose, gate re-run) validated — all seven D-items ✅ 2026-07-14.
- **Remaining:** docs (E) + sign-off (G). Merge to `main` is deferred to cutover (Part F,
  trunk model), not a Phase-1 step.

> **Env note:** if you hit `ModuleNotFoundError` (rs_serve / rs_observability /
> structlog), the shared venv is stale — run `uv sync --all-packages`.

---

## Part A — Offline confirmation (no market needed) ✅ COMPLETE

- [x] **A1. Full suite green.** ✅ 2026-07-13 — `pytest libs/ services/ tools/` → **233
  passed**, no skips (both test DBs up); `ruff` clean; clock-discipline gate OK across
  all five v2 src roots.
  ```bash
  uv sync --all-packages
  .venv/bin/pytest libs/ services/ tools/ -q
  .venv/bin/ruff check libs services tools
  scripts/check_clock_discipline.sh
  ```
- [x] **A2. CI green on the branch.** ✅ 2026-07-13 via `gh` — run
  [29292191663](https://github.com/JWolf34/RocketStocks/actions/runs/29292191663) on
  HEAD `61f6a35`, **conclusion: success**. All three jobs green (v2 members, legacy v1
  suite, docker); the docker job ran both S006 steps explicitly: "Validate v2 compose
  config" (`docker compose -f docker-compose.v2.yml config -q`) and "Build v2 image"
  (`buildx --file Dockerfile.v2`). Only annotations: Node-20 deprecation (cosmetic).
  Self-serve: `gh run list --branch feat/v2-rewrite`.
- [x] **A3. v1 untouched-green.** ✅ 2026-07-13 — `pytest legacy/tests` → **2804 passed**;
  code-freeze parity target intact.

## Part B — Operator prerequisite ✅ PROVISIONED

From [phase0-operator-checklist.md](./phase0-operator-checklist.md) §4. Proven live: the
compose bot resolved `dev_channel_id=710724925438033931` ("testing"), logged in with the
v2 token, and established LISTEN on `alerts`.

- [x] **B1.** ✅ Private dev channel exists (id `710724925438033931`, "testing").
- [x] **B2.** ✅ `RS_DEV_CHANNEL_ID` set in `.env` (bot read + resolved it).
- [x] **B3.** ✅ Dev-bot invited with post/embed permission; `RS_BOT_DISCORD_TOKEN` set
  (distinct from v1's `DISCORD_TOKEN`) — bot logged in successfully.
- [ ] **B4.** Tick checklist §4 boxes + the roadmap "Private dev channel" pre-flight box.
  *(Off-hours doc action — fold into Part E.)*

## Part C — S004 bot skeleton: live acceptance drills ✅ VALIDATED (operator, 2026-07-13)

Ref [specs/S004-bot-skeleton.md](./specs/S004-bot-skeleton.md). **All five criteria
independently validated by the operator, 2026-07-13.** (Spec-box ticking = Part E.)

- [x] **C1. Startup backlog reconcile.** ✅ Backlog posts in id order + stamps
  `posted_at`; immediate restart posts zero.
- [x] **C2. Live NOTIFY path.** ✅ Staged alert posts within seconds on a notify-wake.
- [x] **C3. Severed LISTEN self-heals** *(Finding-3)*. ✅ Post survives a terminated
  listener conn; reconnect visible.
- [x] **C4. Kill/restart (G5 prototype).** ✅ SIGKILL + ≥2 alerts down + restart → each
  posts once, no duplicates.
- [x] **C5. Poison skip (test).** ✅ Failing send skips-and-continues; `post_failures_total`
  increments; healthy alerts post (also green in A1's suite).

## Part D — S006 spine: live acceptance under compose ✅ COMPLETE (2026-07-14)

Ref [specs/S006-spine.md](./specs/S006-spine.md).

- [x] **D1. Fresh-volume compose up on the dev box.** ✅ 2026-07-14 (WSL box) — fresh
  volume ran the full `base → 0001 → 0002 → 0003 → 0004` chain; migrate finished at
  `04:03:10.795Z` and serve (`04:03:12.63Z`) + bot (`04:03:12.69Z`) started ~1.8s
  **after** — and, gated by `service_completed_successfully`, their startup itself
  proves migrate **exited 0**. serve reached "Application startup complete" / Uvicorn on
  :8000; bot resolved the dev channel + `LISTEN established` + Gateway-connected.
  *(Token must be re-seeded after this fresh `up` — `down -v` wiped `schwab_tokens`.)*
- [x] **D2. Structured logs.** ✅ 2026-07-13 — compose logs are one-line JSON with
  `service`/`level`/UTC `timestamp`; `correlation_id` present in-context; third-party
  (discord.py) records render through the same JSON stream.
- [x] **D3. Correlation echo.** ✅ 2026-07-13 — serve returned `X-Correlation-ID`
  (`304066a2…`) and that id appeared on the request's `request completed` record.
- [x] **D4. Excepthooks.** ✅ via A1's `rs_observability` pytest (sys/threading/asyncio
  paths → JSON traceback records).
- [x] **D5. Metrics exposition.** ✅ 2026-07-14 — both halves proven. bot `:9091` serves all
  five ADR-0015 counters (`discord_posts_total`, `post_failures_total`, `sweeps_total`,
  `notify_wakes_total`, `listen_reconnects_total`), counter-typed; ingest `:9090` served its
  counter-typed metrics (`bars_persisted_total`, `alerts_emitted_total`, …) during the D6 run.
- [x] **D6. Live market session under compose** *(roadmap "full live market day")*. ✅
  2026-07-14 — `ingest` ran under compose for one full XNYS session; `stream_bars_5m` 0 → 77
  (+77 5-min bars) and 15 alerts fired + committed by the **compose** bot. Report:
  `data/phase1-acceptance-2026-07-13T23-23-40.md`.
- [x] **D7. Phase-1 gate re-run under compose** *(load-bearing)*. ✅ 2026-07-14 — S007 replay
  against the **compose-run** stack → IDENTICAL, exit 0 (Phase-1 gate PASSED). The re-run
  exercised the session-excerpt fixture (2=2); the full-session 12=12 replay stands from S007
  acceptance.
  ```bash
  python -m rs_ingest.tools.replay --live-db <compose-5498> --expected <7/9 golden>
  ```

## Part E — Documentation reconciliation (S004 done / S006 pending)

- [x] **E1 (S004).** ✅ 2026-07-13 — S004 spec acceptance boxes ticked with an "Accepted
  2026-07-13" note. S006 spec acceptance boxes ticked 2026-07-14 after D5/D6/D7.
- [x] **E2 (S004).** ✅ 2026-07-13 — roadmap status-log entry added for S004 acceptance.
  ⏳ S006 acceptance entry remains (after D6/D7).
- [ ] **E3.** Tick the roadmap Phase-1 exit clauses as they land. (Includes B4:
  checklist §4 + roadmap "Private dev channel" pre-flight box.)

## Part F — Branch model: no Phase-1 merge to `main` (ADR-0001/0028) ✅ N/A

`feat/v2-rewrite` is the long-lived **trunk** for the whole rewrite (Phases 1–3); per-slice
branches merge into the trunk, not `main`. **Phase 1 does not merge to `main`.** Phase-1
acceptance lands on the trunk; the trunk merges to `dev` for the terminal burn-in (ADR-0028)
and to `main` only at cutover — `main` keeps serving v1 untouched until then.

## Part G — Phase-1 exit sign-off → open Phase 2 ⏳ last

- [ ] **G1. Exit checklist all green** (below). Record sign-off as a Status-log entry:
  "Phase 1 closed — walking skeleton accepted on the `feat/v2-rewrite` trunk (merge to
  `main` deferred to cutover)."
- [ ] **G2. Open Phase 2** — begins with the detection-model design session (ADR-0030)
  **before** any detector slice. Named inputs already gathered: the Phase-0 chain-spike
  findings
  ([oi-change-2026-07-09-to-2026-07-10.md](./specs/fixtures/chains/oi-change-2026-07-09-to-2026-07-10.md)),
  the scan-train reality, escalation semantics. This plan hands off there.

---

## Consolidated Phase-1 exit checklist

| # | Exit clause / slice | Confirmed by | Status |
|---|---|---|---|
| 1 | Skeleton runs a full live market day on the dev box | D6 | ✅ +77 bars, 2026-07-14 |
| 2 | S007 replays that day → identical alert decisions | D7 (under compose) | ✅ IDENTICAL under compose, 2026-07-14 |
| 3 | All three services run under compose | D1 (serve+bot+ingest up, migrate gate) | ✅ migrate-gate + serve + bot + ingest all under compose, 2026-07-14 |
| 4 | CI green | A2 | ✅ run 29292191663 (HEAD, success) |
| 5 | Walking skeleton accepted on `feat/v2-rewrite` trunk; merge to `main` deferred to cutover | Part F (trunk model) | ✅ N/A at Phase 1 (ADR-0001/0028) |
| 6 | S004 bot acceptance (5 criteria) | C1–C5 | ✅ operator-validated 2026-07-13 |
| 7 | S006 spine acceptance (7 criteria) | D1–D7 | ✅ all seven met, 2026-07-14 |
| 8 | Offline suite / lint / clock gate green | A1, A3 | ✅ 233 + 2804 passed |
| 9 | Operator dev channel + token provisioned | B1–B4 | ✅ working (B4 doc-tick pending) |

**Critical path:** the market-morning run (D6 → D5-ingest → D7) is **complete** (2026-07-14) —
rows 1/2/3/7 closed. Remaining is off-hours doc/sign-off: **E** (docs) → **G** (sign-off);
merge to `main` is deferred to cutover (Part F, trunk model), not a Phase-1 step.

**Not Phase-1 blockers** (deadlined at the Phase 3→4 boundary — do not gate on them):
box capacity (checklist §1), backup/DR (§2, ADR-0031), ingress/TLS proxy (§3, ADR-0012).
