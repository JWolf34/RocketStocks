# Phase 1 — Acceptance & exit plan

> **Purpose:** the concrete, ordered steps to confirm acceptance of all Phase-1
> slices (S001–S007) and satisfy the roadmap's Phase-1 exit criteria so work can
> open on Phase 2. This is an operational checklist, not a slice spec — it drives
> existing merged code to accepted state; it introduces no new features.
>
> **Exit criteria being satisfied** ([roadmap.md](./roadmap.md) Phase 1 exit):
> the skeleton runs a full live market day on the dev box · S007 replays that day
> and reproduces identical alert decisions · all three services run under compose ·
> CI green · **skeleton merged to `main`** (ADR-0001).
>
> Snapshot taken **2026-07-13**; progress updated **2026-07-13 (late)**.

## ⏭️ What's left for market open tomorrow (read this first)

D1 is now done (fresh-volume migrate-gate + serve/bot verified 2026-07-14). Only **one**
item genuinely needs a live market session:

- **D6** — start `ingest` under compose for one RTH session (bars written; any alert
  posted by the compose bot). This is the roadmap "full live market day" evidence.
- **D5 (ingest half)** — while D6 runs, `curl 127.0.0.1:9090/metrics` (bot `:9091` ✅).

**D7** (replay gate under compose) needs **no** market — run it tonight; the acceptance
script's `preflight` mode does it. **Tonight's prep** (interactive, ~5 min): stop any
other bot on the token → re-seed the Schwab token (`python -m rs_ingest.tools.login`,
since the fresh `up` wiped `schwab_tokens`) → run `preflight` (validates + does D7).

**Precondition (blocker):** exactly **one** bot instance may hold `RS_BOT_DISCORD_TOKEN`
during the run. The compose bot was logging repeated gateway *session-invalidations* (a
second login elsewhere — likely the deploy VM — sharing the token). Stop the other
instance before D6, or D6's "posted by the compose bot" evidence is contaminated.

Then off-hours: **E** (doc reconciliation) → **F** (merge to `main`) → **G** (sign-off).

## Current state (evidence, 2026-07-13)

- **All seven slice branches merged into `feat/v2-rewrite`** (S001–S007 verified
  ancestors of HEAD). Phase 1 is *code-complete*.
- **Part A fully green** (suite/lint/clock + CI on HEAD). **S004 acceptance independently
  validated by operator (2026-07-13)** — all five bot drills pass. **B provisioning
  proven working** (bot resolved the dev channel + logged in). S006's infra half
  (structured logs, correlation, excepthooks, bot metrics) validated live.
- **Remaining:** the four market-hours items above (D1/D6/D5-ingest/D7) + docs (E) +
  merge to `main` (F) + sign-off (G).

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

## Part D — S006 spine: live acceptance under compose (infra ✅ / live pending)

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
- [ ] **D5. Metrics exposition.** 🟡 **half done.** ✅ bot `:9091` serves all five
  ADR-0015 counters (`discord_posts_total`, `post_failures_total`, `sweeps_total`,
  `notify_wakes_total`, `listen_reconnects_total`), counter-typed. ⏳ ingest `:9090`
  pending — validate during D6 (ingest not running off-hours).
- [ ] **D6. Live market session under compose** *(roadmap "full live market day")*. ⏳
  **market hours.** Start `ingest` under compose; over one RTH session bars are written
  and any triggered alert is posted by the **compose** bot.
- [ ] **D7. Phase-1 gate re-run under compose** *(load-bearing)*. ⏳ **market-day run.**
  S007 replay of the 2026-07-09 session against the **compose-run** stack → IDENTICAL
  (12=12), exit 0:
  ```bash
  python -m rs_ingest.tools.replay --live-db <compose-5498> --expected <7/9 golden>
  ```

## Part E — Documentation reconciliation (S004 done / S006 pending)

- [x] **E1 (S004).** ✅ 2026-07-13 — S004 spec acceptance boxes ticked with an "Accepted
  2026-07-13" note. ⏳ S006 spec boxes remain (tick after D6/D7).
- [x] **E2 (S004).** ✅ 2026-07-13 — roadmap status-log entry added for S004 acceptance.
  ⏳ S006 acceptance entry remains (after D6/D7).
- [ ] **E3.** Tick the roadmap Phase-1 exit clauses as they land. (Includes B4:
  checklist §4 + roadmap "Private dev channel" pre-flight box.)

## Part F — Merge skeleton to `main` (ADR-0001) ⏳ after A–E

- [ ] **F1.** With A–E green, open PR `feat/v2-rewrite` → `main`. Explicit exit
  criterion — the branch is not currently an ancestor of `main`.
- [ ] **F2.** Confirm CI green on the PR (incl. docker jobs), then merge.
- [ ] **F3.** Post-merge sanity: `main` builds `Dockerfile.v2`, compose validates.

## Part G — Phase-1 exit sign-off → open Phase 2 ⏳ last

- [ ] **G1. Exit checklist all green** (below). Record sign-off as a Status-log entry:
  "Phase 1 closed — walking skeleton accepted, merged to main."
- [ ] **G2. Open Phase 2** — begins with the detection-model design session (ADR-0030)
  **before** any detector slice. Named inputs already gathered: the Phase-0 chain-spike
  findings
  ([oi-change-2026-07-09-to-2026-07-10.md](./specs/fixtures/chains/oi-change-2026-07-09-to-2026-07-10.md)),
  the scan-train reality, escalation semantics. This plan hands off there.

---

## Consolidated Phase-1 exit checklist

| # | Exit clause / slice | Confirmed by | Status |
|---|---|---|---|
| 1 | Skeleton runs a full live market day on the dev box | D6 | ⏳ market hours |
| 2 | S007 replays that day → identical alert decisions | D7 (under compose) | ⏳ market-day run |
| 3 | All three services run under compose | D1 (serve+bot+ingest up, migrate gate) | 🟡 migrate-gate + serve + bot ✅ 2026-07-14; ingest pending D6 |
| 4 | CI green | A2 | ✅ run 29292191663 (HEAD, success) |
| 5 | Skeleton merged to `main` (ADR-0001) | F1–F3 | ⏳ after A–E |
| 6 | S004 bot acceptance (5 criteria) | C1–C5 | ✅ operator-validated 2026-07-13 |
| 7 | S006 spine acceptance (7 criteria) | D1–D7 | 🟡 D2–D4 ✅, D5-bot ✅; D1/D5-ingest/D6/D7 pending |
| 8 | Offline suite / lint / clock gate green | A1, A3 | ✅ 233 + 2804 passed |
| 9 | Operator dev channel + token provisioned | B1–B4 | ✅ working (B4 doc-tick pending) |

**Critical path:** the market-morning run (D1 → D6 → D5-ingest → D7) is the only gated
work; a single clean session closes rows 1/2/3/7. Then E → F → G off-hours.

**Not Phase-1 blockers** (deadlined at the Phase 3→4 boundary — do not gate on them):
box capacity (checklist §1), backup/DR (§2, ADR-0031), ingress/TLS proxy (§3, ADR-0012).
