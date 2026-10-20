# RocketStocks v2 rewrite - trajectory audit

Measured 2026-10-19 (a Monday), read-only, from `git log --all` (stash refs excluded), `git worktree list`, `gh issue/pr list`, file mtimes, and `wc` on the `phase-2-spec-010` checkout.
All branch tips and the GitHub `pushed_at` were cross-checked against the remote via `gh api`.

## 0. Headline numbers

| Metric | Value |
|---|---|
| First v2 commit | 2026-05-27 (6dfc49a "init rocketstocks v2"); 145 days / 20.7 weeks ago |
| Roadmap created / v1 freeze | 2026-06-11; 130 days / 18.6 weeks ago |
| Sum of roadmap appetites (Phase 0 through burn-in) | 18-23 weeks from 06-11, i.e. cutover due 2026-10-15 to 2026-11-19 |
| Phase 2 opened | 2026-07-15; 96 days / 13.7 weeks ago |
| Phase 2 appetite | 6-8 weeks, due 2026-08-26 to 2026-09-09; now 54 to 40 days over (1.7x to 2.3x appetite elapsed) |
| Phase 2 midpoint re-spec (due ~2026-08-05 to 08-12) | not done; no roadmap status-log entry after 2026-07-15 |
| Phase 2 slices (S008-S017, 10 planned) | 1 code-complete and merged (S008, 0/5 acceptance boxes ticked), 0 accepted, 9 with no code |
| Last commit on any branch | 2026-08-01 23:12 CT (79 days ago) |
| Last commit on the trunk `feat/v2-rewrite` | 2026-07-17 14:42 CT (94 days ago) |
| Last push to GitHub (`pushed_at`) | 2026-08-02 04:12 UTC (78 days ago) |
| Last GitHub activity of any kind | 2026-10-04 (issue comments and closes, no pushes); 15 days ago |
| Weeks with zero commits since 2026-07-20 | 13 of 14 (the 07-27 week has 2 spike commits) |
| Commit-days since 05-27 | 24 of 145 (16.6%); 16 of those fall inside one 17-day sprint, 07-03 to 07-19 |
| v2 Python today | 9,902 lines (src 4,864 / tests 5,038), 120 files |
| v2 docs today | 55,740 words of Markdown (docs/v2 + CONTEXT.md), 32 ADRs, 8 specs |
| Python added in Phase 2 | +2,368 lines (31% growth), all from S008 |
| Docs added in Phase 2 | +13,587 words (32% growth, from 42,153) |

## 1. Timeline: actual vs appetite

### 1.1 Events

| Date | Event |
|---|---|
| 2026-05-27 | First v2 commits: init + architecture overview (6dfc49a, 5cb73b2) |
| 2026-05-28 | 17 ADRs created in one day |
| 2026-06-05 | 10 more ADRs, 18 commits (27 ADRs total) |
| 2026-06-11 | Roadmap created; ADR-0028 strategy locked; v1 code freeze begins (Phase 0 opens) |
| 2026-06-12 / 06-13 | ADR-0029/0030; S001, S002, S003 specs written; last commits before the gap |
| 2026-06-14 to 07-02 | Zero commits (19 days), inside the Phase 0 appetite window |
| 2026-07-03 | Phase 0 documentation review closed (36 commits) |
| 2026-07-05 | S001 (monorepo) and S002 (domain seed, rev-001) implemented |
| 2026-07-06 | Phase 0 pre-flight closed (3 operator boxes still unticked at the branch tip); Phase 1 opened |
| 2026-07-10 | S003 accepted |
| 2026-07-12 | S005 and S007 implemented and accepted same day |
| 2026-07-13 / 07-14 | S004 accepted, S006 accepted |
| 2026-07-15 | Phase 1 closed; Phase 2 opens; phase2-plan.md written |
| 2026-07-16 | ADR-0032 detection model accepted; S008 spec written; S008 coding starts (22 commits) |
| 2026-07-17 | UniverseManager design note; S008 merged to trunk; 18 GitHub planning issues opened |
| 2026-07-18 | 5 commits of S010 listing research (unmerged branch) |
| 2026-07-19 | S011 roster decision note (unmerged branch) |
| 2026-08-01 | 2 commits (S011 price-history spikes); 8 issues closed in a spec-grilling session; S009 probe tooling written but uncommitted |
| 2026-08-02 | Last push to GitHub; 2 more issues closed |
| 2026-08-03 to 08-05 | Probe and fixture-capture nights planned in the S009 handoff; no capture artifacts exist on disk (`data/oi-settle-probe/` is empty, `data/chain-snapshots/` holds only 2026-07-06) |
| 2026-08-05 to 08-12 | Phase 2 midpoint re-spec window; none recorded |
| 2026-08-26 / 09-09 | Phase 2 appetite expires (6w / 8w); no scope cut or log entry |
| 2026-10-03 / 10-04 | 9 issues closed in two batches; comments claim S010, S011a, S011b specs were drafted; none of those files exist on any ref or on this machine (see 3.4) |

### 1.2 Phases

| Phase | Opened | Closed | Appetite | Actual | Actual / appetite | Commits | Commit-days |
|---|---|---|---|---|---|---|---|
| Pre-phase (architecture, ADRs) | 05-27 | 06-10 | none | 15 d | n/a | 22 | 4 |
| Phase 0 (pre-flight + domain pass) | 06-11 | 07-06 (3 operator boxes open) | 14 d | 25 d (3.6 w) | 1.8x | 69 | 5 |
| Phase 1 (walking skeleton) | 07-06 | 07-15 | 21 d | 9 d | 0.43x | 165 | 9 |
| Phase 2 (data plane) | 07-15 | open | 42-56 d | 96 d and counting | 1.7x-2.3x | 54 | 6 |
| Phase 3 (surfaces) | not opened | | 28-42 d | | | 0 | 0 |
| Phase 4 (burn-in, 3-4 market weeks) | not opened | | 21-28 d | | | 0 | 0 |

Roadmap-implied cutover date: 2026-11-18 (Phase 2 open 07-15 + 8w + 6w + 4w), or 2026-11-19 from the Phase 0 start with maximum appetites.
That date is 30 days away, with Phase 2 at 1 of 10 slices and Phases 3 and 4 unstarted.

## 2. Effort mix over time

Method: `git log --exclude=refs/stash --all --since=2026-05-01 --no-merges --numstat -M`, keyed by commit hash (342 unique non-merge commits, 0 duplicate hashes).
`--all` pulls in `refs/stash`; one stash "index" commit (8a0a1bb, 76 lines) was excluded.
Merge commits carry no numstat, so merged commits are not double counted.
Renames use `-M`, so the S001 `git mv` of v1 into `legacy/` counts as 0 lines.
Cells read `commits touching the bucket, +lines added/-lines removed` (git line counts of prose and code alike).

Buckets:
- (a) `docs/v2/**` excluding fixtures, plus `CONTEXT.md`.
- (b) `libs|services|tools/*/src/**`.
- (c) `libs|services|tools/*/tests/**`.
- (d) `scripts/**` plus `docs/v2/specs/fixtures/**`.
- (e) `legacy/**` plus pre-move v1 paths (`src/`, `tests/`, `.claude/`, `docs/alerts`).
- other: CLAUDE.md, CI, Docker, member pyprojects, docs/agents, handoff files.
- uv.lock is split out as its own column.

### 2.1 By month

| Month | Commits | (a) docs | (b) v2 src | (c) v2 tests | (d) spikes + fixtures | (e) legacy | other | uv.lock |
|---|---|---|---|---|---|---|---|---|
| 2026-05 | 35 | 3: +828/-0 | - | - | - | 31: +3,520/-433 | 1: +3/-0 | - |
| 2026-06 | 36 | 34: +937/-92 | - | - | - | 2: +1,488/-0 | 1: +106/-2 | - |
| 2026-07 | 269 | 95: +2,401/-419 | 83: +5,299/-343 | 67: +6,341/-550 | 31: +3,074/-91 | 3: +23/-23 | 30: +710/-144 | 12: +2,883/-0 |
| 2026-08 | 2 | - | - | - | 2: +801/-0 | - | - | - |
| 2026-09 | 0 | - | - | - | - | - | - | - |
| 2026-10 (to 10-19) | 0 | - | - | - | - | - | - | - |
| **Total** | 342 | 132: +4,166/-511 | 83: +5,299/-343 | 67: +6,341/-550 | 33: +3,875/-91 | 36: +5,031/-456 | 32: +819/-146 | 12: +2,883/-0 |

Legacy notes: the May v1 work (31 commits) predates the freeze.
The 2026-06 legacy +1,488 is the `.claude/docs` push (d7de2dd, 06-07) and the `docs/alerts` banners.
Since the 2026-06-11 freeze the only v1 code edits are the S001 relocation to `legacy/` (a pure rename, 0 lines) and bugfixes: a 4-line import fix (2b1a8d0, 07-05) and a dependency declaration fix in the relocated pyproject.
The freeze has held.

### 2.2 By week (Monday start)

| Week of | Commits | (a) docs | (b) v2 src | (c) v2 tests | (d) spikes + fixtures | (e) legacy | other | uv.lock |
|---|---|---|---|---|---|---|---|---|
| 04-27 | 13 | - | - | - | - | 12: +2,796/-4 | 1: +3/-0 | - |
| 05-04, 05-11 | 0 | | | | | | | |
| 05-18 | 19 | - | - | - | - | 19: +724/-429 | - | - |
| 05-25 | 3 | 3: +828/-0 | - | - | - | - | - | - |
| 06-01 | 19 | 18: +454/-65 | - | - | - | 1: +1,472/-0 | 1: +106/-2 | - |
| 06-08 | 17 | 16: +483/-27 | - | - | - | 1: +16/-0 | - | - |
| 06-15, 06-22 | 0 | | | | | | | |
| 06-29 | 52 | 40: +444/-205 | 5: +625/-51 | 5: +502/-73 | 3: +679/-2 | 3: +23/-23 | 12: +362/-139 | 3: +2,734/-0 |
| 07-06 | 113 | 25: +460/-66 | 49: +3,041/-120 | 39: +3,759/-348 | 19: +1,474/-84 | - | 5: +68/-1 | 4: +122/-0 |
| 07-13 | 104 | 30: +1,497/-148 | 29: +1,633/-172 | 23: +2,080/-129 | 9: +921/-5 | - | 13: +280/-4 | 5: +27/-0 |
| 07-20 | 0 | | | | | | | |
| 07-27 | 2 | - | - | - | 2: +801/-0 | - | - | - |
| 08-03 through 10-19 (12 weeks) | 0 | | | | | | | |

### 2.3 By roadmap phase

| Phase | Calendar days | Commits | Commit-days | (a) docs | (b) src | (c) tests | (d) spikes + fixtures | other | uv.lock |
|---|---|---|---|---|---|---|---|---|---|
| Pre-phase 05-27 to 06-10 | 15 | 22 | 4 | +1,282/-65 | 0 | 0 | 0 | +106/-2 | 0 |
| Phase 0 06-11 to 07-05 | 25 | 69 | 5 | +927/-232 | +625/-51 | +502/-73 | +679/-2 | +362/-139 | +2,734 |
| Phase 1 07-06 to 07-14 | 9 | 165 | 9 | +800/-152 | +3,538/-198 | +4,496/-460 | +1,814/-89 | +259/-5 | +149 |
| Phase 2 07-15 to now | 97 | 54 | 6 | +1,157/-62 | +1,136/-94 | +1,343/-17 | +1,382/-0 | +89/-0 | 0 |

Throughput of v2 src + tests lines added:

| Phase | Lines added (src + tests) | Per calendar day | Per commit-day |
|---|---|---|---|
| Phase 1 | 8,034 | 893 | 893 |
| Phase 2 | 2,479 | 25.6 | 413 |

Phase 2 calendar-day throughput is 3% of Phase 1's.
Phase 2 per-commit-day throughput is 46% of Phase 1's, so the collapse is mostly the number of active days, not the rate on active days.

Docs growth (Markdown words, docs/v2 + CONTEXT.md, at the tip of the then-current line):

| Date | Words |
|---|---|
| 2026-06-13 (before any v2 code) | 18,971 |
| 2026-07-05 | 25,900 |
| 2026-07-15 (Phase 2 start, df36ee6, before the plan doc) | 42,153 |
| 2026-07-18 (current tip) | 55,740 |

Docs-to-code ratio today: 5.6 doc words per Python line (11.5 per src line).

## 3. Phase 2 status

### 3.1 Branches and worktrees

`git worktree list` shows 3 worktrees: the main checkout on `phase-2-spec-010` (986feb7), `.claude/worktrees/spec-design` on `phase-2-spec-009` (d945bfb), and `.claude/worktrees/spec-design-2` on `phase-2-spec-011` (7bb3472).
`main` is at d7de2dd (2026-06-08); `feat/v2-rewrite` is 305 commits ahead of it and unmerged by design (merge to main deferred to cutover).

| Branch | Tip date | Unique commits vs trunk | Merged to `feat/v2-rewrite`? | Contents |
|---|---|---|---|---|
| `feat/v2-rewrite` (trunk) | 07-17 | n/a | n/a | S001-S008, ADR-0032, universe-manager.md, phase2-plan.md; tip d945bfb |
| `docs/phase1-close-phase2-plan` | 07-16 | 7 (since Phase 1 close) | yes | Phase 1 close, phase2-plan.md (155 lines), CONTEXT.md init, ADR-0032 (236 lines), glossary |
| `feat/phase-2-universemanager-design` | 07-17 | 6 | yes | universe-manager.md (392 lines, D1-D11), ADR-0019/0025 amendments, CONTEXT terms |
| `feat/spec-008` | 07-17 | 30 | yes | S008 spec (131 lines) + options_chain_snapshot code: src +1,077, tests +1,338, spec/docs +165 (29 files, +2,591/-50 vs base) |
| `phase-2-spec-009` | 07-17 | 0 | yes (identical to trunk tip) | Branch with no commits of its own; worktree `spec-design` holds uncommitted S009 work |
| `phase-2-spec-010` | 07-18 | 5 | no | Listing research only: findings, instrument-class rules, normalization mapping, 4 synthetic/excerpt fixtures (+581 lines, 7 files, all under `docs/v2/specs/fixtures/listing/`). No spec file, no code |
| `phase-2-spec-011` | 08-01 | 3 | no | S011 roster note (6 lines in phase2-plan), 2 Schwab price-history probe scripts (395 lines) + findings docs (406 lines). No spec file, no code |

Uncommitted work in the worktrees (file mtimes 2026-08-01 14:17 to 23:25, nothing newer):

| Worktree | Files | Lines |
|---|---|---|
| `spec-design` (S009) | `HANDOFF-S009.md` 118, `docs/v2/S009-probe-runbook.md` 164, `scripts/spikes/oi_settle_probe.py` 248, `oi_settle_report.py` 305; modified `README.md` +62, `_common.py` +22/-3, `chain_spike.py` +2/-2 | 921 added |
| `spec-design-2` (S011) | `docs/v2/specs/fixtures/history/cagg-compression-2026-08-02.md` 200, `scripts/spikes/cagg_compression_spike.sql` 283 | 483 added |
| 3 git stashes (2026-07-17) | handoff notes for S009 (79 lines) and S010 (76 lines) | 155 |

The S009 handoff says "all uncommitted, all intentional" and "do not commit or stash without being asked".
That is 1,404 added lines of uncommitted work, all dated 08-01, none of it on any remote.

### 3.2 Slice-by-slice (phase2-plan.md S008-S017)

| Slice | Scope | Spec | Code | Status 10-19 |
|---|---|---|---|---|
| S008 | options_chain_snapshot + curated 5-min scan | merged 07-16 | merged 07-17 (24 commits, +2,481 lines src/tests/fixtures) | Implemented; acceptance 0 of 5 boxes ticked (incl. the supervised RTH session); no roadmap status-log entry |
| S009 | options EOD full scan | not drafted (#12 open) | none | 5 of 8 child tickets closed; uncommitted probe tooling; both probe and fixture-capture tickets (#37, #39) open 79 days |
| S010 | symbol master + UniverseManager | claimed drafted 10-03 (issue #22 comment); no file on any local or remote ref; review ticket #23 open | none | Research on unmerged branch; 11 of 12 child tickets closed |
| S011 | equity research history | claimed split into S011a + S011b on 10-03/04 (issue comments); no file on any ref; #35 open | none | Spikes only; 10 of 11 child tickets closed |
| S012 | FreshnessPolicy | none | none | no tickets |
| S013 | notifications layer | none | none | no tickets |
| S014 | pollers | none | none | no tickets |
| S015 | warm-up from durable log | none | none | no tickets |
| S016 | detector-framework seams + canary promotion | none | none | no tickets |
| S017 | the options-flow detector | none | none | no tickets |

Dependency edges on the critical path (phase2-plan.md, as amended 07-16 and 07-19):
- S009, S011 and S015 all depend on S010's first increment.
- S017 depends on S009, S015, S016.
- S010 is the least-specified gate and has been in spec design for 94 days.

### 3.3 Did the detection-model design session happen?

Yes, and it was fast.
- `docs/v2/adr/0032-options-flow-detection-model.md`: created in dacc0d6 (2026-07-16 14:27), accepted in 25cafb0 (2026-07-16 17:40), i.e. day 2 of Phase 2.
- It is 1,861 words, decisions D0-D10: D0 working frame, D1-D8 closed 2026-07-16 (subject = (underlying, side), equity-bar fusion = fuse, baseline, escalation ladder, dataset discriminator, day-2 OI verdict, moneyness capture window, 30-column row schema), D9 warm-up state and D10 taxonomy fields derived.
- The ADR has two commits in its history and no later amendment.
- All 4 roadmap "named inputs" are addressed.
- The ADR then fed S008 (spec the same evening) and defines S015/S016/S017 inputs.
- Nothing downstream of the ADR on the detector side has been started: S015-S017 have no spec, code, or tickets.

### 3.4 Discrepancy: October claims vs repository state

Issue comments dated 2026-10-03/04 state that:
- the S010 spec was "drafted and reconciled on `phase-2-spec-010`" (`docs/v2/specs/S010-symbol-master-and-universe-manager.md`);
- S011a ("on `phase-2-spec-011`") and S011b (`docs/v2/specs/S011b-research-rollups-and-adjustment.md`) were written;
- ADR-0022, ADR-0017 and the roadmap G2(b) were amended (47-day depth, 30-day retry window);
- evidence sits in `data/phase2-planning/s011-30-32.md (local)`.

Checked and not found:
- `git ls-tree` on both branches (local and via `gh api contents` on the remote) returns 404 for the S010, S011a, S011b and S009 spec paths.
- `find /` finds none of those filenames and no `data/phase2-planning/`.
- The remote's `pushed_at` is 2026-08-02, so nothing was pushed after the issue activity.

Either that work lives on another machine or session and is unpushed, or the comments describe work not persisted.
Either way it is invisible in the repo and every number in this report treats it as not delivered.
This should be resolved before relying on any "spec done" claim.

### 3.5 Where Phase 2 effort went

Lines added since 2026-07-15, bucketed by theme (54 commits, +5,107 lines committed; the second table adds the 1,404 uncommitted lines).

| Theme | Commits | Lines added | % of committed |
|---|---|---|---|
| Phase bookkeeping and plan edits | 8 | 258 | 5.1% |
| Detection-model design (ADR-0032, ADR-0017 amend) | 4 | 247 | 4.8% |
| Shared glossary (CONTEXT.md) | 3 | 92 | 1.8% |
| S008 spec | 2 | 153 | 3.0% |
| S008 code + tests + fixtures | 24 | 2,481 | 48.6% |
| UniverseManager design note + ADR amendments | 3 | 405 | 7.9% |
| S010 reference-data research (listings, instrument class, normalization) | 5 | 581 | 11.4% |
| S011 price-history spikes + findings | 2 | 801 | 15.7% |
| Workflow tooling (issue tracker, domain.md, CLAUDE.md refs) | 3 | 89 | 1.7% |
| **Total** | **54** | **5,107** | 100% |

Regrouped by what the plan calls things, including uncommitted work (total 6,511 lines):

| Group | Lines | % |
|---|---|---|
| Detector vertical proper (S015-S017 code, specs) | 0 | 0.0% |
| Detector design (ADR-0032) | 247 | 3.8% |
| Options-scan substrate (S008 spec + code 2,634; S009 probe tooling, uncommitted, 921) | 3,555 | 54.6% |
| Symbol master / UniverseManager / listings / instrument-class / normalization | 986 | 15.1% |
| Equity research history (S011 spikes 801 + uncommitted cagg spike 483) | 1,284 | 19.7% |
| Plan, glossary, tooling | 439 | 6.7% |

Observations:
- Shipping code is 2,481 lines (S008 src and tests), 38.1% of all Phase 2 lines; the other 61.9% is docs, research, spikes and fixtures.
- Reference-data and universe work (symbol master, listings, instrument-class rules, normalization, UM note) is 15.1% of lines but 13 of 34 planning tickets (38%).
- By planning tickets (34 issues, all 2026-07-17 to 08-01): S010 38%, S011 35%, S009 26%, detector 0%.
- Of the 31 child tickets, 26 are closed; the median ticket took 15 days to close, the mean 31 (max 79).
- S008, the only slice delivered, went from spec (07-16 17:36) to merged code (07-17 14:42) in about 21 hours.
- The three specs after it have been in design for 94 (S009), 94 (S010) and 91 (S011) days with no merged spec.
- S011 has already grown from one slice to two (S011a, S011b) per the issue comments.

## 4. GitHub

### 4.1 Issues (`gh issue list --state all`)

| Metric | Value |
|---|---|
| Total | 34 (26 closed, 8 open) |
| Created | 07-17: 18, 07-18: 1, 07-20: 11, 08-01: 4; none since |
| Closed | 07-17: 1, 07-18: 2, 07-20: 4, 08-01: 8, 08-02: 2, 10-03: 2, 10-04: 7 |
| Labels | `wayfinder:grilling` 23, `wayfinder:task` 4, `wayfinder:research` 3, `wayfinder:map` 3, `wayfinder:prototype` 1 |
| v2 related | 34 of 34 (all are Phase 2 spec-planning tickets; zero implementation or bug issues, zero v1 issues) |
| Maps | #6 S009 (8 children: 5 closed), #13 S010 (12 children: 11 closed), #25 S011 (11 children: 10 closed) |

Open issues (8): #6 map S009 (94 d), #12 draft S009 spec (94 d), #13 map S010 (94 d), #23 S010 spec review and merge (94 d), #25 map S011 (91 d), #35 S011 write and review spec (91 d), #37 OI settle probe (79 d), #39 eod_full fixture pair (79 d).
Every spec-drafting and spec-review ticket is still open; the closed tickets are design decisions.

### 4.2 Pull requests (`gh pr list --state all`)

5 total, all MERGED, all from September 2023 (`Analysis` to `main`).
There are zero PRs for v2: the trunk is 305 commits ahead of `main` and every merge into it was a local `git merge`.
The "code review" step in the workflow is therefore not recorded on GitHub.

## 5. Current v2 size (`phase-2-spec-010` checkout, equals trunk plus 5 research commits; working tree clean apart from `.claude/worktrees/`)

### 5.1 Python (`find libs services tools -name '*.py' -not -path '*/.venv/*'`)

| Member | src files / lines | tests files / lines |
|---|---|---|
| libs/analysis | 1 / 1 | 1 / 5 |
| libs/domain | 6 / 306 | 5 / 243 |
| libs/observability | 5 / 345 | 5 / 416 |
| libs/persistence | 12 / 1,004 | 8 / 829 |
| libs/sources | 2 / 71 | 2 / 79 |
| services/bot | 9 / 519 | 8 / 457 |
| services/ingest | 18 / 2,154 | 17 / 2,536 |
| services/serve | 10 / 412 | 7 / 421 |
| tools/migrate | 2 / 52 | 2 / 52 |
| **Total** | **65 / 4,864** | **55 / 5,038** |

Total 120 files, 9,902 lines (raw `wc -l`).
Non-blank non-comment lines: src 3,822, tests 3,786.
Tests are 51% of the code base.
Other v2 artifacts: scripts 1,482 lines (spikes, acceptance, clock gate; .py/.sh/.md/.sql), Alembic head 0005.
Python at Phase 2 start (df36ee6): 7,534 lines; at tip 9,902 (+2,368).

For scale only (scope is not 1:1; v2 retires all six alert types and charting): v1 is 32,116 src lines and 33,191 test lines, so v2 is 15.1% of v1 src and 15.2% of v1 test lines.

### 5.2 Documentation

| Item | Words | Files |
|---|---|---|
| docs/v2 Markdown total | 55,144 | 57 |
| of which ADRs | 18,577 | 32 |
| of which specs (S001-S008) | 11,554 | 8 |
| of which top-level (roadmap 4,315; universe-manager 3,458; architecture-brief 3,057; phase2-plan 2,078; phase1-acceptance-plan 1,587; others) | 16,171 | 9 |
| of which fixture findings (Markdown) | 8,842 | |
| CONTEXT.md | 596 | 1 |
| **docs/v2 + CONTEXT.md** | **55,740** | |
| Fixture data files (jsonl/json/csv/txt), not prose | ~11,300 | 8 |

## 6. Projection (naive)

Today is 2026-10-19.
Roadmap-as-written cutover date: about 2026-11-18.

### 6.1 Assumptions

1. Remaining build work = Phase 2 slices S009-S017 (9 slices) plus Phase 3, which has no slices defined; assumed 8 slices (serve contract and endpoints, auth and rate limits, report design, bot alert posting and trade button, /trade, /setup, watchlists and reports, backtest). Total 17 slices.
2. A slice counts as one unit regardless of size; S011 splitting in two is ignored (this flatters the projection).
3. Burn-in plus gate plus cutover is calendar-bound at 5 weeks (4 market weeks, 1 week for gate and cutover) and does not speed up with effort; G1 alone needs 15 consecutive clean market days.
4. No rework, no failed rehearsals, no new ADR-driven slices, and operator items (backup, capacity, ingress) are not blockers.
5. Two velocity anchors from the history: Phase 2 calendar rate (1 slice in 96 days) and Phase 2 active rate (S008 took about one active week including its spec).

### 6.2 Scenarios

| Scenario | Rule | Phase 2 done | Cutover |
|---|---|---|---|
| A. Observed Phase 2 calendar rate | 1 slice per 96 days, P2 only (9 slices) | 2029-03-01 | 2029-03-01 plus P3 and burn-in |
| A2. Same, all 17 slices | 17 x 96 d | n/a | 2031-04 |
| B. 1 slice per active week at the observed 14% duty cycle (2 of 14 weeks with commits) | 17 / 0.143 + 5 weeks | 2028-01 | 2029-03 |
| C. Same at 25% duty (the 07-03 to now average) | 17 / 0.25 + 5 weeks | 2027-06 | 2028-03 |
| D. Same at 50% duty | 17 / 0.5 + 5 weeks | 2027-02 | 2027-07-19 |
| E. Same at 100% duty from 10-20 (full-time, the July sprint intensity) | 17 + 5 weeks | 2026-12-21 | 2027-03-22 |
| F. Roadmap-faithful restart tomorrow | P2 at 0.6-0.8 w/slice, then P3 4-6 w, burn-in 3-4 w, gate 1 w | 2026-11-26 to 12-08 | 2027-01-21 to 2027-02-23 |
| G. Phase 1 pace (7 slices / 13 days, 0.54 slices per day) | lower bound, skeleton slices were thin | n/a | 2026-12-25 |

Reading the table:
- The observed Phase 2 rate (A, B) puts cutover in 2029 or later, which is a non-answer: it says the project has stopped, not that it is slow.
- Even if July's intensity returned on 10-20 and held with no gaps (E), cutover is about 2027-03-22, 4 months after the roadmap date.
- Under the roadmap's own appetites restarted tomorrow (F), cutover is late January to late February 2027, a slip of 2 to 3 months.
- Burn-in is fixed calendar time, so every week of dormancy before it pushes the date 1:1.

## 7. Interpretation (short)

1. The numbers describe a stop more than a slowdown.
   Phase 1 ran at 893 src+test lines per day and finished in 0.43x its appetite.
   Phase 2 is the same operator after a 17-day sprint (07-03 to 07-19), then 2 commits in 79 days.
   Per active day, Phase 2 output is 46% of Phase 1's; per calendar day it is 3%.
2. Phase 2's active days were spent mostly on design, spec, research and spikes.
   Only 38% of Phase 2 lines are shipping code, all of it S008.
   The detector vertical (S015-S017) is untouched, and none of the three specs written after S008 has merged after 91-94 days.
3. The ADR-0032 design session was not the bottleneck: it closed on day 2 of Phase 2.
4. The critical path runs through the most-researched and least-specified slice.
   S009, S011 and S015 depend on S010 (symbol master), which has research, a design note and 13 tickets but no merged spec.
5. The roadmap's own safeguards did not fire: no midpoint re-spec, no cut-first order invoked, no status-log entry after 07-15, and the appetite expired on 09-09 at the latest with no recorded decision.
6. Whether the project is paused or working elsewhere cannot be resolved from this repo.
   The issue comments of 10-03/04 imply decisions were still being made; the artifacts they cite are not on any ref.
   Recovering or pushing those artifacts is the first measurement fix.

## 8. Measurement notes

- Dates are commit dates in local time (CDT, UTC-5); GitHub timestamps are UTC.
- Line counts are git numstat added/removed, including prose, so docs lines are not comparable to code lines; words are given where it matters.
- Fixtures under `docs/v2/specs/fixtures/` are counted in bucket (d), not (a).
- Phase boundaries come from the roadmap status log, not from commits.
- "Commit-day" = a calendar date with at least one commit on any ref.
- Stash refs excluded; the 3 stashes and the 2 worktrees' uncommitted files are reported separately.
- Items not verifiable here: work on other machines (the project mentions a home-lab VM and WSL), anything unpushed, and chat-only design decisions.
- Process disclosure: while gathering data I accidentally wrote `scratchpad_dummy` (a copy of phase2-plan.md from a stray `git show > file` redirect) into the repo root, noticed it in `git status` on the next command, and deleted it immediately.
  `git status` is back to the original (`?? .claude/worktrees/` only).
  No other repo file was created, edited or committed, no branch was switched, and `git fetch` was not run.
- Working files are in `/private/tmp/claude-501/-Users-john-Documents-Projects-RocketStocks/48d0c7cb-5c8a-4505-9777-88518f6d85b5/scratchpad/audit/`: `effort.py`, `p2buckets.py`, `effort_out.md`, `effort_p2.md`, `roadmap_010.md` (copy of the roadmap at the spec-010 tip).
