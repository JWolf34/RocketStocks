# S001 — Monorepo restructure: `libs`/`services`/`tools` roots, v1 → `legacy/`

**Phase**: 0 · **Appetite**: 2 days · **Depends on**: — · **ADRs / notes**: [0001](../adr/0001-monorepo-restructure.md), [0028](../adr/0028-execution-strategy-greenfield-terminal-burn-in.md)

## Goal

After this slice the four top-level roots from ADR-0001 exist, v1 lives intact at `legacy/` and remains fully deployable and tested from there, and the v2 roots are importable scaffolds CI knows about.

## Interface

**Naming convention — locked at spec review 2026-06-12, binding for every member:**

- Import name = **`rs_` + member directory name, no exceptions** (`libs/domain` → `rs_domain`; `libs/observability` → `rs_observability`, never `rs_obs`). Distribution names take dashes (`rs-domain` ↔ `rs_domain`).
- **src-layout with per-member packaging:** every member is `<root>/<member>/` containing `pyproject.toml` + `src/rs_<member>/` + `tests/`. Each member's `pyproject.toml` declares its allowed dependencies — layer rules are enforced by the dependency closure, not by convention.
- Tests live per member; top-level `tests/` is reserved for cross-service replay/e2e suites.
- `libs/` members at creation: `domain`, `persistence`, `observability`, `analysis` (pure math — indicators, z-scores, flow metrics; renamed from ADR-0001's draft `core`), `sources` (Schwab/ApeWisdom/Nasdaq adapters).
- Repo-dev helpers (lint gates, check scripts) live in `scripts/`, not `tools/` (`tools/` = shipped operational CLIs only).
- **Banned, permanently:** any `rs_common`/`rs_utils` grab-bag. Shared code belongs to the lib it serves or to `domain`.

Structural deliverables:

- `libs/` · `services/` · `tools/` — created, each with a minimal placeholder package that imports cleanly.
- `legacy/` — contains the entire current v1: `legacy/src/rocketstocks/`, `legacy/tests/`, `legacy/pyproject.toml` (moved, not rewritten).
- Root `pyproject.toml` — becomes the workspace root for v2 packages.
- `Dockerfile` / `docker-compose*.yml` — v1 deployment builds from the `legacy/` path (the frozen v1 must stay shippable until cutover).
- CI — runs `legacy/` tests + v2 lint/placeholder tests as separate jobs.
- `CLAUDE.md` — run/test commands updated for the `legacy/` paths.

## Non-goals

No v2 functionality, no schema, no service code (S002/S003+). No changes to v1 behavior — this is a pure relocation; v1 is in code freeze.

## Acceptance criteria

- [x] Full v1 suite (2,804 tests — the roadmap's ~1,007 was a stale estimate) passes from the new location.
- [ ] v1 Docker image builds from `legacy/` and `docker-compose.yml` config validates. *(No Docker on the dev machine — verified by the `docker` CI job on the slice PR.)*
- [x] Each v2 root contains one importable placeholder package; root workspace installs them in a fresh venv.
- [ ] CI green with the two-job split (legacy / v2) — confirmed on the slice PR.
- [x] `git log --follow` traces history through the move (move via `git mv`).

## Verification

```bash
.venv/bin/pytest legacy/tests -q                    # v1 suite green from new path
docker compose -f docker-compose.yml config -q       # compose still valid
docker build -f Dockerfile .                         # v1 image builds
python -c "import rs_domain"                         # v2 placeholder packages importable
```

## Open questions

*(Package naming + member layout were resolved 2026-06-12 and folded into Interface. Both remaining questions resolved at implementation review 2026-07-05:)*

1. **Workspace tooling — resolved: `uv` workspace** at the root (one `uv.lock`, per-member `pyproject.toml`s, `uv sync --all-packages`).
2. **Dev environments — resolved: one shared venv** at the repo root; revisit only if legacy/v2 dependency conflicts actually appear.

## Decisions surfaced

Implementation review + build, 2026-07-05:

- **v1 dependency bug found and fixed (code-freeze bugfix):** `core/analysis/direction_model.py` imports scikit-learn, which was never declared in `pyproject.toml` (it existed only in the stale root `requirements.txt`). A fresh-venv baseline run failed 20 `test_direction_model` tests with `ModuleNotFoundError`. Blast radius: `backtest/` only (offline CLI), never the bot's live loop, so production carried it harmlessly. Fix: `scikit-learn` added to `legacy/pyproject.toml` dependencies (John approved 2026-07-05).
- **`requirements.txt` deleted, not moved:** referenced nowhere in the repo (grep across Dockerfile, compose, CI, README, docs) and factually stale (`psycopg2-binary` vs the psycopg3 v1 uses) — though it was the only place the missing scikit-learn dep was recorded (see above).
- **Dockerfile stays at repo root**, COPY paths point into `legacy/`; relocation deferred until v2's shared service Dockerfile exists and forces the question. Side effect: the old `COPY . .` (which could bake repo-root `.env` into images) is gone.
- **Suite size correction:** the v1 suite is **2,804 tests**, not the ~1,007 the roadmap cited.
- **Members created:** `libs/{domain,persistence,observability,analysis,sources}` (spec mandate) + `services/ingest` (S003's home) + `tools/migrate` (S002's home). `serve`, `bot`, `backfill`, `backtest` are minted by their own slices.
- **Python pinned to 3.13** (`.python-version`) matching the `python:3.13-slim` production image; build backend for v2 members is `uv_build`.
- **Root pytest config uses `--import-mode=importlib`** (testpaths: `libs`, `services`, `tools`) so same-named test files across members collect together; `legacy/` keeps its own pytest config (`rootdir=legacy/`).
- **CI got a third job** beyond the spec's two-job split: `docker` (compose config check + image build) — the dev Mac has no Docker, so CI is the only place the image-build acceptance criterion can be verified. Triggers: `pull_request` (any target) + push to `feat/v2-rewrite`/`main`.
- **Deferred checklist item for the eventual `feat/v2-rewrite` → `main` merge:** retag the current production image (`ghcr.io/jwolf34/rocketstocks:latest`) as `:pre-s001` before merging, so rollback from the relocated build is one compose edit. Nothing in this slice touches `main` or the published image.
