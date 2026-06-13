# S001 — Monorepo restructure: `libs`/`services`/`tools` roots, v1 → `legacy/`

**Phase**: 1 · **Appetite**: 2 days · **Depends on**: — · **ADRs / notes**: [0001](../adr/0001-monorepo-restructure.md), [0028](../adr/0028-execution-strategy-greenfield-terminal-burn-in.md)

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

- [ ] Full v1 suite (~1,007 tests) passes from the new location.
- [ ] v1 Docker image builds from `legacy/` and `docker-compose.yml` config validates.
- [ ] Each v2 root contains one importable placeholder package; root workspace installs them in a fresh venv.
- [ ] CI green with the two-job split (legacy / v2).
- [ ] `git log --follow` traces history through the move (move via `git mv`).

## Verification

```bash
.venv/bin/pytest legacy/tests -q                    # v1 suite green from new path
docker compose -f docker-compose.yml config -q       # compose still valid
docker build -f Dockerfile .                         # v1 image builds
python -c "import rs_domain"                         # v2 placeholder packages importable
```

## Open questions

To resolve at spec review (each carries a recommendation). *(Package naming + member layout were resolved 2026-06-12 and folded into Interface.)*

1. **Workspace tooling.** **Recommendation:** `uv` workspace at the root (one lockfile, per-member `pyproject.toml`s); fallback is plain editable installs of each member.
2. **Dev environments.** One shared venv (simple, name-clash-safe under the `rs_*` prefix) vs. separate venv for `legacy/`. **Recommendation:** one venv until it hurts.

## Decisions surfaced

Filled during implementation.
