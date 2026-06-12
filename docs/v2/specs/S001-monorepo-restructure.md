# S001 — Monorepo restructure: `libs`/`services`/`tools` roots, v1 → `legacy/`

**Phase**: 1 · **Appetite**: 2 days · **Depends on**: — · **ADRs / notes**: [0001](../adr/0001-monorepo-restructure.md), [0028](../adr/0028-execution-strategy-greenfield-terminal-burn-in.md)

## Goal

After this slice the four top-level roots from ADR-0001 exist, v1 lives intact at `legacy/` and remains fully deployable and tested from there, and the v2 roots are importable scaffolds CI knows about.

## Interface

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
python -c "import rs_domain"                         # v2 placeholder importable (name per decision below)
```

## Open questions

To resolve at spec review (each carries a recommendation):

1. **v2 package naming.** `legacy/` owns the `rocketstocks` distribution/import name, and both stacks coexist in dev tooling until cutover. **Recommendation:** short `rs_*` prefix for v2 packages (`rs_domain`, `rs_persistence`, `rs_obs`, `rs_core`, services as `rs_ingest`/`rs_serve`/`rs_bot`); legacy stays untouched (mass-renaming frozen v1 violates the freeze).
2. **Workspace tooling.** **Recommendation:** `uv` workspace at the root (one lockfile, per-package `pyproject.toml`s); fallback is plain editable installs of each package.
3. **Dev environments.** One shared venv (simple, name-clash-safe under rec. 1) vs. separate venv for `legacy/`. **Recommendation:** one venv until it hurts.
