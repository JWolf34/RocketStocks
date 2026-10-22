# RocketStocks

A Discord bot that scans the whole market and posts a small number of stocks per day worth talking about, each with a Track record and a Report of RocketStocks' own data.

## Read this first

- **[docs/vision.md](docs/vision.md) is the only authority** for what RocketStocks is and what gets built; [CONTEXT.md](CONTEXT.md) is its glossary.
- Architectural decisions are ADRs in [docs/adr/](docs/adr/).
- Operations (deploy, Schwab sign-in, Popularity import, rehearsals, backup and restore) are in [docs/operations.md](docs/operations.md).
- Measured Schwab facts are in [docs/reference/schwab-facts.md](docs/reference/schwab-facts.md); the current milestone spec is in `docs/specs/`.
- The previous implementation (v1) is gone from the tree; it lives at git tag `v1-final`.

## Repo layout (monorepo)

uv workspace: the root `pyproject.toml` lists the members, one `uv.lock`, one shared `.venv` (`uv sync --all-packages`).
Python is pinned to 3.13 (`.python-version`).

```
libs/       # shared libraries: domain, persistence, query, observability, analysis, sources
services/   # long-running daemons: ingest, bot
tools/      # one-shot CLIs: migrate, backfill
scripts/    # CI gates, Schwab spikes and acceptance scripts
```

Naming rule: import name = `rs_` + member dir (`libs/domain` -> `rs_domain`); every member is `<root>/<member>/` with `pyproject.toml` + `src/rs_<member>/` + `tests/`.
`rs_common`/`rs_utils` grab-bags are banned.
The layer rules between members are import-linter contracts in the root `pyproject.toml`; `lint-imports` enforces them.

## Conventions

- Time is injected: no ambient clock reads in `src/` (`scripts/check_clock_discipline.sh`); tests use a fake clock.
- DB tests run against a real TimescaleDB, never a mock (see Run / Test below).
- Configuration is pydantic-settings per service (`services/*/src/rs_*/settings.py`), read from the environment or `.env` in the working directory; every variable is documented in `.env.example`.
- Vendor data is never committed: fixtures are synthetic.
- No plan citations (`ADR-NNNN`, `grilling #`, `slice N`) in `src/` or `tests/` (`scripts/check_plan_citations.sh`).
- Schema changes are hand-written Alembic migrations in `libs/persistence`; `rs_migrate check` fails on drift from the ORM models.

## Workflow rules

Always create a new branch before starting a new feature or bug fix.
Once complete, run all relevant tests and confirm success.
Write new test cases, adjust existing tests, and remove unnecessary tests for any changes you make.

## Run / Test / Deploy

```bash
# Set up / update the shared venv (installs every member editable)
uv sync --all-packages

# Tests (all members); a single file or member works as usual
.venv/bin/pytest -q
.venv/bin/pytest services/ingest -q

# Lint
.venv/bin/ruff check libs services tools

# Type-check (mypy + pydantic plugin; config in root pyproject.toml)
.venv/bin/mypy

# Import-layer contracts (import-linter; contracts in root pyproject.toml).
# A new member needs a line in root_packages and in each contract that covers it.
.venv/bin/lint-imports

# Clock-discipline and plan-citation gates
scripts/check_clock_discipline.sh
scripts/check_plan_citations.sh

# Migrations vs ORM models (run against a migrated DB; fails on drift)
RS_DB_URL=postgresql+psycopg://... .venv/bin/python -m rs_migrate check

# DB tests need a TimescaleDB (they skip visibly otherwise); spec container:
#   docker run -d --rm -p 5499:5432 -e POSTGRES_PASSWORD=t timescale/timescaledb:2.30.2-pg16
# Set RS_TEST_DB_URL to make DB tests mandatory (unreachable = failure, as in CI).
# Migrations CLI (DSN via RS_DB_URL; upgrade also applies the per-service roles and
# needs RS_DB_PASSWORD_{INGEST,BOT,MCP,RESEARCH}, or pass --skip-roles on a bare dev DB):
RS_DB_URL=postgresql+psycopg://... .venv/bin/python -m rs_migrate upgrade

# The stack: TimescaleDB (127.0.0.1:5498) + one-shot migrate gate + ingest/bot
# from one image (Dockerfile); JSON logs with correlation_id; metrics on
# 127.0.0.1:9090 (ingest) / :9091 (bot). Needs the required variables in .env
# (copy .env.example). Compose project name is rocketstocks-v2; keep it, it names the volume.
docker compose up -d

# Operator tools (host-side, shared venv; exact procedures in docs/operations.md)
.venv/bin/python -m rs_backfill verify-offset --dump PATH   # then popularity-import --dump PATH --zone ZONE
.venv/bin/python -m rs_ingest.tools.login                   # first Schwab login; later logins use /setup schwab
```

## Agent skills

### Issue tracker

Issues/PRDs live in GitHub Issues (`JWolf34/RocketStocks`) via the `gh` CLI.
Milestone specs (at most about 40 lines) land with the code, not in the tracker.
See `docs/agents/issue-tracker.md`.

### Domain docs

Single-context: one root `CONTEXT.md`; ADRs live at `docs/adr/`.
See `docs/agents/domain.md`.
