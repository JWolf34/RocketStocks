# S005 — `serve` skeleton: authenticated bar reads + `/metrics`

**Phase**: 1 · **Appetite**: 3 days · **Depends on**: S002 (S003's acceptance data used by the live runbook) · **ADRs / notes**: [0012](../adr/0012-serving-api-auth-keys-plus-rate-limits.md), [0015](../adr/0015-observability-stack-personalized.md), [0017](../adr/0017-failure-mode-trimmed-a.md), [0024](../adr/0024-serve-read-model-boundary.md), [0022](../adr/0022-bar-retention-storage-compression.md) (deviation, below)

## Goal

After this slice a long-running `serve` daemon answers authenticated HTTP range reads of stored bars with the v2 response envelope, exposes a key-gated Prometheus `/metrics`, and the operator can provision/revoke API keys. It kills serve's integration risk (FastAPI + async SQLAlchemy session-per-request + auth middleware + metrics) while the contract surface is one endpoint.

## Interface

- `services/serve` — runnable daemon (`python -m rs_serve`, uvicorn), config via pydantic-settings: `rs_db_url` (shared repo-wide variable), `RS_SERVE_`-prefixed tunables (`RS_SERVE_PORT`, default 8000). Engine/session from `rs_persistence.engine` (UTC-pinned).
- **rev-003 migration** in `rs_persistence`: `service_clients(id, name UNIQUE, api_key_hash, scopes JSONB, rate_limit_rps, rate_limit_burst, created_at, revoked_at)` — full ADR-0012 shape now; **only `api_key_hash` + `revoked_at` are read in Phase 1** (scope/rate-limit enforcement is Phase 3, landing before any external key is provisioned — ADR-0012's "day one" = day one of external exposure).
- **Auth middleware** — `X-API-Key` → SHA-256 → `service_clients` lookup. Absent, unknown, or revoked key → uniform `401` (no validity leak). Applies to every route, `/metrics` included (metrics-scope narrowing is Phase 3 — ADR-0015).
- **`GET /v1/bars/{ticker}?start=&end=`** — range read, oldest-first. `start`/`end` required, ISO-8601 UTC (naive input rejected); `422` on missing/invalid/`start >= end`; empty range → `200` with empty `data`. Ticker uppercased. **No row cap** — the cap, its number, and pagination are a named **Phase-3 contract-review input**, decided together with rate-limit sizing (a cap multiplies consumer request counts; the two must be set knowing each other). Response envelope:
  `{"data": [{symbol, bucket, open, high, low, close, volume}, …], "meta": {"as_of": <newest bucket close, null if empty>, "interval": "5m", "source": "stream"}}`
  `meta` is extensible: consumers MUST ignore unknown fields and treat absent `meta.truncated` as `false`; `fetched_at`/`stale_since` join in Phase 2/3 (ADR-0017); `interval` becomes a param (default `5m`) when research history lands.
- **`GET /metrics`** — Prometheus text format, behind the same middleware: process defaults + `http_requests_total{method, path, status}` + request-latency histogram.
- **Catch-all exception handler** — `logger.exception` + clean JSON `500` (no traceback in the body; public-posture surface). The ADR-0015 `SYSTEM_CRITICAL` notification call joins when ADR-0016 lands; the sys/threading/asyncio hooks are S006's shared wiring.
- **Provisioning CLI** — `python -m rs_serve.tools.provision --name <consumer>`: generates `secrets.token_urlsafe(32)`, prints it **once**, stores only the SHA-256 hex digest (deliberate: 256-bit random keys need no slow hash — nothing to brute-force). `--revoke <name>` sets `revoked_at`. This is the permanent provisioning mechanism, retired only by ADR-0012's OAuth trigger conditions (self-service onboarding folds key-minting into that redesign).
- `BarRepository.list_range(symbol, start, end) -> list[Bar]` in `rs_persistence` (generalizes `list_for_day`, which `compare_bars` keeps).

**ADR-0022 deviation (deliberate, temporary):** the endpoint serves the stream-built detection log (`stream_bars_5m`) because it is the only bar data that exists in Phase 1; ADR-0022 assigns serve/backtest/Cowork reads to the official research history. When research history lands (Phase 2), the backing read swaps; the HTTP contract above is designed so consumers can't tell (`meta.source` flips from `"stream"`). Recording, not amending — the ADR stays authoritative.

## Non-goals

No Schwab client / read-through endpoints (Phase 3 — ADR-0024), no scope or rate-limit *enforcement* (Phase 3), no row cap/pagination (Phase-3 contract review, with rate-limit sizing), no OpenAPI contract authoring/review (Phase 3 is contract-first; the skeleton's auto-generated schema is not a reviewed contract), no other endpoints (alerts/quotes/…), no bot consumption (Phase 3), no structlog/correlation_id, no compose entry, no remaining exception hooks (all S006), no research-history backing (Phase 2).

## Acceptance criteria

- [ ] Auth: request with a provisioned key → 200; absent/unknown/revoked key → 401 (uniform body); revocation via CLI takes effect on the next request.
- [ ] Range semantics: oldest-first ordering; empty range → 200 + `data: []`; 422 on missing param, non-ISO input, naive datetime, `start >= end`.
- [ ] Envelope: response matches the shape above; `meta.as_of` equals the newest returned bar's bucket close; `null` when empty.
- [ ] `/metrics`: 401 without key; with key, Prometheus text exposition containing `http_requests_total` that increases across requests.
- [ ] 500 handler: a planted raising route returns JSON with no traceback; the traceback appears in the log.
- [ ] Provisioning CLI round-trip proven in a test: provision → authenticated request succeeds → revoke → 401.
- [ ] Live runbook (below) executed against the 5498 acceptance DB: real S003 session bars returned over HTTP by the daemon entry point.
- [ ] ruff + clock-discipline gate green over `services/serve`.

## Verification

```bash
.venv/bin/pytest libs/ services/serve -q      # unit + DB tests (5499 ephemeral Timescale, RS_TEST_DB_URL)
.venv/bin/ruff check libs services tools
scripts/check_clock_discipline.sh

# Live runbook (~5 min, no market dependency; 5498 = the persistent S003 acceptance DB)
export RS_DB_URL=postgresql+psycopg://postgres:<pw>@localhost:5498/postgres
.venv/bin/python -m rs_migrate upgrade                                  # applies rev-003
.venv/bin/python -m rs_serve.tools.provision --name dev                 # prints KEY once
.venv/bin/python -m rs_serve &
curl -s -H "X-API-Key: $KEY" \
  "localhost:8000/v1/bars/SPY?start=2026-07-09T13:30:00Z&end=2026-07-09T20:00:00Z" \
  | jq '.data | length'                                                 # 78
curl -s -o /dev/null -w '%{http_code}' -H "X-API-Key: wrong" \
  "localhost:8000/v1/bars/SPY?start=2026-07-09T13:30:00Z&end=2026-07-09T20:00:00Z"   # 401
curl -s -H "X-API-Key: $KEY" localhost:8000/metrics | grep http_requests_total
```

## Decisions surfaced

*(empty at review time — pre-implementation decisions from the 2026-07-12 grilling session are folded into the sections above: range-read contract over day-slice, no-cap deferral to Phase 3, real `service_clients` table with enforcement deferred, /metrics behind any valid key, CLI provisioning as the permanent mechanism, FastAPI handler minus notifications, 3-day appetite with cut-first candidates: the 500 handler and `--revoke`.)*
