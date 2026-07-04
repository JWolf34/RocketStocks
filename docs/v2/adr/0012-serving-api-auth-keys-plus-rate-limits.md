# ADR-0012: Serving API auth — per-consumer API keys + rate limits

**Status**: Accepted (amended 2026-07-03)
**Date locked**: 2026-05-27 · **Amended**: 2026-07-03
**Decision-maker**: John Wolf

## Context

The `serve` API has three known consumers at cutover: `bot` (internal), `backtest` CLI (internal, single-run), and **Cowork** (external LLM agent platform on the public internet, with potentially bursty agent fan-out). It needs authentication from day one. Options considered: per-consumer API keys, API keys + rate limits, or OAuth 2.0 client credentials. OAuth's main wins (short-lived-token leak blast radius, dynamic self-service onboarding, delegated auth) don't apply to a 2–3 known-consumer, manually-provisioned, no-web-UI, no-real-money project — and the API-keys → OAuth migration is a well-trodden ~1-day path.

## Decision

**Per-consumer API keys + per-consumer rate limits.**

- `service_clients(id, name, api_key_hash, scopes JSONB, rate_limit_rps, rate_limit_burst, created_at, revoked_at)` table + FastAPI middleware.
- `X-API-Key` header; scopes column for read/write (and `metrics`) split; revocation is an `UPDATE revoked_at`.
- Rate limits enforced per consumer **from day one** — Cowork's agent fan-out could otherwise saturate the Schwab pull and starve bot alerts.
- ~150 LOC of middleware.

**Transport (amended 2026-07-03):** public ingress is the design posture (v2 is built to be self-hostable). `serve` assumes a **TLS-terminating reverse proxy upstream** — the proxy is operator infrastructure, decoupled from the repo (ADR-0031 style; the repo ships no proxy config). API keys must never traverse plaintext beyond the proxy hop; the Prometheus side-ports and metrics-scoped endpoints are never exposed publicly. `serve` stays proxy-friendly (honors forwarded headers where relevant; rate limiting keys on the API key, so client IP is not load-bearing). Verified as a Phase-0 pre-flight item; gate G8's consumer test runs through the real proxy URL.

## Consequences

- Sufficient for the known consumer set; minimal code; instant revocation.
- Rate limiting protects shared upstream resources (Schwab budget) from one runaway consumer.
- The metrics-scoped key gates the Prometheus `/metrics` endpoint on `serve` (ADR-0015).
- Cost: API keys are long-lived — a leaked key is valid until manually revoked. Accepted given manual provisioning and no real-money exposure; mitigated by easy rotation.

## Trigger conditions for revisit (→ OAuth 2.0)

- A 4th *external* consumer arrives that isn't manually onboarded (self-service registration needed).
- A web UI launches with **delegated** auth (Discord users authorizing Cowork to act on their behalf — full authorization-code flow, a different surface).
- Credential-leak blast radius (long-lived keys) becomes a credible concern.

## Change log

- 2026-05-27 — locked.
- 2026-07-03 — amended (transport): public-ingress posture pinned; TLS-terminating reverse proxy assumed upstream as operator infrastructure; metrics ports never public; Phase-0 pre-flight ingress check + G8-over-proxy added (final pre-implementation review).

## See also

- [architecture-brief.md § Auth + identity](../architecture-brief.md#auth--identity)
- [ADR-0013: Identity model](./0013-identity-discord-id-only.md)
- [ADR-0015: Observability stack](./0015-observability-stack-personalized.md)
- [ADR-0031: Backup & DR](./0031-backup-dr-vm-snapshots.md) (the operator-infrastructure precedent)
