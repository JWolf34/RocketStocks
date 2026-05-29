# ADR-0013: Identity — Discord-ID-only

**Status**: Accepted
**Date locked**: 2026-05-27
**Decision-maker**: John Wolf

## Context

v2 user-keyed state (watchlists, paper trades, alert subscriptions, channel config) needs an identity to key on. The bot is the only consumer that touches user identity — Cowork and backtest operate on tickers/industries, not user accounts. The question is whether to model a first-class `User` now (forward-compatible with a future web UI but adds a lookup on every bot interaction) or use the Discord ID directly (simplest, but a future web UI requires a retrofit).

## Decision

**Discord-ID-only.** User-keyed tables FK to `discord_user_id BIGINT NOT NULL`. No `users` table. Discord ID is the natural key — stable, immutable per account, known at every interaction without a lookup.

**Retrofit playbook** (documented for when a trigger fires): add a `users` table; populate from `DISTINCT discord_user_id`; for each user-keyed table add `user_id UUID`, populate via JOIN, swap the FK, drop `discord_user_id`; update queries. ~3–5 days, and it folds naturally into the auth/session work a web UI requires anyway.

## Consequences

- Zero extra tables; no join on the bot's hot path.
- Simplest possible model for the only confirmed user surface (Discord).
- Cost: a future non-Discord identity surface requires the retrofit above. Bounded and well-understood; not pre-paid.

## Trigger conditions for revisit (→ first-class `User`)

- A web UI is committed.
- A non-Discord identity provider is added.
- A third user-bearing surface appears.

## See also

- [architecture-brief.md § Auth + identity](../architecture-brief.md#auth--identity)
- [ADR-0012: Serving API auth](./0012-serving-api-auth-keys-plus-rate-limits.md)
