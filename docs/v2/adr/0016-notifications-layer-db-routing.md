# ADR-0016: Notifications layer + DB-table routing

**Status**: Accepted
**Date locked**: 2026-05-27
**Decision-maker**: John Wolf

## Context

v2 needs to surface observable events to humans across several channels, and one concrete requirement drives the design: **critical system events (e.g., Schwab token expiring) must not be drowned out by market-alert notifications.** That's a *routing* problem, not an aggregation problem — different event categories must reach different destinations. The mechanism also needs to be friendly to self-hosters (who shouldn't have to fork the repo or bind-mount YAML to configure where their notifications go).

## Decision

A **pluggable notifications layer** with multiple backends and category-based routing:

- **Backends**: Discord (webhook), ntfy (phone push), Apprise (unifies Slack/email/SMS/etc.). `NotificationBackend` protocol in `libs/observability/notifications/backends/`.
- **Categories** (each routed independently): `alerts.market`, `alerts.user`, `system.critical`, `system.warning`, `system.info`.
- **Routing config** lives in a **`notification_routes` DB table**, edited via **`/setup notifications add|remove|show`** slash subcommands (under the `/setup` admin umbrella — ADR-0014). Defaults are **seeded via an Alembic migration**. Backend credentials (webhook URLs, ntfy topics) stay in env vars.
- Token-expiry routes to `system.critical` → ntfy push + admin channel, deliberately **off** the noisy `alerts.market` channel.

Routing-config-location was chosen over a repo YAML file specifically for self-hosters: with a DB table + slash commands they configure everything from Discord — no YAML editing, no Docker bind mounts, and config survives image upgrades (it's in their DB, not in code).

## Consequences

- Category routing solves the "drowned out" problem directly: independent destinations per category.
- Self-hosters get zero-friction configuration via slash commands; defaults work out of the box.
- Adding a backend = implement the protocol; adding a category = add an enum value + default route.
- This layer is also the sink for native exception capture (ADR-0015) — critical exceptions fire `system.critical`.
- Cost: a DB table + `/setup notifications` command (~150 LOC more than a static YAML), and route changes are runtime DB state rather than version-controlled.

## See also

- [architecture-brief.md § Observability](../architecture-brief.md#observability)
- [ADR-0014: Bot interface (`/setup` umbrella)](./0014-bot-interface-slash-plus-components.md)
- [ADR-0015: Observability stack](./0015-observability-stack-personalized.md)
