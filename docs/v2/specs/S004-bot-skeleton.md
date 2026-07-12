# S004 — `bot` skeleton: LISTEN → sweep → one embed to the dev channel

**Phase**: 1 · **Appetite**: 3 days · **Depends on**: S002, S003 · **ADRs / notes**: [0002](../adr/0002-three-services-topology.md), [0005](../adr/0005-event-bus-shape.md), [0014](../adr/0014-bot-interface-slash-plus-components.md), [0015](../adr/0015-observability-stack-personalized.md), [0024](../adr/0024-serve-read-model-boundary.md), [0026](../adr/0026-charting-deprecation-and-reports.md), [acceptance-2026-07-09.md Finding 3](fixtures/streaming/acceptance-2026-07-09.md)

## Goal

After this slice a long-running `bot` daemon is the **reference implementation of the Bus-2 consumer contract** (ADR-0005): every alert row `ingest` commits reaches the private dev channel as one minimal embed — live via NOTIFY within seconds, or via reconcile after downtime/disconnect — with `alerts.posted_at` as the consumer-owned cursor. The Finding-3 failure class (a held LISTEN connection that silently receives nothing) is structurally covered, not just handled. This slice prototypes burn-in rehearsal #3 / cutover gate G5.

## Interface

- `services/bot` — workspace member `rs_bot`, runnable daemon (`python -m rs_bot`); deps: discord.py, pydantic-settings, rs-domain, rs-persistence. **Full gateway client** (pinned decision 2), not a webhook/REST poster.
- `BotSettings` (pydantic-settings, ingest pattern): `RS_DB_URL` (shared), `RS_BOT_DISCORD_TOKEN` (SecretStr — deliberately not v1's `DISCORD_TOKEN`; v2 must never read v1's credential during burn-in coexistence), `RS_DEV_CHANNEL_ID` (operator checklist §4), `RS_BOT_SWEEP_SECONDS` (default 60). The NOTIFY channel is imported from `rs_persistence.NOTIFY_CHANNEL` — never re-declared.
- **Single posting path** (pinned decisions 3/4): NOTIFY never posts — it only wakes the sweep. One worker waits on (wake event OR `RS_BOT_SWEEP_SECONDS` timeout), then drains `SELECT … WHERE posted_at IS NULL ORDER BY id`; per row: render → send → `mark_posted` → commit (per-row commit bounds the crash-dupe window to one alert). Delivery is **at-least-once**: a crash inside the send→commit gap re-posts ≤1 alert on restart, identifiable by its alert id; zero dupes under clean kill (the G5 reading). NOTIFY payload (alert id) is logged, never consumed as the work item. Strict id-order posting is a normal-operation property, not a failure-mode guarantee.
- `rs_persistence` additions (pinned decision 10): `PendingAlert` (frozen record: `id: int` + `alert: Alert` — storage identity stays off the domain decision type, S003 pin #5; envelope for future delivery-state fields e.g. Phase-3 `retry_count`); `AlertRepository.list_unposted() -> list[PendingAlert]` (no LIMIT — the sweep drains fully); `AlertRepository.mark_posted(alert_id) -> datetime | None` (`SET posted_at = now() WHERE id = :id AND posted_at IS NULL` returning the stamp — DB-side clock, idempotent; `None` = already marked, caller warns). Alembic **rev-003**: partial index `ix_alerts_unposted` on `alerts (id) WHERE posted_at IS NULL` — the cursor's operational index, near-empty in steady state.
- `rs_bot` internals: **LISTEN loop** — dedicated psycopg `AsyncConnection` (autocommit, `notifies()`; the Finding-3 witness pattern), capped-backoff reconnect, every (re)connect sets the wake event (so "sweep on reconnect" is not a special case); **sweep worker** as above; **`render(PendingAlert) -> Embed`** behind a tiny send protocol so tests never touch Discord. Render exceptions fall back to a **generic embed** (symbol, type, `triggered_at`, payload as text) — an alert can be render-ugly, never render-poisoned (pinned decision 5a). Dev tool `python -m rs_bot.tools.stage_alert` commits a marked test alert via `AlertRepository.insert` (real transactional-NOTIFY path) for the acceptance drills.
- **Embed (minimal, cosmetics non-binding)**: rendered **from the alert row alone** (ADR-0024 — no `serve`, no market-data reads, no post-time enrichment): symbol + alert_type, `triggered_at` as the embed timestamp, the raw payload numbers (`volume`, `threshold` — ADR-0026 transparency), alert id in the footer; message id + DB-derived lag (`posted_at − triggered_at`) logged per post for G5 auditing. Field-level layout is deferred to the post-Phase-2 alert-presentation spec (ADR-0026).
- **Lifecycle**: continuous daemon (unlike ingest's session-per-process). Startup: connect Discord → resolve the dev channel (unresolvable/no-permission → loud exit 1) → start LISTEN → unconditional initial sweep → run until SIGTERM (graceful: finish the in-flight post, exit 0). Failure modes: DB down at startup = loud exit; LISTEN drop = backoff reconnect + recovery sweep; Discord outage = sends fail, rows stay unposted, the periodic sweep self-heals after discord.py resumes; one alert's send fails = ERROR with full payload + counter + skip to the next row, retried every sweep uncapped (pinned decision 5b — the accepted ADR-0005 cost; retry-count/dead-letter/alarm deferred until the ADR-0016 rail exists to receive the alarm).
- **Counters** (ADR-0015 vocabulary; in-memory, logged on every sweep that did work and at shutdown — the bot never exits daily, so shutdown-only logging would be blind): `discord_posts_total`, `post_failures_total`, `sweeps_total`, `notify_wakes_total`, `listen_reconnects_total`. Metrics *exposition* (`:9091`, registry, `rs_observability`) is deferred to one consolidated cross-service metrics spec — recommended home S006; S005's `/metrics` one-liner should get the same re-examination at its spec review (pinned decision 8).

## Non-goals

No slash commands, components, or `💵 Trade` button (Phase 3 — ADR-0014/0026); no notifications layer or `notification_routes` (ADR-0016 slice); no poison retry-count column/alarm (deferred with rationale above); no structlog/`correlation_id` (S006); no Prometheus exposition (consolidated metrics spec); no multi-channel routing (single `RS_DEV_CHANNEL_ID`); no DB roles/GRANTs (S006 follow-on per S003 open question 2); no message-id persistence; no embed layout polish.

## Acceptance criteria

- [ ] **Startup backlog reconcile**: first run against the acceptance DB (port 5498 — holds the S003-era alerts with `posted_at IS NULL`) posts every unposted alert to the dev channel in id order and stamps `posted_at`; an immediate restart posts zero.
- [ ] **Live NOTIFY path**: an alert committed while the bot runs posts within seconds, and the log shows a notify-wake (not the interval timer).
- [ ] **Severed LISTEN self-heals** *(the Finding-3 criterion)*: after `pg_terminate_backend` on the listener's connection, an alert committed during the outage still posts within one sweep interval; the reconnect is visible in logs/counters.
- [ ] **Kill/restart (G5 prototype)**: SIGKILL the bot, commit ≥2 alerts while it is down, restart → exactly those alerts post once each, no duplicates (channel message count == row count).
- [ ] **Poison skip (test)**: with a planted failing send, the sweep continues past the failing row, `post_failures_total` increments, healthy alerts still post.
- [ ] Suite green: `pytest libs/ services/ -q` (LISTEN/cursor integration tests on real ephemeral Postgres — ADR-0023 tier rule, 5499 conftest pattern; render + sweep unit tests against the send protocol); ruff; clock-discipline gate with `services/bot/src` added to SCOPES.

## Verification

```bash
.venv/bin/pytest libs/ services/ -q
.venv/bin/ruff check libs services tools
scripts/check_clock_discipline.sh        # SCOPES now includes services/bot/src

# Live acceptance (operator: RS_DEV_CHANNEL_ID + RS_BOT_DISCORD_TOKEN in .env — checklist §4)
.venv/bin/python -m rs_bot 2>&1 | tee data/bot-acceptance-$(date +%F).log   # criterion 1: backlog drains in id order
# re-run the backlog demo (posted_at is the bot's own cursor column — ADR-0024):
#   psql "$DB" -c "UPDATE alerts SET posted_at = NULL"
.venv/bin/python -m rs_bot.tools.stage_alert                                # criterion 2: notify-wake post
psql "$DB" -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query ILIKE 'LISTEN%'"
.venv/bin/python -m rs_bot.tools.stage_alert                                # criterion 3: posts ≤ one sweep interval
kill -9 <bot pid> && .venv/bin/python -m rs_bot.tools.stage_alert && .venv/bin/python -m rs_bot   # criterion 4
# capstone: one live market session with ingest + bot running together against 5498
```

## Pinned decisions (spec grilling, John, 2026-07-11)

1. **discord.py** re-examined against alternatives and deliberately confirmed (see Decisions surfaced 1).
2. **Full gateway client** — the burn-in/G5 production shape; ADR-0016's webhook backend remains a distinct later rail.
3. **Single posting path** — the sweep is the consumer, NOTIFY is its wake-up line (ADR-0005's "wake-up signal, not a stream"); the correctness mechanism runs on every post so it cannot rot unnoticed (Finding 3 is the cautionary live demo); no NOTIFY-vs-sweep race exists by construction.
4. **At-least-once, post→mark, per-row commit** — dupe window ≤1 alert per crash; exactly-once is unachievable (no Discord idempotency key); G5 reads "no dupes" as the clean-kill guarantee.
5. **Poison policy** — (a) render fallback embed; (b) send failures skip-and-continue, uncapped retry each sweep; escalation machinery deferred until ADR-0016 gives the alarm a rail.
6. **Sweep interval 60 s** (`RS_BOT_SWEEP_SECONDS`) — bounds only the lost-NOTIFY tail; 20% of one bar.
7. **Existing dev-bot application** supplies the token as `RS_BOT_DISCORD_TOKEN`; no privileged intents; checklist §4 extended; `.env.example` extended (exists, v1's), not created.
8. **Metrics consolidation** — slices must *count* (ADR-0015 names) and log; all exposition decisions belong to one cross-service metrics spec (recommended: S006).
9. **Clock gate broad reading** — `services/bot/src` in SCOPES (gate comment updated to "all v2 service/lib src roots as they appear"); `posted_at` stamped DB-side; lag derived from `RETURNING`, no client clock in `rs_bot`.
10. **`PendingAlert` + `list_unposted`/`mark_posted` on `AlertRepository` + rev-003 partial index** as specified in Interface.
11. **Acceptance against the persistent 5498 DB** — the bot stamping `posted_at` there is production behavior on its own cursor column, not evidence contamination; drills staged via the real repository insert path; one live ingest+bot co-run day as capstone.
12. **Spec landed pre-resolved** — the open-questions phase ran as a pre-draft grilling (2026-07-10/11); this section is its record.

## Decisions surfaced

1. **Bot framework re-pin — decided 2026-07-11, pre-recorded:** ADR-0002 named discord.py at Stage 1; per the "v1 must not anchor v2" directive it was re-examined at this spec's grilling (hikari and the discord.py forks considered) and **confirmed on v2's own requirements**: first-class `app_commands` + component `View`/`custom_id` machinery for Phase 3, maintenance velocity, and ADR-0015's capture hooks already written against its API. The skeleton uses none of v1's cog/extension layout.
