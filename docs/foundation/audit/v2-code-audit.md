# RocketStocks v2 code audit

Audited 2026-10-19, read-only. Evidence = file:line on commit `986feb7` (HEAD, branch `phase-2-spec-010`). Everything below was read in the source; command results are from runs I made. Items I could not verify are marked UNVERIFIED.

## 0. Bottom line

The owner's two fears, answered separately.

**"Bloated": mostly no, at the code level.**
- v2 production code is 4,864 tracked lines (src, incl. 290 lines of migrations), of which only about 2,750 are executable code (rest is blank/comment/docstring). Tests are 5,038 lines.
- That is a small codebase. There is almost no dead code. The bloat is elsewhere: 57 markdown files / 4,602 lines under `docs/v2` (32 ADRs), 1,419 lines of spike scripts, and 231 spec/ADR/"pin #"/"grilling #" references sprinkled through 53 of 65 src files.
- Real over-engineering exists in four places: metrics bridging, replay tooling, an empty `rs_analysis` lib plus a calendar-only `rs_sources` lib, and a write-only `outbox`. See section 6.

**"Implemented ineffectively": the local craftsmanship is good, the delivered capability is thin, and the shape will not scale as written.**
- What exists: one streamed symbol (SPY), a one-line volume-threshold "detector", one 10-ticker options-chain scan, one HTTP endpoint, one Discord channel from an env var.
- The product value in the architecture brief (options-flow detection, universe of ~500, warm-up, notifications layer, paper trading, slash commands) does not exist in code.
- The hot path is hard-wired to a single symbol (H1). Ingest is a one-session-then-exit process, not a daemon (H2).
- The sharpest risks are not quality bugs; they are (a) the single-symbol ingest core must be reshaped before the UniverseManager lands, (b) operational fragility (manual daily start, weekly manual Schwab login), (c) alert-write failures are swallowed with only an in-memory counter.

## 1. Where the latest v2 code is

| Branch | Tip | Relation to `feat/v2-rewrite` (d945bfb) |
|---|---|---|
| `feat/v2-rewrite` | d945bfb (2026-07-17) | trunk; merge of `feat/spec-008` (options chain scan) |
| `phase-2-spec-009` | d945bfb | identical commit to trunk (worktree `.claude/worktrees/spec-design`) |
| `phase-2-spec-010` (HEAD, audited) | 986feb7 (2026-07-18) | trunk + 5 commits, ALL docs/fixtures: `docs/v2/specs/fixtures/listing/*` (Nasdaq/Cboe listing excerpts + findings). Zero code diff |
| `phase-2-spec-011` | 7bb3472 (2026-08-01) | trunk + 3 commits: 2 spike scripts (`scripts/spikes/history_daybounds_probe.py` 139 lines, `history_realities_probe.py` 256 lines) + findings docs. Branched from trunk, not from 010 |
| `feat/spec-008`, `feat/s001..s007`, `spike/phase0-schwab`, `docs/phase1-close-phase2-plan`, `feat/phase-2-universemanager-design` | various | all fully merged into trunk (0 commits ahead). The 3 git stashes are handoff `.md` files only |
| `main` | d7de2dd (2026-06-07) | v1 only; 305 commits behind trunk. Origin refs match local for 009/010/011/trunk |

Conclusion: `libs/ services/ tools/ Dockerfile.v2 docker-compose.v2.yml .github/` are byte-identical across trunk, 009, 010, 011 (`git diff --stat` empty). HEAD is the most advanced v2 code line. Exists only on other branches: the two history probe scripts on 011 (spikes, not product code).

CI evidence (via `gh run list`, metadata only, logs not read): CI on d945bfb = success, with `RS_TEST_DB_URL` set so DB tests are mandatory there. So the 82 tests skipped locally ran green in CI at that SHA. No CI run exists for the 010/011 branches.

## 2. Inventory per workspace member

LOC = tracked file lines. "code" = excluding blanks/comments/docstrings (my AST count, migrations excluded).

| Member | Purpose | src LOC (code) | test LOC | Public interface | Key deps | Does today |
|---|---|---|---|---|---|---|
| `libs/domain` (`rs_domain`) | Pydantic models, events, ports | 306 (144) | 243 | `Bar`, `Alert`, `ChainSnapshot`, `OptionQuote`, `BarClosed`, `UtcDatetime`, `SystemClock`, Protocols `Clock/EventSource/Detector/TradingCalendar` | pydantic | Validates UTC-only datetimes at boundaries; frozen models; the replay-ordering key |
| `libs/persistence` (`rs_persistence`) | SQLAlchemy models, repos, Alembic migrations, token store | 1004 (435) | 829 | `AlertRepository`, `BarRepository`, `ChainSnapshotRepository`, `ServiceClientRepository`, `SchwabTokenStore`, `build_engine`, `migration_config`, `NOTIFY_CHANNEL` | sqlalchemy 2 async, psycopg3, alembic | 5 migrations (3 hypertables/tables + enums + compression policy); atomic alert+outbox+NOTIFY insert |
| `libs/observability` | structlog JSON logs, correlation id, excepthooks, Prometheus side-port | 345 (194) | 416 | `configure_logging`, `unit_of_work`, `install_excepthooks`, `start_metrics_server`, 3 collector classes | structlog, prometheus-client | One JSON stdout stream for stdlib + structlog; scrape-time dict-to-Counter bridge |
| `libs/sources` (`rs_sources`) | XNYS calendar adapter only | 71 (35) | 79 | `PandasMarketCalendar` | pandas-market-calendars (pulls pandas, numpy, exchange-calendars) | is_session/open/close/next/prev; `next_session`/`previous_session` unused by prod code |
| `libs/analysis` (`rs_analysis`) | "indicators, z-scores, flow metrics" | 1 (0) | 5 | none | none | Nothing. One-line docstring; its only test asserts the docstring exists |
| `services/ingest` | Schwab stream -> 5-min bars -> Postgres -> detector -> alerts; options-chain scan | 2154 (1357) | 2536 | `python -m rs_ingest`; tools `login`, `compare_bars`, `replay` | schwab-py, authlib, pydantic-settings | See section 3 |
| `services/bot` | LISTEN/NOTIFY + sweep -> Discord embed | 519 (322) | 457 | `python -m rs_bot`, `rs_bot.tools.stage_alert` | discord.py, psycopg | Posts every unposted alert as an embed to one env-configured channel. No slash commands, no buttons |
| `services/serve` | FastAPI read API | 412 (234) | 421 | `python -m rs_serve`, `rs_serve.tools.provision` | fastapi, uvicorn | `GET /v1/bars/{ticker}`, `/metrics`, API-key auth, correlation + metrics middleware |
| `tools/migrate` | Alembic CLI wrapper | 52 (29) | 52 | `python -m rs_migrate upgrade/downgrade/current` | alembic | 52-line wrapper over `migration_config` |

Totals: 4,864 src / 5,038 test lines, 305 tests. Docstring+comment share of src: 23% (identical 22-29% in every non-trivial member). Ingest is 44% of src and 49% of executable code.

Outside the members: `scripts/` 1,419 lines (spikes, acceptance runner, clock gate), `Dockerfile.v2`, `docker-compose.v2.yml`, 2 workflows. Legacy (v1) for scale: 32,116 src / 33,191 test lines.

## 3. Command results (exact)

```
$ .venv/bin/pytest libs/ services/ tools/ -q -p no:cacheprovider
223 passed, 82 skipped, 1 warning in 2.13s            (305 collected)
$ .venv/bin/ruff check libs services tools
All checks passed!
$ scripts/check_clock_discipline.sh
clock discipline OK (libs/domain/src libs/observability/src services/bot/src services/ingest/src services/serve/src)   exit=0
```

- The 1 warning: `websockets.legacy is deprecated` (DeprecationWarning from websockets 16.0 as imported via schwab-py's streaming). Will become a hard break when websockets drops `legacy`.
- DB reachability: `pg_isready` is not installed; I probed with `nc`. 127.0.0.1:5499, :5498, :5432 all closed. `docker ps` fails: Docker daemon not running. I did not start anything. `RS_TEST_DB_URL` is unset.
- All 82 skips have the same reason: `no test database at postgresql+psycopg://postgres:t@localhost:5499/postgres` (gating in `libs/persistence/src/rs_persistence/testing.py:52-57`: skip when URL unset, FAIL when set but unreachable, which is how CI behaves).
- Per member (pass/skip): domain 29/0, observability 29/0, persistence **2/41**, sources 13/0, analysis 1/0, bot 11/**9**, ingest 135/**4**, **serve 0/27**, migrate 3/1.
  - Consequence locally: the whole `serve` service, 41 of 43 persistence tests, the bot sweep/listener, the golden replay, and the migration tests are unverified without Docker. That is 27% of the suite and every test that touches Postgres.
- Coverage run without a DB (coverage file written to scratchpad, not repo): 71% total. Pure-logic modules are high: `aggregator.py` 98%, `sources/live.py` 99%, `chain_scanner.py` 96%, `chains.py` 95%. Untested by ANY test (DB or not): `ingest/__main__.py:143-230` (`run_session` wiring), `bot/__main__.py:59-135` (`run_bot`), `serve/__main__.py` (0%), `tools/login.py` (0%).
- Extra lint pass (not in repo config): `ruff --isolated --select B,ASYNC,S,UP,SIM,PERF,RUF,C4,PIE,TRY,BLE,PLW,I` on src only gave 40 findings: 26 unsorted imports, 6 `BLE001` blind-except (all are the deliberate tier boundaries), 0 bugbear/async/security findings. The repo has no ruff config, so CI lints with ruff defaults only (E4/E7/E9/F).
- No type checker is configured anywhere (no mypy/pyright in pyproject, CI, or venv). Annotations are ~97% complete (domain/bot/migrate 100%) but never verified; `py.typed` markers ship unchecked.

## 4. Works end-to-end vs stub

Based on code plus the repo's own acceptance records (roadmap 2026-07-09/10 live runs, CI green). I could not run live paths (no Schwab token, no DB, no Docker).

**Working (code path complete, tested, and per roadmap exercised live in Phase 1):**
- Schwab L1 equity stream for ONE symbol -> `BarAggregator` -> 5-min `Bar` -> `stream_bars_5m` hypertable -> `ThresholdDetector` -> `alerts` + `outbox` + `pg_notify` in one tx -> bot LISTEN wake + 60s sweep -> Discord embed -> `posted_at` cursor.
- `GET /v1/bars/{ticker}?start&end` behind API-key auth, with correlation id and Prometheus HTTP metrics.
- Options-chain scan, 10 hardcoded tickers, 5-min cycles: bounded REST fetch, +/-10% moneyness window, row cap, retry-once, tombstones, stale-guard with DB restart state, 30-column hypertable with compression policy. Code and tests complete (S008). Repo evidence of the live RTH acceptance session for S008: NONE found (all five acceptance boxes in `docs/v2/specs/S008-options-chain-scan.md:94-110` are still unticked).
- Compose stack: timescaledb -> one-shot migrate gate -> ingest/serve/bot from one image; non-root runtime user.
- Replay: recorded JSONL session replayed through the real pipeline into a throwaway DB, semantic multiset diff against golden decisions.

**Stub, placeholder, or absent (relative to the architecture brief):**
- `libs/analysis`: empty.
- Detection: the only detector is a level-triggered `volume >= 500_000` on the newest bar (`ingest/detector.py:27-38`) with no dedup or cooldown (admitted at `detector.py:1-12`). No options-flow detector, no alert lifecycle, no config-version stamp.
- Universe: hardcoded `symbol="SPY"` (`ingest/settings.py:32`) and `BOOTSTRAP_UNIVERSE` of 10 tickers (`settings.py:21`). No UniverseManager, no warm-up, no multi-session loop, no EOD/1-min backfill, no popularity/earnings sources (ApeWisdom/Nasdaq), no quotes.
- Notifications layer (ADR-0016): absent. `hooks.py:7-8` and `serve/app.py:25` say "joins when ADR-0016 lands". A `SYSTEM_CRITICAL` event today = a log line.
- Bot: no slash commands, buttons, `/trade`, `/setup`; single channel via `RS_DEV_CHANNEL_ID` (`bot/settings.py:22`). `render.py` dumps raw payload keys as fields.
- Serve: 1 endpoint; no row cap/pagination (`bars.py:8,41-48`); scopes and rate limits stored but unenforced (`persistence/models.py:136` docstring: "enforced from Phase 3").
- `outbox` table: written on every alert, read by nothing in prod code (grep: only writer + test truncate). Speculative ledger.
- `Dataset.EOD_FULL`: defined (`domain/models.py:49`), never used by prod code.
- Health: `rs_observability/__init__.py:1` says "logging, metrics, and health"; there is no health endpoint or compose healthcheck for any app service.

## 5. Quality review

Severity: **H** = fix before it constrains the roadmap or loses the product's output; **M** = real bug/risk, bounded blast radius; **L** = hygiene.

### H1. Ingest hot path is single-symbol by construction
- `services/ingest/src/rs_ingest/pipeline.py:64` `self._window: list[Bar]` is one list for the whole session; `:83` appends every bar to it; `:138` hands it to every detector as "all of today's RTH bars". With two symbols the window interleaves them; every detector would have to filter by symbol or silently decide on mixed data. The `Detector.decide(window)` port (`domain/ports.py:34-44`) bakes this contract in.
- `sources/live.py:125` builds one `BarAggregator` for one symbol; `:179` subscribes `[self._symbol]`; `:158-162` emits `sequence=0` because "single symbol => one BarClosed per event_time". The ordering contract in `domain/events.py` needs a real tie-break for multiple symbols closing at the same `event_time` (500 of them at every 5-min boundary).
- `aggregator.py:60` "Single-symbol, single-session bar builder".
- Impact: the ADR-0019 two-band ~500-symbol universe cannot be added by configuration; port, pipeline, live source and aggregator ownership all change. Better to fix the shape (window keyed by symbol, an aggregator per symbol or a multi-symbol aggregator, deterministic per-event-time sequence) before building UniverseManager on top of it.

### H2. Ingest is not a daemon; operations depend on a human
- `ingest/__main__.py:1-8` "one process = one market session ... exits 0 - also when there is no session". `docker-compose.v2.yml:64-68` sets `restart: on-failure:3`, so a clean exit is not restarted. Nothing starts it the next trading day (no scheduler in compose or code; only `scripts/acceptance/phase1_market_run.sh`, a one-off runner).
- `live.py:107-114` derives the session from the UTC date of "now" (documented as exact only for a pre-open ET-morning start). A start at 21:00 ET Friday exits 0 silently as "not a session".
- Schwab refresh tokens hard-expire at 7 days; re-login is a browser flow (`tools/login.py`). On `OAuthError` ingest exits 1 (`__main__.py:208-212`) and `on-failure:3` burns three restarts then stops. No notification path exists to tell the operator.
- No healthchecks on ingest/serve/bot; a hung (not crashed) process is invisible to compose.

### H3. Alert output path is fail-silent
- `pipeline.py:106-120`: any exception in the alert transaction is swallowed (`except Exception`), counted in an in-memory dict (`alerts_dropped`), and logged with the alert content. No dead-letter, no retry, no external signal. The code comment concedes it ("v2 makes the drop recoverable", `pipeline.py:30-32`).
- It is a deliberate tiering call (bar capture is irreplaceable, alerts are re-derivable by replay), and a persistent DB failure does surface at the next bar's crash-loud persist. But alerts are the product; replay recovery only works for deterministic detectors on a durable log, and nothing triggers that replay. Before the first real detector ships, this needs a durable retry or dead-letter plus an out-of-band signal.
- Also `bot/sweep.py:93-107`: a send failure is retried every sweep, uncapped. A payload that Discord rejects deterministically (more than 25 fields, field over 1024 chars; `render.py:35-36` adds one field per payload key without limits) becomes an infinite 60s ERROR loop with a head-of-line "skipped" row forever. Harmless with today's 2-key payload; a real payload will not be.

### H4. Every service runs as the DB superuser, including the public-facing one
- `docker-compose.v2.yml:22` `RS_DB_URL: postgresql+psycopg://postgres:${RS_DB_PASSWORD}@timescaledb:5432/rocketstocks`, shared via the `x-app` anchor by ingest, serve, bot and migrate.
- `serve` is the internet-facing surface (behind a TLS proxy per ADR-0012) and, with this DSN, can read `schwab_tokens` (plaintext refresh token in JSONB, `persistence/models.py:152` `SchwabTokenRow`) and write anything. Only `migrate` needs superuser (migration 0001 runs `CREATE EXTENSION timescaledb`).
- Fix is cheap: per-service roles (serve: SELECT on bars/alerts/service_clients only; bot: alerts + posted_at update; ingest: writes; migrate: owner).
- Related: the DSN embeds `${RS_DB_PASSWORD}` unencoded; a password containing `@ / : %` silently breaks the URL (`docker-compose.v2.yml:22`).

### M1. Verified bug: serve MetricsMiddleware masks cancellation with UnboundLocalError
- `services/serve/src/rs_serve/metrics.py:57-77`: `status` is assigned only on success (`:61`) and in `except Exception` (`:67`). A `BaseException` such as `asyncio.CancelledError` runs the `finally` (`:69-76`) with `status` unbound. Probe (script `scratchpad/audit/probe_metrics_mw.py`): `escaped as: UnboundLocalError cannot access local variable 'status'`. Impact: shutdown/cancel in a request emits a confusing error instead of CancelledError. Low frequency; trivial fix (init `status = 500` before `try`). It went unnoticed because every serve test is DB-gated and none cancels a request.

### M2. Verified bug: ReconnectPolicy overflows after 1,024 consecutive failures
- `services/ingest/src/rs_ingest/sources/live.py:77` `min(self.cap_seconds, float(2**self.consecutive_failures))`. `float(2**1024)` raises OverflowError. Probe: raised at `consecutive_failures = 1024`. At the 30s cap that is about 8.5 hours of continuous failed reconnects. The exception escapes the receiver task and `events()` re-raises it (`live.py:147-148`), crashing the session. Needs a long outage, but it is the exact scenario the reconnect loop exists for. Fix: `2.0 ** min(n, 16)`.

### M3. Bar writes are not idempotent
- `pipeline.py:87-92` `_persist` does a plain insert; `stream_bars_5m` PK is `(symbol, bucket)` (migration 0001). Any duplicate bucket (reconnect replays, a future warm-up that overlaps, a second process) raises IntegrityError, which is deliberately crash-loud, which with `restart: on-failure:3` ends the session after three tries. Upsert-ignore or a deliberate duplicate policy is missing; this will matter the moment warm-up or multiple writers exist.
- Alerts have no uniqueness either (no `(symbol, alert_type, triggered_at)` constraint), so a replay or restart double-alerts.

### M4. Chain scanner invariant gaps
- `chain_scanner.py:256` `_assess` advances the stale-guard baseline (`self._medians[underlying] = median`) BEFORE `_persist` runs (`:237`). If the insert then fails, the baseline moved for data that was never stored.
- `_scan_name` persist failure is caught only by `_run_cycle` (`:168-172`): counted in `scan_errors`, but no tombstone is written, which breaks the "every (cycle, name) pair has rows" membership invariant the module advertises (`:8-9`).
- `normalize_chain` (`chains.py:44-80`) and JSON decoding run synchronously on the event loop that also drives the stream handler. Fine at 10 names (about 6s per cycle at 0.625s spacing); at the planned ~400 names with 2.6k-row chains it adds jitter to stream handling and the cycle itself (400 x 0.625s = 250s) leaves almost no margin in a 300s cycle (`REQUEST_SPACING_SECONDS`, `chain_scanner.py:40`).

### M5. Lower-severity async/lifecycle notes (ingest)
- `ingest/__main__.py:206-230`: `token_store.drain()` (`:217`) runs only on the happy path. The `OAuthError` return (`:208-212`) and any exception skip it, and `finally` (`:222-230`) does not drain. Impact is bounded because Schwab's refresh token is fixed-lifetime (the callback usually persists only a new access token), but the code's own comment (`token_store.py:58-59`) says a lost save "silently loses a rotated refresh_token".
- `live.py:136-153` relies on the async-generator `finally` to cancel the receiver task. When the pipeline raises inside `async for`, Python does not call `aclose()`; cleanup waits for `asyncio.run`'s `shutdown_asyncgens`. Receiver keeps running during the `finally` of `run_session`. Harmless today; an explicit `aclosing` would make it deterministic.
- `sources/recording.py:37` `FixtureRecorder.__init__` truncates the fixture file; with `restart: on-failure:3` and `RS_INGEST_FIXTURE_PATH` set, a mid-session crash-restart wipes the earlier recording.
- Positive: the scanner task has its own fault boundary (`__main__.py:116-139`), cancellation is re-raised, the `finally` cancels and awaits it (`:223-227`), bot shutdown is graceful (SIGTERM sets a stop event, in-flight post finishes, `bot/__main__.py:105-123`).

### M6. Migrations
- Hand-written migrations plus separate ORM models, with no drift check (no `alembic check`/`compare_metadata` anywhere; `grep` found none). Enum members are defined in `domain/models.py:44-60`, mirrored into the ORM `ENUM(...)` from `rs_domain` (`persistence/models.py:62,70`) and hardcoded in migration 0005 (`0005_options_chain_snapshot.py:28-29`). Only `test_enums_pin_spec_vocabulary` (`libs/persistence/tests/test_chain_snapshot.py:119`) guards it, and only for these two enums.
- Migration 0001 does `CREATE EXTENSION IF NOT EXISTS timescaledb` (needs superuser, see H4). Downgrades exist and are trivially correct (drop table/type).
- Precision inconsistency: domain docstring says prices are float64 and NUMERIC is "reserved for accounting-grade tables" (`domain/models.py:1-7`); `options_chain_snapshot` stores prices as NUMERIC and IV/greeks as REAL (float4) (`0005:46-63`). Not a bug (nothing reads it back yet) but it contradicts the stated bit-exact replay rationale for future chain replay.
- No retention and 1-day chunks with 3-day compression: the table is forward-only by design. My rough estimate (not measured): about 1.5M rows/day at 10 names, 40-60M/day at 400 names; compression policy is present, there is no disk alarm.

### M7. Config, secrets, typing, deps hygiene
- Good: all secrets are `SecretStr` (`ingest/settings.py:29`, `bot/settings.py:21`); `.env` is gitignored; every service prefix-scopes its own vars (RS_INGEST_, RS_BOT_, RS_SERVE_) and exits 1 on invalid config; bot token is deliberately not v1's variable.
- Inconsistent: config is read three ways. pydantic-settings in 4 classes (`IngestSettings`, `BotSettings`, `ServeSettings`, `_StageSettings`), `os.environ` directly in `observability/logs.py:42,47` and `tools/migrate/__main__.py:35`. Each class re-declares `rs_db_url`. Four CLI tools use `logging.basicConfig` instead of `configure_logging` (`ingest/tools/login.py:50`, `replay.py:189`, `compare_bars.py:169`, `bot/tools/stage_alert.py:57`).
- Undeclared direct imports (found by AST scan vs each `pyproject.toml`): `ingest` imports `sqlalchemy`, `alembic`, `prometheus_client`, `pydantic`; `bot` imports `psycopg`, `sqlalchemy`, `pydantic`; `serve` imports `sqlalchemy`, `pydantic`; `rs_persistence/testing.py` imports `pytest` (a dev-only dep in a runtime package; `rs_observability/testing.py` is the same pattern). It works only because they arrive transitively. ADR-0001's claim that "layer rules are enforced by each member's dependency closure" is therefore not enforced: nothing prevents `rs_bot` importing `BarRepository` (ADR-0024 says the bot must not read market data), and no import-linter exists.
- `libs/persistence/src/rs_persistence/repository.py:159` uses `assert` in production code for an invariant (`assert median is not None`); also `aggregator.py:218,226`. Stripped under `python -O`.

### L. Hygiene
- Stale docstrings that now contradict the code: `repository.py:1-9` ("Minimal repository - only what the S002 round-trip test needs" above four repositories); `bot/counters.py:1-4` ("Exposition ... is deferred" while `bot/__main__.py:68-70` exposes them); `rs_observability/__init__.py:1` ("and health"); `rs_sources/__init__.py:1-2` ("Schwab, ApeWisdom, Nasdaq - arriving with their slices": the Schwab adapters live in `rs_ingest`, not here).
- 231 spec/ADR/"pin #5b"/"grilling #6"/"rev-00x" references in 53 of 65 src files, 72 more in tests. These are meaningless without the docs and rot as docs move. Prefer a one-line "why" in the code and a link only for ADRs.
- `serve` stacks three `BaseHTTPMiddleware` layers (correlation, metrics, auth; `app.py:63-65`) for one endpoint; auth does one DB round trip per request including `/metrics` (`auth.py:35-37`). Fine at this scale; BaseHTTPMiddleware has known contextvar/cancellation edge cases (M1 is one).
- `bars.py:41-48`: unbounded `[start,end)` range on a keyed user; with the table this small it is a latent DoS only after universe growth.
- Docker: `Dockerfile.v2:6,39` base images float (`uv:python3.13-bookworm-slim` builder vs `python:3.13-slim` runtime; the runtime tag can move to a newer Debian than the builder); compose uses `timescale/timescaledb:latest-pg16` (`:30`). One fat image holds all three services plus schwab-py's flask/werkzeug/jinja2/autopep8/multiprocess/psutil closure (73 packages across the image) and pandas+numpy (for a calendar) - even in `serve`.
- CI (`.github/workflows/ci.yml`): sound structure (locked sync, lint, clock gate, tests with a mandatory TimescaleDB service, compose config validation, both image builds with cache). Gaps: no type check, no import-lint, no container smoke test (`docker compose up` + `rs_migrate current`), no dependency audit/dependabot, no ruff config beyond defaults. `docker-publish.yml` publishes only the v1 image.
- Clock-discipline gate (`scripts/check_clock_discipline.sh`) is a grep for `datetime.now(|datetime.utcnow(|time.time(`. It does not catch `date.today()`, `time.monotonic()`, `asyncio.get_running_loop().time()`, `from datetime import datetime as dt`, and its scopes exclude `libs/persistence/src` and `libs/sources/src`. It is honest about being a lint, but "CI-enforced" (`domain/ports.py:16`) overstates it. Plus the scanner/live source pass `now.date()` in UTC as the session key in two places (`live.py:114`, `chain_scanner.py:112`), a convention duplicated rather than centralised.
- Policy compliance (the owner's own rule, memory note 2026-08-01): raw vendor data is still committed at HEAD: `services/ingest/tests/fixtures/chains/spy-{1748,1749,1750}.json.gz` (3 x ~290 KB full SPY chains, used by `test_chain_scanner.py:394-401`), `services/ingest/tests/fixtures/spy-level1-excerpt-2026-07-06.jsonl` (804 real ticks), `docs/v2/specs/fixtures/{streaming,chains}/*`, and (branch 010 only) four Nasdaq/Cboe listing-file excerpts. The memory note records this as known, deliberately unhurried remediation; flagged here so the gap between policy and tree is visible. Some tests (`test_chains.py:19-26`) also reach into `docs/` and a gitignored `data/` dir by relative path (`Path(__file__).parents[3]`), skipping silently on CI when `data/` is absent.

## 6. Test quality

**Strengths (real, worth preserving):**
- Behavior-first, deterministic: fake clock + scripted fake stream drive reconnect/watchdog/overrun/stale-guard paths without sleeping (`test_live_source.py`, `test_chain_scanner.py`: 0 mock/patch usages, hand-written fakes at Protocol seams).
- Aggregator validated against a real recorded excerpt with expected values cross-checked against an independent builder (`test_aggregator.py:1-14`).
- Golden replay uses hand-derived expectations rather than blessed output, with an explicit rot warning (`test_golden_replay.py:1-14`).
- Persistence and serve run against a real migrated TimescaleDB in a throwaway database; CI fails loudly rather than skipping if the DB is missing (`persistence/testing.py:52-57`). Good ergonomics trade-off.
- `captured_json_stream` asserts on rendered log output, not on mocks.

**Weaknesses:**
- The orchestration layer is the least covered: `run_session` (`ingest/__main__.py:142-230`), `run_bot` (`bot/__main__.py:58-135`), `serve/__main__.py`, `login.py` have no automated test. They are covered only by operator acceptance runs. No automated cross-service test (ingest -> NOTIFY -> bot -> fake sender) exists.
- `test_pipeline.py` monkeypatches `pipeline_mod.BarRepository/AlertRepository` and fakes the session (`:119-125`): it pins call ordering against fakes of the code under test's own collaborators, so it breaks on a harmless rename. It is backed by real-DB tests (`test_golden_replay`, `test_alert_repository`) so acceptable, but it is the mock-heavy outlier.
- `ingest/tests/test_main.py` tests `ScrapeTimeCounters`/`chain_scan_collectors` with `SimpleNamespace` stubs of the scanner/aggregator. That glue exists only because counters are dicts (see design section); the test is coverage of an avoidable adapter.
- Ceremony: 9 `test_import.py` files (3% of tests) including `rs_analysis`'s assert that `__doc__` is truthy; `test_settings.py` (87 lines) mostly re-tests pydantic-settings/SecretStr behavior.
- Everything that touches Postgres, including 100% of `serve`, is skipped by default on a dev machine without Docker, so a casual `pytest` reports green over 27% of the suite not running.

## 7. Design depth

### Genuinely good, keep
- `BarAggregator` (`aggregator.py`): tiny interface (`process_message`, `flush`, `note_disconnect`), pure, absorbs the real complexity (trade-time attribution, baseline-before-bucket rule, late ticks, no-trade buckets). Deep module, 98% covered, validated on real data.
- `ChainScanner` + `normalize_chain`: injected clock/sleep/store/fetch, pure normalization, explicit tombstone/stale semantics. Constructor has 13 parameters but each is a real seam.
- Bus-2 design: `AlertRepository.insert` writes alert + outbox + `pg_notify` in one tx; `mark_posted` idempotent via `posted_at IS NULL` guard; bot treats NOTIFY as a hint and the sweep as the correctness mechanism with a partial index (migration 0003). Small, robust, and the failure modes are documented and drilled.
- `SchwabTokenStore`: correctly handles a sync write callback from inside a running loop (strong task refs, FIFO lock, `drain()`). It just needs `drain()` on all exit paths.
- `UtcDatetime` annotated type at every boundary (domain + FastAPI query params) so naive/non-UTC time cannot enter; engine pins session TZ to UTC (`engine.py:14`).
- `unit_of_work` correlation context (one fresh id per bar/request/post with domain keys) is small and high-value.
- Tier-1/Tier-2 failure thinking in `Pipeline` is explicit and consistent with the ADR (apart from H3's lack of a recovery path).
- The disposable-DB-per-session test fixture and the CI "set URL = no silent skip" rule.

### Shallow, single-implementation, or not paying for itself
- **Metrics bridging (simplify).** `observability/metrics.py` (109 lines + 104 test lines) defines `DictCounterCollector`, `LabeledDictCounterCollector`, `CallableGaugeCollector`, existing so services "keep their plain counter dicts as the increment API" (`metrics.py:1-9`, i.e. to avoid touching earlier accepted slices). Ingest then adds `ScrapeTimeCounters`, `live_aggregator_counters`, `chain_scan_scalar_counters`, `chain_scan_collectors` (`ingest/__main__.py:49-113`, ~65 lines), and a name-normalization hack for `_total` suffixes (`metrics.py:21-30`, test `test_names_without_total_suffix_are_normalized`). `serve/metrics.py:33-44` already uses native `prometheus_client.Counter/Histogram` in 12 lines. Two metric mechanisms coexist. Replace with native Counters/Gauges (with the scrape-time gauge as the only custom collector); deletes about 250 lines including tests and the generic HELP strings ("Value of the X counter, read at scrape time").
- **`rs_analysis` (drop).** Empty package, its own pyproject, Docker COPY line, and a test that asserts a docstring. Create it when the first indicator exists.
- **`rs_sources` (simplify or replace).** A 65-line calendar wrapper whose name implies the data-source layer, but the sources live in `rs_ingest/sources/` and `rs_ingest/chain_adapters.py`. It drags pandas + numpy + exchange-calendars + korean-lunar-calendar + pyluach into every image. `next_session`/`previous_session` (and the 30-day window logic) have no prod caller. Either fold into ingest with a minimal XNYS session table (open/close/early-close) or keep the port and accept the cost knowingly.
- **Replay tooling (simplify).** `replay_check.py` (110) + `tools/replay.py` (218) + `tools/compare_bars.py` (182) + about 375 test lines is roughly 885 lines whose purpose today is to validate a one-line `volume >= N` rule. The replay CLI has six modes (`--plant-raiser`, `--live-db`, `--expected`, `--export-decisions`, ...). The golden-replay pytest (42 lines) plus `Pipeline` already prove the idea. Keep the pytest golden test and the `RecordedEventSource`; reduce the CLI to a test helper or script; keep `compare_bars` as a small ops script (it earned its keep by finding the day-filter bug).
- **Protocols with one production implementation.** `Detector` (ThresholdDetector only + a planted canary), `TradingCalendar` (1), `SnapshotStore` -> `SessionSnapshotStore` (`chain_adapters.py:45-60`, a pure 1:1 delegate to `ChainSnapshotRepository` that exists only to own a session; give the repository a session-factory mode instead), `ChainFetch` (1), `EmbedSender` (1), `EquityStream` (1, a mirror of schwab-py's `StreamClient`). Most of these are legitimate test seams and cost little; the `SnapshotStore` adapter and the `TradingCalendar` package split are the ones that do not pay.
- **Speculative schema/params.** `outbox` (written, never read, no relay planned in v2), `service_clients.scopes/rate_limit_*` (unenforced), `Dataset.EOD_FULL` (unused), `notify_channel` threaded through `Pipeline`, `AlertRepository`, `ListenLoop(channel=)` and tests although the settings docstring says it is "intentionally NOT a tunable" (`ingest/settings.py:41-43`).
- **One `repository.py`** (315 lines) holding four unrelated repositories; split by aggregate when it next changes.
- **`tools/migrate`** as a separate workspace member for a 52-line wrapper over `migration_config`; it could be `python -m rs_persistence.migrate` and one fewer member, Dockerfile COPY, and pyproject. Low value either way; consistent with ADR-0001's "tools = CLIs".
- **Process leakage into code:** see L (231 spec/ADR/pin references).

## 8. Dependencies

Lock versions in `uv.lock` (133 packages total in the workspace lock; v2 image closure = 73 packages). Floors are loose in several `pyproject.toml` files (`structlog`, `prometheus-client` have no bound; `sqlalchemy[asyncio]>=2`); the lock pins them, which is acceptable for a deployed app.

| Package | Locked | Role | Note |
|---|---|---|---|
| **schwab-py** | **1.5.1** | Stream + REST + OAuth | **Highest risk.** Latest on PyPI is also 1.5.1, uploaded 2025-06-30 (about 16 months before 2026-10-19); previous releases 2025-03-09, 2024-09-14. PyPI classifier "Development Status :: 1 - Planning". GitHub: 39 open issues, 17 open PRs, no formal release notices; single maintainer. Requires authlib>=1.6 (the code couples to `authlib...OAuthError`, `live.py:31`, `__main__.py:17`, with a comment acknowledging the coupling). Pulls flask, werkzeug, jinja2, autopep8, multiprocess/dill, psutil, python-dateutil into every image. Emits the `websockets.legacy` DeprecationWarning (websockets 16.0). It is correctly isolated behind `EquityStream`/`ChainFetch`/`SchwabTokenStore`, so replacement (httpx + websockets) is possible but the streaming protocol is the hard part. The platform risk is the Schwab API and the 7-day refresh token, not this library. |
| pandas-market-calendars | 5.4.0 (pandas 3.0.3, numpy 2.5.1, exchange-calendars 4.13.2) | XNYS calendar | Heavy for the job (see 7) |
| sqlalchemy / alembic / psycopg[binary] | 2.0.51 / 1.18.5 / 3.3.4 | persistence | Healthy. One driver family (psycopg3) is a good decision |
| pydantic / pydantic-settings | 2.13.4 / 2.14.2 | models, config | Healthy |
| fastapi / uvicorn / starlette | 0.139.0 / 0.51.0 / 1.3.1 | serve | Healthy; 1 endpoint uses a big framework; fine |
| discord.py | 2.7.1 | bot | Healthy, actively maintained; default intents only |
| structlog / prometheus-client | 26.1.0 / 0.25.0 | observability | Healthy |
| authlib | 1.7.2 | transitive (schwab-py) and declared direct (`authlib>=1.3` in ingest, below schwab-py's own >=1.6) | Harmless mismatch |
| dev: pytest 9.1.1, pytest-asyncio 1.4.0, time-machine 3.2.0 (one use, `test_clock.py`), ruff 0.15.20, httpx 0.28.1 | | | `pytest-asyncio` in auto mode, fine |

I verified staleness only for schwab-py (PyPI + GitHub page via WebFetch). Other versions are what the lock holds; I did not check them against upstream.

## 9. Verdict table

| Module | Verdict | One-line rationale |
|---|---|---|
| `libs/domain` | **Keep** | Small, typed, UTC-enforced, frozen models; the right seam vocabulary. Revisit `Detector.decide(window)` for per-symbol windows (H1) |
| `libs/persistence` | **Keep (trim)** | Solid repos, atomic outbox/NOTIFY, good test fixture. Add alembic drift check, split `repository.py`, decide on `outbox`, make bar insert idempotent (M3), per-service DB roles (H4) |
| `libs/observability` | **Simplify** | Keep `logs.py`, `unit_of_work`, `hooks.py`; replace the dict-to-collector bridge with native Prometheus metrics (about 250 fewer lines incl. tests) |
| `libs/sources` | **Simplify / replace** | A calendar wrapper with misleading name and a pandas-sized dependency tail; fold into ingest with a minimal session table, or keep the port deliberately |
| `libs/analysis` | **Drop (until needed)** | Empty member; a docstring test is not an asset |
| `services/ingest` core: aggregator, live source, chain scanner, chains, token store use | **Keep** | The most valuable code in the repo: deep, pure, deterministic, validated on real data |
| `services/ingest` pipeline + live source shape | **Rewrite (reshape)** | Single-symbol window/aggregator/sequence assumptions (H1) must change before UniverseManager; add durable alert retry (H3), idempotent bars (M3), multi-session daemon lifecycle (H2) |
| `services/ingest` replay/compare tooling | **Simplify** | About 885 lines to validate a one-line rule; keep golden pytest + `RecordedEventSource`, shrink CLI, keep `compare_bars` as a script |
| `services/ingest/__main__.py` wiring | **Simplify** | 250 lines with ~65 of metrics glue and no test; use native metrics, drain tokens in `finally` |
| `services/serve` | **Keep** | Small, correct auth/correlation design; fix M1, add row cap, restrict DB role; every test is DB-gated, so run them in your inner loop |
| `services/bot` | **Keep** | Tight and robust posting path (sweep + LISTEN hint, per-row commit, graceful stop); render needs field limits before real payloads |
| `tools/migrate` | **Keep or fold** | Harmless 52-line wrapper; folding into persistence saves a member |
| `scripts/spikes` (about 1.4k lines) | **Drop from tree** | Throwaway; findings already captured in docs; some now duplicate shipped code |
| `Dockerfile.v2` / compose | **Keep, harden** | Locked non-root builds are good; add per-service DB roles, healthchecks, pinned images, a scheduler or multi-session ingest |
| CI | **Keep, extend** | Good structure; add type checker, import-linter, compose smoke test, dependency audit |

## 10. Prioritized actions

1. Before UniverseManager: reshape ingest for N symbols (H1) and make bar/alert writes idempotent (M3). Highest leverage, avoids rework.
2. Make ingest a supervised multi-session daemon (or add a scheduler) and add a Schwab-token-age signal that reaches you (H2). Without it, "works" means "you babysit it".
3. Replace alert swallow-and-count with durable retry/dead-letter plus one out-of-band signal (H3).
4. Per-service Postgres roles (H4).
5. Fix the two verified bugs (M1, M2) and the `drain()` and tombstone gaps (M4, M5): each is a few lines.
6. Add a type checker and `alembic check` to CI; declare direct deps; add an import-linter rule for the layer boundaries ADR-0001 claims.
7. Delete `rs_analysis` and `scripts/spikes`; simplify metrics; trim replay tooling. That is the "bloat" payoff: roughly 1,500+ lines gone with no capability lost.
8. Resolve the committed-vendor-data gap (section 5, L) when convenient; it is the one item that can cost API access rather than time.

## Appendix: probes and tooling (scratchpad, none touch the repo)

- `scratchpad/audit/probe_metrics_mw.py` (M1 repro), `loc_stats.py` (code vs docstring split), `anno.py` (annotation coverage). Backoff overflow repro was an inline `python -I -c`.
- Repo state check: `git status --short` shows only the pre-existing untracked `.claude/worktrees/`. Coverage data went to `scratchpad/audit/.coverage`.
- Not done: no Docker build, no compose up, no live Schwab/Discord, no DB-backed test run, no type checker run (none installed; I did not install one).
