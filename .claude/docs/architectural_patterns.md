# Architectural Patterns & Design Decisions

## Three-Layer Architecture

```
┌─────────────────────────────────────────┐
│  bot/          (Discord presentation)   │  ← imports core/ and data/
├─────────────────────────────────────────┤
│  core/         (Business logic)         │  ← NO discord imports
├─────────────────────────────────────────┤
│  data/         (Data access)            │  ← NO discord or core imports
└─────────────────────────────────────────┘
```

Enforced by convention. The one cross-layer exception: `core/scheduler/jobs.py` imports `StockData` from `data/` to wire up APScheduler jobs.

---

## StockData — Service Container Singleton

`StockData` (`data/stockdata.py:37-100`) is the single owner of all API clients and repositories. Created once at `__main__.py:16`, passed via constructor injection to `run_bot()`, attached to `bot.stock_data` at `bot/bot.py:163`.

**Rule:** Never call `StockData()` inside a cog or test. Always use `bot.stock_data` (in cogs) or pass it as a constructor argument (in tests).

Full client and repository inventory: see `data.md`.

---

## Content Class Pattern

Content classes in `core/content/` are pure Python — no discord imports. They accept typed dataclasses (defined in `core/content/models.py`) and return `EmbedSpec`.

### Alert Lifecycle

1. `Alert.__init__(data: SomeAlertData)` at `alerts/base.py:23-24` — stores fields in `self.alert_data` dict
2. `alert.build() -> EmbedSpec` at `alerts/base.py:26-31` — constructs embed content (subclasses override)
3. `alert.override_and_edit(prev_alert_data) -> bool` at `alerts/base.py:59-73` — decides re-post via momentum z-score
4. `alert.record_momentum(prev_alert_data)` at `alerts/base.py:75-98` — appends velocity/acceleration snapshot

### Screener Lifecycle

`Screener.__init__(screener_type, data: DataFrame, column_map)` at `screeners/base.py:19-23` filters/renames columns; `build() -> EmbedSpec` at `screeners/base.py:34-35`.

### EmbedSpec Boundary

Core layer returns `EmbedSpec` (`core/content/models.py:43-51`). Bot layer converts via `spec_to_embed()` at `bot/senders/embed_utils.py:8-23`. This is the only bridge between core and discord — keeping discord.Embed out of core.

For complete content class inventory, see `core.md`.

---

## Sender Pattern

### send_alert() — Alert Edit-In-Place (`bot/senders/alert_sender.py:12-120`)

1. Check `dstate.get_alert_message_id(date, ticker, alert_type)` for existing alert today
2. If exists: call `alert.override_and_edit(prev_data)` to decide whether to update
3. If update warranted: call `alert.record_momentum(prev_data)`, post new message linking back to old, persist new `alert_data`
4. If no existing: post new message, store message ID + `alert_data` in DB

### send_screener() — Screener Edit-In-Place (`bot/senders/report_sender.py:40-95`)

1. Check `dstate.get_screener_message_id(screener_type)` for today's message
2. Same day → `message.edit()` in-place
3. New day → `channel.send()` + store new message ID

### send_report() — Simple Send (`bot/senders/report_sender.py:11-37`)

No deduplication. Public (channel) or private (DM via interaction user) routing.

---

## Dual Scheduling System

### APScheduler (`core/scheduler/jobs.py:16-243`)

`register_jobs(aio_sched, stock_data, emitter)` adds 14 cron-triggered data maintenance jobs. Every job is wrapped with `emitter.job_wrapper()` for automatic success/failure notifications. Initialized in `bot.py:87-89`.

### discord.ext.tasks Loops (cogs)

Alert detection and report posting use `@tasks.loop()` decorators in cogs. Each pipeline is wrapped with `_run_task(name, coro)` (see `bot/cogs/alerts.py:127-147`) for per-pipeline error isolation and notification emission.

**Why two systems:** APScheduler for data maintenance jobs that run independently of Discord (no channel context needed). Task loops for content delivery that requires `bot` and channel access.

---

## EventEmitter — Notification Bridge

Thread-safe `queue.Queue`-based bridge (`core/notifications/emitter.py:14-17`) between scheduler/cog threads and the Discord notifications cog.

- `emit(event)` at `:20-25` — non-blocking put
- `drain(max_items=50)` at `:27-35` — non-blocking drain, called by notifications cog every 10s
- `job_wrapper(job_name, func)` at `:37-77` — wraps sync or async callables; emits SUCCESS/FAILURE with elapsed time
- `task_wrapper(task_name)` at `:79-115` — decorator for cog task loop methods (handles `self` as first arg)

Created at `__main__.py:19`, attached to `bot.emitter` at `bot/bot.py:164`.

---

## Channel Routing — Multi-Guild

`ChannelConfigRepository` (`data/channel_config.py`) stores per-guild channel assignments by type: `REPORTS`, `ALERTS`, `SCREENERS`, `NOTIFICATIONS`, `TRADE`.

`RocketStocksBot` provides two helpers (`bot/bot.py:37-56`):
- `get_channel_for_guild(guild_id, config_type)` — single guild, returns `TextChannel | None`
- `iter_channels(config_type)` — yields `(guild_id, TextChannel)` for all configured guilds

Cogs use `iter_channels()` in task loops to post to every configured guild.

---

## Cog Construction Pattern

Every cog follows: `__init__(bot, stock_data)`, creates `DiscordState(db=stock_data.db)` and `MarketUtils()`, starts task loops. Example: `bot/cogs/alerts.py:95-103`, `bot/cogs/reports.py:66-79`.

Cogs are loaded dynamically: `bot/bot.py:118-124` scans `bot/cogs/` and loads every `.py` not starting with `_`. Adding a cog = drop a file in `bot/cogs/`.

**Testing implication:** loops start in `__init__`, so tests must patch loops (and `DiscordState`) before construction. See `tests.md` for the `_make_cog()` pattern.

---

## Constructor Injection

`StockData` (`data/stockdata.py:38-45`) uses default-or-override for every dependency: all parameters default to `None` and fall back to constructing the real implementation. This enables tests to pass mock objects via kwargs without patching module internals.

**Circular dependency avoidance:** `SEC` client needs CIK lookups that live on `StockData`. Solved by injecting a callable: `stockdata.py:58` passes `get_cik_fn=self.get_cik` instead of the full object. Use this pattern for any future circular dep.
