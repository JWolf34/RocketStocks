# Bot Layer (`src/rocketstocks/bot/`)

Presentation layer. **All discord-specific code lives here.** Imports freely from both `core/` and `data/`. Never imported by `core/` or `data/`.

---

## bot/bot.py — Bot Setup and Lifecycle

`RocketStocksBot` class at `bot.py:29-99`. Extends `commands.Bot`.

**Class attributes** (set in `run_bot()` at `:163-165`):
- `bot.stock_data` — `StockData` singleton
- `bot.emitter` — `EventEmitter` for scheduler notifications
- `bot.notification_config` — `NotificationConfig`
- `bot.aio_sched` — `AsyncIOScheduler` (set during `setup_hook`)

**Lifecycle methods:**
- `setup_hook()` at `:58-91` — opens DB pool, creates tables, resolves timezone + notification_filter from DB, initializes Schwab, starts APScheduler, registers persistent views. Called once before the bot connects.
- `close()` at `:93-99` — shuts down APScheduler, closes DB pool.

**Channel routing helpers:**
- `get_channel_for_guild(guild_id, config_type)` at `:37-45` — returns configured `TextChannel` or None
- `iter_channels(config_type)` at `:47-56` — yields `(guild_id, TextChannel)` for all configured guilds

**Startup functions:**
- `create_bot()` at `:102-105` — factory, sets intents
- `run_bot(stock_data, emitter, notification_config)` at `:150-166` — top-level entry called from `__main__.py:23`. Attaches dependencies, registers `on_ready` event, calls `bot.run()`.
- `load(bot)` at `:118-124` — scans `bot/cogs/` and loads every `.py` file not starting with `_` as a discord extension. Adding a new cog = drop a file in `bot/cogs/`.

---

## bot/cogs/ — Cog Modules

**Construction pattern:** `__init__(bot, stock_data)` creates a `DiscordState` and `MarketUtils` instance, then starts all task loops. Tests must patch loops **before** constructing the cog — see `.claude/docs/tests.md`.

| File | Purpose | Key loops/commands |
|------|---------|-------------------|
| `alerts.py` | Alert detection and delivery | `detect_popularity_surges` (30m), `process_alerts` (5m) — 5 pipelines via asyncio.gather; `/alert history`, `/alert stats`, `/alert summary`, `/alert subscriptions` |
| `reports.py` | Scheduled reports and screeners | 8 task loops (screeners, earnings spotlight, earnings results, weekly earnings); `/report`, `/news` |
| `data.py` | On-demand data slash commands | No loops; `/data` app_commands.Group with ~15 subcommands (quote, price, fundamentals, options, stats, earnings, analyst, etc.) |
| `watchlists.py` | Watchlist management | No loops; `/watchlist create/add/remove/list/delete` |
| `paper_trading.py` | Virtual paper trading | `process_queued_orders` loop; `/trade buy/sell/portfolio/history/leaderboard/performance` |
| `notifications.py` | Bot health channel | `poll_notifications` loop (10s drain cycle); latency check; disconnect/reconnect monitoring |
| `config.py` | Per-guild configuration | No loops; `/config set-channel/set-timezone/set-notifications/view` |
| `admin.py` | Debug and admin tools | No loops; admin-only commands for data inspection |
| `schwab_auth.py` | Schwab OAuth re-auth | No loops; `/schwab-status`, `/schwab-auth` (Discord-native OAuth — no callback server, user pastes URL) |
| `utils.py` | Help system | No loops; `/help` with category-based navigation |

### Alerts cog detail

- `detect_popularity_surges` loop (30m) — fetches ApeWisdom data, calls `evaluate_popularity_surge()`, inserts to `popularity_surges` table via `SurgeRepository`
- `process_alerts` loop (5m) — runs 5 pipelines concurrently via `asyncio.gather`: `_earnings_pipeline`, `_watchlist_pipeline`, `_popularity_confirmation_pipeline`, `_volume_accumulation_pipeline`, `_breakout_pipeline`
- `_run_task(name, coro)` helper wraps each pipeline call with try/except and emits `NotificationEvent` on success/failure
- `_volume_accumulation_pipeline` — requires both volume divergence (`vol_z >= 2.5`, `abs(price_z) < 1.0`, `divergence >= 1.5`) **and** unusual options activity (at least one contract with volume/OI >= 3.0) to fire. No unusual options activity = ticker skipped entirely. All VA alerts have `signal_strength='volume_plus_options'`.
- `_breakout_pipeline` — confirms pending VA signals when price z-score since flag >= 1.5; minimum 10-minute delay after VA detection; signals expire after 8 hours

---

## bot/senders/ — Message Sending Functions

| File | Function | Behavior |
|------|---------|---------|
| `alert_sender.py:12` | `send_alert(alert, channel, dstate, view, role_mention)` | Edit-in-place: checks DiscordState for existing alert today; calls `override_and_edit()` to decide if update warranted; calls `record_momentum()` before persisting; links back to prior message |
| `report_sender.py:11` | `send_report(content, channel, interaction, visibility, files, view)` | Simple embed send; public (channel) or private (DM) routing |
| `report_sender.py:40` | `send_screener(content, channel, dstate, view, files)` | Edit-in-place by day: same-day → `message.edit()`, new day → new message |
| `notification_sender.py` | `format_notification(event, config)` | Formats `NotificationEvent` into Discord embed |
| `embed_utils.py:8` | `spec_to_embed(spec: EmbedSpec) -> discord.Embed` | Core-to-bot boundary — converts `EmbedSpec` to `discord.Embed` |

---

## bot/views/ — Discord UI Components

| File | Contents |
|------|---------|
| `alert_views.py` | `AlertButtons` (chart + report links), `PopularitySurgeAlertButtons` (with doc URL), `WatchlistSelect` (dropdown); per-alert documentation URLs defined as module-level constants |
| `report_views.py` | Buttons for report embeds: `StockReportButtons`, `ComparisonReportButtons`, `TechnicalReportButtons`, `OptionsReportButtons`, screener buttons |
| `paper_trading_views.py` | Trade confirmation views with buy/sell/cancel buttons |
| `subscription_views.py` | `AlertSubscriptionSelect` (dropdown for alert role subscription), `SubscriptionEntryView` (persistent view re-registered on startup via `bot.py:91`) |
