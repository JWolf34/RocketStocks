# Slice 1 runbook

This is the operator's guide to running the slice-1 stack (attention surge) and to the live run and rehearsals that close the slice.
Each procedure lists the exact commands and what to record in the slice pull request.
The criteria it serves are rows 5, 8, 9, 10 and 11 of the [slice 1 spec](../specs/01-attention-surge.md).

## Conventions

- Run everything from the repo root on the deploy VM.
  `COMPOSE` below means `docker compose -f docker-compose.v2.yml`.
- `.env` holds the v2 variables listed in `.env.example`.
  Its `RS_DB_URL` is the superuser DSN on `127.0.0.1:5498`, used by host-side tools (`rs_migrate`, `rs_backfill`).
- Host-side tools run from the shared venv: `uv sync --all-packages`, then `.venv/bin/python -m ...`.
  Export the DSN into the shell first (do not source `.env`: it has unquoted spaces and CRLF line endings):
  ```
  export RS_DB_URL="$(grep '^RS_DB_URL=' .env | cut -d= -f2- | tr -d '\r')"
  ```
- SQL snippets run through the database container.
  `psql` below is a shell function; define it once per shell, and note the `-X`, `-At` and heredoc uses pass extra flags to it:
  ```
  COMPOSE="docker compose -f docker-compose.v2.yml"
  psql() { $COMPOSE exec -T timescaledb psql -U postgres -d rocketstocks "$@"; }
  ```
- Never paste real dumps, tokens or webhook URLs into the pull request or the repo.
  Record row counts, ids and timestamps only.

## Variables to set in production

These are all in `.env.example` with a comment.
Four need your attention:

- `RS_BOT_ADMIN_USER_ID`: set it to your own Discord user id.
  Without it, token-age DMs go to the Discord application's owner, who may not be the guild owner, and only the guild owner may run `/setup schwab`.
- `RS_BOT_ALERT_CHANNEL_ID`: the private channel alerts post to (renamed from `RS_DEV_CHANNEL_ID`).
- `RS_OPS_WEBHOOK_URL`: a webhook on a private ops channel.
  Ingest and the bot both read it.
  Unset, ops notices are only logged at ERROR.
- `RS_EDGAR_USER_AGENT`: required by ingest, for example `RocketStocks you@example.com`.

Optional tunables you may touch during the rehearsals: `RS_BOT_POST_MAX_ATTEMPTS` (default 5), `RS_BOT_SWEEP_SECONDS` (60), `RS_INGEST_AUTH_POLL_SECONDS` (15), `RS_INGEST_TOKEN_POLL_SECONDS` (60), `RS_INGEST_JOB_FAILURE_THRESHOLD` (3).
Compose passes each one through only when it is set in `.env` or the shell.

## 1. First deploy

1. Create `.env` from `.env.example` and fill every uncommented line.
   Generate each role password with `openssl rand -hex 24`.
   `RS_DB_PASSWORD` and the password inside `RS_DB_URL` must match.
2. Create the Discord application and bot, invite it to your server with the permission to send messages and embed links in the alert channel, and copy the token into `RS_BOT_DISCORD_TOKEN`.
   The bot uses default intents only.
3. Check the file parses and the image builds:
   ```
   $COMPOSE config -q
   $COMPOSE build
   ```
4. Start the database and the migrate gate, and read the migrate log:
   ```
   $COMPOSE up -d timescaledb
   $COMPOSE run --rm migrate
   $COMPOSE run --rm --no-deps migrate python -m rs_migrate current
   $COMPOSE run --rm --no-deps migrate python -m rs_migrate check
   ```
   `current` must print the head revision and `check` must print nothing.
5. Start the stack and wait for health:
   ```
   $COMPOSE up -d --wait
   $COMPOSE ps
   ```
   `ingest`, `serve` and `bot` must be `healthy`.
   Ingest starts with no Schwab token, so it pauses Schwab jobs and sends one ops notice; the ApeWisdom poll keeps running.
   Do section 3 next.
6. Confirm the poll is writing:
   ```
   $COMPOSE logs ingest | grep "popularity poll"
   psql -c "SELECT polled_at, count(*) FROM popularity_snapshot WHERE origin = 'live' GROUP BY 1 ORDER BY 1 DESC LIMIT 3"
   ```
7. Verify the three backup conditions of ADR-0031 on the VM: block-level crash-consistent snapshots, off-host storage, and a rehearsed restore (section 7.3).
   The home lab VM has no cloud default to lean on, so check each one explicitly.

To stop the stack: `$COMPOSE down` (the volume stays).
To start over from nothing, `$COMPOSE down -v` deletes the database volume.
Pre-cutover data is disposable, so this is the supported reset.

## 2. Timescale image bump

The compose file and CI pin `timescale/timescaledb:2.30.2-pg16`, which replaced 2.28.2.
A volume created by 2.28.2 keeps the old extension version until you update it.

Either wipe the volume (preferred while the data is disposable):
```
$COMPOSE down -v
$COMPOSE up -d timescaledb
$COMPOSE run --rm migrate
$COMPOSE up -d --wait
```

Or keep the data and update the extension in place:
```
$COMPOSE pull timescaledb
$COMPOSE up -d timescaledb
psql -X -c "ALTER EXTENSION timescaledb UPDATE"
psql -c "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
$COMPOSE run --rm migrate
$COMPOSE restart ingest bot serve
```
`ALTER EXTENSION ... UPDATE` must be the first statement in a fresh session, which `psql -X -c` is.
`extversion` must read `2.30.2`.

Record the Timescale tag and `extversion` in the pull request.

## 3. Schwab sign-in with `/setup schwab`

The bot never holds the token.
It passes text through the `schwab_auth_request` table and ingest does the OAuth exchange.

1. In the Discord server, as the guild owner or the user in `RS_BOT_ADMIN_USER_ID`, run `/setup schwab`.
2. Open the sign-in URL the bot shows (only you can see it), sign in to Schwab and approve.
3. The browser lands on the registered callback URL (`https://127.0.0.1:8182` by default) and shows a connection error.
   That is expected.
   Copy the full URL from the address bar, which contains `code=`.
4. Paste it into the modal the bot offers.
   The code is single-use and the URL is never logged or echoed.
5. The bot reports the outcome.
   Ingest then logs `Schwab token renewed - resuming Schwab jobs` and sends one ops notice.
   Confirm with `psql -c "SELECT issued_at FROM schwab_token_issue"`.

If the request fails:

- A `/setup schwab` request open across an ingest restart fails with "Ingest restarted before the sign-in finished. Run /setup schwab again."
  Just run the command again.
- A mistyped paste is rejected before it uses up the sign-in.
  A rejected exchange needs a fresh `/setup schwab`.
- A request left open longer than 10 minutes expires; run the command again.

The local fallback, when Discord or the bot is down, is a login from the host.
On a host with a browser:
```
.venv/bin/python -m rs_ingest.tools.login
```
On a headless host (the deploy VM), the manual flow prints the sign-in URL and takes the pasted callback URL at the prompt:
```
.venv/bin/python scripts/acceptance/schwab_manual_login.py
```
Both need `SCHWAB_API_KEY`, `SCHWAB_API_SECRET`, `RS_EDGAR_USER_AGENT` and a superuser or ingest `RS_DB_URL` in the environment.
Ingest notices the newer token on its next token poll (`RS_INGEST_TOKEN_POLL_SECONDS`).

The Schwab refresh token lives seven days.
The bot DMs you at 24 hours and 6 hours before it expires; re-run `/setup schwab` then.

## 4. Produce the v1 popularity dump

v1 is muted and still collecting ApeWisdom popularity.
Dump its `popularity` table while it is still polling, because the offset check in section 5 needs both stacks polling at the same time.

From the v1 compose project (the database service is named `postgres`):
```
docker compose exec -T postgres pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  --data-only --table=popularity --no-owner > ~/popularity.sql
wc -l ~/popularity.sql
```
Keep the file outside the repo.
It is vendor data and never gets committed.
Record the table's row count and range from v1, to compare with the import:
```
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT count(*), min(datetime), max(datetime) FROM popularity"
```

## 5. Verify the offset and import

v1 wrote naive local timestamps in a container zone nobody documented.
The import refuses to run until `verify-offset` records the zone against live data.

1. The v2 poll must have been running alongside v1 long enough to overlap on many rows.
   The default minimum is 1000 overlapping rows (`--min-overlap` changes it).
   Check the live extent:
   ```
   psql -c "SELECT min(polled_at), max(polled_at), count(*) FROM popularity_snapshot WHERE origin = 'live'"
   ```
2. Find and record the zone (it needs at least 99% of the overlap to match one candidate):
   ```
   .venv/bin/python -m rs_backfill verify-offset --dump ~/popularity.sql
   ```
   Exit 0 prints the winning zone and records it.
   Exit 1 means no candidate cleared the bar or there was too little overlap; wait for more overlap and rerun.
3. Import, naming the recorded zone (for example `America/Chicago`):
   ```
   .venv/bin/python -m rs_backfill popularity-import --dump ~/popularity.sql --zone <zone>
   ```
4. Rerun the same import command.
   It must report `inserted 0`.
5. Cross-check the daylight-saving cases from the spec by listing the imported boundaries around each change:
   ```
   psql -c "SELECT DISTINCT polled_at FROM popularity_snapshot WHERE origin = 'v1_import' AND polled_at >= '2026-03-08 04:00+00' AND polled_at < '2026-03-08 12:00+00' ORDER BY 1"
   psql -c "SELECT DISTINCT polled_at FROM popularity_snapshot WHERE origin = 'v1_import' AND polled_at >= '2025-11-02 03:00+00' AND polled_at < '2025-11-02 10:00+00' ORDER BY 1"
   ```
   A local zone leaves no rows in the spring-forward hour of 2026-03-08 (a one-hour gap in the first list) and loses the repeated hour on 2025-11-02 to v1's primary key (a one-hour gap in the second).
   The matched zone must agree with what these lists show.

Record in the pull request: the dump row count and range, the verify-offset report (overlap rows, matched rows, rate, zone), the first import's `parsed/inserted/rejected` line, and the rerun's `inserted 0`.
Once this passes, retiring v1 is an operator step.

## 6. Record the live alert

Done when a real surge posts.
This is criterion 8.

1. Leave the stack running with a valid token (section 3), the imported history (section 5) and bars loaded.
   After the import, measure the base rate once rather than waiting for the Sunday job:
   ```
   $COMPOSE exec ingest python -m rs_research event-study attention_surge
   ```
2. Find the alert:
   ```
   psql -c "SELECT id, symbol, rule_version, alert_price, quote_time, triggered_at, posted_at, post_attempts FROM alerts WHERE alert_type = 'attention_surge' ORDER BY id DESC LIMIT 5"
   ```
3. Confirm in Discord that the message has the facts block (price move, filings, next earnings), the base rate, the `rule_version` and the `alert_price`, and that it was never edited.
4. Confirm which base rate it showed:
   ```
   psql -c "SELECT input, n, hit_rate_5d, median_excess_5d, computed_at FROM base_rate WHERE alert_type = 'attention_surge' ORDER BY computed_at DESC LIMIT 3"
   ```
   The bot shows the `logged` row once its N passes 30, otherwise the `reconstructed` row.

Record: the alert id, symbol, `rule_version`, `alert_price`, `quote_time`, `posted_at`, the Discord message link, and the base rate row's `input` and `n`.
A screenshot of the message is fine; it has no vendor payload.

If no surge fires for days, stage a clearly marked test alert through the real write path to prove the posting path (it does not count as the live alert):
```
.venv/bin/python -m rs_bot.tools.stage_alert
```

## 7. Rehearsals

Each rehearsal is a pass or fail on the observed outcome, never on elapsed time.
Do them in this order, with a healthy stack and a fresh token.

### 7.1 Token expiry (criterion 9)

Goal: bars pause, one ops notice goes out, polling continues, and recovery works after a re-login.

1. Note the baseline:
   ```
   curl -s 127.0.0.1:9090/metrics | grep -E "schwab_token_dead|cron_job_runs"
   psql -c "SELECT max(polled_at) FROM popularity_snapshot"
   ```
2. Kill the refresh token.
   Waiting seven days works; this forces it.
   schwab-py stores `{"creation_timestamp": ..., "token": {...}}`; check the shape first with `psql -c "SELECT jsonb_object_keys(token_data) FROM schwab_tokens"`.
   ```
   psql -c "UPDATE schwab_tokens SET token_data = jsonb_set(jsonb_set(token_data, '{token,refresh_token}', '\"expired\"'), '{token,expires_at}', '0')"
   psql -c "DELETE FROM daily_bar WHERE symbol = 'SPY' AND session_date = (SELECT max(session_date) FROM daily_bar WHERE symbol = 'SPY')"
   $COMPOSE restart ingest
   ```
   The restart makes ingest reload the broken token; the zero `expires_at` forces a refresh on the first Schwab call.
   Deleting SPY's newest bar guarantees that call: the surge-bars job runs at startup but calls Schwab only for missing bars, and it refetches the deleted bar once the token is back.
3. Wait for the Schwab call, which comes within a minute of the restart.
   Expect, in `$COMPOSE logs ingest`: `Schwab token dead - pausing Schwab jobs`.
   Expect in the ops channel: exactly one notice, "Schwab token is dead ...".
   Expect `schwab_token_dead 1` on the metrics port and `skipped_token_dead` outcomes for the Schwab jobs.
4. Confirm polling continues: a new `popularity_snapshot` boundary appears within 30 minutes.
5. Recover: run `/setup schwab` (section 3).
   Expect the log line `Schwab token renewed - resuming Schwab jobs`, one "renewed" ops notice and `schwab_token_dead 0`.
6. Confirm the Schwab jobs run again (`cron_job_runs{outcome="success"}` rises for `surge_bars`).

Record: the log lines and ops notice texts with timestamps, the metric values before, during and after, and the newest `popularity_snapshot.polled_at` during the outage.

### 7.2 Ingest kill mid-poll (criterion 10)

Goal: no lost or duplicated rows or alerts after a restart.

1. Capture counts and the newest boundary:
   ```
   psql -c "SELECT (SELECT count(*) FROM popularity_snapshot) AS snapshots, (SELECT count(*) FROM alerts) AS alerts, (SELECT count(DISTINCT dedup_key) FROM alerts) AS distinct_keys"
   ```
2. Kill ingest hard while a poll is running.
   Polls fire on the :00 and :30 UTC boundaries.
   A poll takes a few seconds, so kill within about two seconds of the boundary.
   Kill the container's main process from the host, which Docker treats as a crash:
   ```
   sudo kill -9 "$(docker inspect -f '{{.State.Pid}}' rocketstocks-v2-ingest-1)"
   ```
   Do not use `$COMPOSE kill` here: Docker treats it as a manual stop, and the restart policy never brings the container back.
3. Confirm the restart policy brought ingest back (`$COMPOSE ps` shows it up again, with a fresh start time).
   Ingest runs the poll at startup, so it finishes the interrupted boundary.
4. Repeat the count query.
   Expect: snapshot rows for the interrupted boundary are present exactly once (the key is `polled_at, ticker` with `ON CONFLICT DO NOTHING`), `alerts` equals `distinct_keys`, and a rerun of the same boundary added no alert.
   ```
   psql -c "SELECT polled_at, count(*), count(DISTINCT ticker) FROM popularity_snapshot GROUP BY 1 ORDER BY 1 DESC LIMIT 3"
   psql -c "SELECT count(*) - count(DISTINCT dedup_key) AS duplicate_alerts FROM alerts WHERE dedup_key IS NOT NULL"
   ```
   `duplicate_alerts` must be 0 and each boundary's `count` must equal its `count(DISTINCT ticker)`.
5. A graceful stop is a separate, simpler check: `$COMPOSE stop ingest` must exit within `stop_grace_period` (45s) with a clean shutdown log.

Record: the before and after counts, the interrupted boundary, the restart log lines, and `duplicate_alerts = 0`.

### 7.3 Backup restore (criterion 11)

Goal: a restored snapshot to a scratch VM starts the stack and its row counts match.

1. Before taking the snapshot, record exact counts on the live VM, with ingest stopped so they hold still:
   ```
   $COMPOSE stop ingest bot
   $COMPOSE exec -T timescaledb psql -U postgres -d rocketstocks -At > counts-live.txt <<'SQL'
   SELECT format('SELECT %L AS t, count(*) FROM %I', tablename, tablename)
   FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename \gexec
   SQL
   ```
   Keep `counts-live.txt` outside the repo.
2. Trigger a VM snapshot with your usual tool and wait until it is complete.
   Take it right after the counts, with ingest and the bot still stopped, so the two agree.
   Confirm the snapshot is off-host (ADR-0031 condition 2).
3. Restore it to a scratch VM.
   Check out the same commit, copy `.env`, and start only the database:
   ```
   $COMPOSE up -d timescaledb
   ```
   Postgres replays its WAL on start; confirm `database system is ready to accept connections` in `$COMPOSE logs timescaledb`.
4. Confirm the schema is at head, without starting the services (they would write new rows):
   ```
   $COMPOSE run --rm migrate
   ```
5. Produce the same count file on the scratch VM (the command from step 1, writing `counts-restored.txt`), copy `counts-live.txt` over, and compare:
   ```
   diff counts-live.txt counts-restored.txt
   ```
   An empty diff passes.
6. Start the stack to prove it comes up healthy on the restored data, then shut the scratch stack down:
   ```
   $COMPOSE up -d --wait
   $COMPOSE down
   ```
   On the live VM, restart the services you stopped in step 1 with `$COMPOSE up -d ingest bot`.

Record: the snapshot timestamp, restore duration, `diff` result (paste the per-table counts, not data), and the ADR-0031 condition check (crash-consistent, off-host, rehearsed).
Per ADR-0031, after a real restore re-run the daily-bar backfill for the lost window; the bot's `posted_at` sweep handles alert state normally.

## 8. Operating procedures

### A failed-to-post alert

The bot abandons an alert after `RS_BOT_POST_MAX_ATTEMPTS` (default 5) failed sends and tells ops.
Such an alert has `posted_at` NULL and `post_attempts >= RS_BOT_POST_MAX_ATTEMPTS`.

Find them:
```
psql -c "SELECT id, symbol, triggered_at, post_attempts FROM alerts WHERE posted_at IS NULL AND post_attempts >= 5 ORDER BY id"
```
(use your configured `RS_BOT_POST_MAX_ATTEMPTS` instead of 5.)

Fix the cause first (channel permission, Discord outage, an oversized field), then re-queue by resetting the counter:
```
psql -c "UPDATE alerts SET post_attempts = 0 WHERE id = <id>"
```
To re-queue every abandoned alert at once, drop the `id` filter and use the same `posted_at IS NULL AND post_attempts >= 5` condition.
The bot's next sweep (within `RS_BOT_SWEEP_SECONDS`, 60 by default) posts it.
Never set `posted_at` by hand unless you posted the message yourself.

### Logs, metrics and health

- Logs are JSON on stdout: `$COMPOSE logs -f ingest bot serve`.
  Each record carries `service` and a `correlation_id`.
- Metrics are on `127.0.0.1:9090` (ingest) and `127.0.0.1:9091` (bot); serve exposes `/metrics` behind its API key on port 8000.
- Health: `$COMPOSE ps`.
  Ingest's check reads the loop heartbeat from its metrics, so a hung loop turns it unhealthy; `RS_INGEST_HEALTH_MAX_AGE_SECONDS` sets the limit.
  The bot probes its metrics port and serve probes that its HTTP server answers.

### Ops notices to expect

- `Schwab token is dead ...` and `Schwab token renewed ...`: section 3 and 7.1.
- `Ingest job <name> has failed N times in a row ...` after `RS_INGEST_JOB_FAILURE_THRESHOLD` consecutive failures, and `... recovered.` when it clears.
- `Alert #<id> ... failed to post after N attempts ...`: the procedure above.
- The weekly base-rate job failing.
