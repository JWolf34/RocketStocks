# Operations

This is the operator's guide to running RocketStocks on the deploy VM.
It covers deploying the stack, signing in to Schwab, importing v1's Popularity history, the two rehearsals, day-to-day procedures, and what to back up and how to restore it.
Each procedure lists the exact commands and what to expect.

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
- Never paste real dumps, tokens or webhook URLs into a pull request or the repo.
  Row counts, ids and timestamps are fine.

## Variables to set in production

These are all in `.env.example` with a comment.
Four need setting:

- `RS_BOT_ADMIN_USER_ID`: set it to your own Discord user id.
  Without it, token-age DMs go to the Discord application's owner, who may not be the guild owner, and only the guild owner may run `/setup schwab`.
- `RS_BOT_ALERT_CHANNEL_ID`: the private channel Alerts post to.
- `RS_OPS_WEBHOOK_URL`: a webhook on a private ops channel.
  Ingest and the bot both read it.
  Unset, Ops pings are only logged at ERROR.
- `RS_EDGAR_USER_AGENT`: required by ingest, for example `RocketStocks you@example.com`.

Optional tunables you may touch during the rehearsals: `RS_BOT_POST_MAX_ATTEMPTS` (default 5), `RS_BOT_SWEEP_SECONDS` (60), `RS_INGEST_AUTH_POLL_SECONDS` (15), `RS_INGEST_TOKEN_POLL_SECONDS` (60).
Compose passes each one through only when it is set in `.env` or the shell.

## First deploy

1. Create `.env` from `.env.example` and fill every uncommented line.
   Generate each `RS_DB_PASSWORD_*` role password with `openssl rand -hex 24`.
   `RS_DB_PASSWORD` and the password inside `RS_DB_URL` must match.
2. Create the Discord application and bot, invite it to your server with the permission to send messages and embed links in the Alert channel, and copy the token into `RS_BOT_DISCORD_TOKEN`.
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
   `ingest` and `bot` must be `healthy`.
   Ingest starts with no Schwab token, so it pauses Schwab jobs and sends one Ops ping; the Popularity poll keeps running.
   Do the Schwab sign-in next.
6. Confirm the poll is writing:
   ```
   $COMPOSE logs ingest | grep "popularity poll"
   psql -c "SELECT polled_at, count(*) FROM popularity_snapshot WHERE origin = 'live' GROUP BY 1 ORDER BY 1 DESC LIMIT 3"
   ```
7. Set up your backups before the data is worth keeping (see "What to back up and how to restore").

To stop the stack: `$COMPOSE down` (the volume stays).
To start over from nothing, `$COMPOSE down -v` deletes the database volume.
Do this only while the data is disposable, or after you have a backup you trust.

## Timescale image bump

The compose file and CI pin one `timescale/timescaledb` image tag.
A volume created by an older image keeps the old extension version until you update it.

Either wipe the volume (only while the data is disposable):
```
$COMPOSE down -v
$COMPOSE up -d timescaledb
$COMPOSE run --rm migrate
$COMPOSE up -d --wait
```

Or keep the data and update the extension in place.
Take a backup first.
```
$COMPOSE pull timescaledb
$COMPOSE up -d timescaledb
psql -X -c "ALTER EXTENSION timescaledb UPDATE"
psql -c "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
$COMPOSE run --rm migrate
$COMPOSE restart ingest bot
```
`ALTER EXTENSION ... UPDATE` must be the first statement in a fresh session, which `psql -X -c` is.
`extversion` must equal the version in the image tag.

## Schwab sign-in with `/setup schwab`

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
   Ingest then logs `Schwab token renewed - resuming Schwab jobs` and sends one Ops ping.
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

## Import v1 Popularity

v1 is muted and still collecting Popularity from ApeWisdom.
Import its history from 2026-03-03 onward, where its 30-minute polling is near-complete.
Rows before that date are not imported.

### 1. Dump the v1 table

Dump v1's `popularity` table while v1 is still polling, because the offset check in step 2 needs both stacks polling at the same time.
The importer reads a CSV with a header row, so export only the window.

From the v1 compose project (the database service is named `postgres`):
```
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "COPY (SELECT datetime, rank, ticker, mentions, upvotes, rank_24h_ago, mentions_24h_ago FROM popularity WHERE datetime >= '2026-03-03' ORDER BY datetime, rank) TO STDOUT WITH CSV HEADER" > ~/popularity.csv
wc -l ~/popularity.csv
```
Keep the file outside the repo.
It is vendor data and never gets committed.
Note v1's row count and range for the same window, to compare with the import:
```
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT count(*), min(datetime), max(datetime) FROM popularity WHERE datetime >= '2026-03-03'"
```

### 2. Verify the offset

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
   .venv/bin/python -m rs_backfill verify-offset --dump ~/popularity.csv
   ```
   Exit 0 prints the winning zone and records it.
   Exit 1 means no candidate cleared the bar or there was too little overlap; wait for more overlap and rerun.

### 3. Import

1. Import, naming the recorded zone (for example `America/Chicago`):
   ```
   .venv/bin/python -m rs_backfill popularity-import --dump ~/popularity.csv --zone <zone>
   ```
2. Rerun the same import command.
   It must report `inserted 0`.
3. Cross-check the daylight-saving change inside the window.
   List the imported boundaries around the March spring-forward Sunday (substitute its date):
   ```
   psql -c "SELECT DISTINCT polled_at FROM popularity_snapshot WHERE origin = 'v1_import' AND polled_at >= '<spring-forward date> 04:00+00' AND polled_at < '<spring-forward date> 12:00+00' ORDER BY 1"
   ```
   v1 polled on the local wall clock, which skips an hour at the change, and the importer converts each row to UTC, so the right zone gives an unbroken 30-minute series (for `America/Chicago` on 2026-03-08: 07:30, then 08:00 UTC).
   A one-hour hole or a doubled hour means the zone is wrong; stop and rerun `verify-offset`.

Expect: the dump's row count and range match v1's, the first import reports `parsed/inserted/rejected` counts that agree with the dump, and the rerun reports `inserted 0`.
Once this passes, retiring v1 is an operator step.

## Check a live Alert

Done when a real Popularity Alert posts and carries a Track record.

1. Leave the stack running with a valid token, the imported history and bars loaded.
   After the import, measure the Track record once rather than waiting for the weekly job:
   ```
   $COMPOSE exec ingest python -m rs_research event-study popularity_surge
   ```
2. Find the Alert:
   ```
   psql -c "SELECT id, symbol, rule_version, alert_price, quote_time, triggered_at, posted_at, post_attempts FROM alerts WHERE alert_type = 'popularity_surge' ORDER BY id DESC LIMIT 5"
   ```
3. Confirm in Discord that the message shows its Track record, the `rule_version` and the `alert_price`, and that it was never edited.
4. Confirm which Track record it showed:
   ```
   psql -c "SELECT input, n, hit_rate_5d, median_excess_5d, computed_at FROM track_record WHERE alert_type = 'popularity_surge' ORDER BY computed_at DESC LIMIT 3"
   ```
   The bot shows the `logged` row once its N passes 30, otherwise the `reconstructed` row.

If no surge fires for days, stage a clearly marked test Alert through the real write path to prove the posting path (it is not a real Alert):
```
.venv/bin/python -m rs_bot.tools.stage_alert
```

## Rehearsals

Each rehearsal is a pass or fail on the observed outcome, never on elapsed time.
Do them in this order, with a healthy stack and a fresh token.

### Token expiry

Goal: Schwab jobs pause, one Ops ping goes out, the Popularity poll continues, and recovery works after a re-login.

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
   Expect in the ops channel: exactly one Ops ping, "Schwab token is dead ...".
   Expect `schwab_token_dead 1` on the metrics port and `skipped_token_dead` outcomes for the Schwab jobs.
4. Confirm polling continues: a new `popularity_snapshot` boundary appears within 30 minutes.
5. Recover: run `/setup schwab` (see "Schwab sign-in").
   Expect the log line `Schwab token renewed - resuming Schwab jobs`, one "renewed" Ops ping and `schwab_token_dead 0`.
6. Confirm the Schwab jobs run again (`cron_job_runs{outcome="success"}` rises for `surge_bars`).

### Ingest kill mid-poll

Goal: no lost or duplicated rows or Alerts after a restart.

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
   Expect: snapshot rows for the interrupted boundary are present exactly once (the key is `polled_at, ticker` with `ON CONFLICT DO NOTHING`), `alerts` equals `distinct_keys`, and a rerun of the same boundary added no Alert.
   ```
   psql -c "SELECT polled_at, count(*), count(DISTINCT ticker) FROM popularity_snapshot GROUP BY 1 ORDER BY 1 DESC LIMIT 3"
   psql -c "SELECT count(*) - count(DISTINCT dedup_key) AS duplicate_alerts FROM alerts WHERE dedup_key IS NOT NULL"
   ```
   `duplicate_alerts` must be 0 and each boundary's `count` must equal its `count(DISTINCT ticker)`.
5. A graceful stop is a separate, simpler check: `$COMPOSE stop ingest` must exit within `stop_grace_period` (45s) with a clean shutdown log.

## Operating procedures

### A failed-to-post Alert

The bot abandons an Alert after `RS_BOT_POST_MAX_ATTEMPTS` (default 5) failed sends and sends an Ops ping.
Such an Alert has `posted_at` NULL and `post_attempts >= RS_BOT_POST_MAX_ATTEMPTS`.

Find them:
```
psql -c "SELECT id, symbol, triggered_at, post_attempts FROM alerts WHERE posted_at IS NULL AND post_attempts >= 5 ORDER BY id"
```
(use your configured `RS_BOT_POST_MAX_ATTEMPTS` instead of 5.)

Fix the cause first (channel permission, Discord outage, an oversized field), then re-queue by resetting the counter:
```
psql -c "UPDATE alerts SET post_attempts = 0 WHERE id = <id>"
```
To re-queue every abandoned Alert at once, drop the `id` filter and use the same `posted_at IS NULL AND post_attempts >= 5` condition.
The bot's next sweep (within `RS_BOT_SWEEP_SECONDS`, 60 by default) posts it.
Never set `posted_at` by hand unless you posted the message yourself.

### Logs, metrics and health

- Logs are JSON on stdout: `$COMPOSE logs -f ingest bot`.
  Each record carries `service` and a `correlation_id`.
- Metrics are on `127.0.0.1:9090` (ingest) and `127.0.0.1:9091` (bot).
- Health: `$COMPOSE ps`.
  Ingest's check reads the loop heartbeat from its metrics, so a hung loop turns it unhealthy; `RS_INGEST_HEALTH_MAX_AGE_SECONDS` sets the limit.
  The bot's check probes its metrics port.

### Ops pings to expect

A source pings once when it starts failing and once when it clears; repeated failures in between say nothing.
The sources are ApeWisdom, Schwab, Nasdaq, EDGAR and Discord.
A ping reads `<source> is failing (<reason>). See the <service> logs.` and later `<source> recovered.`.
The reason is an exception type or a count, never vendor text; the logs have the detail.

What counts as failing:

- A source failing: a connection error, a timeout, a 5xx, a 429 or a payload that is not what it should be.
- More than about 5% of a sweep's per-stock requests failing, and at least two requests.
- Never one stock: a 400 or 404 for a symbol, no data for it, or a single failed request among many pings nothing.

Which pings to expect, by cause:

- `ApeWisdom is failing ...`: the Popularity poll could not fetch the listing, or fetched it empty.
  The poll fails without a second ping; a boundary is missed until the next poll.
- `Schwab is failing ...`: quotes for an Alert's facts failed, or more than about 5% of the daily-bar sweep did.
  The Alert still posts, with the missing facts labelled.
- `Nasdaq is failing ...` and `EDGAR is failing ...`: the earnings calendar or the filings lookup failed.
  The next-earnings fact falls back to the EDGAR estimate, then to missing.
- `Discord is failing ...`: the bot's sends failed for a reason other than one Alert (a rejected embed is one Alert).
  Unposted Alerts stay queued and the sweep retries them.
- `Ingest job <name> is failing (<type>)` and `Ingest job <name> recovered.`: a job failed for a reason that is not a source, such as the database being down or a bug.
  It pings on its first failure, not after several, and once when the job next succeeds.
- `Schwab token is dead ...` and `Schwab token renewed ...`: see "Schwab sign-in" and the token-expiry rehearsal.
  A dead token has these pings only; it does not also send `Schwab is failing`.
- `Alert #<id> ... failed to post after N attempts ...`: the procedure above.
- The weekly Track record job failing.

One cause sends one ping: a source failure that also fails its job does not send a job ping on top.

Health is remembered in process only.
After an ingest or bot restart, a source that is still failing pings once more when the next request fails, and one that recovered while the service was down says nothing.

## What to back up and how to restore

Backups, disk monitoring and host health are the operator's job, done with your own tools.
This section says what is irreplaceable and how to bring it back.

### What is irreplaceable

Everything lives in the one Postgres database `rocketstocks` in the `timescaledb` service.
Three kinds of data cannot be fetched again:

- Popularity snapshots (`popularity_snapshot`): ApeWisdom serves only the current reading, so a missed poll is gone for good.
- Options chain snapshots (`options_chain_snapshot`): Schwab serves only the current chain.
- User state such as paper trades, settings and roles.
  These are written by the bot and live in the same database.

Daily bars are repaired automatically, and Baselines, Track records and Reports are rebuilt at will.
Back up the whole database anyway; it is the simplest way to be sure none of the three is missed, and it keeps the history of posted Alerts too.

A dump also contains the Schwab refresh token (`schwab_tokens`).
Store backups as you would a credential.
Losing the token costs only one `/setup schwab`.

### How to back up

Use whichever of these fits your host, and store the result off the VM.

- A logical dump, taken while the stack runs:
  ```
  $COMPOSE exec -T timescaledb pg_dump -U postgres -d rocketstocks -Fc > rocketstocks.dump
  ```
  `pg_dump` prints a warning about circular foreign keys on `continuous_agg`; it is expected.
- A block-level, crash-consistent snapshot of the VM disk or the `timescale_data` volume.
  Postgres replays its write-ahead log when it starts from such a snapshot.

Keep the files out of the repo.
They are vendor data and never get committed.

### Restore from a dump

Restore into a fresh, empty database: a new volume, with the stack down.

1. Stop the stack, delete the old database volume and start only the database on a new empty one.
   `down -v` deletes the existing database, so be sure that is what you want.
   On a new host there is no volume yet, and `$COMPOSE down` alone is enough.
   ```
   $COMPOSE down -v
   $COMPOSE up -d timescaledb
   ```
   Do not run the migrate gate yet: the dump carries the schema.
2. Put Timescale into restore mode, load the dump, then leave restore mode.
   Each statement runs in its own session, as shown.
   ```
   psql -X -At -c "SELECT timescaledb_pre_restore()"
   $COMPOSE exec -T timescaledb pg_restore -U postgres -d rocketstocks --no-owner --no-privileges < rocketstocks.dump
   psql -X -At -c "SELECT timescaledb_post_restore()"
   ```
   The image's Timescale version must be the same as or newer than the one that took the dump, and the Postgres major version must match.
   Read the `pg_restore` output: a clean restore prints nothing and exits 0.
3. Re-apply the schema gate and the per-service database roles.
   The dump leaves out role grants (`--no-privileges`), and roles belong to the cluster, not the database, so the migrate gate recreates them from the `RS_DB_PASSWORD_*` values in `.env`.
   If the dump is from an older release, it also upgrades the schema to head.
   ```
   $COMPOSE run --rm migrate
   $COMPOSE run --rm --no-deps migrate python -m rs_migrate current
   $COMPOSE run --rm --no-deps migrate python -m rs_migrate check
   ```
   `current` must print the head revision and `check` must print nothing.
   Verify the restore now (see "Verify a restore"), before anything writes new rows.
4. Start the stack and watch it come up healthy on the restored data:
   ```
   $COMPOSE up -d --wait
   $COMPOSE ps
   ```
5. Account for the gap between the backup and now.
   Popularity snapshots and chain snapshots from that window are lost for good.
   Daily bars repair themselves on the next runs.
   If the restored token is older than seven days, run `/setup schwab`.

### Restore from a volume snapshot

1. Restore the snapshot to the volume and start only the database: `$COMPOSE up -d timescaledb`.
2. Confirm `database system is ready to accept connections` in `$COMPOSE logs timescaledb`.
3. Run the migrate gate and the `current` and `check` commands from step 3 above, then start the stack.

### Verify a restore

Compare per-table row counts against counts recorded when the backup was taken.
Record them at backup time:
```
psql -X -At > counts-backup.txt <<'SQL'
SELECT format('SELECT %L AS t, count(*) FROM %I', tablename, tablename)
FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename \gexec
SQL
```
After the restore and the migrate gate, and before starting ingest and the bot (they write new rows), produce the same file and compare:
```
psql -X -At > counts-restored.txt <<'SQL'
SELECT format('SELECT %L AS t, count(*) FROM %I', tablename, tablename)
FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename \gexec
SQL
diff counts-backup.txt counts-restored.txt
```
An empty diff passes.
The counts only match exactly when ingest and the bot were stopped between recording the counts and taking the backup.
For a backup taken while they run, expect each restored count to fall between the counts recorded just before and just after the backup.

Also check the newest data is where it should be:
```
psql -c "SELECT max(polled_at) FROM popularity_snapshot"
psql -c "SELECT count(*), max(fetched_at) FROM options_chain_snapshot"
```
