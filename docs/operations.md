# Operations

This is the operator's guide to running RocketStocks on the deploy VM.
It covers deploying the stack, signing in to Schwab, the two rehearsals, day-to-day procedures, and what to back up and how to restore it.
Each procedure lists the exact commands and what to expect.

## Conventions

- Run everything from the repo root on the deploy VM.
  `COMPOSE` below means `docker compose -f docker-compose.yml`, with `sudo` in front if your user cannot reach Docker.
- `.env` holds the variables listed in `.env.example`.
- Tools run inside the image, so the VM needs no Python of its own.
  `$COMPOSE run --rm --no-deps migrate python -m <tool>` runs one with the superuser DSN the migrate service carries inside the compose network.
  A file the tool reads goes in as a read-only mount, for example `-v ~/file:/data/file:ro` before `migrate`.
- On a development machine the same tools also run from the shared venv (`uv sync --all-packages`, then `.venv/bin/python -m ...`), with `.env`'s `RS_DB_URL`, the superuser DSN on `127.0.0.1:5498`, exported first (do not source `.env`: it has unquoted spaces and CRLF line endings):
  ```
  export RS_DB_URL="$(grep '^RS_DB_URL=' .env | cut -d= -f2- | tr -d '\r')"
  ```
- SQL snippets run through the database container.
  `psql` below is a shell function; define it once per shell, and note the `-X`, `-At` and heredoc uses pass extra flags to it:
  ```
  COMPOSE="docker compose -f docker-compose.yml"
  psql() { $COMPOSE exec -T timescaledb psql -U postgres -d rocketstocks "$@"; }
  ```
- Never paste real dumps, tokens or webhook URLs into a pull request or the repo.
  Row counts, ids and timestamps are fine.

## Variables to set in production

These are all in `.env.example` with a comment.
Four need setting:

- `RS_BOT_ADMIN_USER_ID`: set it to your own Discord user id.
  Without it, token-age DMs go to the Discord application's owner, who may not be the guild owner, and only the guild owner may run `/setup schwab`.
  `/setup` is hidden from everyone but server administrators, so the operator also needs the Administrator permission (see "Discord permissions and role pings").
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
2. Create the Discord application and bot, invite it to your server with the permissions in "Discord permissions and role pings", and copy the token into `RS_BOT_DISCORD_TOKEN`.
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

## Upgrade to a new version

Take a backup first (see "How to back up"): migrations change stored rows.
```
git fetch && git checkout <branch> && git pull
$COMPOSE build
$COMPOSE stop ingest bot
$COMPOSE run --rm migrate
$COMPOSE run --rm --no-deps migrate python -m rs_migrate check
$COMPOSE up -d --wait
$COMPOSE ps
```
Ingest and the bot stop before the migrations so neither writes against a schema it does not know.
If the build fails with "Workspace member ... is missing a `pyproject.toml`", a package removed upstream left its directory behind holding ignored files such as `__pycache__`; `git clean -fdX -- libs services tools` clears ignored files under those three directories only (never run it at the root, where `.env` is ignored too).
If `migrate` fails, its log names the problem and nothing has started on the new schema; fix it, or restore the backup and start the previous version again.
`check` must print nothing, and `ingest` and `bot` must be `healthy`.
A poll boundary missed while ingest was stopped is caught up once it starts, if it is the one boundary before the current one; older ones are lost.

The compose file was named `docker-compose.v2.yml` before v1 was removed.
On a VM still running from that name, stop with the old name before pulling, then continue with the new one; the project name `rocketstocks-v2` is unchanged, so the containers and the volume carry over:
```
docker compose -f docker-compose.v2.yml stop ingest bot
```

The migrate gate also drops the `rs_mcp` and `rs_research` database roles that an earlier release created, wherever they exist.
If your `.env` still has `RS_DB_PASSWORD_MCP` and `RS_DB_PASSWORD_RESEARCH`, nothing reads them any more and they can be deleted.

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

## Discord permissions and role pings

The bot needs these permissions, granted by its role in the server (and not denied in the Alert channel):

- View Channels, Send Messages, Embed Links and Read Message History, to post Alerts and to check the channel before a resend.
- Create Public Threads and Send Messages in Threads, for the thread under each Alert and its replies.
- Manage Roles, so `/pings` can give and take the Alert roles on a member's behalf.
  The member needs no permission of their own.

Discord lets a bot manage only roles below its own highest role.
Move the bot's role above every role it manages (Server Settings, Roles, drag the bot's role up).
It cannot manage the `@everyone` role, a role an integration owns, or any role at or above its own.

Alerts ping by Trigger, so people opt in to what pings them:

1. Create a server role for each Trigger you want pinged, for example `popularity-surge`, and turn on "Allow anyone to @mention this role" for it.
   A role that is not mentionable is still shown in the Alert, but Discord does not notify its members.
   The alternative, giving the bot the Mention Everyone permission, lets it notify any role and is not needed.
2. As an administrator, run `/setup roles add`, choose the Trigger and the role.
   The bot refuses a role it cannot manage and says why, so the order problem shows up here and not at the first Alert.
   `/setup roles show` lists each Trigger's role and flags one that was deleted or that the bot can no longer manage, and `/setup roles remove` unmaps a Trigger (members keep the role).
3. Members run `/pings` and choose the Alerts they want; the bot adds and removes the role.
   The menu is private to the member.
   If the bot could not change a role, the menu says which one.

`/setup` is hidden from members who are not administrators (Administrator is the default member permission).
A server administrator can widen or narrow that under Server Settings, Integrations.
`/setup schwab` also checks that the caller is the guild owner or `RS_BOT_ADMIN_USER_ID`.

An Alert mentions only the role mapped to its Trigger, and Discord is told to ping nothing else.
A reply in an Alert's thread never pings.
If the role mapped to an Alert's Trigger was deleted, the Alert posts without the mention and the operator gets one Ops ping per missing role, `Alert #<id> ... posted without its role ping`; run `/setup roles add` again with a new role.
The mapping is stored in the `trigger_role` table of the database, so the backup carries it.

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

The local fallback, when Discord or the bot is down, is a login from the VM.
The manual flow prints the sign-in URL and takes the pasted callback URL at the prompt; it runs in the ingest service, which carries the Schwab credentials and the ingest DSN:
```
$COMPOSE run --rm --no-deps -v "$PWD/scripts:/scripts:ro" ingest python /scripts/acceptance/schwab_manual_login.py
```
On a development machine with a browser, `.venv/bin/python -m rs_ingest.tools.login` opens the sign-in itself; it needs `SCHWAB_API_KEY`, `SCHWAB_API_SECRET`, `RS_EDGAR_USER_AGENT` and a superuser or ingest `RS_DB_URL` in the environment.
Ingest notices the newer token on its next token poll (`RS_INGEST_TOKEN_POLL_SECONDS`).

The Schwab refresh token lives seven days.
The bot DMs you at 24 hours and 6 hours before it expires; re-run `/setup schwab` then.

## Imported v1 Popularity

Popularity from v1 was imported once, from 2026-03-03 onward, and those rows carry `origin = 'v1_import'`.
The importer was v2 code, removed after the import; it lives in the repository history before the commit that removed it.

## Check a live Alert

Done when a real Popularity Alert posts and carries a Track record.

1. Leave the stack running with a valid token, Popularity history and bars loaded.
   Once the daily-bars job has caught up (its log line `surge_bars through ...` with nothing failed), measure the Track record once rather than waiting for the nightly job at 20:00 Eastern:
   ```
   $COMPOSE exec ingest python -m rs_ingest.outcomes
   ```
   The job reads every stored Popularity snapshot, so it takes minutes.
   It rewrites the whole Track record each run, so a rerun is always safe.
2. Find the Alert:
   ```
   psql -c "SELECT id, symbol, rule_version, alert_price, quote_time, triggered_at, delivery, message_id, thread_id, post_attempts FROM alerts WHERE trigger = 'popularity_surge' ORDER BY id DESC LIMIT 5"
   ```
3. Confirm in Discord that the message reads well on a phone and was never edited:
   a one-line talking point naming the mentions multiple and the rank move;
   the reasons (the Trigger and the sub-rules that fired);
   the price with its As-of time, as a timestamp in your own zone;
   the Track record as a small table per horizon (1, 5 and 20 days), with its case count and how many are live, and "too few cases" for a horizon under 30 Outcomes (the shares and the median multiple show "few" when fewer than 30 of them have a move size, and a "no size" row counts those without one);
   and the next Catalyst, an earnings date marked estimated when it is.
   A fact that could not be gathered shows as `unavailable:` with its reason, and the Ops ping names it.
   The footer opens with `alert #<id>` and names the Trigger version.
   Volume does not appear: it moves to the Report.
4. Confirm which Track record it showed:
   ```
   psql -c "SELECT horizon, cases, live_cases, outcomes, unpriced, split_excluded, no_move_size, share_at_2x, ordinary_share, median_multiple, median_excess, computed_at FROM track_record WHERE trigger = 'popularity_surge' ORDER BY rule_version DESC, horizon"
   ```
   There is one row per horizon (1, 5 and 20 sessions).
   `live_cases` counts the cases the live poll stored; the rest are replayed from imported and stored history, which the rule was written having seen.
   The bot says "too few cases" for a horizon with fewer than 30 `outcomes`.
   It shows "few" for the shares and the median multiple when `outcomes - no_move_size` is under 30, and counts `no_move_size` in the "no size" row.
5. To check one Outcome by hand, list a stock's cases and compare with its daily bars:
   ```
   psql -c "SELECT day, horizon, case_at, entry_session, exit_session, stock_return, benchmark_return, excess_return, move_multiple, live FROM outcome WHERE symbol = 'GME' ORDER BY day, horizon"
   ```
   The entry is the open of `entry_session` and the exit the close of `exit_session` in `daily_bar`; SPY is measured over the same sessions.
   A large `unpriced` count means bars are missing (run the daily-bars job and then the Track record job again); a `split_excluded` count means stored bars are not marked split-adjusted and those cases were left out rather than measured.

If no surge fires for days, stage a clearly marked test Alert through the real write path to prove the posting path (it is not a real Alert; its symbol is `TEST-` and the time, because the database allows one Alert per stock per Eastern day):
```
$COMPOSE run --rm --no-deps migrate python -m rs_bot.tools.stage_alert
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
   psql -c "SELECT (SELECT count(*) FROM popularity_snapshot) AS snapshots, (SELECT count(*) FROM candidate) AS candidates, (SELECT count(*) FROM alerts) AS alerts"
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
   Expect: snapshot rows for the interrupted boundary are present exactly once (the key is `polled_at, ticker` with `ON CONFLICT DO NOTHING`), every hit has exactly one `candidate` (its key is the Trigger, rule version, stock and hit time), and a rerun of the same boundary added no Alert.
   A candidate and the Alert it produced commit in one transaction, so a kill leaves both or neither.
   ```
   psql -c "SELECT polled_at, count(*), count(DISTINCT ticker) FROM popularity_snapshot GROUP BY 1 ORDER BY 1 DESC LIMIT 3"
   psql -c "SELECT (SELECT count(*) FROM candidate WHERE verdict = 'posted' AND alert_id IS NULL) AS posted_without_alert, (SELECT count(*) FROM (SELECT symbol, day FROM alerts WHERE reply_to IS NULL GROUP BY 1, 2 HAVING count(*) > 1) d) AS stocks_alerted_twice_a_day"
   ```
   Both counts must be 0 and each boundary's `count` must equal its `count(DISTINCT ticker)`.
5. A graceful stop is a separate, simpler check: `$COMPOSE stop ingest` must exit within `stop_grace_period` (45s) with a clean shutdown log.

### Bot restart after an unrecorded send

Goal: a send the bot made but did not record is found in history after a restart, and nothing is posted twice.

1. Post a marked test Alert and let the bot deliver it:
   ```
   $COMPOSE run --rm --no-deps migrate python -m rs_bot.tools.stage_alert
   psql -c "SELECT id, delivery, message_id, thread_id FROM alerts ORDER BY id DESC LIMIT 1"
   ```
   Expect `posted`, with a message id and a thread id, and one message in Discord with a thread under it.
2. Put the row back as a crash between the send and its record leaves it:
   ```
   $COMPOSE stop bot
   psql -c "UPDATE alerts SET delivery = 'sending', posted_at = NULL, message_id = NULL, thread_id = NULL WHERE id = <id>"
   $COMPOSE start bot
   ```
3. Expect the row back at `posted` with the same `message_id` as before and Discord still showing one message.
   The bot log has `was sent before the last stop and never recorded`, and `posts_reconciled_total` is 1 on the metrics port.
   `posted_at` is the time the message was created in Discord, not the restart time, because the Track record times a case from it.

## Operating procedures

### A failed-to-post Alert

The bot abandons an Alert or reply after `RS_BOT_POST_MAX_ATTEMPTS` (default 5) failed sends.
It sends an Ops ping for it unless Discord itself is already reported failing, because then the `Discord is failing` ping is the one cause and the queued Alerts add none of their own.
Such a row has `delivery` of `pending` or `sending` and `post_attempts >= RS_BOT_POST_MAX_ATTEMPTS`.

Find them:
```
psql -c "SELECT id, symbol, reply_to, delivery, triggered_at, post_attempts FROM alerts WHERE delivery IN ('pending', 'sending') AND post_attempts >= 5 ORDER BY id"
```
(use your configured `RS_BOT_POST_MAX_ATTEMPTS` instead of 5.)

Fix the cause first (channel permission, Discord outage, an oversized field), then re-queue by resetting the counter:
```
psql -c "UPDATE alerts SET post_attempts = 0 WHERE id = <id>"
```
To re-queue every abandoned row at once, drop the `id` filter and use the same `delivery IN ('pending', 'sending') AND post_attempts >= 5` condition.
The bot's next sweep (within `RS_BOT_SWEEP_SECONDS`, 60 by default) picks it up.
A row marked `sending` is looked up in the channel's (or its thread's) history first, so re-queuing never posts a second copy of a message that did get out.

An Alert or reply whose hit time is more than an hour old when the bot picks it up is recorded with `delivery` of `stale` and is not posted.
So a re-queue after the hour has passed records the row as stale instead of posting it, which is the intended result of the stale rule.
Never set `delivery`, `message_id` or `posted_at` by hand unless you posted the message yourself.

### An Alert that may or may not have posted

After a crash between a send and its record, the row stays `sending`, and the bot checks history before sending it again.
If the history cannot be read, the bot posts nothing for that row and sends an Ops ping that names it (or the `Discord is failing` ping, when that is the cause).
The row is checked again every sweep, so the ping is not repeated; fix the cause (a missing Read Message History permission, a Discord outage) and the next sweep settles it.
Find rows waiting like this:
```
psql -c "SELECT id, symbol, reply_to, sending_at FROM alerts WHERE delivery = 'sending' ORDER BY id"
```

### What posts and why

Every hit of the Popularity Trigger is stored as a `candidate` with a verdict, a `raw_score` and a `strength`; only some become Alerts.
The raw score is how far the hit is from the stock's own Baseline: the median and spread of the stock's mentions and rank at every polled boundary of the 14 Eastern days before the hit's day began, so a stock is Unusual against its own normal and never against a cutoff shared by all stocks.
At a polled boundary where the stock was absent from the stored listing, its reading is zero mentions at the bottom rank (one past the last rank stored at that boundary), so a newcomer has a Baseline of "not discussed" and its first surge scores very high.
A boundary with no stored snapshot at all is downtime, not absence, and has no reading.
With fewer than 48 polled boundaries in the window (a fresh install in its first day) no stock has a Baseline: the candidates have no score or strength and are `below_bar`.
Strength is the share of the Trigger's hits over the 30 Eastern days before the hit's day that scored no higher.
That reference is a replay of the current rule over the stored Popularity, not the `candidate` table, so it is whole from the first day (imported history included) and is the same whatever ingest was doing last month.
It is computed once per Eastern day and held in memory, so the first poll after a restart takes up to a few minutes and later polls add one day's replay at most.
If fewer than 7 of the 30 days were polled, or none held a hit, there is nothing to rank against: the poll stores its snapshots, records every hit as a `below_bar` candidate with no strength, posts nothing, and then fails with `ReferenceUnavailable`, which sends an Ops ping for the `popularity_poll` job.

The bar is a strength, set from the same 30 days so that the typical (median) polled day has about 5 stocks at or above it (`BAR_TARGET_PER_DAY`).
A stock counts once a day however often it hits, and days without polls are left out, so downtime does not move the bar.
The verdicts, in the order they are decided:

- `stale`: the hit is more than an hour old when decided (a catch-up after an outage).
  Recorded, never posted.
- `reply`: the stock already has its Alert today and this hit escalates, and the stock has had fewer than 3 replies today.
  Escalation is a sub-rule neither the Alert nor an earlier reply carried, or strength at least `REPLY_STRENGTH_MARGIN` above the highest strength announced so far that day.
  A reply is a row in `alerts` with `reply_to` set, posts in the Alert's thread, never pings and never uses the budget.
- `repeat`: the stock already has its Alert today and this hit does not escalate (or the replies are used up).
- `below_bar`: no Alert yet, and the strength is under the bar.
  The stock can still get its first Alert later in the day.
- `over_budget`: no Alert yet, the strength clears the bar, and the day already has 5 Alerts (`DAILY_BUDGET`).
  The budget never frees within a day, so this is final for the stock until the next Eastern day.
- `posted`: the rest.

A day is the US Eastern calendar day.
Hits found at the same boundary are decided strongest first, so the budget goes to the strongest of them.
The log line `reference for <day>` gives the hits, the polled days and the bar behind a day.
To see why a stock did not post, read its candidates:
```
psql -c "SELECT hit_at, verdict, raw_score, strength FROM candidate WHERE symbol = 'GME' AND day = '2026-10-21' ORDER BY hit_at"
```

### Tune the selector on the stored history

`scripts/calibrate_selection.py` replays the rule, the score, the bar and the verdicts over every stored Popularity snapshot and prints aggregates only: per Eastern day, the candidates, the stocks hit and the verdict counts; the score percentiles; and a summary of Alerts and replies per day for several bar targets and reply margins.
It reads `popularity_snapshot` in a read-only transaction through the ingest container's own `RS_DB_URL`, uses only the standard library and psycopg, and needs no rebuild or restart.
From the host, in the directory with the compose file:
```
$COMPOSE exec -T ingest python - < scripts/calibrate_selection.py > selector-calibration.txt
```
Progress goes to the terminal on stderr and the report to the file.
It takes minutes (the database computes each stock's Baseline for each day), and holds nothing but the hits in memory.
Pass other settings after the dash: `python - --targets 3,4,5 --margins 0.05,0.1`.
The constants it recommends changing live in `services/ingest/src/rs_ingest/selector.py`.

### Daily bars

The `surge_bars` job keeps daily bars for SPY and for every stock the Popularity Trigger ever hit, found by replaying the Trigger over all stored Popularity.
Each stock gets about 270 sessions before its first hit (a year for its typical move, plus margin) and every session since, up to the last settled one.
It runs at startup and each weekday 90 minutes after the close, and a rerun with nothing missing makes no Schwab call.

A stock whose stored history starts after the start it needs is extended back once.
`bar_request` records the earliest start already asked of Schwab per symbol, so a listing younger than the lookback is asked once and not every night.

Schwab's daily history is split-adjusted as of the moment it is fetched, so bars are stored with `adjusted` true.
A split after bars were stored leaves them at the old scale beside new bars at the new scale, and reverse splits are common among Popularity stocks.
So a tail fetch starts 3 sessions before the newest stored bar, on the same call, and compares the closes of the overlap with the stored ones.
If any differs by more than 1 percent, the stock's stored bars are deleted and replaced by a fresh full fetch from its needed start, in one transaction, and the job logs `stored bars no longer match Schwab` with the stock and the first disagreement.
If that fetch fails the stored bars stay as they were and the next run tries again; an empty answer (a delisted or halted stock) never replaces or deletes anything.
A stock already at the last settled session is not checked until the next session adds a bar, so a split can sit unrepaired for up to a day, and in that window the Track record job's split guard does not cover it, because its bars are marked adjusted.
An answer that lacks every overlap session (a halted stock) cannot be compared, so its new bars are stored, the job logs `storing the new bars unverified` with the stored and answered dates, and the run summary counts it in `N unverified`; nothing is refetched for it.
Count repairs in the run summary (`N repaired`):
```
$COMPOSE logs ingest | grep "surge_bars through"
```

The first run on a database that already holds Popularity history, or after upgrading from a version with a shorter lookback, makes one call per Popularity-hit stock at the shared 100 calls a minute (thousands of stocks, under an hour per thousand), and the job's timeout is 3 hours.
Progress logs every 100 symbols (`surge_bars: N of M symbols done`).
If the run stops (timeout, a 429, a restart), the next run resumes where it left off, because each symbol's bars and request are committed together.
Watch it with:
```
$COMPOSE logs -f ingest | grep surge_bars
psql -c "SELECT count(*) AS asked FROM bar_request"
```

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
- `Discord is failing ...`: the bot's sends or history reads failed for a reason other than one Alert (a rejected embed is one Alert).
  Undelivered Alerts stay queued and the sweep retries them; one older than an hour when Discord returns is recorded as stale and not posted.
- `Ingest job <name> is failing (<type>)` and `Ingest job <name> recovered.`: a job failed for a reason that is not a source, such as the database being down or a bug.
  It pings on its first failure, not after several, and once when the job next succeeds.
- `Schwab token is dead ...` and `Schwab token renewed ...`: see "Schwab sign-in" and the token-expiry rehearsal.
  A dead token has these pings only; it does not also send `Schwab is failing`.
- `Alert #<id> ... failed to post after N attempts ...`: the procedure above.
  It is sent only when Discord is not already reported failing.
- `Alert #<id> ... may or may not have posted ...`: the history check above could not be read.
- `Alert #<id> <stock> <trigger> posted with facts unavailable: ...`: the Alert posted, and the ping names which supporting facts were missing and why.
  It is sent once per Alert, when it posts.
- The nightly Track record job failing (`Ingest job track_record is failing ...`); the stored Track record stays as it was until the next successful run.
- `Alert #<id> <stock> <trigger> posted without its role ping: the role mapped to <trigger> ... no longer exists`: see "Discord permissions and role pings".
  It is sent once per missing role, not once per Alert, so a deleted role does not ping for every Alert until it is mapped again.

One cause sends one ping: a source failure that also fails its job does not send a job ping on top.

Health is remembered in process only.
After an ingest or bot restart, a source that is still failing pings once more when the next request fails, and one that recovered while the service was down says nothing.

## What to back up and how to restore

Backups, disk monitoring and host health are the operator's job, done with your own tools.
This section says what is irreplaceable and how to bring it back.

### What is irreplaceable

Everything lives in the one Postgres database `rocketstocks` in the `timescaledb` service.
Two kinds of data cannot be fetched again:

- Popularity snapshots (`popularity_snapshot`): ApeWisdom serves only the current reading, so a missed poll is gone for good.
- User state such as paper trades, settings and roles.
  These are written by the bot and live in the same database.

Daily bars are repaired automatically, and Baselines, Track records and Reports are rebuilt at will.
Back up the whole database anyway; it is the simplest way to be sure neither is missed, and it keeps the history of posted Alerts too.

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
   Popularity snapshots from that window are lost for good.
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
```
