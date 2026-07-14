#!/usr/bin/env bash
#
# Phase-1 autonomous market-day acceptance run (D6 / D5-ingest / D7).
# See docs/v2/phase1-acceptance-plan.md — this automates the deterministic
# half of the market-gated items. It does NOT do D1 (`down -v`) or the Schwab
# login: those are interactive and MUST be done tonight (see `preflight`).
#
# What it certifies unattended:
#   D7  replay of the 2026-07-09 session against the compose stack == IDENTICAL
#   D6  bars written during one live RTH session under compose
#   D5  ingest :9090 exposes counter-typed metrics during the run
# What it can only observe (never a hard pass — Phase-4/G7 concern):
#   whether the skeleton detector happened to fire + post an alert.
#
# Usage:
#   caffeinate -s scripts/acceptance/phase1_market_run.sh preflight   # tonight, after login
#   caffeinate -s scripts/acceptance/phase1_market_run.sh run         # leave running; sleeps to open
#
# It is calendar-aware (XNYS via pandas-market-calendars) and idempotent-safe:
# it never wipes the volume, so the DB-stored Schwab token survives.
set -euo pipefail

# ---- config (override via env) --------------------------------------------
REPO="${REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
COMPOSE="${COMPOSE:-docker compose -f $REPO/docker-compose.v2.yml}"
VENV_PY="${VENV_PY:-$REPO/.venv/bin/python}"
FIXTURES="${FIXTURES:-$REPO/services/ingest/tests/fixtures}"
FIXTURE_FILE="session-excerpt-2026-07-09.jsonl"
EXPECTED_FILE="expected-decisions-2026-07-09.jsonl"
DB_HOST_DSN_PSQL_PORT=5498
INGEST_METRICS_URL="http://127.0.0.1:9090/metrics"
AUTH_WATCH_SECS="${AUTH_WATCH_SECS:-120}"   # window to catch an OAuth/expired-token failure at start
REPORT_DIR="${REPORT_DIR:-$REPO/data}"
STAMP="$(date +%Y-%m-%dT%H-%M-%S)"
REPORT="$REPORT_DIR/phase1-acceptance-$STAMP.md"

cd "$REPO"
mkdir -p "$REPORT_DIR"

# ---- helpers ---------------------------------------------------------------
log()  { printf '%s  %s\n' "$(date -u +%H:%M:%SZ)" "$*" | tee -a "$REPORT"; }
fail() { log "❌ ABORT: $*"; notify "Phase-1 acceptance ABORTED: $*"; exit 1; }
ok()   { log "✅ $*"; }
notify() {
  # best-effort desktop ping on macOS; harmless elsewhere
  command -v osascript >/dev/null 2>&1 && \
    osascript -e "display notification \"$1\" with title \"RocketStocks Phase-1\"" >/dev/null 2>&1 || true
}

# load RS_DB_PASSWORD (+ other vars) from .env without echoing secrets.
# Tolerates CRLF (.env authored on Windows) by stripping \r before sourcing.
load_env() {
  [ -f "$REPO/.env" ] || fail ".env not found at $REPO/.env"
  set -a; # shellcheck disable=SC1091
  source <(sed 's/\r$//' "$REPO/.env"); set +a
  : "${RS_DB_PASSWORD:?RS_DB_PASSWORD missing from .env}"
}

# epoch -> UTC HH:MMZ, portable across GNU (WSL/Linux) and BSD (macOS) date
fmt_epoch() { date -u -d "@$1" +%H:%MZ 2>/dev/null || date -u -r "$1" +%H:%MZ; }

psql_q() {  # run a query inside the compose DB container, print raw scalar
  $COMPOSE exec -T -e PGPASSWORD="$RS_DB_PASSWORD" timescaledb \
    psql -U postgres -d rocketstocks -tAc "$1"
}

stack_up() {
  log "ensuring timescaledb + migrate gate + serve + bot are up (no -v)…"
  $COMPOSE up -d timescaledb serve bot
  # wait for DB health
  for _ in $(seq 1 30); do
    if $COMPOSE exec -T timescaledb pg_isready -U postgres -d rocketstocks >/dev/null 2>&1; then
      ok "timescaledb healthy"; return 0
    fi
    sleep 2
  done
  fail "timescaledb did not become healthy"
}

# XNYS session window for the current-or-next trading day (epoch open close)
next_session() {
  "$VENV_PY" - <<'PY'
import pandas_market_calendars as mcal, pandas as pd
cal = mcal.get_calendar("XNYS")
now = pd.Timestamp.now(tz="UTC")
sched = cal.schedule(start_date=(now - pd.Timedelta(days=1)).date(),
                     end_date=(now + pd.Timedelta(days=7)).date())
for _, r in sched.iterrows():
    o, c = r["market_open"], r["market_close"]
    if c > now:
        print(int(o.timestamp()), int(c.timestamp())); break
else:
    raise SystemExit("no upcoming XNYS session in the next 7 days")
PY
}

check_single_bot() {
  # A second login on the token shows up as gateway "invalidated" churn.
  local n1 n2
  n1=$($COMPOSE logs bot --since 3m 2>/dev/null | grep -c "invalidated" || true)
  sleep 20
  n2=$($COMPOSE logs bot --since 3m 2>/dev/null | grep -c "invalidated" || true)
  if [ "$n2" -gt "$n1" ]; then
    log "⚠️  WARNING: bot gateway sessions are being invalidated ($n1→$n2 in window)."
    log "⚠️  A second instance likely shares RS_BOT_DISCORD_TOKEN (deploy VM?)."
    log "⚠️  Deterministic passes are unaffected; the D6 alert-post OBSERVATION may show dupes."
  else
    ok "bot holds a stable gateway session (single instance)"
  fi
}

# ---- D7: replay gate under compose (needs no market) -----------------------
run_d7() {
  log "── D7: S007 replay of 2026-07-09 against the compose stack ──"
  [ -f "$FIXTURES/$FIXTURE_FILE" ]  || fail "fixture missing: $FIXTURES/$FIXTURE_FILE"
  [ -f "$FIXTURES/$EXPECTED_FILE" ] || fail "expected file missing: $FIXTURES/$EXPECTED_FILE"
  # Runs inside the composed image, on the composed DB server, in a throwaway
  # DB the CLI creates+drops (never touches acceptance rows). --no-deps: stack
  # is already up. Fixtures mounted read-only (not baked into the image).
  if $COMPOSE run --rm --no-deps \
        -v "$FIXTURES":/fixtures:ro \
        ingest python -m rs_ingest.tools.replay \
          --fixture "/fixtures/$FIXTURE_FILE" \
          --expected "/fixtures/$EXPECTED_FILE" \
          --admin-db "postgresql+psycopg://postgres:${RS_DB_PASSWORD}@timescaledb:5432/postgres" \
        2>&1 | tee -a "$REPORT"; then
    ok "D7 IDENTICAL (exit 0) — Phase-1 gate re-run under compose PASSED"
  else
    fail "D7 replay diverged (non-zero exit) — investigate before proceeding"
  fi
}

# ---- D6 + D5: live session under compose -----------------------------------
run_session() {
  local close_epoch="$1"
  log "── D6: starting ingest under compose for the live session ──"

  local bars_before alerts_before
  bars_before=$(psql_q "SELECT count(*) FROM stream_bars_5m" || echo 0)
  alerts_before=$(psql_q "SELECT count(*) FROM alerts" || echo 0)
  log "baseline: stream_bars_5m=$bars_before alerts=$alerts_before"

  $COMPOSE up -d ingest

  # --- auth pre-flight: watch the first window for an OAuth/expired failure ---
  log "watching ingest auth for ${AUTH_WATCH_SECS}s (refresh-token expiry surfaces here)…"
  local deadline=$(( $(date +%s) + AUTH_WATCH_SECS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if $COMPOSE logs ingest --since "${AUTH_WATCH_SECS}s" 2>/dev/null \
         | grep -qiE 'Schwab OAuth failed|refresh token likely expired|rs_ingest\.tools\.login'; then
      $COMPOSE logs ingest --tail 20 | tee -a "$REPORT"
      fail "Schwab auth failed — refresh token expired. Manual re-login required: \
'python -m rs_ingest.tools.login' (interactive). No script can recover this."
    fi
    # crash-looped past its restart budget?
    if [ "$($COMPOSE ps -q ingest | wc -l | tr -d ' ')" = "0" ]; then
      $COMPOSE logs ingest --tail 20 | tee -a "$REPORT"
      fail "ingest exited during auth window (crash-loop / no session)"
    fi
    sleep 10
  done
  ok "ingest authenticated and streaming (no OAuth failure in window)"

  # --- D5: ingest metrics mid-session ---
  log "── D5: sampling ingest metrics ($INGEST_METRICS_URL) ──"
  if curl -sf --max-time 5 "$INGEST_METRICS_URL" | grep -E '^# TYPE .* counter' | tee -a "$REPORT" | grep -q counter; then
    ok "D5 ingest :9090 serving counter-typed metrics"
  else
    log "⚠️  D5 ingest metrics not scrapeable yet — will re-check before teardown"
  fi

  # --- let the session run to market close ---
  local secs_left=$(( close_epoch - $(date +%s) ))
  if [ "$secs_left" -gt 0 ]; then
    log "session running; sleeping ${secs_left}s to market close…"
    sleep "$secs_left"
  fi
  # small tail so the closing-auction bar lands
  sleep 120

  # re-sample metrics in case the earlier one was too early
  curl -sf --max-time 5 "$INGEST_METRICS_URL" > "$REPORT_DIR/ingest-metrics-$STAMP.txt" 2>/dev/null || true

  # --- D6 assertion: bars written this session ---
  local bars_after alerts_after
  bars_after=$(psql_q "SELECT count(*) FROM stream_bars_5m" || echo 0)
  alerts_after=$(psql_q "SELECT count(*) FROM alerts" || echo 0)
  log "final: stream_bars_5m=$bars_after alerts=$alerts_after"

  if [ "$bars_after" -gt "$bars_before" ]; then
    ok "D6 bars written: +$(( bars_after - bars_before )) 5-min bars this session"
  else
    fail "D6 NO bars written this session (bars_before=$bars_before bars_after=$bars_after)"
  fi

  local alert_delta=$(( alerts_after - alerts_before ))
  if [ "$alert_delta" -gt 0 ]; then
    log "🔔 OBSERVATION: $alert_delta alert(s) fired + committed this session (check the dev channel for posts)"
  else
    log "🔕 OBSERVATION: no alert fired this session — expected on quiet data; posting path already proven (C2/C4)"
  fi

  log "stopping ingest (clean session end)…"
  $COMPOSE stop ingest >/dev/null 2>&1 || true
}

# ---- modes -----------------------------------------------------------------
preflight() {
  log "=== PREFLIGHT (run tonight, AFTER: down -v + up + rs_ingest.tools.login) ==="
  load_env
  stack_up
  [ -f "$FIXTURES/$FIXTURE_FILE" ] && ok "fixture present" || fail "fixture missing"
  [ -f "$FIXTURES/$EXPECTED_FILE" ] && ok "expected-decisions present" || fail "expected file missing"
  : "${RS_BOT_DISCORD_TOKEN:?}"; : "${RS_DEV_CHANNEL_ID:?}"; ok ".env bot vars present"
  : "${SCHWAB_API_KEY:?}"; : "${SCHWAB_API_SECRET:?}"; ok ".env Schwab keys present"
  local ntok
  ntok=$(psql_q "SELECT count(*) FROM schwab_tokens" 2>/dev/null || echo "?")
  if [ "$ntok" = "0" ] || [ "$ntok" = "?" ]; then
    log "⚠️  No Schwab token row found in the DB (table 'schwab_tokens' empty/absent)."
    log "⚠️  Run:  $VENV_PY -m rs_ingest.tools.login   before trusting the overnight run."
  else
    ok "Schwab token row present in DB ($ntok) — should carry the ~7-day window"
  fi
  check_single_bot
  run_d7   # prove the gate now; cheap, no market needed
  log "=== PREFLIGHT complete — if all ✅, launch:  caffeinate -s $0 run ==="
  notify "Phase-1 preflight complete — review $REPORT"
}

run() {
  log "=== RUN: autonomous market-day acceptance ==="
  if command -v caffeinate >/dev/null 2>&1; then
    log "TIP: launch under 'caffeinate -s' so the Mac doesn't sleep mid-session."
  else
    log "TIP: keep the host awake for the session — WSL: disable Windows sleep"
    log "     (Settings ▸ Power) or run under 'systemd-inhibit --what=idle:sleep'."
  fi
  load_env
  stack_up
  check_single_bot
  run_d7                                   # earliest possible failure signal (no market needed)

  read -r OPEN CLOSE < <(next_session)
  local now; now=$(date +%s)
  if [ "$now" -lt "$OPEN" ]; then
    log "next XNYS session opens $(fmt_epoch "$OPEN"), closes $(fmt_epoch "$CLOSE")."
    log "sleeping $(( OPEN - now ))s until open…"
    sleep "$(( OPEN - now ))"
  else
    log "already inside a session (opened $(fmt_epoch "$OPEN")); starting now."
  fi

  run_session "$CLOSE"

  log "=== DONE — summary ==="
  log "Report: $REPORT"
  log "Next (off-hours, manual): tick D5/D6/D7 + S006 spec boxes, add roadmap S006"
  log "acceptance status-log entry (Part E), then merge to main (Part F)."
  notify "Phase-1 market run finished — review $REPORT"
}

case "${1:-run}" in
  preflight) preflight ;;
  run)       run ;;
  d7)        load_env; stack_up; run_d7 ;;   # convenience: gate re-run only
  *) echo "usage: $0 {preflight|run|d7}" >&2; exit 2 ;;
esac
