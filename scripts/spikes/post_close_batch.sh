#!/bin/bash
# Post-close batch runner (throwaway). Start any time during the session in its own
# terminal:
#
#   caffeinate -is scripts/spikes/post_close_batch.sh
#
# Sleeps until 20:05 UTC (15:05 CT, after the collectors' 15:02 self-stop), then runs
# the three post-close analyses in order. Everything is logged to
# data/spike-recordings/post-close-batch-<date>.log.
set -u
cd "$(dirname "$0")/../.."
PY=.venv/bin/python

target=$(date -u -j -f "%Y-%m-%d %H:%M:%S" "$(date -u +%Y-%m-%d) 20:05:00" +%s)
now=$(date +%s)
if [ "$now" -lt "$target" ]; then
  echo "sleeping $((target - now))s until 20:05 UTC (15:05 CT)…"
  sleep $((target - now))
fi

log="data/spike-recordings/post-close-batch-$(date +%Y-%m-%d).log"
{
  echo "=== stream_compare ($(date -u +%H:%M:%SZ)) ==="
  $PY scripts/spikes/stream_compare.py
  echo "=== chain_diff ($(date -u +%H:%M:%SZ)) ==="
  $PY scripts/spikes/chain_diff.py
  echo "=== burst_probe ($(date -u +%H:%M:%SZ)) ==="
  $PY scripts/spikes/burst_probe.py
  echo "=== batch complete ($(date -u +%H:%M:%SZ)) ==="
} 2>&1 | tee "$log"
echo "outputs: docs/v2/specs/fixtures/ (two md files) + data/spike-recordings/ (burst probe log)"
echo "full log: $log"
