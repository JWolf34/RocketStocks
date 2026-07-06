#!/usr/bin/env bash
# Clock-discipline gate (ADR-0023): domain/detection code never reads ambient
# time — it receives an injected Clock or concrete event timestamps.
# Allowlisted: the Clock implementation itself. Tests are exempt (time-machine
# is the sanctioned escape hatch there).
#
# Scope grows with the codebase: add detection-bearing directories here as
# they appear (e.g. services/ingest src when S003 lands).
set -euo pipefail
cd "$(dirname "$0")/.."

SCOPES=(
  "libs/domain/src"
)
ALLOWLIST_RE='rs_domain/clock\.py'
PATTERN='datetime\.now\(|datetime\.utcnow\(|time\.time\('

violations=$(grep -rnE "$PATTERN" "${SCOPES[@]}" --include='*.py' | grep -vE "$ALLOWLIST_RE" || true)

if [[ -n "$violations" ]]; then
  echo "clock-discipline violations (inject Clock instead — ADR-0023):" >&2
  echo "$violations" >&2
  exit 1
fi
echo "clock discipline OK (${SCOPES[*]})"
