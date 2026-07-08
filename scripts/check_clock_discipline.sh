#!/usr/bin/env bash
# Clock-discipline gate (ADR-0023): domain/detection code never reads ambient
# time — it receives an injected Clock or concrete event timestamps.
# Allowlisted: the Clock implementation itself. Tests are exempt (time-machine
# is the sanctioned escape hatch there).
#
# Scope grows with the codebase: add detection-bearing directories here as
# they appear (e.g. services/ingest src when S003 lands). A missing scope
# directory is an ERROR, not a pass — the gate must never go silently dead.
set -euo pipefail
cd "$(dirname "$0")/.."

SCOPES=(
  "libs/domain/src"
  "services/ingest/src"
)
ALLOWLIST_RE='rs_domain/clock\.py'
PATTERN='datetime\.now\(|datetime\.utcnow\(|time\.time\('

for scope in "${SCOPES[@]}"; do
  if [[ ! -d "$scope" ]]; then
    echo "clock-discipline gate: scope directory missing: $scope (renamed? fix SCOPES)" >&2
    exit 2
  fi
done

set +e
matches=$(grep -rnE "$PATTERN" "${SCOPES[@]}" --include='*.py')
grep_status=$?
set -e
if (( grep_status > 1 )); then
  echo "clock-discipline gate: grep failed with status $grep_status" >&2
  exit 2
fi

violations=$(printf '%s' "$matches" | grep -vE "$ALLOWLIST_RE" || true)

if [[ -n "$violations" ]]; then
  echo "clock-discipline violations (inject Clock instead — ADR-0023):" >&2
  echo "$violations" >&2
  exit 1
fi
echo "clock discipline OK (${SCOPES[*]})"
