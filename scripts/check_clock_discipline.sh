#!/usr/bin/env bash
# Clock-discipline gate: domain/detection code never reads ambient
# time - it receives an injected Clock or concrete event timestamps.
# Allowlisted: the Clock implementation itself. Tests are exempt (time-machine
# is the sanctioned escape hatch there).
#
# Scope is every v2 member's src/ (libs/*, services/*, tools/*), globbed so a
# new member is covered the day it appears. A root with no member src dirs is
# an ERROR, not a pass - the gate must never go silently dead.
#
# Banned: wall-clock and monotonic reads (datetime.now/utcnow/today,
# date.today, time.time, time.monotonic, `from time import time|monotonic`) and
# aliasing datetime/date/time (`import datetime as dt`), which would hide the
# calls above from this grep. time.perf_counter (duration measurement) is fine.
#
# CLOCK_GATE_ROOT overrides the repo root (the gate's own test uses it).
set -euo pipefail
cd "${CLOCK_GATE_ROOT:-$(dirname "$0")/..}"

shopt -s nullglob
SCOPES=()
for root in libs services tools; do
  members=("$root"/*/src)
  if (( ${#members[@]} == 0 )); then
    echo "clock-discipline gate: no member src dirs under $root/ (moved? fix the gate)" >&2
    exit 2
  fi
  SCOPES+=("${members[@]}")
done

ALLOWLIST_RE='rs_domain/clock\.py'
NOT_ATTR='(^|[^[:alnum:]_.])'
PATTERN="datetime\.now\(|datetime\.utcnow\(|datetime\.today\(|date\.today\("
PATTERN+="|${NOT_ATTR}time\.(time|monotonic)(_ns)?\("
PATTERN+="|from[[:space:]]+time[[:space:]]+import[[:space:]].*(time|monotonic)"
PATTERN+="|(^|[[:space:],(])(datetime|date|time|monotonic)[[:space:]]+as[[:space:]]"

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
  echo "clock-discipline violations (inject Clock instead):" >&2
  echo "$violations" >&2
  exit 1
fi
echo "clock discipline OK (${SCOPES[*]})"
