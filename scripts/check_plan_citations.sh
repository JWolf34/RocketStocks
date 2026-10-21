#!/usr/bin/env bash
# Plan-citation gate: v2 source and tests never cite a superseded plan or use
# retired domain words. A comment keeps the reason a rule exists, never the
# number of the document, slice or session that once decided it.
#
# Scope is every v2 member's src/ and tests/ (libs/*, services/*, tools/*),
# globbed so a new member is covered the day it appears. A root with no member
# src dirs is an ERROR, not a pass - the gate must never go silently dead.
#
# Banned everywhere in scope:
#   ADR-NNNN                  decision-record citations
#   grilling #                grilling-session citations
#   slice N / slice-N / slice_N   slice labels (case-insensitive S); the
#                             English "slice of a list" does not trip it
#   S0NN                      numbered slice labels such as S004 (whole token)
# Banned except in the allowlist below (case-insensitive):
#   attention                 retired word for Popularity
#   base_rate / base rate / BaseRate   retired word for Track record
#   alert_type / alert type   retired word for Trigger
#
# Allowlist (explicit and minimal; each entry is history that must keep an old
# name, and applies only to the retired-word patterns):
#   Alembic revision files - they create and rename the pre-rename schema
#   test_migrations_0006_onward.py - seeds pre-rename rows to prove the rename
#
# PLAN_CITATIONS_GATE_ROOT overrides the repo root (the gate's own test uses it).
set -euo pipefail
cd "${PLAN_CITATIONS_GATE_ROOT:-$(dirname "$0")/..}"

shopt -s nullglob
SCOPES=()
for root in libs services tools; do
  members=("$root"/*/src)
  if (( ${#members[@]} == 0 )); then
    echo "plan-citations gate: no member src dirs under $root/ (moved? fix the gate)" >&2
    exit 2
  fi
  SCOPES+=("${members[@]}" "$root"/*/tests)
done

CITATION_RE='ADR-[0-9]{4}|grilling #|[Ss]lice[ _-]?[0-9]|(^|[^[:alnum:]_])S0[0-9]{2}($|[^[:alnum:]_])'
NAME_RE='attention|base[_ ]?rate|alert[_ ]type'
ALLOW_NAME_RE='^libs/persistence/src/rs_persistence/migrations/versions/|^libs/persistence/tests/test_migrations_0006_onward\.py:'

scan() {  # scan <pattern> [grep flags]; prints matches, exits 2 on a grep failure
  local pattern=$1 out status
  shift
  set +e
  out=$(grep -rnIE "$@" --exclude-dir=__pycache__ --exclude='*.pyc' -e "$pattern" "${SCOPES[@]}")
  status=$?
  set -e
  if (( status > 1 )); then
    echo "plan-citations gate: grep failed with status $status" >&2
    exit 2
  fi
  printf '%s' "$out"
}

citations=$(scan "$CITATION_RE")
name_hits=$(scan "$NAME_RE" -i)
names=$(printf '%s' "$name_hits" | grep -vE "$ALLOW_NAME_RE" || true)

violations=$(printf '%s\n%s' "$citations" "$names" | grep -v '^$' || true)

if [[ -n "$violations" ]]; then
  echo "plan-citation violations (state the reason in words, use the glossary words):" >&2
  echo "$violations" >&2
  exit 1
fi
echo "plan citations OK (${SCOPES[*]})"
