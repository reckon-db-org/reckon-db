#!/usr/bin/env bash
# Common Test as the gate runs it: `rebar3 ct', failing on a suite that did
# not load as well as on a failed case (reckon-db#20).
#
# A suite ct cannot load (a stale copy left in _build whose source is gone, a
# module missing all/0) is reported as `error_in_suite: FAILED', is left out
# of "All N tests passed", and `rebar3 ct' exits 0: ct_framework counts that
# case as failed only when it was force-failed, and the missing-module path
# never is. A CT hook sees the case but cannot change the count, so this
# reads ct's own report instead.
#
#   - rebar3 ct failed           -> its exit status
#   - a suite did not load       -> exit 1, and ct's lines about it are printed
#   - neither                    -> exit 0
#
# Usage:
#   scripts/check_ct.sh [ARGS]       run `rebar3 ct ARGS' and check its output
#   scripts/check_ct.sh --log F      check an existing log (exit 2 if F is unreadable)
set -uo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR" || exit 1

STATUS=0
if [ "${1:-}" = "--log" ]; then
  LOG="${2:-}"
  [ -r "$LOG" ] || { echo "check_ct: cannot read log '$LOG'" >&2; exit 2; }
else
  LOG="$(mktemp)"; trap 'rm -f "$LOG"' EXIT
  rebar3 ct "$@" 2>&1 | tee "$LOG"
  STATUS=${PIPESTATUS[0]}
fi

UNLOADED="$(sed -e 's/\x1b\[[0-9;]*m//g' "$LOG" | grep -E '==> error_in_suite|==> .*(cannot be compiled or loaded|all/0 is missing)')"
if [ -n "$UNLOADED" ]; then
  echo "check_ct: a suite did not load, so its cases never ran:" >&2
  printf '%s\n' "$UNLOADED" >&2
  exit 1
fi
exit "$STATUS"
