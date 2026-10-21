#!/usr/bin/env bash
# Dialyzer as a RATCHET rather than a pass/fail gate. (Lint has no ratchet:
# the structural floor is a plain `rebar3 lint' gate, macula-codebase-checks#9.)
#
# This repository has known dialyzer findings that predate its CI. Failing the
# job on all of them would make main red on every push, which gives no green
# signal at all and trains everyone to ignore the result. Suppressing them
# would lose the debt. So: the known findings are recorded in
# `.dialyzer_baseline`, this script fails only when a finding appears that is
# NOT in that file, and a finding that disappears is reported so its line can
# be struck from the baseline.
#
#   - a NEW finding            -> exit 1, and the finding is printed
#   - a finding FIXED          -> exit 0, with a note to strike it
#   - unchanged                -> exit 0
#
# Usage:
#   scripts/check_ratchet.sh                     run dialyzer, check .dialyzer_baseline
#   scripts/check_ratchet.sh --update            rewrite the baseline from this run
#   scripts/check_ratchet.sh --log F             check an existing log
#
# A baseline entry is `<file>\t<sha1 of the warning text>\t<the first 110
# A finding is recorded once per occurrence, so an identical finding twice in
# one file is two lines. That is deliberate: it means the baseline caps HOW
# MANY such findings a file may have, and a third one fails even though its
# text is already known.
#
# characters>`. The position is deliberately NOT part of the key: a line number
# changes whenever anything above it is edited, and a baseline that churns on
# unrelated edits is one people delete. The text carries the function and the
# types, which is what identifies the finding.
set -uo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR" || exit 1
MODE=check
LOG=""
TOOL=dialyzer
BASELINE=".dialyzer_baseline"
RUN=(rebar3 dialyzer)
while [ $# -gt 0 ]; do
  case "$1" in
    --update) MODE=update; shift ;;
    --log) LOG="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
if [ -z "$LOG" ]; then
  LOG="$(mktemp)"; trap 'rm -f "$LOG"' EXIT
  "${RUN[@]}" > "$LOG" 2>&1
fi

# dialyzer: a bare file line, then "Line N Column M: text".
# The key does not include the position: a line moves whenever anything above
# it is edited, and a baseline that churns on unrelated edits is one people delete.
normalise() {
  sed -e 's/\x1b\[[0-9;]*m//g' "$LOG" | awk '
    /^[A-Za-z0-9_\/.-]+\.(erl|hrl)$/ { file = $0; next }
    /^Line [0-9]+ Column [0-9]+: / {
      text = $0; sub(/^Line [0-9]+ Column [0-9]+: /, "", text)
      if (file != "") print file "\t" text
      next
    }' | while IFS=$'\t' read -r f t; do
      printf '%s\t%s\t%s\n' "$f" "$(printf '%s' "$t" | sha1sum | cut -c1-12)" "$(printf '%.110s' "$t")"
    done | sort
}

# The same findings with their whole text, for reading: a baseline line shows
# 110 characters, which is not enough to tell two similiar findings apart or to
# see what a failing run is asking you to accept.
normalise_full() {
  sed -e 's/\x1b\[[0-9;]*m//g' "$LOG" | awk '
    /^[A-Za-z0-9_\/.-]+\.(erl|hrl)$/ { file = $0; next }
    /^Line [0-9]+ Column [0-9]+: / {
      text = $0; sub(/^Line [0-9]+ Column [0-9]+: /, "", text)
      if (file != "") print file "\t" text
      next
    }' | while IFS=$'\t' read -r f t; do
      printf '%s\t%s\t%s\n' "$f" "$(printf '%s' "$t" | sha1sum | cut -c1-12)" "$t"
    done | sort
}

FOUND="$(normalise)"
FULL="$(normalise_full)"
FOUND_N="$(printf '%s\n' "$FOUND" | grep -c . || true)"
REPORTED_N="$(sed -e 's/\x1b\[[0-9;]*m//g' "$LOG" | sed -n 's/.*Warnings occurred running dialyzer: \([0-9]*\).*/\1/p' | tail -1)"
REPORTED_N="${REPORTED_N:-0}"

if [ "$MODE" = update ]; then
  { echo "# Known dialyzer findings, recorded as a ratchet. See scripts/dialyzer_ratchet.sh."
    echo "# $TOOL reported $REPORTED_N warnings when this was written."
    echo "# A new finding fails CI. A fixed finding should have its line deleted here."
    printf '%s\n' "$FOUND"
  } > "$BASELINE"
  echo "wrote $BASELINE with $FOUND_N entries ($TOOL reported $REPORTED_N)"
  exit 0
fi

[ -f "$BASELINE" ] || { echo "no $BASELINE; create it with: scripts/check_ratchet.sh --update"; exit 1; }
KNOWN="$(grep -v '^#' "$BASELINE" | grep . | sort)"

NEW="$(comm -23 <(printf '%s\n' "$FOUND") <(printf '%s\n' "$KNOWN"))"
GONE="$(comm -13 <(printf '%s\n' "$FOUND") <(printf '%s\n' "$KNOWN"))"
NEW_N="$(printf '%s\n' "$NEW" | grep -c . || true)"
GONE_N="$(printf '%s\n' "$GONE" | grep -c . || true)"

echo "$TOOL: $REPORTED_N reported, $FOUND_N parsed, $(printf '%s\n' "$KNOWN" | grep -c . || true) in the baseline"

if [ "$GONE_N" -gt 0 ]; then
  echo
  echo "$GONE_N finding(s) in the baseline are FIXED. Strike these lines from $BASELINE:"
  printf '%s\n' "$GONE" | sed 's/^/  - /'
fi

if [ "$NEW_N" -gt 0 ]; then
  echo
  echo "::error::$NEW_N $TOOL finding(s) not in the baseline:"
  printf '%s\n' "$NEW" | while IFS=$'\t' read -r f h _; do
    printf '  + %s\t%s\n' "$f" "$h"
    printf '%s\n' "$FULL" | awk -F'\t' -v f="$f" -v h="$h" '$1 == f && $2 == h { print "      " $3; exit }'
  done
  echo
  echo "Fix them, or if they are genuinely acceptable, add them with --update and say why in the commit."
  exit 1
fi

echo "OK: no $TOOL finding outside the baseline."
exit 0
