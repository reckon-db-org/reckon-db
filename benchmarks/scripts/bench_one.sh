#!/usr/bin/env bash
#
# Shared single-slice runner template. Consumer repos copy this into
# their `benchmarks/scripts/` directory.
#
# Calls into the shared harness facade `reckon_bench_harness:run_slice/4'.
#
# Usage:
#     ./scripts/bench_one.sh --slice <name> --scenario <name> --profile <profile> --out <path>

set -euo pipefail

SLICE=""
SCENARIO=""
PROFILE="local-dev"
OUT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --slice)    SLICE="$2";    shift 2 ;;
        --scenario) SCENARIO="$2"; shift 2 ;;
        --profile)  PROFILE="$2";  shift 2 ;;
        --out)      OUT="$2";      shift 2 ;;
        *)
            printf 'unknown argument: %s\n' "$1" >&2
            exit 2 ;;
    esac
done

if [[ -z "$SLICE" || -z "$SCENARIO" || -z "$OUT" ]]; then
    printf 'required: --slice --scenario --out\n' >&2
    exit 2
fi

BENCH_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BENCH_DIR"

SCENARIO_FILE="slices/${SLICE}/scenarios/${SCENARIO}.eterm"
if [[ ! -f "$SCENARIO_FILE" ]]; then
    printf 'no such scenario: %s\n' "$SCENARIO_FILE" >&2
    exit 3
fi

# Ensure the bench profile is compiled before invoking the escript.
rebar3 as bench compile >/dev/null

./scripts/run_slice.escript "$SLICE" "$SCENARIO_FILE" "$OUT" "$PROFILE"
