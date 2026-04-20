#!/usr/bin/env bash
#
# Shared orchestrator template. Consumer repos (reckon-db/benchmarks,
# reckon-gater/benchmarks, evoq/benchmarks, reckon-evoq/benchmarks,
# reckon-ecosystem/benchmarks) copy this into their `benchmarks/
# scripts/` directory.
#
# Usage (from a consumer repo's benchmarks/ directory):
#     ./scripts/bench.sh --slice <name> --scenario <name> --profile <profile>
#     ./scripts/bench.sh --all --profile <profile>
#
# Either --slice or --all must be specified.

set -euo pipefail

SLICE=""
SCENARIO="baseline"
PROFILE="local-dev"
RUN_ALL=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --slice)    SLICE="$2";    shift 2 ;;
        --scenario) SCENARIO="$2"; shift 2 ;;
        --profile)  PROFILE="$2";  shift 2 ;;
        --all)      RUN_ALL=1;     shift   ;;
        -h|--help)
            sed -n '2,13p' "$0"
            exit 0 ;;
        *)
            printf 'unknown argument: %s\n' "$1" >&2
            exit 2 ;;
    esac
done

BENCH_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BENCH_DIR"

if [[ "$PROFILE" != "local-dev" ]]; then
    VERIFY="${BENCH_DIR}/hardware_profiles/${PROFILE}/verify.sh"
    if [[ ! -x "$VERIFY" ]]; then
        printf 'profile %s has no executable verify.sh at %s\n' "$PROFILE" "$VERIFY" >&2
        exit 3
    fi
    "$VERIFY"
fi

RUN_ID="$(date -u +%Y-%m-%dT%H:%M:%SZ)_$(git rev-parse --short HEAD 2>/dev/null || echo nogit)_otp$(erl -noshell -eval 'io:format("~s",[erlang:system_info(otp_release)]),halt().')_${PROFILE}"
RESULTS_DIR="results/${RUN_ID}"
mkdir -p "$RESULTS_DIR"

printf 'run-id: %s\n' "$RUN_ID"
printf 'results: %s\n' "$RESULTS_DIR"

capture_system_info() {
    printf '{\n'
    printf '  "run_id": "%s",\n' "$RUN_ID"
    printf '  "profile": "%s",\n' "$PROFILE"
    printf '  "started_utc": "%s",\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '  "git_commit": "%s",\n' "$(git rev-parse HEAD 2>/dev/null || echo nogit)"
    printf '  "otp_release": "%s",\n' "$(erl -noshell -eval 'io:format("~s",[erlang:system_info(otp_release)]),halt().')"
    printf '  "kernel": "%s",\n' "$(uname -r)"
    printf '  "cpu_model": "%s"\n' "$(awk -F: '/model name/{print $2; exit}' /proc/cpuinfo | sed 's/^ *//')"
    printf '}\n'
}
capture_system_info > "${RESULTS_DIR}/system_info.json"

run_one() {
    local slice="$1"
    local scenario="$2"
    ./scripts/wipe_data.sh
    ./scripts/bench_one.sh \
        --slice    "$slice" \
        --scenario "$scenario" \
        --profile  "$PROFILE" \
        --out      "${RESULTS_DIR}/${slice}__${scenario}.json"
}

if [[ "$RUN_ALL" == 1 ]]; then
    for slice_dir in slices/*/; do
        slice_name="$(basename "$slice_dir")"
        [[ "$slice_name" == "README.md" ]] && continue
        [[ "$slice_name" == "retired" ]] && continue
        for scenario_file in "$slice_dir"scenarios/*.eterm; do
            [[ -f "$scenario_file" ]] || continue
            scenario_name="$(basename "$scenario_file" .eterm)"
            run_one "$slice_name" "$scenario_name"
        done
    done
elif [[ -n "$SLICE" ]]; then
    run_one "$SLICE" "$SCENARIO"
else
    printf 'either --slice <name> or --all must be specified\n' >&2
    exit 2
fi

printf 'done. summarising...\n'
./scripts/summarise_run.sh "$RESULTS_DIR" > "${RESULTS_DIR}/summary.md" || \
    printf '(no summariser yet — skip)\n'
printf 'summary: %s/summary.md (if present)\n' "$RESULTS_DIR"
