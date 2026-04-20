#!/usr/bin/env bash
#
# Produce a minimal markdown summary from a run directory. Placeholder
# until `reckon_bench_harness_report' is implemented. For now, dumps
# the headline numbers from each JSON file in the run directory.
#
# Usage:
#     ./scripts/summarise_run.sh <run-dir>

set -euo pipefail

if [[ $# -ne 1 ]]; then
    printf 'usage: %s <run-dir>\n' "$0" >&2
    exit 2
fi

RUN_DIR="$1"
if [[ ! -d "$RUN_DIR" ]]; then
    printf 'not a directory: %s\n' "$RUN_DIR" >&2
    exit 2
fi

printf '# Benchmark run summary\n\n'
printf 'Run directory: `%s`\n\n' "$RUN_DIR"

if [[ -f "${RUN_DIR}/system_info.json" ]]; then
    printf '## System\n\n```json\n'
    cat "${RUN_DIR}/system_info.json"
    printf '\n```\n\n'
fi

printf '## Slices\n\n'
printf '| slice | scenario | throughput ops/s | p50 (ms) | p99 (ms) | p99.9 (ms) |\n'
printf '|---|---|---:|---:|---:|---:|\n'

for f in "${RUN_DIR}"/*__*.json; do
    [[ -f "$f" ]] || continue
    python3 - "$f" <<'PYTHON'
import json, sys
d = json.load(open(sys.argv[1]))
lat = d.get("latency_ns", {})
def ms(ns): return f"{(ns/1_000_000):.3f}" if isinstance(ns, (int, float)) else "-"
print(f"| {d.get('slice','?')} | {d.get('scenario','?')} | "
      f"{d.get('throughput_ops_sec',0):.1f} | "
      f"{ms(lat.get('p50',0))} | {ms(lat.get('p99',0))} | {ms(lat.get('p99_9',0))} |")
PYTHON
done
