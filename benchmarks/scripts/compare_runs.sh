#!/usr/bin/env bash
#
# Diff two benchmark result directories. Placeholder until
# `reckon_bench_harness_compare' is implemented. For now, a simple
# text diff highlighting p99 and throughput deltas.
#
# Usage:
#     ./scripts/compare_runs.sh <baseline-run-dir> <candidate-run-dir>

set -euo pipefail

if [[ $# -ne 2 ]]; then
    printf 'usage: %s <baseline-run-dir> <candidate-run-dir>\n' "$0" >&2
    exit 2
fi

BASELINE="$1"
CANDIDATE="$2"

if [[ ! -d "$BASELINE" || ! -d "$CANDIDATE" ]]; then
    printf 'both arguments must be directories\n' >&2
    exit 2
fi

python3 - "$BASELINE" "$CANDIDATE" <<'PYTHON'
import json, os, sys
base_dir, cand_dir = sys.argv[1], sys.argv[2]

def load_slice_runs(d):
    out = {}
    for fn in sorted(os.listdir(d)):
        if fn.startswith("system_info") or not fn.endswith(".json"):
            continue
        p = os.path.join(d, fn)
        data = json.load(open(p))
        key = (data.get("slice"), data.get("scenario"))
        out[key] = data
    return out

base = load_slice_runs(base_dir)
cand = load_slice_runs(cand_dir)

print("# Run comparison\n")
print(f"Baseline:  {base_dir}")
print(f"Candidate: {cand_dir}\n")

print("| slice | scenario | throughput Δ% | p99 Δ% |")
print("|---|---|---:|---:|")

regressed = False
for key in sorted(set(base.keys()) | set(cand.keys())):
    slice_, scenario = key
    b = base.get(key)
    c = cand.get(key)
    if not b or not c:
        print(f"| {slice_} | {scenario} | - | missing |")
        continue
    b_thr = b.get("throughput_ops_sec", 0) or 1
    c_thr = c.get("throughput_ops_sec", 0)
    b_p99 = b.get("latency_ns", {}).get("p99", 0) or 1
    c_p99 = c.get("latency_ns", {}).get("p99", 0)
    thr_delta = (c_thr - b_thr) / b_thr * 100
    p99_delta = (c_p99 - b_p99) / b_p99 * 100
    materially_slower_thr = thr_delta < -3.0
    materially_slower_p99 = p99_delta > 5.0
    if materially_slower_thr or materially_slower_p99:
        regressed = True
    flag = " ⚠️" if (materially_slower_thr or materially_slower_p99) else ""
    print(f"| {slice_} | {scenario} | {thr_delta:+.1f}%{flag} | {p99_delta:+.1f}%{flag} |")

if regressed:
    print("\n**Regression flagged.** See METHODOLOGY.md § Regression detection.")
    sys.exit(1)
PYTHON
