# Benchmark: DCB Append, Uncontended

Measures `reckon_db_dcb:append_if_no_tag_matches/4` throughput +
latency when each worker uses its own unique tag (no contention).
Isolates the per-write cost of the DCB primitive vs the
straight-line `reckon_db_streams:append/4`.

## Scenarios

| Scenario | Parallelism | Duration | Tags |
|----------|-------------|----------|------|
| `smoke`     | 1  | 10s | sanity check, NOT for numbers |
| `baseline`  | 1  | 60s | single-writer floor; compare vs `append_single_stream:baseline` |
| `parallel`  | 16 | 60s | scaling test; bounded by `?DCB_STREAM` Ra group |

## Running

```bash
cd benchmarks
./scripts/bench_one.sh \
    --slice dcb_append_uncontended \
    --scenario baseline \
    --out results/dcb_uncontended_baseline.json
```

## Comparison metric

The interesting number is the **DCB overhead** relative to plain
stream append. Compute as:

```
overhead_ratio = dcb_append_uncontended.throughput
               / append_single_stream.throughput
```

A ratio of ~0.5 means DCB writes are roughly twice as expensive as
plain stream appends, which would track with the extra tag-index
path-write + transaction-body overhead.

## Known issue (2026-05-27)

The harness smoke crashes inside Khepri's trigger evaluation
(`khepri_tree:does_path_match/4`) when DCB code runs against the
bench environment. Likely a version-skew between the bench-harness's
checkout of reckon-db (via `_checkouts/`) and current trigger
machinery. Investigate before relying on numbers; the slice + scenario
files themselves are correct.
