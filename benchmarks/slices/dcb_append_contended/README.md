# Benchmark: DCB Append, Contended

Measures `reckon_db_dcb:append_if_no_tag_matches/4` behaviour when
all workers contend on the SAME tag — the canonical uniqueness /
allocation / rate-limit pattern.

## Scenarios

| Scenario | Parallelism | Duration | Notes |
|----------|-------------|----------|-------|
| `smoke`             | 4   | 10s | sanity check |
| `baseline`          | 16  | 60s | typical contention level |
| `heavy_contention`  | 64  | 60s | high-contention stress |

## What to look for

- **Commit rate**: roughly `1 / parallelism` per worker. Aggregate
  commit-rate is the sustained DCB commit throughput on this hardware.
- **Conflict rate**: dominates total attempt rate. Conflicts are
  cheaper than commits (no writes), so attempt throughput >>
  commit throughput.
- **Latency distribution**: bimodal expected — fast aborts (`context_changed`)
  vs slower commits.

Each worker tracks its own `last_seq`. On conflict, it updates the
cutoff to the conflict's `max_seq` and retries on the next iteration.
Both `{ok, _}` and `{error, {context_changed, _}}` count as "operations"
in the harness's throughput numbers.

## Running

```bash
cd benchmarks
./scripts/bench_one.sh \
    --slice dcb_append_contended \
    --scenario baseline \
    --out results/dcb_contended_baseline.json
```

## Known issue (2026-05-27)

Same runtime issue as `dcb_append_uncontended` — see that slice's
README for the version-skew note. The slice + scenario files are
correct; the bench-harness wiring needs an update.
