# reckon-db Workload Taxonomy

Slices in this directory answer questions about the **storage engine** — not the gateway API, not the framework. For gateway-layer or framework-layer slices, see the corresponding `benchmarks/` directories in `reckon-gater`, `evoq`, and `reckon-evoq`.

Status legend: `pending` / `wip` / `done` / `published`.

---

## P0 — baseline credibility

| Slice | Question it answers | Status |
|---|---|---|
| `append_single_stream` | Sustained single-stream append throughput + p99 tail | done |
| `append_many_streams` | Fanned-out append behaviour across N streams | pending |
| `read_event_by_id` | Point-read latency | pending |
| `fanout_to_subscribers` | End-to-end: append → subscriber notified | pending |
| `sweep_event_size` | Throughput + latency curve across 1 KB → 1 MB payloads | pending |

### Baseline numbers — single-node dev, 2026-06-22

| Slice | Workers | Throughput | p50 | p90 | p99 |
|---|---|---|---|---|---|
| `append_single_stream` | 1 | 40.8 ops/s | 24.1 ms | 30.7 ms | 36.4 ms |

## DCB — Dynamic Consistency Boundary

| Slice | Question it answers | Status |
|---|---|---|
| `dcb_append_uncontended` | Per-write cost of `append_if_no_tag_matches` with unique tags (no contention) | done |
| `dcb_append_contended` | DCB throughput and conflict rate when all workers share one tag | done |

### Baseline numbers — single-node dev, 2026-06-22

Raw JSON in `results/baseline_dcb_*.json`. Measured on a single-node dev environment (no Raft
replication RTT). Production cluster numbers will be lower due to quorum round-trips (~2–5 ms LAN).

| Slice | Workers | Throughput | p50 | p90 | p99 | Notes |
|---|---|---|---|---|---|---|
| `dcb_append_uncontended` | 1 | 49.3 ops/s | 18.9 ms | 30.8 ms | 47.1 ms | DCB overhead vs plain append ≈ 1.2× (within noise on single node) |
| `dcb_append_contended` | 16 | 245.4 ops/s total | 57.6 ms | 130.0 ms | 160.8 ms | ~15 commits/s; remainder are `{context_changed}` conflict-aborts that re-read and retry |

DCB overhead vs `append_single_stream` on this workload is negligible at 1 worker because the
tag-index write is cheaper than the per-stream version check on a fresh stream. Under contention
the total throughput reflects all operations (commits + conflict retries counted equally).

---

## P1 — differentiating numbers

| Slice | Question it answers | Status |
|---|---|---|
| `scale_cluster` | Raft overhead at 1 / 3 / 5 nodes | pending |
| `contend_on_stream` | Concurrent writer contention on one stream | pending |
| `snapshot_aggregate` | Snapshot write cost + restore-from-snapshot speed | pending |
| `measure_cost_per_event` | CPU-ms / memory-MB-s / disk-bytes per append | pending |

## P2 — situational

| Slice | Question it answers | Status |
|---|---|---|
| `recover_after_crash` | Recovery-time distribution after forced node loss | pending |
| `read_range` | Range-read throughput + latency (replay scenarios) | pending |
| `replay_under_write_load` | Replay throughput while writes continue | pending |

## Naming

Verbs. Slices describe what the workload DOES. If the name reads like a noun, rename before committing.

## Adding a slice

Copy `slices/append_single_stream/`. Rename to a verb. Implement the `reckon_bench_slice` behaviour. Add a row here.

## Cross-repo pairs

Several slices in this directory are designed to be run **paired** with slices in other repositories, to produce layer-overhead deltas. The paired-run orchestration lives in `reckon-ecosystem/benchmarks/`.

| This slice | Paired with | Answers |
|---|---|---|
| `append_single_stream` | `reckon-gater/benchmarks/slices/append_events_via_gater` | Gateway overhead (pure storage vs API call) |
| `append_single_stream` | `evoq/benchmarks/slices/dispatch_command` | Framework overhead (pure storage vs full stack) |

Paired runs do not live in either side's repo; they live in ecosystem.
