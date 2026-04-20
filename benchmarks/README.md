# reckon-db Benchmarks

Performance benchmarks for reckon-db — the BEAM-native event store.

These benchmarks measure the **storage layer directly** via `reckon_db_streams` and related modules. For consumer-facing API benchmarks (through the gater), see `reckon-gater/benchmarks/`. For framework-level benchmarks (evoq dispatch, projection catchup), see `evoq/benchmarks/`. For paired and comparative benchmarks that span multiple layers, see `reckon-ecosystem/benchmarks/`.

---

## What this directory answers

- Sustained append throughput on one stream / across N streams
- Append latency distribution (p50 → p99.99)
- Read latency for point reads and range reads
- Subscription fanout latency
- Raft overhead at different cluster sizes
- Event-size sensitivity
- Operational cost per event (CPU-ms, memory, disk write amplification)

## Running

From this directory:

```
./scripts/bench.sh --slice append_single_stream --scenario baseline --profile local-dev
```

Results land in `results/<run-id>/` alongside a `summary.md`.

## Slice taxonomy

See [WORKLOADS.md](WORKLOADS.md).

## Methodology

Shared methodology lives in [reckon-bench-harness/METHODOLOGY.md](https://github.com/reckon-db-org/reckon-bench-harness/blob/main/METHODOLOGY.md). Publication policy, regression detection, statistical rigour, and the what-to-do-when-numbers-disappoint pre-commit are all authoritative there.

## Hardware profiles

Shared profiles live in [reckon-bench-harness/hardware_profiles/](https://github.com/reckon-db-org/reckon-bench-harness/blob/main/hardware_profiles/). Supported: `local-dev`, `nanode-2gb`, `linode-8gb`, `hetzner-cx32`, `bare-metal-i9`.

## Slice pattern

Each slice is a vertical folder that owns its workload, scenarios, and README. See `slices/README.md` for the pattern; `slices/append_single_stream/` is the canonical worked example.

## Status

Early. `local-dev` runs are validation-only, never published. See methodology for the rules under which numbers leave the repository.
