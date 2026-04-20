# append_single_stream

**Question answered:** How fast can we sustainably append to one stream via `reckon_db_streams:append/4`, and what does the p99 tail look like?

This slice measures the **storage layer directly** — no gateway, no framework overhead. For the same question through the gater API, see `reckon-gater/benchmarks/slices/append_events_via_gater/`. For the layer-overhead delta, see `reckon-ecosystem/benchmarks/paired/reckon_db_vs_gater/`.

## Scenario parameters

- `store_id` — the reckon-db store to target (default: `bench_store`)
- `event_size_bytes` — payload size per event in the `data` field (default: 256)
- `parallelism` — concurrent writers against the single stream (default: 1)
- `duration_seconds` — measurement window after warmup (default: 60)

## Metrics produced

- `throughput_ops_sec` — sustained append rate
- `latency_ns_{p50,p90,p95,p99,p99_9,p99_99}` — append-call latency distribution
- `cpu_ms_per_op` — average CPU time per append
- `memory_high_water_mb` — peak RSS during measurement
- `disk_bytes_per_op` — average disk-write amplification per event

## Non-goals

- Cross-stream fanout behaviour → `append_many_streams`
- Subscriber notification latency → `fanout_to_subscribers`
- Raft overhead at cluster scale → `scale_cluster`

This slice is deliberately **single-node, single-stream** so the baseline is easy to interpret.

## Scenarios

- `baseline.eterm` — 1 writer, 256-byte data payload, 60-second window
- `high_throughput.eterm` — 32 parallel writers on the same stream
- `large_events.eterm` — 1 writer, 64 KB payload

## Reading results

Headline number is **p99 latency at sustained throughput**, not peak throughput. Throughput without a latency budget is marketing, not measurement.
