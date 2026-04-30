# Plan: Pluggable Backends

**Status:** Planning — behaviours sketched, not yet wired in
**Created:** 2026-04-19
**Last Updated:** 2026-04-19

## Overview

Today `reckon-db` is tightly coupled to Khepri (which sits on Ra). Measurements on Hetzner cx33 show **~51 KB disk write per 256-byte event (200× write amplification) and ~22 appends/sec sustained**. Root cause: Khepri is a tree-structured replicated KV store designed for RabbitMQ's internal metadata — not an event log. Every event pays Raft consensus + tree insertion + WAL + segment write + fsync per commit.

This plan extracts the hot-path contract into `reckon_db_log_backend` (and two sibling behaviours for snapshots and subscriptions) so alternative engines — starting with a RocksDB-backed Rust NIF — can be dropped in without touching the rest of the stack.

## Motivation

- 51 KB/op disk amplification is ~25-50× worse than EventStoreDB's custom log format.
- 22 ops/sec is ~2-3 orders of magnitude below ESDB on comparable hardware.
- The gap is architectural, not tuning. No amount of Khepri tuning will close it.
- The fix is well-understood: keep Ra/Khepri for the control plane (cluster metadata, subscriptions, schemas, capability tokens), move the log to a purpose-built engine.

## Phases

- [x] **Phase 1: Sketch behaviours.** Define `reckon_db_log_backend`, `reckon_db_snapshot_backend`, `reckon_db_subscription_backend` as pure behaviour modules — no implementation, just contracts. *(Complete — committed alongside this plan.)*
- [ ] **Phase 2: Extract Khepri backend.** Move existing `reckon_db_streams` logic into `reckon_db_khepri_log_backend` implementing `reckon_db_log_backend`. Same for snapshots and subscriptions. The facade stays API-compatible; only the delegation changes.
- [ ] **Phase 3: Profile the extracted Khepri backend.** Confirm the 51 KB/op and 22 ops/sec reproduce identically after extraction. If the numbers shift, the extraction introduced unintended behaviour.
- [ ] **Phase 4: Prototype RocksDB backend.** `reckon_db_rocksdb_log_backend` + Rustler NIF crate `reckon_db_log_nif` (or wrap `rust-rocksdb`). Implement only the `reckon_db_log_backend` callbacks. Keep snapshots + subscriptions in Khepri.
- [ ] **Phase 5: A/B benchmark.** Run the existing bench suite against both backends on identical hardware. Decision gate: if RocksDB is ≥10× throughput + ≥10× lower disk amplification, commit to the direction. If <10×, investigate other bottlenecks first (the gater's `route_call` is the next suspect).
- [ ] **Phase 6: Ship as opt-in.** Release reckon-db 2.1 with `{backend, reckon_db_rocksdb_log_backend}` as an opt-in store config. Default stays Khepri until soak-tested.
- [ ] **Phase 7: Default flip.** After production soak (multiple deployments, replay-from-scratch verified, crash-recovery verified), make RocksDB the default in reckon-db 3.0.

## Files to Create/Modify

### Phase 1 (this commit)

| File | Purpose | Status |
|------|---------|--------|
| `src/reckon_db_log_backend.erl` | Behaviour — hot-path log contract | ✅ sketched |
| `src/reckon_db_snapshot_backend.erl` | Behaviour — snapshot storage | ✅ sketched |
| `src/reckon_db_subscription_backend.erl` | Behaviour — subscription state | ✅ sketched |
| `plans/PLAN_PLUGGABLE_BACKENDS.md` | This document | ✅ written |

### Phase 2

| File | Purpose |
|------|---------|
| `src/reckon_db_khepri_log_backend.erl` | Extract `reckon_db_streams:do_append/4`, `do_read/5`, etc. into a `-behaviour(reckon_db_log_backend)` module |
| `src/reckon_db_khepri_snapshot_backend.erl` | Extract `reckon_db_snapshots` internals |
| `src/reckon_db_khepri_subscription_backend.erl` | Extract `reckon_db_subscriptions` internals |
| `src/reckon_db_streams.erl` | Refactor — delegate to the configured backend via `reckon_db_store_registry` |
| `src/reckon_db_config.erl` | Add `backend` field to `store_config()` record (default: `reckon_db_khepri_log_backend`) |
| `src/reckon_db_system_sup.erl` | Call the configured backend's `init/1` during store startup |
| `src/reckon_db_store_registry.erl` | Cache `{BackendMod, BackendState}` per store; expose `lookup/1` for facades |
| `include/reckon_db.hrl` | Extend `store_config` record |

### Phase 4 (RocksDB prototype)

| File | Purpose |
|------|---------|
| `src/reckon_db_rocksdb_log_backend.erl` | Erlang facade calling the NIF |
| `native/reckon_db_log_nif/Cargo.toml` | New crate in reckon-nifs |
| `native/reckon_db_log_nif/src/lib.rs` | Rust implementation — column-family-per-stream, batched fsync, zstd |
| `test/integration/reckon_db_rocksdb_log_backend_SUITE.erl` | Mirror the khepri backend suite against RocksDB |

## Success Criteria

### Phase 1 (this PR)
- [x] Three behaviour modules compile standalone (no implementations required)
- [x] Callbacks cover every operation `reckon_db_streams`, `reckon_db_snapshots`, `reckon_db_subscriptions` expose today
- [x] Optional callbacks clearly marked for features backends may skip

### Phase 2
- [ ] Extracted `reckon_db_khepri_*_backend` modules implement the respective behaviour
- [ ] `reckon_db_streams` and the other facades call the backend via the registry
- [ ] Existing test suite passes unchanged (both eunit and common_test)
- [ ] Benchmark numbers on cx33 match ±5% of pre-extraction baseline

### Phase 5 (decision gate)
- [ ] Paired bench on same hardware, same scenario: `pair_storage_bare` against both backends
- [ ] RocksDB backend ≥10× throughput OR investigation reopens
- [ ] Disk amplification ≤10 KB/op OR investigation reopens

## Non-Goals

- **Replacing Ra/Khepri entirely.** Ra/Khepri is the right tool for cluster metadata, subscription state, schema registry, capability-token state. The plan leaves all of that in place.
- **Breaking the public API.** `reckon_db_streams:append/4`, `reckon_gater_api:append_events/3`, `evoq_dispatcher:dispatch/2` — all three facades keep identical signatures and semantics. Only the bytes-to-disk path changes.
- **Supporting every possible backend at once.** The behaviour is scoped to unblock the RocksDB prototype. Additional backends (LMDB, fjall, redb, ESDB-compatible remote) are future options with their own decision gates.

## Risks

| Risk | Mitigation |
|------|-----------|
| Extraction introduces subtle behaviour changes | Phase 3's re-benchmark at same numbers. Phase 2 keeps the extraction commit mechanical — no new logic. |
| RocksDB dep adds compile-time burden | Already paying for `reckon-nifs` Rust toolchain. Incremental cost is small. |
| Replication story for RocksDB is DIY | Initial RocksDB backend is single-node only. Cluster mode stays Khepri-backed. Multi-node RocksDB replication = future work. |
| Backend swap breaks in-flight upgrades | Default stays Khepri through reckon-db 2.x. RocksDB is opt-in. No forced migration. |

## Decisions Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-04-19 | Three behaviours (log, snapshot, subscription) not one | Allows fast-log backend to keep using Khepri for the cold-path storage (subscriptions need Raft consensus anyway) without implementing those callbacks. |
| 2026-04-19 | `state()` opaque term, passed through by facade | Matches OTP behaviour convention, lets backends use any internal state shape. |
| 2026-04-19 | Keep facade module names unchanged | `reckon_db_streams:append/4` stays the public entry. Only its implementation changes. Zero consumer migration needed. |
| 2026-04-19 | Phase 5 is a hard gate, not a rubber stamp | If RocksDB doesn't deliver 10× we look elsewhere (gater overhead, sync semantics) before committing months to a custom Rust log engine. |

## Open Questions

- **Global offset semantics.** Does `read_all/3` need a strict total order, or is per-store ordering enough? RocksDB column families make per-stream cheap but global-order iteration expensive. Might need a dedicated "global log" column family that gets every event appended twice.
- **Replication strategy for RocksDB phase.** Options: (a) single-leader + sync-replica + Ra for leader election, (b) per-stream Ra groups (explosion), (c) cluster-wide single Ra log for replicated WAL only (interesting — minimal Ra use). Decision deferred to phase 4 prototype.
- **Backend-versioning.** If a store was created by Khepri backend and we switch to RocksDB, we need a migration tool. Probably a one-shot `reckon_db_migrate:convert/3` that reads via old backend and writes via new. Defer to phase 6.

## Links

- Benchmark data driving this plan: `macula-demo/infrastructure/bench-hetzner-cx32/results/SUMMARY.md`
- Ra benchmarks for context: https://github.com/rabbitmq/ra
- EventStoreDB log format overview: https://developers.eventstore.com/server/v24.6/operations.html
