# Changelog

All notable changes to reckon-db will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.6] - 2025-12-26

### Fixed

- **Dependency conflict**: Removed direct `ra` dependency (khepri provides it).
  Updated to `reckon_db_gater ~> 0.6.5` which removed stale ra from its lock file.

## [0.4.5] - 2025-12-26

### Fixed

- **Dependency conflict**: Updated `ra` dependency from exact `2.16.12` to `~> 2.17.1`
  to resolve conflict with `reckon_db_gater ~> 0.6.4` which requires `ra ~> 2.17.1`

## [0.4.4] - 2025-12-22

### Added

- **Configuration Guide**: Comprehensive configuration documentation
  - Store configuration options (data_dir, mode, pool sizes)
  - Health probing configuration
  - Consistency checking and persistence intervals
  - Erlang (sys.config) and Elixir (config.exs) examples
  - Complete development/staging/production examples
  - Performance tuning recommendations
  - Telemetry events reference

## [0.4.3] - 2025-12-22

### Added

- **Gateway Worker Handlers**:
  - `delete_stream` - Delete streams via gateway
  - `read_by_event_types` - Native Khepri type filtering via gateway
  - `get_subscription` - Get subscription details including checkpoint

These handlers support the erl-evoq-esdb adapter improvements.

## [0.4.2] - 2025-12-22

### Added

- **Cluster Consistency Checker** (`reckon_db_consistency_checker.erl`):
  - Split-brain detection via membership consensus verification
  - Leader consensus verification across all cluster nodes
  - Raft log consistency checks (term and commit index)
  - Quorum status monitoring with margin calculation
  - Four status levels: `healthy`, `degraded`, `split_brain`, `no_quorum`
  - Configurable check intervals (default: 5000ms)
  - Status change callbacks for alerting
  - Telemetry events: `[reckon_db, consistency, ...]`

- **Active Health Prober** (`reckon_db_health_prober.erl`):
  - Fast failure detection via active probing (default: 2000ms intervals)
  - Three probe types: `ping`, `rpc`, `khepri`
  - Configurable failure threshold (default: 3 consecutive failures)
  - Node status tracking: `healthy`, `suspect`, `failed`, `unknown`
  - Recovery detection with callbacks
  - Telemetry events: `[reckon_db, health, ...]`

- **Cluster Consistency Guide** (`guides/cluster_consistency.md`):
  - Split-brain problem explanation and prevention strategies
  - Consistency checker usage and configuration
  - Health prober integration patterns
  - Quorum management and recovery procedures
  - Circuit breaker and load balancer integration examples

- **Architecture Diagrams** (SVG):
  - `assets/consistency_checker.svg` - Consistency checker architecture
  - `assets/split_brain_detection.svg` - Split-brain detection flow
  - `assets/health_probing.svg` - Health probing timeline

### Tests

- 35 unit tests for consistency checker
- 37 unit tests for health prober
- All 72 new tests passing

## [0.4.1] - 2025-12-22

### Added

- **Server-Side Documentation Guides**:
  - `guides/temporal_queries.md` - Point-in-time queries, timestamp filtering, cluster behavior
  - `guides/scavenging.md` - Event lifecycle, archival backends, safety guarantees
  - `guides/causation.md` - Causation/correlation tracking, graph building, DOT export
  - `guides/stream_links.md` - Derived streams, filter/transform patterns
  - `guides/schema_evolution.md` - Schema registry, version-based upcasting, validation
  - `guides/memory_pressure.md` - Pressure levels, callbacks, integration patterns
  - `guides/storage_internals.md` - Khepri paths, version padding, cluster replication

- **Architecture Diagrams** (SVG):
  - `assets/temporal_query_flow.svg` - Temporal query processing flow
  - `assets/scavenge_lifecycle.svg` - Event lifecycle state machine
  - `assets/causation_graph.svg` - Causation chain visualization
  - `assets/stream_links.svg` - Stream linking architecture
  - `assets/schema_upcasting.svg` - Schema version upcasting flow
  - `assets/memory_levels.svg` - Memory pressure level thresholds
  - `assets/khepri_paths.svg` - Khepri storage path structure

### Changed

- **Documentation Improvements**:
  - Replaced ASCII diagrams with professional SVG graphics
  - `snapshot_recovery.svg` - Performance comparison visualization
  - `event_fanout.svg` - Multi-subscriber event delivery diagram
  - Updated `rebar.config` ex_doc with new guides organized into Core Concepts, Advanced Features, and Operations sections

## [0.4.0] - 2025-12-22

### Added

- **Enterprise Edition NIFs**: High-performance Rust NIFs with pure Erlang fallbacks
  - Community Edition (hex.pm) uses pure Erlang implementations
  - Enterprise Edition (git + Rust) gets 5-100x speedups for specific operations
  - Automatic fallback detection via `persistent_term`

- **esdb_crypto_nif** (Phase 1):
  - `nif_base58_encode/1` - Fast Base58 encoding for DIDs
  - `nif_base58_decode/1` - Fast Base58 decoding
  - Uses Bitcoin alphabet, ~5x faster than pure Erlang

- **esdb_archive_nif** (Phase 2):
  - `nif_compress/1,2` - Zstd compression with configurable level
  - `nif_decompress/1` - Zstd decompression
  - `nif_compress_batch/1,2` - Batch compression for multiple items
  - `nif_decompress_batch/1` - Batch decompression
  - ~10x faster than zlib, better compression ratios

- **esdb_hash_nif** (Phase 3):
  - `nif_xxhash64/1,2` - 64-bit xxHash with optional seed
  - `nif_xxhash3/1` - Modern xxHash3 (SIMD optimized)
  - `nif_partition_hash/2` - Hash to partition number
  - `nif_stream_partition/3` - Combined store+stream routing
  - `nif_partition_hash_batch/2` - Batch hashing for bulk ops
  - `nif_fnv1a/1` - FNV-1a for small keys
  - `nif_fast_phash/2` - Drop-in phash2 replacement

- **esdb_aggregate_nif** (Phase 3):
  - `nif_aggregate_events/2` - Bulk fold with tagged value semantics
  - `nif_sum_field/2` - Vectorized sum accumulation for numeric fields
  - `nif_count_where/3` - Count events matching field condition
  - `nif_merge_tagged_batch/1` - Batch map merge with tagged values
  - `nif_finalize/1` - Unwrap tagged values ({sum, N}, {overwrite, V})
  - `nif_aggregation_stats/1` - Event statistics (counts, unique fields)

- **esdb_filter_nif** (Phase 3):
  - `nif_filter_events/2` - Filter events by compiled predicate
  - `nif_filter_count/2` - Count matching events without collecting
  - `nif_compile_predicate/1` - Pre-compile filter predicates
  - `nif_partition_events/2` - Partition events by predicate (matching/non-matching)
  - `nif_first_match/2` - Find first matching event
  - `nif_find_all/2` - Find all matching events with indexes
  - `nif_any_match/2`, `nif_all_match/2` - Boolean aggregate predicates

- **esdb_graph_nif** (Phase 4):
  - `nif_build_edges/1` - Build edge list from event causation relationships
  - `nif_find_roots/1`, `nif_find_leaves/1` - Find root/leaf nodes
  - `nif_topo_sort/1` - Topological sort (Kahn's algorithm via petgraph)
  - `nif_has_cycle/1` - Detect cycles in causation graph
  - `nif_graph_stats/1` - Calculate node/edge/depth statistics
  - `nif_to_dot/1,2` - Generate Graphviz DOT format
  - `nif_has_path/2` - Check if path exists between nodes
  - `nif_get_ancestors/2`, `nif_get_descendants/2` - BFS path finding

### Changed

- **Build profiles**:
  - Added `enterprise` profile with Rust NIF compilation hooks
  - Added `enterprise_test` profile for testing with NIFs
  - Build with `rebar3 as enterprise compile` to enable NIFs

### Documentation

- Updated README with Enterprise/Community edition information
- Added NIF function documentation with academic references

## [0.3.1] - 2025-12-20

### Changed

- **Version padding**: Increased from 6 to 12 characters (`?VERSION_PADDING` macro)
  - Previous: 999,999 events per stream max (~2.7 hours at 100 events/sec)
  - Now: 999,999,999,999 events per stream max (~317 years at 100 events/sec)
  - Supports long-running neuroevolution, IoT, and continuous event streams

### Fixed

- **EDoc errors**: Removed backticks and markdown from EDoc comments (breaks hex.pm docs)

## [0.3.0] - 2025-12-20

### Added

- **Capability-Based Security** (`esdb_capability_verifier.erl`, `esdb_revocation.erl`):
  - Server-side verification of UCAN-inspired capability tokens
  - Ed25519 signature verification using issuer's public key from DID
  - Token expiration and not-before time validation
  - Resource URI pattern matching (exact, wildcard suffix, prefix)
  - Action permission checking with wildcard support
  - Token revocation management (ETS-based, gossip integration planned)
  - Issuer revocation for compromised identities
  - Content-addressed token IDs (CIDs) for revocation tracking
  - Comprehensive unit tests (13 verifier tests + 6 revocation tests)

This completes Phase 3 of the decentralized security implementation.
Client-side token creation is in reckon-gater, server-side verification is here.

### Changed

- **Documentation**: Replaced ASCII diagrams with SVG in README and guides

### Fixed

- **README API documentation**: Fixed incorrect function signatures
  - Subscriptions: Added missing `unsubscribe/3`, `get/2` functions
  - Snapshots: Fixed `load/3` → `load_at/3`, `delete/3` → `delete_at/3`, added `exists/2`, `exists_at/3`
  - Aggregator: Completely rewrote section - was showing non-existent API (`foldl/4`, `foldl_from_snapshot/4`)
- **guides/snapshots.md**: Fixed `load/3` → `load_at/3`, `delete/3` → `delete_at/3`, rewrote aggregator example
- **guides/cqrs.md**: Fixed subscription key usage in emitter group join
- **guides/subscriptions.md**: Fixed invalid map access syntax
- **guides/event_sourcing.md**: Fixed aggregator foldl signature (takes events list, not store/stream)

## [0.2.0] - 2024-12-19

### Added

- **End-to-end tests**: 24 comprehensive e2e tests for gater integration:
  - Worker registration (4 tests)
  - Stream operations via gater (9 tests)
  - Subscription operations (4 tests)
  - Snapshot operations (4 tests)
  - Load balancing (3 tests)
- **Subscriptions**: Added `ack/4` function for acknowledging event delivery

### Fixed

- **Gateway worker API compatibility**:
  - `get_version` now handles integer return correctly
  - Snapshot operations use correct function names (`save`, `load_at`, `delete_at`)
  - Subscription unsubscribe uses correct 3-arg version
- **Header conflicts**: Added `ifndef` guards for `DEFAULT_TIMEOUT` macro

### Changed

- **reckon-gater integration**: Updated to work with gater's pg-based registry (replacing Ra)
- **Test counts**: Now 72 unit + 53 integration + 24 e2e = 149 total tests

## [0.1.0] - 2024-12-18

### Added

- Initial release of reckon-db, a BEAM-native Event Store built on Khepri/Ra
- Event stream operations:
  - `append/4,5` - Write events with optimistic concurrency control
  - `read/5` - Read events from streams (forward/backward)
  - `get_version/2` - Get current stream version
  - `exists/2` - Check if stream exists
  - `list_streams/1` - List all streams in store
  - `delete/2` - Soft delete streams
- Subscription system:
  - Stream subscriptions - events from specific streams
  - Event type subscriptions - events by type across streams
  - Pattern subscriptions - wildcard stream matching
  - Payload subscriptions - content-based filtering
- Snapshot management:
  - `save/5` - Save aggregate state snapshots
  - `load/2,3` - Load latest or specific version snapshots
  - `list/2` - List all snapshots for a stream
  - `delete/3` - Delete old snapshots
- Aggregation utilities:
  - `foldl/4` - Fold over events with accumulator
  - `foldl_from_snapshot/4` - Fold starting from latest snapshot
- Cluster support:
  - UDP multicast discovery (LibCluster gossip compatible)
  - Automatic Khepri/Ra cluster formation
  - Node monitoring and failover
  - Leader election and tracking
- Emitter pools for high-throughput event delivery
- Partitioned writers for concurrent stream writes
- BEAM telemetry integration with configurable handlers
- Comprehensive test suite (72 unit + 53 integration tests)
- Educational guides:
  - Event Sourcing fundamentals
  - CQRS patterns
  - Subscriptions usage
  - Snapshots optimization

### Dependencies

- Khepri 0.17.2 - Raft-based distributed storage
- Ra 2.16.12 - Raft consensus implementation
- Telemetry 1.3.0 - BEAM telemetry for observability
