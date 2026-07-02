# reckon-db guides

Developer + operator reference for [reckon-db](../README.md), the BEAM-native
event store built on Khepri/Ra. Each page is self-contained; read by topic or
follow one of the suggested orders below.

## Core event sourcing

| Guide | What it covers |
|---|---|
| [Event Sourcing Paradigms](event_sourcing_paradigms.md) | Entity, relationship, and process-centric modelling; how reckon-db supports each. |
| [Event Sourcing](event_sourcing.md) | Streams, append/read, versioning, optimistic concurrency, event metadata, and the secondary index. |
| [CQRS](cqrs.md) | Command/query separation with reckon-db; building read models from event streams. |
| [Subscriptions](subscriptions.md) | Persistent subscriptions by stream, event type, pattern, and payload; acking and delivery. |
| [Snapshots](snapshots.md) | Aggregate-state snapshots: save, load, load-at-version, and replay acceleration. |
| [Stream Links](stream_links.md) | Linking events across streams and building projection streams. |
| [System Streams](system_streams.md) | The `$` namespace and store-internal system streams. |
| [Temporal Queries](temporal_queries.md) | Time-based reads over event history. |

## DCB and CCC (conditional append)

| Guide | What it covers |
|---|---|
| [Dynamic Consistency Boundaries (DCB)](dcb.md) | Conditional append on a tag-filter context query; tag and `event_type` leaves, boolean algebra, the seven filter shapes. |
| [DCB: Raft Design](dcb_raft_design.md) | Why Raft single-leader serialization makes the DCB primitive sound; index layout and the `try_append` transaction path. |
| [CCC Payload Indexes](ccc.md) | Command Context Consistency: `{payload, Key}` / `{payload_hash, [Keys]}` index declarations, payload filters, and the Horus extraction constraint. |

## Operations

| Guide | What it covers |
|---|---|
| [Configuration](configuration.md) | sys.config / app env: stores, pools, timeouts, telemetry, cluster discovery (multicast + k8s DNS), cluster secret. |
| [Cluster Consistency](cluster_consistency.md) | Split-brain prevention, quorum behaviour, and consistency guarantees. |
| [Memory Pressure](memory_pressure.md) | Memory-pressure monitoring and back-pressure behaviour. |
| [Scavenging](scavenging.md) | Event lifecycle, deleting old events, and the snapshot/replay interaction. |
| [Schema Evolution](schema_evolution.md) | Versioning event shapes and upcasting on read. |
| [Store Inspector](store_inspector.md) | Inspecting a live store's contents and structure. |
| [Telemetry](telemetry.md) | The full telemetry event catalogue and how to attach logger/metrics/tracing handlers. |

## Internals

| Guide | What it covers |
|---|---|
| [Storage Internals](storage_internals.md) | The Khepri on-disk layout, Model C structural aggregate-type namespace, and the write-maintained secondary index. |

## Suggested reading orders

**I am modelling a domain on reckon-db:**

1. [Event Sourcing Paradigms](event_sourcing_paradigms.md)
2. [Event Sourcing](event_sourcing.md)
3. [CQRS](cqrs.md), then [Subscriptions](subscriptions.md) and [Snapshots](snapshots.md)
4. [Schema Evolution](schema_evolution.md) when your events start changing shape

**I need strong consistency across streams (DCB/CCC):**

1. [Dynamic Consistency Boundaries (DCB)](dcb.md)
2. [CCC Payload Indexes](ccc.md)
3. [DCB: Raft Design](dcb_raft_design.md) for the why

**I am operating a reckon-db cluster:**

1. [Configuration](configuration.md)
2. [Cluster Consistency](cluster_consistency.md)
3. [Memory Pressure](memory_pressure.md), [Scavenging](scavenging.md), [Store Inspector](store_inspector.md)

**I want to understand how it stores things:**

1. [Storage Internals](storage_internals.md)
2. [DCB: Raft Design](dcb_raft_design.md)

For the ecosystem map (gater, evoq, gateway, Go client, and more), see the
[Reckon stack](../README.md#reckon-stack) section in the top-level README.
