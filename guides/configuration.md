# Configuration Guide

This guide covers all configuration options for reckon-db, with examples for both Erlang (sys.config) and Elixir (config.exs).

## Quick Start

### Erlang (sys.config)

```erlang
[
  {reckon_db, [
    {stores, [
      {my_store, [
        {data_dir, "/var/lib/reckon_db/my_store"},
        {mode, single},
        {timeout, 5000},
        {writer_pool_size, 10},
        {reader_pool_size, 10}
      ]}
    ]},
    {telemetry_handlers, [logger]}
  ]}
].
```

### Elixir (config.exs)

```elixir
config :reckon_db,
  stores: [
    my_store: [
      data_dir: "/var/lib/reckon_db/my_store",
      mode: :single,
      timeout: 5_000,
      writer_pool_size: 10,
      reader_pool_size: 10
    ]
  ],
  telemetry_handlers: [:logger]
```

## Configuration Reference

### Store Configuration

Stores are the primary configuration. Each store is an independent event store instance backed by Khepri/Ra.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `stores` | proplist | `[]` | List of store configurations |

Each store in the list has:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `data_dir` | string | `/var/lib/reckon_db/{store_id}` | Data directory for Khepri/Ra |
| `mode` | atom | `single` | Operation mode: `single` or `cluster` |
| `timeout` | integer | 5000 | Default timeout in milliseconds |
| `writer_pool_size` | integer | 10 | Number of writer workers |
| `reader_pool_size` | integer | 10 | Number of reader workers |
| `gateway_pool_size` | integer | 1 | Number of gateway workers |

#### Erlang Example

```erlang
{stores, [
  {orders_store, [
    {data_dir, "/bulk0/reckon_db/orders"},
    {mode, cluster},
    {timeout, 10000},
    {writer_pool_size, 20},
    {reader_pool_size, 50}
  ]},
  {users_store, [
    {data_dir, "/bulk0/reckon_db/users"},
    {mode, single},
    {writer_pool_size, 5},
    {reader_pool_size, 10}
  ]}
]}
```

#### Elixir Example

```elixir
config :reckon_db,
  stores: [
    orders_store: [
      data_dir: "/bulk0/reckon_db/orders",
      mode: :cluster,
      timeout: 10_000,
      writer_pool_size: 20,
      reader_pool_size: 50
    ],
    users_store: [
      data_dir: "/bulk0/reckon_db/users",
      mode: :single,
      writer_pool_size: 5,
      reader_pool_size: 10
    ]
  ]
```

### Pool Sizes (Global Defaults)

These set defaults for all stores that don't specify their own values.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `writer_pool_size` | integer | 10 | Default writer pool size |
| `reader_pool_size` | integer | 10 | Default reader pool size |
| `gateway_pool_size` | integer | 1 | Default gateway pool size |

#### Erlang Example

```erlang
{writer_pool_size, 20},
{reader_pool_size, 50},
{gateway_pool_size, 2}
```

#### Elixir Example

```elixir
config :reckon_db,
  writer_pool_size: 20,
  reader_pool_size: 50,
  gateway_pool_size: 2
```

### Worker Timeouts

Idle timeout for reader and writer workers.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `reader_idle_timeout_ms` | integer | 60000 | Reader idle timeout (ms) |
| `writer_idle_timeout_ms` | integer | 60000 | Writer idle timeout (ms) |

#### Erlang Example

```erlang
{reader_idle_timeout_ms, 120000},  %% 2 minutes
{writer_idle_timeout_ms, 120000}
```

#### Elixir Example

```elixir
config :reckon_db,
  reader_idle_timeout_ms: 120_000,
  writer_idle_timeout_ms: 120_000
```

### Health Probing

Health probing monitors store nodes and triggers failover in cluster mode.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `health_probe_interval` | integer | 5000 | Probe interval in milliseconds |
| `health_probe_timeout` | integer | 2000 | Probe timeout in milliseconds |
| `health_failure_threshold` | integer | 3 | Failures before marking unhealthy |
| `health_probe_type` | atom | `ping` | Probe type: `ping` or `deep` |

**Probe Types:**

- `ping` - Simple node reachability check (fast, low overhead)
- `deep` - Checks Khepri/Ra cluster health (thorough, higher overhead)

#### Erlang Example

```erlang
{health_probe_interval, 10000},
{health_probe_timeout, 5000},
{health_failure_threshold, 5},
{health_probe_type, deep}
```

#### Elixir Example

```elixir
config :reckon_db,
  health_probe_interval: 10_000,
  health_probe_timeout: 5_000,
  health_failure_threshold: 5,
  health_probe_type: :deep
```

### Consistency Checking

Periodic consistency verification for cluster mode.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `consistency_check_interval` | integer | 60000 | Check interval in milliseconds |

#### Erlang Example

```erlang
{consistency_check_interval, 30000}  %% 30 seconds
```

#### Elixir Example

```elixir
config :reckon_db,
  consistency_check_interval: 30_000
```

### Persistence

Controls periodic snapshot persistence to disk.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `persistence_interval` | integer | 60000 | Persistence interval in milliseconds |

#### Erlang Example

```erlang
{persistence_interval, 30000}  %% 30 seconds
```

#### Elixir Example

```elixir
config :reckon_db,
  persistence_interval: 30_000
```

### Telemetry

Configure telemetry handlers for metrics and logging.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `telemetry_handlers` | list | `[logger]` | List of telemetry handlers |

**Available Handlers:**

- `logger` - Logs events via OTP logger

#### Erlang Example

```erlang
{telemetry_handlers, [logger]}
```

#### Elixir Example

```elixir
config :reckon_db,
  telemetry_handlers: [:logger]
```

## Complete Configuration Examples

### Single Node Development

```erlang
%% Erlang sys.config
[
  {reckon_db, [
    {stores, [
      {dev_store, [
        {data_dir, "/tmp/reckon_db/dev"},
        {mode, single}
      ]}
    ]},
    %% Small pools for development
    {writer_pool_size, 2},
    {reader_pool_size, 5},
    %% Fast feedback on issues
    {health_probe_interval, 2000},
    {health_failure_threshold, 1},
    %% Frequent persistence for testing
    {persistence_interval, 5000}
  ]}
].
```

```elixir
# Elixir config/dev.exs
config :reckon_db,
  stores: [
    dev_store: [
      data_dir: "/tmp/reckon_db/dev",
      mode: :single
    ]
  ],
  writer_pool_size: 2,
  reader_pool_size: 5,
  health_probe_interval: 2_000,
  health_failure_threshold: 1,
  persistence_interval: 5_000
```

### Production Cluster

```erlang
%% Erlang sys.config
[
  {reckon_db, [
    {stores, [
      {main_store, [
        {data_dir, "/bulk0/reckon_db/main"},
        {mode, cluster},
        {timeout, 10000},
        {writer_pool_size, 50},
        {reader_pool_size, 100},
        {gateway_pool_size, 5}
      ]}
    ]},
    %% Production health monitoring
    {health_probe_interval, 5000},
    {health_probe_timeout, 3000},
    {health_failure_threshold, 3},
    {health_probe_type, deep},
    %% Consistency and persistence
    {consistency_check_interval, 60000},
    {persistence_interval, 30000},
    %% Longer idle timeouts
    {reader_idle_timeout_ms, 300000},
    {writer_idle_timeout_ms, 300000}
  ]}
].
```

```elixir
# Elixir config/runtime.exs
config :reckon_db,
  stores: [
    main_store: [
      data_dir: "/bulk0/reckon_db/main",
      mode: :cluster,
      timeout: 10_000,
      writer_pool_size: 50,
      reader_pool_size: 100,
      gateway_pool_size: 5
    ]
  ],
  # Production health monitoring
  health_probe_interval: 5_000,
  health_probe_timeout: 3_000,
  health_failure_threshold: 3,
  health_probe_type: :deep,
  # Consistency and persistence
  consistency_check_interval: 60_000,
  persistence_interval: 30_000,
  # Longer idle timeouts
  reader_idle_timeout_ms: 300_000,
  writer_idle_timeout_ms: 300_000
```

### Multi-Store Setup

```elixir
# Elixir config.exs - Multiple stores for different domains
config :reckon_db,
  stores: [
    # High-write orders store
    orders_store: [
      data_dir: "/bulk0/reckon_db/orders",
      mode: :cluster,
      writer_pool_size: 100,
      reader_pool_size: 50
    ],
    # Read-heavy analytics store
    analytics_store: [
      data_dir: "/bulk1/reckon_db/analytics",
      mode: :single,
      writer_pool_size: 5,
      reader_pool_size: 200
    ],
    # Low-volume user store
    users_store: [
      data_dir: "/bulk0/reckon_db/users",
      mode: :single,
      writer_pool_size: 10,
      reader_pool_size: 20
    ]
  ]
```

## Cluster Mode Configuration

When running in cluster mode, additional Erlang VM configuration is needed.

### vm.args

```
## Node name (required for clustering)
-name store1@192.168.1.10

## Cookie for cluster authentication
-setcookie my_cluster_cookie

## Enable distribution
-proto_dist inet_tcp

## Increase distribution buffer
+zdbbl 32768
```

### Connecting Nodes

Cluster formation happens via Khepri/Ra. Use the API to join nodes:

```erlang
%% On the joining node
reckon_db:join_cluster(my_store, 'store1@192.168.1.10').
```

```elixir
# Elixir
:reckon_db.join_cluster(:my_store, :"store1@192.168.1.10")
```

## Data Directory Guidelines

### Linux/Production

Store data on separate disk partitions for performance:

```erlang
{data_dir, "/bulk0/reckon_db/my_store"}
```

### Development

Use temp directory for ephemeral data:

```erlang
{data_dir, "/tmp/reckon_db/dev_store"}
```

### Docker/Container

Mount a volume for persistence:

```yaml
volumes:
  - esdb_data:/var/lib/reckon_db
```

```erlang
{data_dir, "/var/lib/reckon_db/my_store"}
```

## Performance Tuning

### High-Write Workloads

```elixir
config :reckon_db,
  stores: [
    high_write: [
      writer_pool_size: 100,      # Many concurrent writers
      reader_pool_size: 20,       # Fewer readers needed
      mode: :cluster              # Distribute writes
    ]
  ],
  persistence_interval: 60_000    # Less frequent persistence
```

### High-Read Workloads

```elixir
config :reckon_db,
  stores: [
    high_read: [
      writer_pool_size: 10,       # Fewer writers needed
      reader_pool_size: 200,      # Many concurrent readers
      mode: :cluster              # Read from any replica
    ]
  ],
  reader_idle_timeout_ms: 600_000 # Keep readers alive longer
```

### Low-Latency Requirements

```elixir
config :reckon_db,
  stores: [
    low_latency: [
      timeout: 2_000,             # Fast timeouts
      gateway_pool_size: 10       # More gateway workers
    ]
  ],
  health_probe_interval: 1_000,   # Fast failure detection
  health_failure_threshold: 1     # Immediate failover
```

## Telemetry Events

reckon-db emits telemetry events for monitoring:

| Event | Measurements | Metadata |
|-------|--------------|----------|
| `[reckon_db, append, start]` | - | store_id, stream |
| `[reckon_db, append, stop]` | duration | store_id, stream, count |
| `[reckon_db, append, exception]` | duration | store_id, stream, reason |
| `[reckon_db, read, start]` | - | store_id, stream |
| `[reckon_db, read, stop]` | duration | store_id, stream, count |
| `[reckon_db, read, exception]` | duration | store_id, stream, reason |
| `[reckon_db, health, probe]` | latency | store_id, node, status |
| `[reckon_db, health, change]` | - | store_id, node, old, new |

### Attaching Handlers

```elixir
# Elixir
:telemetry.attach_many(
  "reckon-db-metrics",
  [
    [:reckon_db, :append, :stop],
    [:reckon_db, :read, :stop],
    [:reckon_db, :health, :change]
  ],
  &MyApp.Metrics.handle_event/4,
  nil
)
```

```erlang
%% Erlang
telemetry:attach_many(
  <<"reckon-db-metrics">>,
  [
    [reckon_db, append, stop],
    [reckon_db, read, stop],
    [reckon_db, health, change]
  ],
  fun my_metrics:handle_event/4,
  undefined
).
```

## See Also

- [Storage Internals](storage_internals.md) - How data is stored
- [Cluster Consistency](cluster_consistency.md) - Consistency guarantees
- [Memory Pressure](memory_pressure.md) - Memory management
- [Snapshots](snapshots.md) - Snapshot configuration
