# Telemetry

ReckonDB is instrumented with [`telemetry`](https://hexdocs.pm/telemetry) — the
standard BEAM instrumentation library. Every significant operation emits a
telemetry event; you attach handlers to turn those events into logs, metrics,
traces, or alerts. Nothing leaves the node on its own: telemetry is in-process
pub/sub, and emitting an event for which no handler is attached costs a single
ETS lookup.

This guide covers how the mechanism works, how to attach handlers (including
metrics exporters), and the full event catalogue.

## How it works

An event is a list of atoms — a stable name like `[reckon_db, stream, write, stop]`.
When the operation happens, ReckonDB calls:

```erlang
telemetry:execute(Event, Measurements, Metadata).
```

- **Measurements** — a map of *numbers* (durations, counts, sizes). These feed
  counters and histograms.
- **Metadata** — a map of *context* (store_id, stream_id, reason). These become
  labels/tags you filter and group by.

A handler is a 4-arity function attached to one or more events. It runs
synchronously in the process that emitted the event, so handlers must be cheap
— do the heavy lifting (aggregation, network I/O) in a separate process, or use
an exporter library that already does.

## Quick start

Attach the built-in logger handler and you will see ReckonDB events in your
logs:

```erlang
ok = reckon_db_telemetry:attach_default_handler().
```

This is done automatically at application start when the `telemetry_handlers`
environment key includes `logger` (the default — see [Configuration](#configuration)).

### Attach your own handler

```erlang
reckon_db_telemetry:attach(my_metrics, fun my_app_metrics:handle/4, #{}).
```

`my_app_metrics:handle/4` receives `(EventName, Measurements, Metadata, Config)`
for every ReckonDB event. A minimal example that counts stream writes:

```erlang
handle([reckon_db, stream, write, stop], #{event_count := N}, #{store_id := Store}, _Cfg) ->
    my_counter:add({writes, Store}, N),
    ok;
handle(_Event, _Measurements, _Metadata, _Cfg) ->
    ok.
```

Detach when done:

```erlang
reckon_db_telemetry:detach(my_metrics).
```

## The facade

`reckon_db_telemetry` is the public entry point:

| Function | Purpose |
|---|---|
| `attach_default_handler/0` | Attach the built-in logger handler. |
| `detach_default_handler/0` | Remove it. |
| `attach/3` | Attach a custom `HandlerId, Fun, Config` across ReckonDB events. |
| `detach/1` | Remove a handler by id. |
| `emit/3` | Emit an event (thin wrapper over `telemetry:execute/3`). |
| `span/3` | Run a function bracketed by `:start`/`:stop` events with a duration. |

> **Note on `attach/3` and the default handler.** Both attach to the list
> returned by the internal `all_events/0`, which currently covers the core
> stream / subscription / snapshot / cluster / store / emitter events. Events in
> the extended categories below (temporal, scavenge, causation, backpressure,
> schema, memory, consistency, health, link) are *emitted* but are not in that
> list — to receive them, attach directly to the event name:
>
> ```erlang
> telemetry:attach(my_id, [reckon_db, consistency, quorum, lost], fun m:h/4, #{}).
> ```

## Metrics exporters

For a datacenter deployment, bridge telemetry to your metrics backend instead of
(or alongside) the logger:

- **Prometheus** — use [`telemetry_metrics_prometheus`](https://hexdocs.pm/telemetry_metrics_prometheus)
  to define counters/histograms over these events and scrape them.
- **OpenTelemetry** — use [`opentelemetry`](https://hexdocs.pm/opentelemetry) +
  a telemetry bridge to export spans/metrics via OTLP.
- **StatsD / custom** — attach a handler that forwards to your collector.

Because the event names are stable, exporter configuration is just a mapping
from event → metric. Example (Elixir `telemetry_metrics` style, shown for
reference):

```
counter("reckon_db.stream.write.stop", tags: [:store_id])
distribution("reckon_db.stream.write.stop.duration", unit: {:native, :microsecond})
last_value("reckon_db.consistency.quorum.lost")
```

## Configuration

At application start ReckonDB reads:

```erlang
{reckon_db, [
    {telemetry_handlers, [logger]}   %% default
]}.
```

- `[logger]` — attach the built-in logger handler (default).
- `[]` — attach nothing; emit events silently until you attach your own.
- Add your own atoms and extend `reckon_db_app`'s handler wiring, or simply call
  `reckon_db_telemetry:attach/3` from your own supervision tree.

## Event catalogue

All events, their measurements, and metadata. Durations are in microseconds
unless the key says otherwise.

### Stream

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, stream, write, start]` | `system_time` | `store_id, stream_id, event_count, expected_version` |
| `[reckon_db, stream, write, stop]` | `duration, event_count` | `store_id, stream_id, new_version` |
| `[reckon_db, stream, write, error]` | `duration` | `store_id, stream_id, reason` |
| `[reckon_db, stream, read, start]` | `system_time` | `store_id, stream_id, start_version, count, direction` |
| `[reckon_db, stream, read, stop]` | `duration, event_count` | `store_id, stream_id` |

### Subscription

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, subscription, created]` | `system_time` | `store_id, subscription_id, type, selector` |
| `[reckon_db, subscription, deleted]` | `system_time` | `store_id, subscription_id` |
| `[reckon_db, subscription, event_delivered]` | `duration` | `store_id, subscription_id, event_id, event_type` |
| `[reckon_db, subscription, checkpoint]` | `position` | `store_id, subscription_name` |
| `[reckon_db, subscription, backpressure, warning]` | `queue_size, max_size` | `store_id, subscription_key` |
| `[reckon_db, subscription, backpressure, dropped]` | `count` | `store_id, subscription_key, strategy` |

### Snapshot

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, snapshot, created]` | `system_time, size_bytes` | `store_id, stream_id, version` |
| `[reckon_db, snapshot, read]` | `duration, size_bytes` | `store_id, stream_id, version` |

### Store

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, store, started]` | `system_time` | `store_id, mode, data_dir` |
| `[reckon_db, store, stopped]` | `system_time, uptime_ms` | `store_id, reason` |

### Cluster

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, cluster, node, up]` | `system_time` | `store_id, node, member_count` |
| `[reckon_db, cluster, node, down]` | `system_time` | `store_id, node, member_count, reason` |
| `[reckon_db, cluster, leader, elected]` | `system_time` | `store_id, leader_node, previous_leader` |

> The `leader, elected` event fires on first-leader detection *and* every
> leadership change (see [Cluster Consistency](cluster_consistency.md)). It is
> the recommended hook for "run this singleton on whichever node currently
> leads store X" — attach, compare `leader_node` to `node()`, start/stop your
> work accordingly.

### Emitter

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, emitter, broadcast]` | `duration` | `store_id, subscription_id, event_id, recipient_count` |
| `[reckon_db, emitter, pool, created]` | `system_time` | `store_id, subscription_id, pool_size` |

### Temporal queries

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, temporal, read_until]` | `duration, event_count` | `store_id, stream_id, timestamp` |
| `[reckon_db, temporal, read_range]` | `duration, event_count` | `store_id, stream_id, timestamp ({from, to})` |

### Scavenge

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, scavenge, complete]` | `duration, deleted_count` | `store_id, stream_id, archived` |

### Causation

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, causation, query]` | `duration, event_count` | `store_id, id, query_type` |

### Schema

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, schema, registered]` | `version` | `store_id, event_type` |
| `[reckon_db, schema, unregistered]` | `version (0)` | `store_id, event_type` |
| `[reckon_db, schema, upcasted]` | `duration` | `store_id, event_type, from_version, to_version` |

### Memory pressure

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, memory, pressure_changed]` | `usage_ratio` | `old_level, new_level` |

### Consistency

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, consistency, check, complete]` | `duration_us` | `store_id, status, checks` |
| `[reckon_db, consistency, status, changed]` | `system_time` | `store_id, old_status, new_status` |
| `[reckon_db, consistency, split_brain, detected]` | `system_time` | `store_id, result` |
| `[reckon_db, consistency, quorum, lost]` | `system_time` | `store_id, available_nodes, required_quorum` |
| `[reckon_db, consistency, quorum, restored]` | `system_time` | `store_id, available_nodes, required_quorum` |

### Health prober

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, health, probe, complete]` | `duration_us, success_count, failure_count` | `store_id` |
| `[reckon_db, health, node, failed]` | `system_time, consecutive_failures` | `store_id, node` |
| `[reckon_db, health, node, recovered]` | `system_time` | `store_id, node` |

### Stream links

| Event | Measurements | Metadata |
|---|---|---|
| `[reckon_db, link, created]` | `system_time` | `store_id, link_name` |
| `[reckon_db, link, deleted]` | `system_time` | `store_id, link_name` |
| `[reckon_db, link, started]` | `system_time` | `store_id, link_name` |
| `[reckon_db, link, stopped]` | `system_time` | `store_id, link_name` |

## What to watch

A pragmatic starter dashboard:

- **Throughput / latency** — `stream.write.stop` and `stream.read.stop` counts +
  duration histograms, grouped by `store_id`.
- **Errors** — rate of `stream.write.error`.
- **Cluster health** — `consistency.quorum.lost` / `quorum.restored`,
  `cluster.leader.elected` (leadership churn), `health.node.failed`.
- **Backpressure** — `subscription.backpressure.warning` / `dropped` signal that
  a subscriber can't keep up.
- **Memory** — `memory.pressure_changed` transitions.

The event names are the source of truth; the definitive list with inline
measurement/metadata docs lives in `include/reckon_db_telemetry.hrl`.
