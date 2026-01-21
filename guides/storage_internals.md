# Storage Internals

This guide covers the internal storage architecture of reckon-db, including Khepri path structures, data organization, and cluster replication behavior.

## Overview

reckon-db uses **Khepri** as its storage layer. Khepri is a tree-like key-value store built on Ra (Raft consensus), providing:

- Strong consistency across cluster nodes
- Automatic replication
- Tree-structured data organization
- Efficient prefix queries

## Architecture

![Khepri Storage Paths](assets/khepri_paths.svg)

## Khepri Path Structure

All data is organized under hierarchical paths:

```
[root]
├── [streams]
│   └── [StreamId]
│       └── [PaddedVersion] -> #event{}
├── [snapshots]
│   └── [StreamId]
│       └── [PaddedVersion] -> #snapshot{}
├── [subscriptions]
│   └── [SubscriptionName] -> #subscription{}
├── [schemas]
│   └── [StoreId]
│       └── [EventType] -> schema_map()
├── [links]
│   └── [StoreId]
│       └── [LinkName] -> #link{}
└── [metadata]
    └── [Key] -> Value
```

## Streams Storage

### Path Format

```erlang
?STREAMS_PATH ++ [StreamId, PaddedVersion]
%% Example: [streams, <<"orders-123">>, <<"000000000042">>]
```

### Version Padding

Versions are zero-padded to 12 characters for lexicographic ordering:

```erlang
-define(VERSION_PADDING, 12).

pad_version(Version, Length) ->
    VersionStr = integer_to_list(Version),
    Padding = Length - length(VersionStr),
    PaddedStr = lists:duplicate(Padding, $0) ++ VersionStr,
    list_to_binary(PaddedStr).

%% Examples:
%% 0 -> <<"000000000000">>
%% 42 -> <<"000000000042">>
%% 999999999999 -> <<"999999999999">>
```

This supports up to 999,999,999,999 events per stream (~317 years at 100 events/sec).

### Event Record

```erlang
-record(event, {
    event_id :: binary(),           %% UUID
    stream_id :: binary(),          %% Stream identifier
    version :: non_neg_integer(),   %% 0-indexed position
    event_type :: binary(),         %% Event type name
    data :: map(),                  %% Event payload
    metadata :: map(),              %% Event metadata
    epoch_us :: integer()           %% Timestamp (microseconds since epoch)
}).
```

### Reading Events

Events are read using Khepri path queries:

```erlang
%% Read specific version
khepri:get(StoreId, [streams, StreamId, PaddedVersion]).

%% Read range of versions
Pattern = [streams, StreamId, ?KHEPRI_WILDCARD_STAR],
khepri:get_many(StoreId, Pattern).
```

## Snapshots Storage

### Path Format

```erlang
?SNAPSHOTS_PATH ++ [StreamId, PaddedVersion]
%% Example: [snapshots, <<"orders-123">>, <<"000000000100">>]
```

### Snapshot Record

```erlang
-record(snapshot, {
    stream_id :: binary(),
    version :: non_neg_integer(),   %% Event version this snapshot represents
    state :: term(),                %% Serialized aggregate state
    metadata :: map(),
    created_at :: integer()         %% Timestamp
}).
```

### Latest Snapshot Query

```erlang
%% Get all snapshots for a stream, sorted by version descending
Pattern = [snapshots, StreamId, ?KHEPRI_WILDCARD_STAR],
{ok, Snapshots} = khepri:get_many(StoreId, Pattern),
%% Sort by version descending to get latest first
```

## Subscriptions Storage

### Path Format

```erlang
?SUBSCRIPTIONS_PATH ++ [SubscriptionName]
%% Example: [subscriptions, <<"my-projection">>]
```

### Subscription Record

```erlang
-record(subscription, {
    name :: binary(),
    type :: stream | event_type | event_pattern | event_payload,
    selector :: binary() | map(),
    handler :: pid() | function(),
    options :: map(),
    created_at :: integer()
}).
```

## Schema Registry Storage

### Path Format

```erlang
?SCHEMAS_PATH ++ [StoreId, EventType]
%% Example: [schemas, my_store, <<"OrderPlaced">>]
```

### Schema Structure

```erlang
#{
    event_type => <<"OrderPlaced">>,
    version => 3,
    upcast_from => #{
        1 => fun(Data) -> ... end,
        2 => fun(Data) -> ... end
    },
    validator => fun(Data) -> ok | {error, Reason} end,
    description => <<"Current order event schema">>,
    registered_at => 1735689600000
}
```

## Links Storage

### Path Format

```erlang
?LINKS_PATH ++ [StoreId, LinkName]
%% Example: [links, my_store, <<"high-value-orders">>]
```

### Link Record

```erlang
-record(link, {
    name :: binary(),
    source :: source_spec(),
    filter :: fun((event()) -> boolean()) | undefined,
    transform :: fun((event()) -> event()) | undefined,
    backfill :: boolean(),
    created_at :: integer(),
    status :: running | stopped | error,
    processed :: non_neg_integer(),
    last_event :: binary() | undefined
}).
```

## Cluster Replication

### Ra Consensus

All writes go through Raft consensus:

```
   Write Request
        │
        ▼
   ┌─────────┐
   │  Leader │ ◄─── All writes go here
   └────┬────┘
        │
   ┌────┴────┬────────┐
   │         │        │
   ▼         ▼        ▼
┌──────┐ ┌──────┐ ┌──────┐
│Follow│ │Follow│ │Follow│
│  1   │ │  2   │ │  3   │
└──────┘ └──────┘ └──────┘
```

### Consistency Guarantees

| Operation | Guarantee |
|-----------|-----------|
| Write (append) | Strongly consistent (quorum) |
| Read | Strongly consistent (from leader) |
| Cross-stream | No transaction (best effort) |

### Failover Behavior

1. Leader fails
2. Ra elects new leader (typically < 1 second)
3. Writes resume on new leader
4. In-flight writes may need retry

## Storage Operations

### Writing Events

```erlang
%% Append event
PaddedVersion = pad_version(Version, ?VERSION_PADDING),
Path = [streams, StreamId, PaddedVersion],
khepri:put(StoreId, Path, Event).
```

### Reading Events

```erlang
%% Read specific version
{ok, Event} = khepri:get(StoreId, [streams, StreamId, PaddedVersion]).

%% Read all events in stream
Pattern = [streams, StreamId, ?KHEPRI_WILDCARD_STAR],
{ok, EventsMap} = khepri:get_many(StoreId, Pattern).
```

### Deleting Events (Scavenging)

```erlang
%% Delete individual event
khepri:delete(StoreId, [streams, StreamId, PaddedVersion]).
```

### Listing Streams

```erlang
%% Get all stream IDs
Pattern = [streams, ?KHEPRI_WILDCARD_STAR],
{ok, StreamNodes} = khepri:get_many(StoreId, Pattern),
StreamIds = maps:keys(StreamNodes).
```

## Memory Considerations

### Khepri In-Memory

Khepri keeps data in memory for fast access:

- All paths and values are in memory
- Raft log is also in memory (up to snapshot interval)
- Consider total event size when planning capacity

### Memory Estimation

```erlang
%% Rough estimate per event
EventMemory = byte_size(term_to_binary(Event)),
TotalEvents = 1000000,
ApproxMemory = EventMemory * TotalEvents * 1.5,  %% 1.5x for overhead
```

### Reducing Memory

1. **Scavenging**: Remove old events
2. **Snapshots**: Enable state recovery without all events
3. **Archival**: Move to cold storage before scavenging

## Disk Persistence

### Ra Snapshots

Ra periodically snapshots the Raft log to disk:

```erlang
%% Default location
DataDir = application:get_env(ra, data_dir, "ra_data"),
%% Store-specific subdirectory
StoreDir = filename:join(DataDir, atom_to_list(StoreId)).
```

### Snapshot Interval

Configure via Ra settings:

```erlang
%% In sys.config
{ra, [
    {segment_max_entries, 65536},    %% Entries per segment
    {wal_max_size_bytes, 134217728}  %% 128MB WAL size
]}
```

## Querying Patterns

### Prefix Queries

```erlang
%% All events for a stream
[streams, <<"orders-123">>, ?KHEPRI_WILDCARD_STAR]

%% All snapshots for a stream
[snapshots, <<"orders-123">>, ?KHEPRI_WILDCARD_STAR]
```

### Existence Checks

```erlang
%% Check if stream exists
case khepri:exists(StoreId, [streams, StreamId]) of
    true -> stream_exists;
    false -> no_stream
end.
```

### Conditional Updates

```erlang
%% Optimistic concurrency via expected version
case khepri:get(StoreId, [streams, StreamId, PaddedExpectedVersion]) of
    {ok, _} ->
        %% Version exists, write next
        khepri:put(StoreId, [streams, StreamId, PaddedNextVersion], NewEvent);
    {error, {khepri, node_not_found, _}} ->
        {error, wrong_expected_version}
end.
```

## Performance Tips

### 1. Batch Writes

```erlang
%% Write multiple events in single Ra command
Events = [Event1, Event2, Event3],
khepri:transaction(StoreId, fun() ->
    lists:foreach(fun(E) ->
        khepri:put([streams, StreamId, pad_version(E#event.version)], E)
    end, Events)
end).
```

### 2. Use Snapshots

```erlang
%% Avoid replaying thousands of events
{ok, Snapshot} = load_latest_snapshot(StoreId, StreamId),
{ok, NewEvents} = read_events_since(StoreId, StreamId, Snapshot#snapshot.version),
State = apply_events(Snapshot#snapshot.state, NewEvents).
```

### 3. Monitor Memory

```erlang
%% Check Khepri memory usage
KhepriInfo = khepri:info(StoreId),
MemoryUsed = proplists:get_value(memory, KhepriInfo).
```

## Troubleshooting

### Common Issues

| Issue | Cause | Resolution |
|-------|-------|------------|
| Slow writes | No quorum | Check cluster health |
| High memory | Too many events | Enable scavenging |
| Stale reads | Reading during partition | Wait for partition heal |

### Diagnostic Commands

```erlang
%% Cluster status
ra:members(StoreId).

%% Leader info
khepri:get_leader(StoreId).

%% Store statistics
khepri:info(StoreId).
```

## See Also

- [Temporal Queries](temporal_queries.md) - Time-based event retrieval
- [Scavenging](scavenging.md) - Event lifecycle management
- [Memory Pressure](memory_pressure.md) - Memory monitoring
