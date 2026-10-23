# Stream Links and Projections

Stream links enable derived streams from source streams through filtering and transformation. This guide covers the server-side implementation, lifecycle management, and use cases.

## Overview

The `reckon_db_links` module provides:

| Function | Purpose |
|----------|---------|
| `create/2` | Create a new link definition |
| `delete/2` | Remove a link |
| `get/2` | Get link configuration |
| `list/1` | List all links |
| `start/2` | Start link processing |
| `stop/2` | Stop link processing |
| `info/2` | Get detailed link statistics |

## Architecture

![Stream Links Architecture](assets/stream_links.svg)

## What is a Stream Link?

A stream link is a derived stream that:

1. **Subscribes** to one or more source streams
2. **Filters** events based on a predicate function
3. **Transforms** events (optional) before writing
4. **Writes** matching events to a link stream

Link streams are named with a `$link:` prefix and behave like regular streams.

## Creating Links

### Basic Link

```erlang
ok = reckon_db_links:create(my_store, #{
    name => <<"high-value-orders">>,
    source => #{type => stream_pattern, pattern => <<"orders-*">>},
    filter => fun(E) -> maps:get(total, E#event.data, 0) > 1000 end
}).
```

### Link with Transform

```erlang
ok = reckon_db_links:create(my_store, #{
    name => <<"flagged-orders">>,
    source => #{type => stream_pattern, pattern => <<"orders-*">>},
    filter => fun(E) -> maps:get(total, E#event.data, 0) > 5000 end,
    transform => fun(E) ->
        NewData = maps:put(flagged, true, E#event.data),
        NewData2 = maps:put(flagged_at, erlang:system_time(millisecond), NewData),
        E#event{data = NewData2}
    end
}).
```

### Link with Backfill

Process existing events when starting:

```erlang
ok = reckon_db_links:create(my_store, #{
    name => <<"all-payments">>,
    source => #{type => stream_pattern, pattern => <<"payments-*">>},
    backfill => true  %% Process existing events on start
}).
```

## Source Specifications

### Single Stream

```erlang
source => #{type => stream, stream_id => <<"orders-018f6a7b8c9d4e5f60718293a4b5c6db">>}
```

### Stream Pattern (Wildcard)

```erlang
source => #{type => stream_pattern, pattern => <<"orders-*">>}
source => #{type => stream_pattern, pattern => <<"*-completed">>}
```

### All Streams

```erlang
source => #{type => all}
%% Excludes $link: streams to prevent loops
```

A link never follows its own stream. A `stream_pattern` link follows other
`$link:` streams only when its pattern starts with `$link:`, so a pattern
such as `*` does not pick up derived streams. Two such `$link:` pattern
links whose patterns match each other's streams feed each other without
end: every event one writes is a source event for the other.

`create/2` refuses a source it cannot follow with
`{error, {invalid_source, Source}}` and stores nothing: a `stream` source
needs a non-empty binary `stream_id` (not the link's own stream), a
`stream_pattern` source a non-empty binary `pattern`.

## Link Lifecycle

### Starting a Link

```erlang
ok = reckon_db_links:start(my_store, <<"high-value-orders">>).
```

This starts the link's consumer, one process per running link in the
cluster, which:
1. Subscribes to the source: the stream itself for a `stream` source,
   `$all` (with the source match applied per event) for `stream_pattern`
   and `all`
2. Sets the link status to `running`
3. Writes the backfill if `backfill => true`: every matching stream, read
   from its start
4. Follows the source live from there

If the link cannot subscribe, `start/2` answers the subscribe error and the
link keeps its status.

### Stopping a Link

```erlang
ok = reckon_db_links:stop(my_store, <<"high-value-orders">>).
```

This:
1. Ends the link's consumer and removes its subscription
2. Updates link status to `stopped`

A later `start/2` continues from where the link stopped, so the link also
gets the events the source received in between.

### Restarts and Crashes

The consumer stores, with the link, the last version it has handled in each
source stream. An event at or below it is passed over, and a gap is read
from the source stream before the next event is taken. So each source event
that passes the filter is in the link stream once:

- a consumer that crashes is restarted by its supervisor and picks up from
  the stored positions;
- a link stored as `running` is resumed when the store's leader activates,
  after a restart or a leadership change, including what the source
  received while it was down.

With the positions it stores the link-stream version its last append
reached. On start, every link-stream event past that version raises its
source stream's position, so an append that landed just before a crash,
with events from one source stream or several, is not written again.

### A Link That Cannot Progress

A transform that crashes or returns something other than an `#event{}`, or
an event in the link stream that does not name its source position, would
fail again on every restart. The link stops instead:

- it writes the events it took before that one, and nothing for that one:
  never the untransformed event, so a transform that redacts cannot leak
  what it was meant to remove;
- it removes its subscription and its status becomes `error`, with a
  `reason` that names the source position and not the event's data:
  `{transform_crashed, Stream, Version, Class}`,
  `{transform_returned_no_event, Stream, Version}` or
  `{unreadable_link_event, LinkStreamVersion}`;
- its consumer ends normally, so it is not restarted, and every other link
  in the store keeps running.

Fix the transform (recreate the link) and `start/2` it again. A filter that
crashes selects nothing for that event; the link goes on.

The derived event keeps the source event's id and type, and its metadata
names where it came from: `source_stream` and `source_version`.

### Checking Link Status

```erlang
{ok, Info} = reckon_db_links:info(my_store, <<"high-value-orders">>),
%% #{
%%     name => <<"high-value-orders">>,
%%     source => #{type => stream_pattern, pattern => <<"orders-*">>},
%%     status => running,
%%     processed => 1523,
%%     link_stream => <<"$link:high-value-orders">>,
%%     link_stream_version => 156,
%%     created_at => 1735689600000,
%%     last_event => <<"evt-789">>
%% }
```

A link with status `error` also carries `reason`, in `info/2`, `get/2` and
`list/1`.

## Khepri Storage

### Link Definitions

Links are stored at:

```
[links, StoreId, LinkName] -> #link{} record
```

### Link Streams

Link output is written to:

```
[streams, <<"$link:LinkName">>, PaddedVersion] -> #event{}
```

### Link Record Structure

```erlang
-record(link, {
    name :: binary(),
    source :: source_spec(),
    filter :: fun((event()) -> boolean()) | undefined,
    transform :: fun((event()) -> event()) | undefined,
    backfill :: boolean(),
    created_at :: integer(),
    status = stopped :: running | stopped | error,
    processed = 0 :: non_neg_integer(),
    last_event :: binary() | undefined,
    positions :: #{binary() => integer()} | undefined,
    link_version = -1 :: integer(),
    reason :: term()
}).
```

## Subscribing to Link Streams

Link streams are regular streams:

```erlang
%% Subscribe to the link stream
reckon_db_subscriptions:subscribe(
    my_store,
    stream,
    <<"$link:high-value-orders">>,
    <<"my-subscription">>,
    #{handler => fun handle_high_value_order/1}
).

%% Read from the link stream
{ok, Events} = reckon_db_streams:read(
    my_store,
    <<"$link:high-value-orders">>,
    0, 100, forward
).
```

## Event Transformation

### Preserved Metadata

When events are written to link streams, source information is preserved:

```erlang
event_to_map(#event{stream_id = S, version = V}, Derived) ->
    #{
        event_id => Derived#event.event_id,
        event_type => Derived#event.event_type,
        data => Derived#event.data,
        metadata => maps:merge(Derived#event.metadata, #{
            source_stream => S,
            source_version => V
        })
    }.
```

`Derived` is what the transform returned (the source event itself without
one). `source_stream` and `source_version` name the source event, whatever
the transform did to the event's own stream id and version.

### Custom Transforms

Transform functions receive the full event record:

```erlang
transform => fun(Event) ->
    %% Add computed field
    Data = Event#event.data,
    Total = maps:get(quantity, Data, 0) * maps:get(price, Data, 0),
    NewData = maps:put(computed_total, Total, Data),

    %% Return modified event
    Event#event{data = NewData}
end
```

## Pattern Matching

Wildcard patterns are converted to regex:

```erlang
wildcard_to_regex(Pattern) ->
    Escaped = re:replace(Pattern, <<"[.^$+?{}\\[\\]\\\\|()]">>,
                         <<"\\\\&">>, [global, {return, binary}]),
    Converted = binary:replace(Escaped, <<"*">>, <<".*">>, [global]),
    <<"^", Converted/binary, "$">>.
```

Examples:
- `orders-*` matches every `orders` stream, such as `orders-018f6a7b8c9d4e5f60718293a4b5c6db`
- `order*` matches both the `order` and the `orders` streams
- `*-018f*` matches streams of any prefix whose id starts with `018f`

A pattern is matched against whole stream ids, which always have the shape
`<prefix>-<32 hex>` (see [Stream Ids](https://hexdocs.pm/reckon_gater/stream_ids.html)).

## Use Cases

### 1. Event Type Aggregation

Collect all payment events across customer streams:

```erlang
ok = reckon_db_links:create(my_store, #{
    name => <<"all-payments">>,
    source => #{type => all},
    filter => fun(E) ->
        EventType = E#event.event_type,
        EventType =:= <<"PaymentReceived">> orelse
        EventType =:= <<"PaymentFailed">> orelse
        EventType =:= <<"RefundIssued">>
    end,
    backfill => true
}).
```

### 2. High-Value Transaction Monitoring

```erlang
ok = reckon_db_links:create(my_store, #{
    name => <<"high-value-txns">>,
    source => #{type => stream_pattern, pattern => <<"account-*">>},
    filter => fun(E) ->
        Amount = abs(maps:get(amount, E#event.data, 0)),
        Amount > 10000
    end,
    transform => fun(E) ->
        E#event{metadata = maps:put(flagged_reason, <<"high_value">>,
                                     E#event.metadata)}
    end
}).
```

### 3. Audit Trail

```erlang
ok = reckon_db_links:create(my_store, #{
    name => <<"security-audit">>,
    source => #{type => all},
    filter => fun(E) ->
        EventType = E#event.event_type,
        lists:member(EventType, [
            <<"UserLoggedIn">>,
            <<"PasswordChanged">>,
            <<"PermissionGranted">>,
            <<"DataExported">>
        ])
    end
}).
```

### 4. Real-Time Analytics Feed

```erlang
ok = reckon_db_links:create(my_store, #{
    name => <<"analytics-feed">>,
    source => #{type => all},
    filter => fun(_) -> true end,  %% All events
    transform => fun(E) ->
        %% Strip PII, keep only analytics-relevant fields
        SafeData = maps:with([event_type, timestamp, stream_id], #{
            event_type => E#event.event_type,
            timestamp => E#event.epoch_us,
            stream_id => E#event.stream_id
        }),
        E#event{data = SafeData}
    end
}).
```

## Telemetry

Link operations emit telemetry:

```erlang
%% Event: [reckon_db, link, created | started | stopped | deleted]
%% Measurements: #{system_time => integer()}
%% Metadata: #{store_id => atom(), link_name => binary()}
```

## Error Handling

| Error | Cause | Resolution |
|-------|-------|------------|
| `{error, not_found}` | Link does not exist | Verify link name |
| `{error, already_exists}` | Link name in use | Choose different name |
| Filter crash | Filter function threw exception | Returns `false` (event skipped) |
| Transform crash | Transform function threw exception | Link stops, status `error`, `reason` `{transform_crashed, ...}`; nothing written for that event |
| Transform returned no event | Transform returned something other than `#event{}` | Link stops, status `error`, `reason` `{transform_returned_no_event, ...}` |

## Best Practices

### 1. Use Defensive Filters

```erlang
filter => fun(E) ->
    try
        maps:get(amount, E#event.data, 0) > 1000
    catch
        _:_ -> false
    end
end
```

### 2. Avoid Expensive Transforms

Transforms run for every matching event. Keep them lightweight.

### 3. Consider Backfill Costs

Backfill reads every matching stream from its start, 500 events per read,
before the link follows live events (those wait in the consumer's mailbox
meanwhile). For large sources, consider whether the link needs the history
at all: without `backfill`, a new link starts at each stream's current
version.

### 4. Gateway Clients Do Not See Link Errors

Through the gateway, `create_link`, `start_link`, `stop_link` and
`delete_link` are casts: a refused source or a failed start is logged on
the store's node, not returned. Check the link with `get_link` or
`link_info` afterwards.

### 5. Monitor Processed Counts

```erlang
%% Check link is keeping up
{ok, Info} = reckon_db_links:info(my_store, <<"my-link">>),
Processed = maps:get(processed, Info),
%% Alert if falling behind
```

## See Also

- [Subscriptions](subscriptions.md) - Event subscription patterns
- [Storage Internals](storage_internals.md) - Khepri path structure
