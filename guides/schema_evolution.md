# Schema Evolution and Upcasting

Schema evolution enables changing event structures over time without breaking existing consumers. This guide covers schema registration, version management, and automatic upcasting.

## Overview

The `reckon_db_schema` module provides:

| Function | Purpose |
|----------|---------|
| `register/3` | Register a schema for an event type |
| `unregister/2` | Remove a schema |
| `get/2` | Get schema for an event type |
| `list/1` | List all registered schemas |
| `get_version/2` | Get current schema version |
| `upcast/2` | Upcast a list of events |
| `upcast_event/2` | Upcast a single event |
| `validate/2` | Validate event against schema |

## Architecture

![Schema Upcasting Flow](../assets/schema_upcasting.svg)

## Schema Registration

### Basic Registration

```erlang
ok = reckon_db_schema:register(my_store, <<"OrderPlaced">>, #{
    version => 1,
    description => <<"Initial order event schema">>
}).
```

### Registration with Upcasting

```erlang
ok = reckon_db_schema:register(my_store, <<"OrderPlaced">>, #{
    version => 3,
    upcast_from => #{
        1 => fun(Data) ->
            %% V1 -> V2: Add shipping_address field
            maps:put(shipping_address, maps:get(address, Data, #{}), Data)
        end,
        2 => fun(Data) ->
            %% V2 -> V3: Rename customer_id to buyer_id
            BuyerId = maps:get(customer_id, Data),
            maps:remove(customer_id, maps:put(buyer_id, BuyerId, Data))
        end
    }
}).
```

### Registration with Validation

```erlang
ok = reckon_db_schema:register(my_store, <<"PaymentReceived">>, #{
    version => 1,
    validator => fun(Data) ->
        case maps:is_key(amount, Data) andalso maps:is_key(currency, Data) of
            true -> ok;
            false -> {error, missing_required_fields}
        end
    end
}).
```

## Schema Storage

Schemas are stored in Khepri at:

```
[schemas, StoreId, EventType] -> schema_map()
```

### Schema Structure

```erlang
-type schema() :: #{
    event_type := binary(),       %% Event type name
    version := pos_integer(),     %% Current version (1, 2, 3, ...)
    upcast_from => #{             %% Version -> Transform function
        pos_integer() => fun((map()) -> map())
    },
    validator => fun((map()) -> ok | {error, term()}),
    description => binary(),
    registered_at := integer()    %% Timestamp
}.
```

## Upcasting

### How Upcasting Works

When reading events, upcasting transforms old versions to the current schema:

```erlang
%% Read events (may contain multiple versions)
{ok, Events} = reckon_db_streams:read(my_store, <<"orders-123">>, 0, 100, forward),

%% Upcast all events to current schema versions
UpcastedEvents = reckon_db_schema:upcast(my_store, Events).
```

### Upcasting Flow

```
Event (v1) ──▶ upcast_from[1] ──▶ Event (v2) ──▶ upcast_from[2] ──▶ Event (v3)
```

Events are upcasted through each version step sequentially.

### Version Detection

Event versions are stored in metadata:

```erlang
#event{
    metadata = #{
        schema_version => 2  %% Default: 1 if not present
    }
}
```

After upcasting, the `schema_version` is updated:

```erlang
%% Before: schema_version => 1
%% After upcast to v3: schema_version => 3
```

## Cluster Behavior

### Consistency

Schema registration is replicated through Khepri/Ra:

1. Schema registered on any node
2. Replicated through Raft consensus
3. Available on all cluster nodes

### Version Conflicts

If different nodes have different schema versions:

- Upcasting uses the local node's schema
- Ensure schema updates are coordinated
- Use rolling updates for schema changes

## Evolution Strategies

### 1. Additive Changes (Safe)

Add new optional fields:

```erlang
%% V1 -> V2: Add optional field with default
1 => fun(Data) ->
    maps:put(priority, <<"normal">>, Data)
end
```

### 2. Renaming Fields

Rename while preserving data:

```erlang
%% V1 -> V2: Rename customerId to customer_id
1 => fun(Data) ->
    CustomerId = maps:get(customerId, Data),
    maps:remove(customerId, maps:put(customer_id, CustomerId, Data))
end
```

### 3. Splitting Fields

Split one field into multiple:

```erlang
%% V1 -> V2: Split name into first_name and last_name
1 => fun(Data) ->
    Name = maps:get(name, Data, <<"">>),
    [First | Rest] = binary:split(Name, <<" ">>),
    Last = iolist_to_binary(lists:join(<<" ">>, Rest)),
    Data2 = maps:remove(name, Data),
    maps:merge(Data2, #{first_name => First, last_name => Last})
end
```

### 4. Merging Fields

Combine multiple fields:

```erlang
%% V1 -> V2: Merge address fields into address map
1 => fun(Data) ->
    Address = #{
        street => maps:get(street, Data, <<"">>),
        city => maps:get(city, Data, <<"">>),
        zip => maps:get(zip, Data, <<"">>)
    },
    Data2 = maps:without([street, city, zip], Data),
    maps:put(address, Address, Data2)
end
```

### 5. Type Changes

Convert field types:

```erlang
%% V1 -> V2: Convert amount from cents (integer) to dollars (float)
1 => fun(Data) ->
    Cents = maps:get(amount, Data, 0),
    Dollars = Cents / 100.0,
    maps:put(amount, Dollars, Data)
end
```

## Validation

### On Write

Validate events before appending:

```erlang
Event = #event{event_type = <<"OrderPlaced">>, data = #{...}},
case reckon_db_schema:validate(my_store, Event) of
    ok ->
        reckon_db_streams:append(my_store, StreamId, ExpectedVersion, [Event]);
    {error, Reason} ->
        {error, {validation_failed, Reason}}
end
```

### Custom Validators

```erlang
validator => fun(Data) ->
    Amount = maps:get(amount, Data, 0),
    Currency = maps:get(currency, Data, undefined),

    case {Amount > 0, Currency =/= undefined} of
        {true, true} -> ok;
        {false, _} -> {error, {invalid_amount, Amount}};
        {_, false} -> {error, missing_currency}
    end
end
```

## Querying Schemas

### List All Schemas

```erlang
{ok, Schemas} = reckon_db_schema:list(my_store),
%% [#{event_type => <<"OrderPlaced">>, version => 3, registered_at => ...}, ...]
```

### Get Specific Schema

```erlang
{ok, Schema} = reckon_db_schema:get(my_store, <<"OrderPlaced">>),
%% #{event_type => <<"OrderPlaced">>, version => 3, upcast_from => #{...}, ...}
```

### Get Current Version

```erlang
{ok, Version} = reckon_db_schema:get_version(my_store, <<"OrderPlaced">>),
%% 3
```

## Telemetry

Schema operations emit telemetry:

```erlang
%% Registration/unregistration
%% Event: [reckon_db, schema, registered | unregistered]
%% Measurements: #{version => integer()}
%% Metadata: #{store_id => atom(), event_type => binary()}

%% Upcasting
%% Event: [reckon_db, schema, upcasted]
%% Measurements: #{duration => integer()}
%% Metadata: #{store_id => atom(), event_type => binary(),
%%             from_version => integer(), to_version => integer()}
```

## Best Practices

### 1. Never Remove Required Fields

```erlang
%% BAD: Field removed without migration
%% V1: #{order_id, customer_id, items}
%% V2: #{order_id, items}  -- customer_id removed!

%% GOOD: Deprecate, then remove in later version
%% V1 -> V2: Mark deprecated
%% V2 -> V3: Actually remove (after all consumers updated)
```

### 2. Make New Fields Optional

```erlang
%% GOOD: New field has default value
1 => fun(Data) ->
    maps:put_new(priority, <<"normal">>, Data)
end
```

### 3. Test Upcasting Thoroughly

```erlang
%% Test each version transition
test_v1_to_v2() ->
    V1Data = #{order_id => <<"123">>, customerId => <<"cust-1">>},
    V2Data = upcast_v1_to_v2(V1Data),
    ?assertEqual(<<"cust-1">>, maps:get(customer_id, V2Data)),
    ?assertNot(maps:is_key(customerId, V2Data)).
```

### 4. Version Incrementally

```erlang
%% GOOD: One version per change
%% V1 -> V2: Add field
%% V2 -> V3: Rename field
%% V3 -> V4: Change type

%% BAD: Multiple changes per version
%% V1 -> V2: Add field AND rename field AND change type
```

### 5. Document Schema Changes

```erlang
ok = reckon_db_schema:register(my_store, <<"OrderPlaced">>, #{
    version => 3,
    description => <<"V3: Renamed customer_id to buyer_id for consistency">>,
    upcast_from => #{...}
}).
```

## Error Handling

| Error | Cause | Resolution |
|-------|-------|------------|
| `{error, {invalid_version, V}}` | Version not positive integer | Use version >= 1 |
| `{error, not_found}` | Schema not registered | Register schema first |
| Upcast crash | Upcast function threw | Event returned unchanged, logged |

## See Also

- [Event Sourcing](event_sourcing.md) - Event design principles
- [Subscriptions](subscriptions.md) - Event consumption
- [Storage Internals](storage_internals.md) - Khepri path structure
