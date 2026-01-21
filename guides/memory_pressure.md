# Memory Pressure Monitoring

Memory pressure monitoring enables adaptive behavior when system memory becomes constrained. This guide covers the monitoring architecture, pressure levels, and integration patterns.

## Overview

The `reckon_db_memory` module provides:

| Function | Purpose |
|----------|---------|
| `start_link/0,1` | Start the memory monitor |
| `level/0` | Get current pressure level |
| `configure/1` | Update thresholds |
| `on_pressure_change/1` | Register callback |
| `remove_callback/1` | Remove callback |
| `get_stats/0` | Get memory statistics |
| `check_now/0` | Force immediate check |

## Architecture

![Memory Pressure Levels](assets/memory_levels.svg)

## Pressure Levels

| Level | Default Threshold | Description |
|-------|-------------------|-------------|
| `normal` | < 70% | Full caching, all features enabled |
| `elevated` | 70-85% | Reduce cache sizes, flush more often |
| `critical` | > 85% | Pause non-essential operations, aggressive cleanup |

## Starting the Monitor

### Default Configuration

```erlang
{ok, _Pid} = reckon_db_memory:start_link().
```

### Custom Configuration

```erlang
{ok, _Pid} = reckon_db_memory:start_link(#{
    elevated_threshold => 0.60,    %% Trigger elevated at 60%
    critical_threshold => 0.80,    %% Trigger critical at 80%
    check_interval => 5000         %% Check every 5 seconds
}).
```

## Checking Pressure Level

### Current Level

```erlang
Level = reckon_db_memory:level(),
%% normal | elevated | critical
```

### With Statistics

```erlang
Stats = reckon_db_memory:get_stats(),
%% #{
%%     level => normal,
%%     memory_used => 2147483648,      %% bytes
%%     memory_total => 4294967296,     %% bytes
%%     last_check => 1735689600000,    %% timestamp
%%     callback_count => 3
%% }
```

## Registering Callbacks

### On Pressure Change

```erlang
CallbackRef = reckon_db_memory:on_pressure_change(fun(Level) ->
    case Level of
        normal ->
            logger:info("Memory pressure normalized"),
            restore_full_functionality();
        elevated ->
            logger:warning("Memory pressure elevated"),
            reduce_cache_sizes();
        critical ->
            logger:error("Memory pressure critical!"),
            pause_non_essential_operations()
    end
end).
```

### Removing Callback

```erlang
ok = reckon_db_memory:remove_callback(CallbackRef).
```

## Memory Detection

### How Memory is Measured

The monitor uses `memsup` (SASL memory supervisor):

```erlang
get_memory_info() ->
    MemData = memsup:get_system_memory_data(),
    case MemData of
        [] ->
            %% Fallback to erlang:memory()
            ErlangMem = erlang:memory(),
            Used = proplists:get_value(total, ErlangMem, 0),
            {Used, Used * 2};
        _ ->
            Total = proplists:get_value(total_memory, MemData, 0),
            Free = proplists:get_value(free_memory, MemData, 0),
            Cached = proplists:get_value(cached_memory, MemData, 0),
            Buffered = proplists:get_value(buffered_memory, MemData, 0),
            Available = Free + Cached + Buffered,
            Used = Total - Available,
            {Used, Total}
    end.
```

### Available Memory Calculation

Available memory includes:
- Free memory
- Cached memory (can be reclaimed)
- Buffered memory (can be reclaimed)

This provides a more accurate picture than just free memory.

## Integration Patterns

### 1. Subscription Backpressure

```erlang
%% In reckon_db_emitter or subscription handler
init(State) ->
    CallbackRef = reckon_db_memory:on_pressure_change(fun(Level) ->
        gen_server:cast(self(), {memory_pressure, Level})
    end),
    {ok, State#{memory_callback => CallbackRef}}.

handle_cast({memory_pressure, critical}, State) ->
    %% Pause subscription delivery
    {noreply, State#{paused => true}};
handle_cast({memory_pressure, _}, State) ->
    %% Resume
    {noreply, State#{paused => false}}.
```

### 2. Cache Management

```erlang
handle_info(check_cache, State) ->
    case reckon_db_memory:level() of
        critical ->
            ets:delete_all_objects(my_cache),
            logger:warning("Cache cleared due to memory pressure");
        elevated ->
            evict_oldest_entries(my_cache, 0.5);  %% Evict 50%
        normal ->
            ok
    end,
    {noreply, State}.
```

### 3. Write Throttling

```erlang
append_with_throttle(Store, Stream, Version, Events) ->
    case reckon_db_memory:level() of
        critical ->
            timer:sleep(100),  %% Slow down writes
            append_with_throttle(Store, Stream, Version, Events);
        elevated ->
            timer:sleep(10),   %% Minor throttle
            reckon_db_streams:append(Store, Stream, Version, Events);
        normal ->
            reckon_db_streams:append(Store, Stream, Version, Events)
    end.
```

## Configuration

### Dynamic Reconfiguration

```erlang
ok = reckon_db_memory:configure(#{
    elevated_threshold => 0.75,
    critical_threshold => 0.90,
    check_interval => 15000
}).
```

### Getting Current Config

```erlang
Config = reckon_db_memory:get_config(),
%% #{
%%     elevated_threshold => 0.70,
%%     critical_threshold => 0.85,
%%     check_interval => 10000
%% }
```

## Forcing Checks

```erlang
%% Force immediate memory check
CurrentLevel = reckon_db_memory:check_now().
```

Useful for:
- Testing pressure response
- After known memory-heavy operations
- Before starting large batch jobs

## Telemetry

Pressure changes emit telemetry:

```erlang
%% Event: [reckon_db, memory, pressure_changed]
%% Measurements:
%%   #{usage_ratio => float()}  %% 0.0 - 1.0
%% Metadata:
%%   #{old_level => atom(), new_level => atom()}
```

### Example Handler

```erlang
telemetry:attach(
    <<"memory-pressure-handler">>,
    [reckon_db, memory, pressure_changed],
    fun(_Event, #{usage_ratio := Ratio}, #{old_level := Old, new_level := New}, _Config) ->
        logger:notice("Memory pressure: ~p -> ~p (~.1f%)",
                      [Old, New, Ratio * 100])
    end,
    #{}
).
```

## Cluster Considerations

### Local Monitoring

Memory pressure is monitored locally on each node:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Node 1      │    │     Node 2      │    │     Node 3      │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ Memory      │ │    │ │ Memory      │ │    │ │ Memory      │ │
│ │ Monitor     │ │    │ │ Monitor     │ │    │ │ Monitor     │ │
│ │ (normal)    │ │    │ │ (elevated)  │ │    │ │ (normal)    │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

Each node manages its own memory pressure independently.

### Cross-Node Awareness

For cluster-wide memory awareness, consider:

```erlang
%% Broadcast pressure to other nodes
on_pressure_change(fun(Level) ->
    rpc:multicall(nodes(), my_module, handle_remote_pressure, [node(), Level])
end).
```

## Best Practices

### 1. Register Early

```erlang
%% In application start
start(_Type, _Args) ->
    {ok, _} = reckon_db_memory:start_link(),
    register_pressure_handlers(),
    ...
```

### 2. Graceful Degradation

```erlang
%% Implement graceful degradation, not hard failures
handle_pressure(critical) ->
    %% Don't crash, just slow down
    reduce_batch_sizes(),
    increase_flush_intervals(),
    pause_optional_projections();
handle_pressure(elevated) ->
    %% Warning mode
    reduce_cache_ttls();
handle_pressure(normal) ->
    restore_defaults().
```

### 3. Monitor Recovery

```erlang
%% Track recovery from pressure
handle_pressure_change(OldLevel, NewLevel) ->
    case {OldLevel, NewLevel} of
        {critical, elevated} ->
            logger:info("Memory recovering - still elevated");
        {critical, normal} ->
            logger:info("Memory fully recovered");
        {elevated, critical} ->
            logger:error("Memory degrading to critical");
        _ ->
            ok
    end.
```

### 4. Test Under Pressure

```erlang
%% In tests, simulate pressure
test_critical_behavior() ->
    %% Force critical level
    meck:new(reckon_db_memory, [passthrough]),
    meck:expect(reckon_db_memory, level, fun() -> critical end),

    %% Test behavior
    ?assertEqual(expected_behavior, my_module:do_something()),

    meck:unload(reckon_db_memory).
```

## SASL Configuration

For `memsup` to work, configure SASL:

```erlang
%% In sys.config
{sasl, [
    {sasl_error_logger, {file, "log/sasl.log"}},
    {errlog_type, error}
]},
{os_mon, [
    {start_memsup, true},
    {memsup_system_only, false},
    {memory_check_interval, 60}  %% OS check interval (seconds)
]}
```

## See Also

- [Subscriptions](subscriptions.md) - Event subscription with backpressure
- [Storage Internals](storage_internals.md) - Khepri memory usage
- [Scavenging](scavenging.md) - Free memory by removing old events
