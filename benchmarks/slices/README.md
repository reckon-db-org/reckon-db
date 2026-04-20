# Slice Pattern

Every slice is a vertical folder that owns its workload module, scenarios, and README. Required shape:

```
slices/<verb_subject>/
├── README.md
├── src/
│   └── <verb_subject>.erl        # implements reckon_bench_slice behaviour
└── scenarios/
    ├── baseline.eterm
    └── <scenario>.eterm
```

## Workload module contract

Implement the `reckon_bench_slice` behaviour (from `reckon-bench-harness`):

```erlang
-module(<verb_subject>).
-behaviour(reckon_bench_slice).
-export([describe/0, setup/1, run/2, teardown/2]).
```

Callbacks:

| Callback | Purpose |
|---|---|
| `describe/0` | Returns `#{question, units, metrics[, behaviours_exercised]}` |
| `setup/1` | Runs **once**, returns initial state each worker receives a copy of |
| `run/2` | Performs one operation; state is threaded **per worker only** |
| `teardown/2` | Runs **once**, receives initial state |

## State semantics

Runner threads state **per worker**, not globally:

- `setup/1` runs once before workers start; each worker starts with a **copy** of its return value.
- `run/2` evolves each worker's copy independently.
- `teardown/2` receives the initial state from `setup/1`, not any worker's final state.

What belongs in plain state: constants read by `run/2` (stream ids, payloads, store ids), per-worker counters that evolve independently (local `next_seq`).

What must NOT live in plain state: truly shared mutable data (e.g. a counter many workers increment). Maps are copy-on-write — workers cannot observe each other through the state map.

For shared-mutable data, create an ETS table in `setup/1` and stash the tid:

```erlang
setup(_Scenario) ->
    Shared = ets:new(bench_shared, [public, set, {write_concurrency, true}]),
    ets:insert(Shared, {next_seq, 0}),
    #{store_id => bench_store, stream_id => fresh_stream_id(), shared => Shared}.

run(#{store_id := Store, stream_id := Stream, shared := Shared} = State, _) ->
    Seq = ets:update_counter(Shared, next_seq, 1),
    {ok, _} = reckon_db_streams:append(Store, Stream, any,
        [#{event_type => <<"bench.appended_v1">>, data => #{seq => Seq}}]),
    {ok, State}.

teardown(#{shared := Shared}, _) ->
    ets:delete(Shared),
    ok.
```

Use ETS only when cross-worker visibility is actually needed. Most slices don't.

## Scenario files

Erlang term files consumed at run time:

```erlang
#{
    event_size_bytes => 256,
    parallelism      => 1,
    duration_seconds => 60,
    tags             => [baseline, steady_state]
}.
```

Tags flow into result metadata so analyses can filter / group.

## Naming

Verbs. Always verbs.

Good: `append_single_stream`, `read_event_by_id`, `fanout_to_subscribers`, `scale_cluster`, `measure_cost_per_event`.

Bad: `throughput_test`, `append_perf`, `basic_bench`, `performance_suite`.

## Canonical example

`slices/append_single_stream/` is the worked example — copy it as a template.
