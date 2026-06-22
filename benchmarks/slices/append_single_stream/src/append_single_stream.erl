%% @doc Append-single-stream workload.
%%
%% Measures sustained throughput and latency of
%% `reckon_db_streams:append/4' against one stream on one store.
%%
%% Event shape follows `reckon_db_streams:new_event/0':
%%     #{event_type := binary(),
%%       data       := map() | binary(),
%%       metadata   => map(),
%%       tags       => [binary()],
%%       event_id   => binary()}
%%
%% The scenario's `event_size_bytes' controls the size of the `data'
%% payload (a binary of that many bytes of `$x').
-module(append_single_stream).

-behaviour(reckon_bench_slice).

-export([
    describe/0,
    setup/1,
    run/2,
    teardown/2
]).

-define(STREAM_TYPE,   <<"bench">>).
-define(EVENT_TYPE,    <<"bench.appended_v1">>).

%% ---------------------------------------------------------------
%% reckon_bench_slice callbacks
%% ---------------------------------------------------------------

describe() ->
    #{
        question => <<
            "How fast can we sustainably append to one stream via "
            "reckon_db_streams:append/4, and what does the p99 tail look like?"
        >>,
        units => #{
            throughput_ops_sec => <<"appends per second, sustained">>,
            latency_ns_p50     => <<"median append-call latency, nanoseconds">>,
            latency_ns_p99     => <<"99th percentile append-call latency, nanoseconds">>,
            cpu_ms_per_op      => <<"average CPU milliseconds per append">>,
            disk_bytes_per_op  => <<"average disk write-amplification per event">>
        },
        metrics => [
            throughput_ops_sec,
            latency_ns_p50,
            latency_ns_p90,
            latency_ns_p95,
            latency_ns_p99,
            latency_ns_p99_9,
            latency_ns_p99_99,
            cpu_ms_per_op,
            memory_high_water_mb,
            disk_bytes_per_op
        ]
    }.

setup(Scenario) ->
    Size     = maps:get(event_size_bytes, Scenario, 256),
    StoreId  = maps:get(store_id,         Scenario, bench_store),
    StreamId = fresh_stream_id(),
    Payload  = binary:copy(<<$x>>, Size),
    #{
        store_id   => StoreId,
        stream_id  => StreamId,
        data_bytes => Payload,
        next_seq   => 0
    }.

%% ExpectedVersion semantics (from reckon_db_streams:append/4 docs):
%%   -1 = NO_STREAM (first write)
%%   -2 = ANY_VERSION (no version check)
%%   N >= 0 = stream version must equal N
%% We use ANY_VERSION so parallel writers do not race on versions.
-define(ANY_VERSION, -2).

run(#{store_id   := Store,
      stream_id  := Stream,
      data_bytes := Payload,
      next_seq   := Seq} = State, _Scenario) ->
    Event = #{
        event_type => ?EVENT_TYPE,
        data       => #{seq => Seq, payload => Payload}
    },
    {ok, _Version} = reckon_db_streams:append(Store, Stream, ?ANY_VERSION, [Event]),
    {ok, State#{next_seq => Seq + 1}}.

teardown(#{store_id := Store, stream_id := Stream} = _State, _Scenario) ->
    %% Best-effort cleanup. The store itself is managed by the bench
    %% operator (started outside this slice); we only own the stream
    %% we wrote to.
    _ = reckon_db_streams:delete(Store, Stream),
    ok.

%% ---------------------------------------------------------------
%% Internals
%% ---------------------------------------------------------------

fresh_stream_id() ->
    reckon_gater_stream_id:new(?STREAM_TYPE).
