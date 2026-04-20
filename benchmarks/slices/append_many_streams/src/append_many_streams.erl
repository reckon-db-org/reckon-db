%% @doc Append workload where each worker writes to its OWN stream.
%%
%% Tests the hypothesis that the single-core bottleneck we see on
%% `append_single_stream' is Ra's per-Raft-group serialization. If it
%% is, then N parallel streams (= N parallel Ra groups) should scale
%% linearly up to the number of scheduler threads.
%%
%% Each worker's first call to run/2 initialises its own stream_id —
%% workers run in independent Erlang processes, so they get independent
%% state.
-module(append_many_streams).

-behaviour(reckon_bench_slice).

-export([describe/0, setup/1, run/2, teardown/2]).

-define(STREAM_PREFIX, <<"bench.append_many_streams.">>).
-define(EVENT_TYPE,    <<"bench.appended_v1">>).
-define(ANY_VERSION,   -2).

describe() ->
    #{
        question => <<
            "Does throughput scale with parallel streams, or does Ra "
            "serialize at the store level?"
        >>,
        units => #{
            throughput_ops_sec => <<"appends/sec, summed across all workers">>
        },
        metrics => []
    }.

setup(Scenario) ->
    Size     = maps:get(event_size_bytes, Scenario, 256),
    StoreId  = maps:get(store_id,         Scenario, bench_store),
    Payload  = binary:copy(<<$x>>, Size),
    %% No stream_id here — each worker lazy-initialises its own on
    %% first call. setup/1 returns shared constants only.
    #{
        store_id   => StoreId,
        data_bytes => Payload,
        stream_id  => undefined,
        next_seq   => 0
    }.

run(#{stream_id := undefined} = State, Scenario) ->
    %% First call by this worker — pick a unique stream id.
    run(State#{stream_id => fresh_stream_id()}, Scenario);
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

teardown(_State, _Scenario) ->
    %% Workers own their own streams; this teardown only runs with the
    %% initial shared state (no stream). We could walk all created
    %% streams and delete them, but wipe_data.sh nukes everything
    %% between runs anyway.
    ok.

fresh_stream_id() ->
    <<?STREAM_PREFIX/binary,
      (integer_to_binary(erlang:unique_integer([positive])))/binary>>.
