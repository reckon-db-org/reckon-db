%% @doc DCB conditional-append, uncontended workload.
%%
%% Measures the per-write cost of `reckon_db_dcb:append_if_no_tag_matches/4'
%% when each worker uses its OWN unique tag (so no two workers contend
%% on the same context query). This isolates the cost of:
%%
%%   1. The Khepri transaction overhead (counter + tip read,
%%      conditional check, event + tag-index put, counter update)
%%   2. The tag-index path-write (one extra Khepri put per tag)
%%   3. Ra serialization on the single `?DCB_STREAM` pseudo-stream
%%
%% Comparison point: `append_single_stream` for the closest equivalent
%% non-DCB write. Expected differences:
%%
%%   - DCB has higher per-call latency (transaction + tag-index write)
%%   - DCB throughput is bounded by `?DCB_STREAM`'s Ra group (single
%%     consensus group for ALL DCB writes, contended or not), so
%%     uncontended parallel workers DON'T scale linearly the way
%%     `append_many_streams` does.
%%
%% Each worker tracks its own `last_seq` to maintain a correct cutoff:
%% the cutoff for the next write is the seq returned by the previous
%% one (or `-1` on first call). Since no other worker writes this
%% tag, the cutoff is always tight and every write succeeds.
%% @end
-module(dcb_append_uncontended).

-behaviour(reckon_bench_slice).

-export([describe/0, setup/1, run/2, teardown/2]).

-define(EVENT_TYPE, <<"bench.dcb_uncontended_v1">>).
-define(TAG_PREFIX, <<"bench.dcb.uncontended.">>).

describe() ->
    #{
        question => <<
            "What is the per-write cost of append_if_no_tag_matches "
            "when each writer has its own tag (no contention)?"
        >>,
        units => #{
            throughput_ops_sec => <<"DCB appends/sec, summed across workers">>,
            latency_ns_p50     => <<"median append_if_no_tag_matches latency, ns">>,
            latency_ns_p99     => <<"99th percentile append latency, ns">>
        },
        metrics => [
            throughput_ops_sec,
            latency_ns_p50,
            latency_ns_p90,
            latency_ns_p95,
            latency_ns_p99,
            latency_ns_p99_9,
            cpu_ms_per_op,
            memory_high_water_mb,
            disk_bytes_per_op
        ]
    }.

setup(Scenario) ->
    Size    = maps:get(event_size_bytes, Scenario, 256),
    StoreId = maps:get(store_id,         Scenario, bench_store),
    Payload = binary:copy(<<$x>>, Size),
    %% Worker-local state: each worker process gets its own setup/1
    %% call, so worker_tag is unique. `last_seq = -1` means "I've seen
    %% nothing yet" — first call uses cutoff=-1.
    #{
        store_id   => StoreId,
        worker_tag => fresh_tag(),
        data_bytes => Payload,
        last_seq   => -1
    }.

run(#{store_id   := Store,
      worker_tag := Tag,
      data_bytes := Payload,
      last_seq   := Cutoff} = State, _Scenario) ->
    Event = #{
        event_type => ?EVENT_TYPE,
        data       => #{payload => Payload},
        tags       => [Tag]
    },
    case reckon_db_dcb:append_if_no_tag_matches(
           Store, {any_of, [Tag]}, Cutoff, [Event]) of
        {ok, NewSeq} ->
            {ok, State#{last_seq => NewSeq}};
        {error, _} = Error ->
            %% Should not happen in uncontended mode — no other worker
            %% writes this tag. If we see this, the harness will
            %% surface it and the slice fails.
            Error
    end.

teardown(_State, _Scenario) ->
    %% wipe_data.sh cleans the store between runs; no per-worker
    %% cleanup needed.
    ok.

fresh_tag() ->
    <<?TAG_PREFIX/binary,
      (integer_to_binary(erlang:unique_integer([positive])))/binary>>.
