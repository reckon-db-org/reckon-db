%% @doc DCB conditional-append, contended workload.
%%
%% Measures `reckon_db_dcb:append_if_no_tag_matches/4' behavior when
%% ALL workers contend on the SAME tag. This is the canonical
%% uniqueness / allocation / rate-limit pattern.
%%
%% Expected behavior:
%%
%%   - At any moment, exactly ONE worker's transaction is in-flight
%%     (Ra serializes them on the `?DCB_STREAM` consensus group).
%%   - The winner's append commits; the others see `context_changed`,
%%     update their local cutoff to the new latest seq, and retry on
%%     the next `run/2` call.
%%   - So success rate per worker is roughly `1 / num_workers`. The
%%     aggregate success rate across all workers is the store's
%%     sustained DCB write throughput.
%%
%% Both `{ok, _}` (commit) and `{error, {context_changed, _}}`
%% (conflict) count as "operations" in throughput. The interesting
%% comparison is throughput-in-conflict vs throughput-uncontended.
%% Massive conflict throughput with low commit throughput indicates
%% retry waste; conflicts cheaper than commits is expected because
%% conflicts abort early without writing.
%% @end
-module(dcb_append_contended).

-behaviour(reckon_bench_slice).

-export([describe/0, setup/1, run/2, teardown/2]).

-define(EVENT_TYPE, <<"bench.dcb_contended_v1">>).
-define(CONTENDED_TAG, <<"bench.dcb.contended.shared">>).

describe() ->
    #{
        question => <<
            "When N writers contend on the same DCB tag, what is the "
            "throughput floor (per-worker conflict rate vs commit rate)?"
        >>,
        units => #{
            throughput_ops_sec => <<"DCB append attempts/sec across workers">>,
            latency_ns_p50     => <<"median attempt latency, ns">>,
            latency_ns_p99     => <<"99th percentile attempt latency, ns">>
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
    #{
        store_id   => StoreId,
        data_bytes => Payload,
        %% last_seq starts at -1 ("saw nothing"). On each attempt the
        %% worker either succeeds (advance to the committed seq) or
        %% conflicts (advance to the conflict's max-seq so the next
        %% attempt has a fresh cutoff).
        last_seq   => -1,
        commits    => 0,
        conflicts  => 0
    }.

run(#{store_id   := Store,
      data_bytes := Payload,
      last_seq   := Cutoff} = State, _Scenario) ->
    Event = #{
        event_type => ?EVENT_TYPE,
        data       => #{payload => Payload},
        tags       => [?CONTENDED_TAG]
    },
    case reckon_db_dcb:append_if_no_tag_matches(
           Store, {any_of, [?CONTENDED_TAG]}, Cutoff, [Event]) of
        {ok, NewSeq} ->
            Commits = maps:get(commits, State) + 1,
            {ok, State#{last_seq => NewSeq, commits => Commits}};
        {error, {context_changed, MaxSeq}} ->
            %% Expected under contention. Update our cutoff and retry
            %% next iteration. The conflict still counts as an
            %% operation toward throughput numbers (retry latency
            %% matters as much as commit latency).
            Conflicts = maps:get(conflicts, State) + 1,
            {ok, State#{last_seq => MaxSeq, conflicts => Conflicts}};
        {error, _} = Error ->
            Error
    end.

teardown(_State, _Scenario) ->
    %% wipe_data.sh between runs.
    ok.
