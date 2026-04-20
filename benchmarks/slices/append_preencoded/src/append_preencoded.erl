%% @doc Pre-encoded event append — tests whether off-heap binary churn
%% is the GC driver.
%%
%% `append_single_stream` builds a FRESH event map on every run/2 call:
%%     #{event_type => <<"...">>, data => #{seq => N, payload => <<...>>}}
%% That creates:
%%   - a new outer map (2 keys)
%%   - a new inner map (2 keys)
%%   - a new integer for seq
%%   - passes the shared refc payload binary
%%
%% The per-call map allocations are cheap individually, but when we're
%% doing 20-80 of them per second AND passing them through 6 gen_server
%% mailboxes each, the combined allocation rate drives `sweep_off_heap'
%% to 22% of CPU.
%%
%% Hypothesis: build ONE event map in setup/1, reuse the same reference
%% every call. No per-call map allocation. No per-call binary
%% construction. Binary refc stays at 1 (shared across all append
%% mailbox hops) so GC has less sweep work per event.
%%
%% If throughput increases substantially vs append_single_stream, the
%% GC overhead was indeed caused by the per-call event construction —
%% and the fix is cheap: build events once, reuse. If throughput is
%% the same, the GC is elsewhere (likely inside ra/khepri themselves).
-module(append_preencoded).

-behaviour(reckon_bench_slice).

-export([describe/0, setup/1, run/2, teardown/2]).

-define(STREAM_PREFIX, <<"bench.append_preencoded.">>).
-define(EVENT_TYPE,    <<"bench.appended_v1">>).
-define(ANY_VERSION,   -2).

describe() ->
    #{
        question => <<
            "Does reusing ONE event map (pre-built in setup/1) cut GC "
            "pressure vs building a fresh event map per run?"
        >>,
        units   => #{},
        metrics => []
    }.

setup(Scenario) ->
    Size     = maps:get(event_size_bytes, Scenario, 256),
    StoreId  = maps:get(store_id,         Scenario, bench_store),
    StreamId = fresh_stream_id(),
    Payload  = binary:copy(<<$x>>, Size),
    %% Build the event ONCE. This same map reference will be sent
    %% every call — BEAM passes maps by reference within a process,
    %% and as part of a message the map header is copied but the
    %% refc-binary payload's refcount is just incremented.
    Event = #{
        event_type => ?EVENT_TYPE,
        data       => Payload  %% raw binary instead of nested map
    },
    #{
        store_id  => StoreId,
        stream_id => StreamId,
        event     => Event,
        n         => 0
    }.

run(#{store_id := Store, stream_id := Stream, event := Event, n := N} = State, _Scenario) ->
    {ok, _Version} = reckon_db_streams:append(Store, Stream, ?ANY_VERSION, [Event]),
    {ok, State#{n => N + 1}}.

teardown(#{store_id := Store, stream_id := Stream}, _Scenario) ->
    _ = reckon_db_streams:delete(Store, Stream),
    ok.

fresh_stream_id() ->
    <<?STREAM_PREFIX/binary,
      (integer_to_binary(erlang:unique_integer([positive])))/binary>>.
