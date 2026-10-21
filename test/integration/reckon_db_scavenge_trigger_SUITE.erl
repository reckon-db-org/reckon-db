%% @doc Scavenging a stream that has an active subscription trigger must not
%% take the store down (rabbitmq/khepri#405) and the trigger must keep
%% delivering afterwards.
%%
%% A scavenge deletes tree nodes, and deleting a node fires the change
%% events every registered trigger is filtered on. Two unrelated defects
%% live on that path, and this suite catches both:
%%
%%   - khepri's own, the one this release exists for: on 0.17.2 through
%%     0.18.0 `does_path_match/4' badmatched on `node_not_found' when a
%%     DELETE was evaluated against a trigger whose filter has a
%%     node-inspecting condition (ours carry `#if_has_data{}'), and the
%%     badmatch was applied inside Ra's machine - the store stopped
%%     serving reads and appends, not just that one trigger. Fixed
%%     upstream in 0.17.8 / 0.18.1 / 0.19.0 (khepri#398, #405).
%%   - ours: 0.19 calls the trigger's stored procedure with a
%%     `#khepri_trigger{}' record where 0.18 passed the node's props map.
%%     Our proc badmapped on it; khepri catches a crashing sproc in
%%     `khepri_event_handler' and logs it, so this one does NOT poison the
%%     store - it silently stopped all trigger delivery, with no failing
%%     append and no failing read. `make_trigger_proc/2' accepts both
%%     shapes.
%%
%% Both clauses are needed: the shape is decided at trigger REGISTRATION,
%% so a trigger registered before a machine-version bump keeps firing with
%% the props map after it, and an upgraded store has triggers of both
%% shapes until reckon-db re-registers.
%%
%% This is the regression test that says reckon-db is on a version where
%% both hold.
-module(reckon_db_scavenge_trigger_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

%% CT callbacks
-export([
    all/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    scavenging_with_an_active_trigger_keeps_the_store_serving/1
]).

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [scavenging_with_an_active_trigger_keeps_the_store_serving].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(telemetry),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    PgScope = reckon_db_test_helpers:start_pg_scope(),
    [{pg_scope, PgScope} | Config].

end_per_suite(Config) ->
    reckon_db_test_helpers:stop_pg_scope(proplists:get_value(pg_scope, Config, already_running)),
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    TCStr = atom_to_list(TestCase),
    DataDir = "/tmp/reckon_db_scavenge_trigger_" ++ TCStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    StoreId = list_to_atom("scavenge_trigger_" ++ TCStr ++ "_" ++ Rand),
    case khepri:start(DataDir, StoreId) of
        {ok, _} ->
            ok = khepri:put(StoreId, [streams], #{}),
            ok = khepri:put(StoreId, [subscriptions], #{}),
            ok = khepri:put(StoreId, [procs], #{}),
            [{data_dir, DataDir}, {store_id, StoreId} | Config];
        {error, Reason} ->
            ct:fail("Failed to start Khepri: ~p", [Reason])
    end.

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    khepri:stop(StoreId),
    os:cmd("rm -rf " ++ proplists:get_value(data_dir, Config)),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc GIVEN a stream with a live subscription (so a trigger is
%%      registered on it) and delivered events
%%      WHEN the stream is scavenged, deleting nodes the trigger matches
%%      THEN the store keeps serving reads and appends
%%      AND the trigger keeps delivering the events appended after it
scavenging_with_an_active_trigger_keeps_the_store_serving(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"scavtrigger-001">>),
    SubName = <<"scavenge_trigger_sub">>,

    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName, #{subscriber => self()}),
    EmitterName = reckon_db_emitter_group:emitter_name(StoreId, SubKey),
    {ok, EmitterPid} = reckon_db_emitter:start_link(StoreId, SubKey, self(), EmitterName),

    try
        Before = [<<"before_scavenge_", (integer_to_binary(N))/binary>> || N <- lists:seq(1, 3)],
        {ok, _} = reckon_db_streams:append(StoreId, StreamId, -2,
                                           [event(T) || T <- Before]),
        receive_all(Before),

        %% Delete the first two versions. Their nodes are the ones the
        %% trigger is registered on, so this is the delete the crash came
        %% from.
        {ok, Result} = reckon_db_scavenge:scavenge(
                         StoreId, StreamId,
                         #{before_version => 2, require_snapshot => false}),
        %% With before_version => 2 the cutoff is version 1, so exactly
        %% versions 0 and 1 go. Asserting the count keeps this a test of the
        %% delete path the trigger fires on: a scavenge that deleted nothing
        %% never exercises it.
        ?assertMatch(#{deleted_count := 2, deleted_versions := {0, 1}}, Result),

        %% The store still serves. A poisoned machine fails here, not in the
        %% trigger: this is the difference between "one subscription broke"
        %% and "the store is gone".
        ok = khepri:put(StoreId, [probe], still_serving),
        ?assertMatch({ok, still_serving}, khepri:get(StoreId, [probe])),

        %% And the trigger still fires: an append after the scavenge reaches
        %% the subscriber it was not the target of.
        {ok, _} = reckon_db_streams:append(StoreId, StreamId, -2,
                                           [event(<<"after_scavenge_1">>)]),
        receive_all([<<"after_scavenge_1">>])
    after
        gen_server:stop(EmitterPid),
        reckon_db_subscriptions:unsubscribe(StoreId, SubKey)
    end,
    ok.

%%====================================================================
%% Helpers
%%====================================================================

event(Type) ->
    #{event_type => Type, data => #{<<"k">> => Type}}.

receive_all(Types) ->
    receive_all(Types, 5000).

receive_all([], _Timeout) ->
    ok;
receive_all(Types, Timeout) ->
    receive
        {events, Events} when is_list(Events) ->
            Seen = [E#event.event_type || E <- Events],
            receive_all(Types -- Seen, Timeout)
    after Timeout ->
        ct:fail("Did not receive ~p within ~b ms", [Types, Timeout])
    end.
