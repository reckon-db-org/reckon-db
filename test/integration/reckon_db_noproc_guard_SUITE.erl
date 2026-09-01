%% @doc Tests for graceful handling of missing processes during startup/shutdown races.
%%
%% These tests verify the fixes for the "noproc crash" pattern that was
%% discovered when hecate-mail on beam01 crash-looped 160 times because a
%% stale subscription in Khepri referenced an emitter supervisor that
%% wasn't running yet. The same pattern existed in four other modules:
%%
%% 1. reckon_db_emitter_sup — start_emitter_pool/stop_emitter_pool
%% 2. reckon_db_streams_reader — start_new_reader
%% 3. reckon_db_streams_writer — start_new_writer
%% 4. reckon_db_store_coordinator — join_cluster/join_cluster/should_handle_nodeup
%% 5. reckon_db_discovery — get_discovered_nodes
%%
%% The pattern: calling supervisor:start_child/2 or gen_server:call/2,3
%% on a named process that isn't registered yet. Both functions throw
%% {exit, {noproc, ...}} as an EXCEPTION — they don't return {error, ...}.
%% Code that only handles {error, Reason} crashes.
%%
%% @author rgfaber

-module(reckon_db_noproc_guard_SUITE).

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
    %% Emitter supervisor guards
    emitter_sup_start_returns_error_when_not_running/1,
    emitter_sup_stop_returns_error_when_not_running/1,
    stale_subscription_does_not_crash_leader/1,
    stale_subscription_leader_still_activates/1,
    stale_subscription_alive_subscription_still_works/1,
    %% Streams reader/writer guards
    streams_reader_error_when_sup_not_running/1,
    streams_writer_error_when_sup_not_running/1,
    streams_reader_works_after_store_starts/1,
    streams_writer_works_after_store_starts/1,
    %% Store coordinator guards
    coordinator_join_cluster_returns_error_when_not_running/1,
    coordinator_join_cluster_node_returns_error_when_not_running/1,
    coordinator_should_handle_nodeup_false_when_not_running/1,
    coordinator_join_works_after_store_starts/1,
    %% Discovery guards
    discovery_get_nodes_returns_error_when_not_running/1,
    discovery_get_nodes_works_in_single_mode/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {seconds, 30}}].

all() ->
    [
     emitter_sup_start_returns_error_when_not_running,
     emitter_sup_stop_returns_error_when_not_running,
     stale_subscription_does_not_crash_leader,
     stale_subscription_leader_still_activates,
     stale_subscription_alive_subscription_still_works,
     streams_reader_error_when_sup_not_running,
     streams_writer_error_when_sup_not_running,
     streams_reader_works_after_store_starts,
     streams_writer_works_after_store_starts,
     coordinator_join_cluster_returns_error_when_not_running,
     coordinator_join_cluster_node_returns_error_when_not_running,
     coordinator_should_handle_nodeup_false_when_not_running,
     coordinator_join_works_after_store_starts,
     discovery_get_nodes_returns_error_when_not_running,
     discovery_get_nodes_works_in_single_mode
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    RaDataDir = "/tmp/reckon_db_noproc_guard_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),

    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),

    case application:ensure_all_started(reckon_db) of
        {ok, _} -> ok;
        {error, {already_started, reckon_db}} -> ok
    end,

    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    RaDataDir = proplists:get_value(ra_data_dir, Config),
    os:cmd("rm -rf " ++ RaDataDir),
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    TCStr = atom_to_list(TestCase),
    DataDir = "/tmp/reckon_db_noproc_" ++ TCStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    StoreId = list_to_atom("noproc_" ++ TCStr ++ "_" ++ Rand),

    StoreConfig = #store_config{
        store_id = StoreId,
        data_dir = DataDir,
        mode = single,
        writer_pool_size = 1,
        reader_pool_size = 1,
        gateway_pool_size = 1,
        options = #{}
    },

    [{data_dir, DataDir}, {store_id, StoreId}, {store_config, StoreConfig} | Config].

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    DataDir = proplists:get_value(data_dir, Config),
    catch reckon_db_sup:stop_store(StoreId),
    timer:sleep(200),
    os:cmd("rm -rf " ++ DataDir),
    ok.

%%====================================================================
%% Emitter Supervisor Guards
%%====================================================================

%% @doc GIVEN no emitter supervisor is running for a store
%%      WHEN start_emitter_pool is called
%%      THEN it returns {error, {emitter_sup_not_running, ...}}
%%           instead of throwing {exit, {noproc}}
emitter_sup_start_returns_error_when_not_running(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    SupName = reckon_db_naming:emitter_sup_name(StoreId),

    ?assertEqual(undefined, whereis(SupName)),

    FakeSubscription = #subscription{
        id = <<"fake_sub_001">>,
        type = by_stream,
        selector = <<"$all">>,
        subscription_name = <<"fake">>,
        subscriber_pid = self(),
        created_at = erlang:system_time(millisecond),
        pool_size = 1
    },

    Result = reckon_db_emitter_sup:start_emitter_pool(StoreId, FakeSubscription),
    ?assertMatch({error, {emitter_sup_not_running, _}}, Result),
    ok.

%% @doc GIVEN no emitter supervisor is running for a store
%%      WHEN stop_emitter_pool is called
%%      THEN it returns {error, {emitter_sup_not_running, ...}}
%%           instead of throwing {exit, {noproc}}
emitter_sup_stop_returns_error_when_not_running(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    SupName = reckon_db_naming:emitter_sup_name(StoreId),

    ?assertEqual(undefined, whereis(SupName)),

    Result = reckon_db_emitter_sup:stop_emitter_pool(StoreId, <<"fake_sub_002">>),
    ?assertMatch({error, {emitter_sup_not_running, _}}, Result),
    ok.

%% @doc GIVEN a store with a persisted subscription whose subscriber PID
%%      is dead (e.g. after a container restart)
%%      WHEN the leader activates and tries to start emitters for it
%%      THEN the leader does NOT crash — it logs a warning and continues
%%
%%      This is the regression test for the hecate-mail crash loop on
%%      beam01 (2026-09-01). A stale subscription persisted in Khepri
%%      had dead subscriber PIDs. supervisor:start_child threw {noproc},
%%      which crashed the leader worker, which crashed the store, which
%%      crashed the application — 160 restarts in a loop.
stale_subscription_does_not_crash_leader(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StoreConfig = proplists:get_value(store_config, Config),

    {ok, _} = reckon_db_sup:start_store(StoreConfig),
    ok = wait_for_leader(StoreId, 10000),

    StreamId = reckon_db_test_helpers:sid(<<"stale-crash-001">>),
    SubName = <<"stale_crash_test">>,
    DeadSubscriber = spawn(fun() -> receive die -> ok end end),

    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => DeadSubscriber}
    ),

    timer:sleep(300),

    DeadSubscriber ! die,
    timer:sleep(100),
    ?assertEqual(false, is_process_alive(DeadSubscriber)),

    reckon_db_sup:stop_store(StoreId),
    timer:sleep(500),

    {ok, _} = reckon_db_sup:start_store(StoreConfig),

    ok = wait_for_leader(StoreId, 10000),
    ?assertEqual(true, reckon_db_leader:is_active(StoreId)),

    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%% @doc GIVEN a store with a stale subscription that was restarted
%%      WHEN the leader activates
%%      THEN the leader reports active despite the stale subscription
stale_subscription_leader_still_activates(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StoreConfig = proplists:get_value(store_config, Config),

    {ok, _} = reckon_db_sup:start_store(StoreConfig),
    ok = wait_for_leader(StoreId, 10000),

    StreamId = reckon_db_test_helpers:sid(<<"stale-activate-001">>),
    SubName = <<"stale_activate_test">>,
    DeadSubscriber = spawn(fun() -> receive die -> ok end end),

    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => DeadSubscriber}
    ),

    timer:sleep(300),

    DeadSubscriber ! die,
    timer:sleep(100),

    reckon_db_sup:stop_store(StoreId),
    timer:sleep(500),

    {ok, _} = reckon_db_sup:start_store(StoreConfig),

    ok = wait_for_leader(StoreId, 10000),
    ?assertEqual(true, reckon_db_leader:is_active(StoreId)),

    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%% @doc GIVEN a store with both a stale and a live subscription
%%      WHEN the store is restarted and the leader activates
%%      THEN the stale subscription is skipped gracefully
%%           AND the live subscription's emitter pool starts successfully
%%           AND events are delivered to the live subscriber
stale_subscription_alive_subscription_still_works(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StoreConfig = proplists:get_value(store_config, Config),

    {ok, _} = reckon_db_sup:start_store(StoreConfig),
    ok = wait_for_leader(StoreId, 10000),

    %% Create a subscription with a subscriber that will die
    StaleStreamId = reckon_db_test_helpers:sid(<<"stale-mixed-001">>),
    DeadSubscriber = spawn(fun() -> receive die -> ok end end),
    {ok, StaleSubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StaleStreamId, <<"stale_mixed">>,
        #{subscriber => DeadSubscriber}
    ),

    timer:sleep(300),

    %% Kill the stale subscriber
    DeadSubscriber ! die,
    timer:sleep(100),

    %% Stop and restart the store
    reckon_db_sup:stop_store(StoreId),
    timer:sleep(500),
    {ok, _} = reckon_db_sup:start_store(StoreConfig),
    ok = wait_for_leader(StoreId, 10000),

    %% The leader should have activated despite the stale subscription
    ?assertEqual(true, reckon_db_leader:is_active(StoreId)),

    %% A NEW live subscription should work after restart
    LiveStreamId = reckon_db_test_helpers:sid(<<"live-mixed-001">>),
    {ok, LiveSubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, LiveStreamId, <<"live_mixed">>,
        #{subscriber => self()}
    ),

    timer:sleep(300),

    LiveEvent = #{
        event_type => <<"live_event_v1">>,
        data => #{<<"key">> => <<"value">>}
    },
    {ok, _} = reckon_db_streams:append(StoreId, LiveStreamId, -2, [LiveEvent]),

    receive
        {events, [Received]} ->
            ?assertEqual(<<"live_event_v1">>, Received#event.event_type)
    after 5000 ->
        ct:fail("Live subscription did not receive event — stale subscription broke the leader")
    end,

    %% Cleanup
    catch reckon_db_subscriptions:unsubscribe(StoreId, StaleSubKey),
    catch reckon_db_subscriptions:unsubscribe(StoreId, LiveSubKey),
    ok.

%%====================================================================
%% Streams Reader/Writer Guards
%%====================================================================

%% @doc GIVEN no streams supervisor is running for a store
%%      WHEN get_reader is called (which calls start_new_reader internally)
%%      THEN it raises an error tuple (not a noproc exception)
%%           that the caller can catch
streams_reader_error_when_sup_not_running(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    SupName = reckon_db_naming:streams_sup_name(StoreId),

    ?assertEqual(undefined, whereis(SupName)),

    Result = try
        reckon_db_streams_reader:get_reader(StoreId, <<"test-stream">>),
        no_error
    catch
        error:{streams_sup_not_running, _} -> caught_error;
        exit:{noproc, _} -> noproc_exception
    end,

    ?assertEqual(caught_error, Result),
    ok.

%% @doc GIVEN no streams supervisor is running for a store
%%      WHEN get_writer is called (which calls start_new_writer internally)
%%      THEN it raises an error tuple (not a noproc exception)
%%           that the caller can catch
streams_writer_error_when_sup_not_running(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    SupName = reckon_db_naming:streams_sup_name(StoreId),

    ?assertEqual(undefined, whereis(SupName)),

    Result = try
        reckon_db_streams_writer:get_writer(StoreId, <<"test-stream">>),
        no_error
    catch
        error:{streams_sup_not_running, _} -> caught_error;
        exit:{noproc, _} -> noproc_exception
    end,

    ?assertEqual(caught_error, Result),
    ok.

%% @doc GIVEN a running store with streams supervisor
%%      WHEN get_reader is called
%%      THEN a reader worker is started and can read events
streams_reader_works_after_store_starts(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StoreConfig = proplists:get_value(store_config, Config),

    {ok, _} = reckon_db_sup:start_store(StoreConfig),
    ok = wait_for_leader(StoreId, 10000),

    StreamId = reckon_db_test_helpers:sid(<<"reader-works-001">>),
    Event = #{
        event_type => <<"reader_test_v1">>,
        data => #{<<"key">> => <<"value">>}
    },
    {ok, _} = reckon_db_streams:append(StoreId, StreamId, -2, [Event]),

    Reader = reckon_db_streams_reader:get_reader(StoreId, StreamId),
    ?assert(is_pid(Reader)),
    ?assert(is_process_alive(Reader)),

    {ok, [ReadEvent]} = reckon_db_streams_reader:read(StoreId, StreamId, 0, 10, forward),
    ?assertEqual(<<"reader_test_v1">>, ReadEvent#event.event_type),
    ok.

%% @doc GIVEN a running store with streams supervisor
%%      WHEN get_writer is called
%%      THEN a writer worker is started and can append events
streams_writer_works_after_store_starts(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StoreConfig = proplists:get_value(store_config, Config),

    {ok, _} = reckon_db_sup:start_store(StoreConfig),
    ok = wait_for_leader(StoreId, 10000),

    StreamId = reckon_db_test_helpers:sid(<<"writer-works-001">>),

    Writer = reckon_db_streams_writer:get_writer(StoreId, StreamId),
    ?assert(is_pid(Writer)),
    ?assert(is_process_alive(Writer)),

    Event = #{
        event_type => <<"writer_test_v1">>,
        data => #{<<"key">> => <<"value">>}
    },
    {ok, _Version} = reckon_db_streams_writer:append(StoreId, StreamId, -2, [Event]),

    {ok, [ReadEvent]} = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),
    ?assertEqual(<<"writer_test_v1">>, ReadEvent#event.event_type),
    ok.

%%====================================================================
%% Store Coordinator Guards
%%====================================================================

%% @doc GIVEN no store coordinator is running
%%      WHEN join_cluster is called
%%      THEN it returns {error, not_started} instead of throwing {exit, {noproc}}
coordinator_join_cluster_returns_error_when_not_running(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    Name = reckon_db_naming:coordinator_name(StoreId),

    ?assertEqual(undefined, whereis(Name)),

    Result = reckon_db_store_coordinator:join_cluster(StoreId),
    ?assertEqual({error, not_started}, Result),
    ok.

%% @doc GIVEN no store coordinator is running
%%      WHEN join_cluster/2 (join specific node) is called
%%      THEN it returns {error, not_started} instead of throwing {exit, {noproc}}
coordinator_join_cluster_node_returns_error_when_not_running(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    Name = reckon_db_naming:coordinator_name(StoreId),

    ?assertEqual(undefined, whereis(Name)),

    Result = reckon_db_store_coordinator:join_cluster(StoreId, 'some_node@localhost'),
    ?assertEqual({error, not_started}, Result),
    ok.

%% @doc GIVEN no store coordinator is running
%%      WHEN should_handle_nodeup is called
%%      THEN it returns false instead of throwing {exit, {noproc}}
coordinator_should_handle_nodeup_false_when_not_running(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    Name = reckon_db_naming:coordinator_name(StoreId),

    ?assertEqual(undefined, whereis(Name)),

    Result = reckon_db_store_coordinator:should_handle_nodeup(StoreId),
    ?assertEqual(false, Result),
    ok.

%% @doc GIVEN a running store with coordinator (cluster mode)
%%      WHEN join_cluster is called
%%      THEN it returns a valid result (not {error, not_started})
%%           — in cluster mode with no peers, this is no_nodes
coordinator_join_works_after_store_starts(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    ClusterConfig = (proplists:get_value(store_config, Config))#store_config{mode = cluster},

    {ok, _} = reckon_db_sup:start_store(ClusterConfig),
    ok = wait_for_leader(StoreId, 10000),

    Name = reckon_db_naming:coordinator_name(StoreId),
    ?assertNotEqual(undefined, whereis(Name)),

    Result = reckon_db_store_coordinator:join_cluster(StoreId),
    ?assertNotEqual({error, not_started}, Result),
    ok.

%%====================================================================
%% Discovery Guards
%%====================================================================

%% @doc GIVEN no discovery process is running
%%      WHEN get_discovered_nodes is called
%%      THEN it returns {error, not_running} instead of throwing {exit, {noproc}}
discovery_get_nodes_returns_error_when_not_running(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    Name = reckon_db_naming:discovery_name(StoreId),

    ?assertEqual(undefined, whereis(Name)),

    Result = reckon_db_discovery:get_discovered_nodes(StoreId),
    ?assertEqual({error, not_running}, Result),
    ok.

%% @doc GIVEN a running store in cluster mode
%%      WHEN get_discovered_nodes is called
%%      THEN it returns a list (empty when no peers found) — not an error
discovery_get_nodes_works_in_single_mode(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    ClusterConfig = (proplists:get_value(store_config, Config))#store_config{mode = cluster},

    %% Discovery requires a cluster secret to start in cluster mode
    application:set_env(reckon_db, cluster_secret, <<"test-secret">>),

    {ok, _} = reckon_db_sup:start_store(ClusterConfig),
    ok = wait_for_leader(StoreId, 10000),

    Name = reckon_db_naming:discovery_name(StoreId),
    ?assertNotEqual(undefined, whereis(Name)),

    Result = reckon_db_discovery:get_discovered_nodes(StoreId),
    ?assert(is_list(Result)),

    application:unset_env(reckon_db, cluster_secret),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% @private Wait for leader activation by polling reckon_db_leader:is_active
wait_for_leader(StoreId, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_leader_loop(StoreId, Deadline).

wait_for_leader_loop(StoreId, Deadline) ->
    case reckon_db_leader:is_active(StoreId) of
        true ->
            ok;
        false ->
            Now = erlang:monotonic_time(millisecond),
            case Now >= Deadline of
                true ->
                    ct:fail("Leader did not activate within timeout (store: ~p)", [StoreId]);
                false ->
                    timer:sleep(100),
                    wait_for_leader_loop(StoreId, Deadline)
            end
    end.
