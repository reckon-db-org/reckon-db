%% @doc Integration test for leader-activated emitter pools
%%
%% Verifies that in single-node mode, the node_monitor detects the Ra
%% leader and activates the LeaderWorker, which starts emitter pools
%% for all subscriptions. This ensures event delivery works without
%% manual intervention.
%%
%% Root cause of the bug:
%%   reckon_db_node_monitor was only started under cluster_sup (cluster
%%   mode). In single mode, no leader detection ran, so emitter pools
%%   were never started and event delivery silently failed.
%%
%% The fix (two parts):
%%   1. Move node_monitor to system_sup so it runs in ALL modes
%%   2. In single mode, reschedule leader checks until Ra leader is
%%      detected (matching ex-esdb StoreCluster poll loop pattern)
%%
%% Full pipeline under test:
%%   store starts -> node_monitor detects leader -> LeaderWorker
%%   activates -> subscribe -> LeaderTracker starts emitter pool
%%   -> append -> Khepri trigger -> emitter_group:broadcast
%%   -> emitter worker -> subscriber
%%
%% @author rgfaber

-module(reckon_db_emitter_autostart_SUITE).

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
    leader_activates_in_single_mode/1,
    subscribe_starts_emitter_pool_after_leader/1,
    subscribe_emitter_joins_pg_group/1,
    subscribe_then_append_delivers_event/1,
    subscribe_by_event_type_delivers_event/1,
    subscribe_multiple_subscriptions_all_deliver/1,
    subscribe_with_pool_size_starts_correct_workers/1,
    subscribe_then_unsubscribe_stops_emitter_pool/1,
    duplicate_subscribe_returns_already_exists/1,
    dead_subscriber_stops_emitter_pool/1,
    health_monitor_reports_healthy/1,
    health_monitor_detects_missing_pool/1,
    bare_khepri_subscribe_no_emitter_pool/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {seconds, 30}}].

all() ->
    [
        leader_activates_in_single_mode,
        subscribe_starts_emitter_pool_after_leader,
        subscribe_emitter_joins_pg_group,
        subscribe_then_append_delivers_event,
        subscribe_by_event_type_delivers_event,
        subscribe_multiple_subscriptions_all_deliver,
        subscribe_with_pool_size_starts_correct_workers,
        subscribe_then_unsubscribe_stops_emitter_pool,
        duplicate_subscribe_returns_already_exists,
        dead_subscriber_stops_emitter_pool,
        health_monitor_reports_healthy,
        health_monitor_detects_missing_pool,
        bare_khepri_subscribe_no_emitter_pool
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    RaDataDir = "/tmp/reckon_db_autostart_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),

    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),

    %% pg scope is now supervised by reckon_db_sup (1.3.1 fix).
    %% Do NOT start it manually — reckon_db application handles it.

    %% Start reckon_db application (provides reckon_db_sup)
    case application:ensure_all_started(reckon_db) of
        {ok, _} -> ok;
        {error, {already_started, reckon_db}} -> ok
    end,

    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    RaDataDir = proplists:get_value(ra_data_dir, Config),
    os:cmd("rm -rf " ++ RaDataDir),
    ok.

init_per_testcase(bare_khepri_subscribe_no_emitter_pool, Config) ->
    %% This test uses a bare Khepri store without reckon_db sup tree
    init_bare_khepri_testcase(Config);
init_per_testcase(TestCase, Config) ->
    %% Use reckon_db_sup:start_store for full supervision tree
    init_full_store_testcase(TestCase, Config).

end_per_testcase(bare_khepri_subscribe_no_emitter_pool, Config) ->
    cleanup_bare_khepri_testcase(Config);
end_per_testcase(_TestCase, Config) ->
    cleanup_full_store_testcase(Config).

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc GIVEN a reckon_db store started in single mode
%%      WHEN we wait for leader activation
%%      THEN the leader worker reports active within a reasonable time
%%
%%      This is the core test for the node_monitor fix. Before the fix,
%%      node_monitor was only started in cluster mode, so leader
%%      activation never happened in single mode.
leader_activates_in_single_mode(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    %% Leader should activate within the check interval
    ok = wait_for_leader(StoreId, 10000),

    %% Verify leader worker is active
    ?assertEqual(true, reckon_db_leader:is_active(StoreId)),

    %% Verify this node is recognized as leader
    ?assertEqual(true, reckon_db_store_coordinator:is_leader(StoreId)),

    ok.

%% @doc GIVEN a store with active leader
%%      WHEN a process subscribes to a stream
%%      THEN an emitter pool process is started by the LeaderTracker
subscribe_starts_emitter_pool_after_leader(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$pool-start-001">>,
    SubName = <<"pool_start_test">>,

    ok = wait_for_leader(StoreId, 10000),

    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => self()}
    ),

    %% Allow LeaderTracker to process the subscription notification
    timer:sleep(200),

    %% Verify the emitter pool supervisor is running
    PoolName = reckon_db_emitter_pool:name(StoreId, SubKey),
    ?assertNotEqual(undefined, whereis(PoolName)),

    %% Cleanup
    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%% @doc GIVEN a subscription with leader-started emitter pool
%%      WHEN we check the pg group for the subscription
%%      THEN emitter workers have joined the group
subscribe_emitter_joins_pg_group(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$pg-join-001">>,
    SubName = <<"pg_join_test">>,

    ok = wait_for_leader(StoreId, 10000),

    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => self()}
    ),

    %% Give emitter workers time to register with pg
    timer:sleep(300),

    %% Verify emitter workers joined the pg group
    Members = reckon_db_emitter_group:members(StoreId, SubKey),
    ?assert(length(Members) > 0),

    %% Cleanup
    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%% @doc GIVEN a subscriber (no manual emitter creation)
%%      WHEN an event is appended to the subscribed stream
%%      THEN the subscriber receives the event automatically
%%
%%      This is the key regression test. Before the fix, this would
%%      hang because no emitter pool existed to deliver events.
subscribe_then_append_delivers_event(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$delivery-001">>,
    SubName = <<"delivery_test">>,

    ok = wait_for_leader(StoreId, 10000),

    %% Subscribe — emitter pool started by LeaderTracker
    {ok, _SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => self()}
    ),

    %% Allow emitter to join pg group
    timer:sleep(300),

    %% Append an event
    Event = #{
        event_type => <<"venture_initiated_v1">>,
        data => #{<<"venture_id">> => <<"v-001">>, <<"name">> => <<"Test">>}
    },
    {ok, _Version} = reckon_db_streams:append(StoreId, StreamId, -2, [Event]),

    %% Subscriber should receive the event
    receive
        {events, [ReceivedEvent]} ->
            ?assertEqual(<<"venture_initiated_v1">>, ReceivedEvent#event.event_type),
            ?assertEqual(StreamId, ReceivedEvent#event.stream_id),
            ReceivedData = ReceivedEvent#event.data,
            ?assertEqual(<<"v-001">>, maps:get(<<"venture_id">>, ReceivedData))
    after 5000 ->
        ct:fail("Subscriber did not receive event — leader activation or emitter startup failed")
    end,

    ok.

%% @doc GIVEN a subscriber filtering by event type
%%      WHEN matching and non-matching events are appended
%%      THEN only matching events are delivered
subscribe_by_event_type_delivers_event(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    TargetType = <<"division_designed_v1">>,
    SubName = <<"type_filter_test">>,

    ok = wait_for_leader(StoreId, 10000),

    {ok, _SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, event_type, TargetType, SubName,
        #{subscriber => self()}
    ),

    timer:sleep(300),

    %% Append a matching event
    MatchStream = <<"division$div-001">>,
    MatchEvent = #{
        event_type => <<"division_designed_v1">>,
        data => #{<<"division_id">> => <<"div-001">>}
    },
    {ok, _} = reckon_db_streams:append(StoreId, MatchStream, -2, [MatchEvent]),

    %% Append a non-matching event to a different stream
    OtherStream = <<"venture$v-002">>,
    OtherEvent = #{
        event_type => <<"venture_archived_v1">>,
        data => #{<<"venture_id">> => <<"v-002">>}
    },
    {ok, _} = reckon_db_streams:append(StoreId, OtherStream, -2, [OtherEvent]),

    %% Should receive the matching event
    receive
        {events, [ReceivedEvent]} ->
            ?assertEqual(<<"division_designed_v1">>, ReceivedEvent#event.event_type)
    after 5000 ->
        ct:fail("Did not receive matching event")
    end,

    %% Should NOT receive the non-matching event
    receive
        {events, [Unexpected]} ->
            ct:fail("Received unexpected event: ~p", [Unexpected#event.event_type])
    after 1000 ->
        ok
    end,

    ok.

%% @doc GIVEN two independent stream subscriptions
%%      WHEN events are appended to each stream
%%      THEN both subscribers receive their respective events
subscribe_multiple_subscriptions_all_deliver(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    Stream1 = <<"venture$multi-v1">>,
    Stream2 = <<"division$multi-d1">>,

    ok = wait_for_leader(StoreId, 10000),

    %% Two subscriptions for the same subscriber process
    {ok, _Key1} = reckon_db_subscriptions:subscribe(
        StoreId, stream, Stream1, <<"multi_sub_1">>,
        #{subscriber => self()}
    ),
    {ok, _Key2} = reckon_db_subscriptions:subscribe(
        StoreId, stream, Stream2, <<"multi_sub_2">>,
        #{subscriber => self()}
    ),

    timer:sleep(300),

    %% Append to both streams
    {ok, _} = reckon_db_streams:append(StoreId, Stream1, -2,
        [#{event_type => <<"from_stream_1_v1">>, data => #{}}]),
    {ok, _} = reckon_db_streams:append(StoreId, Stream2, -2,
        [#{event_type => <<"from_stream_2_v1">>, data => #{}}]),

    %% Collect both events
    Received = collect_events(2, 5000),
    ?assertEqual(2, length(Received)),

    ReceivedTypes = [E#event.event_type || E <- Received],
    ?assert(lists:member(<<"from_stream_1_v1">>, ReceivedTypes)),
    ?assert(lists:member(<<"from_stream_2_v1">>, ReceivedTypes)),

    ok.

%% @doc GIVEN a subscription with pool_size > 1
%%      WHEN the emitter pool starts via leader activation
%%      THEN the correct number of emitter workers join the pg group
subscribe_with_pool_size_starts_correct_workers(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$pool-size-test">>,
    SubName = <<"pool_size_3_test">>,
    PoolSize = 3,

    ok = wait_for_leader(StoreId, 10000),

    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => self(), pool_size => PoolSize}
    ),

    timer:sleep(500),

    %% Verify correct number of workers in pg group
    Members = reckon_db_emitter_group:members(StoreId, SubKey),
    ?assertEqual(PoolSize, length(Members)),

    %% Cleanup
    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%% @doc GIVEN a subscription with leader-started emitter pool
%%      WHEN the subscription is removed
%%      THEN the emitter pool is stopped
subscribe_then_unsubscribe_stops_emitter_pool(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$unsub-pool-stop">>,
    SubName = <<"unsub_pool_stop_test">>,

    ok = wait_for_leader(StoreId, 10000),

    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => self()}
    ),

    timer:sleep(200),

    %% Verify pool is running
    PoolName = reckon_db_emitter_pool:name(StoreId, SubKey),
    ?assertNotEqual(undefined, whereis(PoolName)),

    %% Unsubscribe
    ok = reckon_db_subscriptions:unsubscribe(StoreId, SubKey),

    %% Allow cleanup to propagate
    timer:sleep(200),

    %% Verify emitter group is empty
    Members = reckon_db_emitter_group:members(StoreId, SubKey),
    ?assertEqual([], Members),

    ok.

%% @doc GIVEN an existing subscription with emitter pool
%%      WHEN subscribe is called again with the same name
%%      THEN it returns already_exists and the original pool keeps running
duplicate_subscribe_returns_already_exists(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$dup-idempotent">>,
    SubName = <<"dup_idempotent_test">>,

    ok = wait_for_leader(StoreId, 10000),

    %% First subscription
    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => self()}
    ),

    timer:sleep(200),

    %% Remember the emitter pool pid
    PoolName = reckon_db_emitter_pool:name(StoreId, SubKey),
    OriginalPoolPid = whereis(PoolName),
    ?assertNotEqual(undefined, OriginalPoolPid),

    %% Duplicate subscription should fail
    Result = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => self()}
    ),
    ?assertMatch({error, {already_exists, SubName}}, Result),

    %% Original pool should still be running (same pid)
    ?assertEqual(OriginalPoolPid, whereis(PoolName)),

    %% Cleanup
    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%% @doc GIVEN a subscription with subscriber_pid
%%      WHEN the subscriber process dies and an event is delivered
%%      THEN the emitter pool is stopped (dead subscriber cleanup)
dead_subscriber_stops_emitter_pool(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$sub-death">>,
    SubName = <<"subscriber_death_test">>,

    ok = wait_for_leader(StoreId, 10000),

    %% Spawn a subscriber that will die
    Subscriber = spawn(fun() ->
        receive die -> ok end
    end),

    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => Subscriber}
    ),

    timer:sleep(300),

    %% Verify pool is running
    PoolName = reckon_db_emitter_pool:name(StoreId, SubKey),
    ?assertNotEqual(undefined, whereis(PoolName)),

    %% Kill the subscriber
    Subscriber ! die,
    timer:sleep(100),
    ?assertEqual(false, is_process_alive(Subscriber)),

    %% Append an event — this triggers delivery to dead subscriber,
    %% which should stop the emitter pool
    Event = #{
        event_type => <<"test_event_v1">>,
        data => #{<<"key">> => <<"value">>}
    },
    {ok, _} = reckon_db_streams:append(StoreId, StreamId, -2, [Event]),

    %% Wait for the async pool stop to complete
    timer:sleep(500),

    %% Emitter pool should be stopped
    ?assertEqual(undefined, whereis(PoolName)),

    %% Cleanup subscription record
    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%% @doc GIVEN a store with active leader and healthy subscriptions
%%      WHEN health_check is called
%%      THEN the report shows all healthy
health_monitor_reports_healthy(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    ok = wait_for_leader(StoreId, 10000),

    %% Create a subscription with a live subscriber (self)
    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, <<"test$health-ok">>, <<"health_ok_test">>,
        #{subscriber => self()}
    ),

    timer:sleep(300),

    %% Run health check
    {ok, Report} = reckon_db_subscription_health:health_check(StoreId),

    %% Verify healthy report
    ?assertEqual(healthy, maps:get(status, Report)),
    ?assertEqual([], maps:get(stale_subscriptions, Report)),
    ?assertEqual([], maps:get(orphaned_pools, Report)),
    ?assertEqual([], maps:get(missing_pools, Report)),
    ?assert(maps:get(healthy_count, Report) > 0),

    %% Cleanup
    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%% @doc GIVEN a subscription whose emitter pool was stopped
%%      WHEN the health monitor runs
%%      THEN it detects the missing pool and restarts it
health_monitor_detects_missing_pool(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    ok = wait_for_leader(StoreId, 10000),

    %% Create a subscription
    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, <<"test$health-missing">>, <<"health_missing_test">>,
        #{subscriber => self()}
    ),

    timer:sleep(300),

    %% Verify pool is running
    PoolName = reckon_db_emitter_pool:name(StoreId, SubKey),
    ?assertNotEqual(undefined, whereis(PoolName)),

    %% Manually stop the pool (simulate a crash that wasn't recovered)
    reckon_db_emitter_pool:stop(StoreId, SubKey),
    timer:sleep(200),
    ?assertEqual(undefined, whereis(PoolName)),

    %% Health check should detect the missing pool
    {ok, Report1} = reckon_db_subscription_health:health_check(StoreId),
    ?assert(length(maps:get(missing_pools, Report1)) > 0),

    %% Trigger the periodic check manually (send the message)
    MonitorName = reckon_db_naming:health_monitor_name(StoreId),
    MonitorName ! check_health,
    timer:sleep(500),

    %% After cleanup, the pool should be restarted
    ?assertNotEqual(undefined, whereis(PoolName)),

    %% Health check should now report healthy
    {ok, Report2} = reckon_db_subscription_health:health_check(StoreId),
    ?assertEqual([], maps:get(missing_pools, Report2)),

    %% Cleanup
    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%% @doc GIVEN a bare Khepri store (no reckon_db supervision tree)
%%      WHEN a subscription is created
%%      THEN subscribe succeeds (subscription stored in Khepri)
%%           but no emitter pool exists (no emitter supervisor)
bare_khepri_subscribe_no_emitter_pool(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$bare-khepri">>,
    SubName = <<"bare_khepri_test">>,

    %% Subscribe — should succeed (stores subscription in Khepri)
    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => self()}
    ),

    ?assert(is_binary(SubKey)),
    ?assertEqual(true, reckon_db_subscriptions:exists(StoreId, SubKey)),

    %% Emitter pool should NOT be running (no emitter_sup)
    PoolName = reckon_db_emitter_pool:name(StoreId, SubKey),
    ?assertEqual(undefined, whereis(PoolName)),

    %% Cleanup
    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%%====================================================================
%% Internal: Test Case Setup Helpers
%%====================================================================

%% @private Full store setup with reckon_db supervision tree
init_full_store_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    TCStr = atom_to_list(TestCase),
    DataDir = "/tmp/reckon_db_autostart_" ++ TCStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    StoreId = list_to_atom("autostart_" ++ TCStr ++ "_" ++ Rand),

    StoreConfig = #store_config{
        store_id = StoreId,
        data_dir = DataDir,
        mode = single,
        writer_pool_size = 1,
        reader_pool_size = 1,
        gateway_pool_size = 1,
        options = #{}
    },

    case reckon_db_sup:start_store(StoreConfig) of
        {ok, _Pid} ->
            [{data_dir, DataDir}, {store_id, StoreId} | Config];
        {error, Reason} ->
            ct:fail("Failed to start store: ~p", [Reason])
    end.

%% @private Cleanup full store
cleanup_full_store_testcase(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    DataDir = proplists:get_value(data_dir, Config),
    reckon_db_sup:stop_store(StoreId),
    os:cmd("rm -rf " ++ DataDir),
    ok.

%% @private Bare Khepri store setup (no reckon_db sup tree)
init_bare_khepri_testcase(Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_autostart_bare_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    StoreId = list_to_atom("bare_khepri_" ++ Rand),

    case khepri:start(DataDir, StoreId) of
        {ok, _} ->
            khepri:put(StoreId, [streams], #{}),
            khepri:put(StoreId, [subscriptions], #{}),
            khepri:put(StoreId, [procs], #{}),
            [{data_dir, DataDir}, {store_id, StoreId} | Config];
        {error, Reason} ->
            ct:fail("Failed to start bare Khepri: ~p", [Reason])
    end.

%% @private Cleanup bare Khepri store
cleanup_bare_khepri_testcase(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    khepri:stop(StoreId),
    DataDir = proplists:get_value(data_dir, Config),
    os:cmd("rm -rf " ++ DataDir),
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

%% @private Collect N events from the mailbox
collect_events(N, Timeout) ->
    collect_events(N, Timeout, []).

collect_events(0, _Timeout, Acc) ->
    lists:reverse(Acc);
collect_events(N, Timeout, Acc) ->
    receive
        {events, Events} when is_list(Events) ->
            collect_events(N - length(Events), Timeout,
                           lists:reverse(Events) ++ Acc)
    after Timeout ->
        lists:reverse(Acc)
    end.
