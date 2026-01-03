%% @doc EUnit tests for reckon_db_naming module
%% @author Macula.io

-module(reckon_db_naming_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Supervisor Names Tests
%%====================================================================

system_sup_name_test() ->
    ?assertEqual(reckon_db_system_my_store, reckon_db_naming:system_sup_name(my_store)),
    ?assertEqual(reckon_db_system_test, reckon_db_naming:system_sup_name(test)).

core_sup_name_test() ->
    ?assertEqual(reckon_db_core_my_store, reckon_db_naming:core_sup_name(my_store)),
    ?assertEqual(reckon_db_core_default, reckon_db_naming:core_sup_name(default)).

persistence_sup_name_test() ->
    ?assertEqual(reckon_db_persistence_my_store, reckon_db_naming:persistence_sup_name(my_store)).

notification_sup_name_test() ->
    ?assertEqual(reckon_db_notification_my_store, reckon_db_naming:notification_sup_name(my_store)).

cluster_sup_name_test() ->
    ?assertEqual(reckon_db_cluster_my_store, reckon_db_naming:cluster_sup_name(my_store)).

gateway_sup_name_test() ->
    ?assertEqual(reckon_db_gateway_my_store, reckon_db_naming:gateway_sup_name(my_store)).

streams_sup_name_test() ->
    ?assertEqual(reckon_db_streams_my_store, reckon_db_naming:streams_sup_name(my_store)).

emitter_sup_name_test() ->
    ?assertEqual(reckon_db_emitter_my_store, reckon_db_naming:emitter_sup_name(my_store)).

leader_sup_name_test() ->
    ?assertEqual(reckon_db_leader_my_store, reckon_db_naming:leader_sup_name(my_store)).

%%====================================================================
%% Worker Names Tests
%%====================================================================

store_name_test() ->
    ?assertEqual(my_store, reckon_db_naming:store_name(my_store)).

store_mgr_name_test() ->
    ?assertEqual(reckon_db_store_mgr_my_store, reckon_db_naming:store_mgr_name(my_store)).

subscriptions_store_name_test() ->
    ?assertEqual(reckon_db_subscriptions_my_store, reckon_db_naming:subscriptions_store_name(my_store)).

snapshots_store_name_test() ->
    ?assertEqual(reckon_db_snapshots_my_store, reckon_db_naming:snapshots_store_name(my_store)).

leader_name_test() ->
    ?assertEqual(reckon_db_leader_worker_my_store, reckon_db_naming:leader_name(my_store)).

leader_tracker_name_test() ->
    ?assertEqual(reckon_db_leader_tracker_my_store, reckon_db_naming:leader_tracker_name(my_store)).

discovery_name_test() ->
    ?assertEqual(reckon_db_discovery_my_store, reckon_db_naming:discovery_name(my_store)).

coordinator_name_test() ->
    ?assertEqual(reckon_db_coordinator_my_store, reckon_db_naming:coordinator_name(my_store)).

node_monitor_name_test() ->
    ?assertEqual(reckon_db_node_monitor_my_store, reckon_db_naming:node_monitor_name(my_store)).

%%====================================================================
%% Pool Names Tests
%%====================================================================

writer_pool_name_test() ->
    ?assertEqual(reckon_db_writer_pool_my_store, reckon_db_naming:writer_pool_name(my_store)).

reader_pool_name_test() ->
    ?assertEqual(reckon_db_reader_pool_my_store, reckon_db_naming:reader_pool_name(my_store)).

emitter_pool_name_test() ->
    Result = reckon_db_naming:emitter_pool_name(my_store, <<"sub_123">>),
    ?assertEqual('reckon_db_emitter_pool_my_store_sub_123', Result).

%%====================================================================
%% Group Names Tests
%%====================================================================

pg_group_name_test() ->
    ?assertEqual({my_store, streams}, reckon_db_naming:pg_group_name(my_store, streams)),
    ?assertEqual({default, emitters}, reckon_db_naming:pg_group_name(default, emitters)).

tracker_group_key_test() ->
    Key1 = reckon_db_naming:tracker_group_key(my_store, streams),
    Key2 = reckon_db_naming:tracker_group_key(my_store, emitters),
    %% Different features should produce different keys
    ?assertNotEqual(Key1, Key2),
    %% Same input should produce same key
    ?assertEqual(Key1, reckon_db_naming:tracker_group_key(my_store, streams)).

emitter_group_key_test() ->
    ?assertEqual({my_store, <<"sub_1">>, emitters},
                 reckon_db_naming:emitter_group_key(my_store, <<"sub_1">>)).
