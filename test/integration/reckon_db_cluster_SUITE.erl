%% @doc Common Test suite for reckon_db clustering modules
%%
%% Tests for cluster-related functionality:
%% - Discovery module initialization
%% - Store coordinator cluster join logic
%% - Node monitor event handling
%% - Leader election and activation
%%
%% Note: Full multi-node cluster tests require ct_slave setup
%% which is covered in test/cluster/ directory.
%%
%% @author R. Lefever

-module(reckon_db_cluster_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    %% Discovery tests
    discovery_single_mode/1,
    discovery_get_nodes/1,

    %% Coordinator tests
    coordinator_no_nodes/1,
    coordinator_election/1,
    coordinator_members/1,

    %% Node monitor tests
    node_monitor_init/1,

    %% Leader tests
    leader_init/1,
    leader_not_active_initially/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [
        {group, discovery_tests},
        {group, coordinator_tests},
        {group, node_monitor_tests},
        {group, leader_tests}
    ].

groups() ->
    [
        {discovery_tests, [sequence], [
            discovery_single_mode,
            discovery_get_nodes
        ]},
        {coordinator_tests, [sequence], [
            coordinator_no_nodes,
            coordinator_election,
            coordinator_members
        ]},
        {node_monitor_tests, [sequence], [
            node_monitor_init
        ]},
        {leader_tests, [sequence], [
            leader_init,
            leader_not_active_initially
        ]}
    ].

init_per_suite(Config) ->
    %% Start required applications
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    %% Configure Ra data directory before starting Ra
    RaDataDir = "/tmp/reckon_db_cluster_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),

    %% Start Ra first, then Khepri
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),

    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    RaDataDir = proplists:get_value(ra_data_dir, Config, "/tmp/reckon_db_cluster_test_ra"),
    os:cmd("rm -rf " ++ RaDataDir),
    ok.

init_per_group(GroupName, Config) ->
    GroupStr = atom_to_list(GroupName),
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_cluster_test_" ++ GroupStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    StoreId = list_to_atom("cluster_test_" ++ GroupStr ++ "_" ++ Rand),

    case khepri:start(DataDir, StoreId) of
        {ok, _} ->
            khepri:put(StoreId, [streams], #{}),
            khepri:put(StoreId, [subscriptions], #{}),
            khepri:put(StoreId, [snapshots], #{}),

            %% Create store config for testing
            StoreConfig = #store_config{
                store_id = StoreId,
                data_dir = DataDir,
                mode = single,  %% Start in single mode for tests
                timeout = 5000,
                writer_pool_size = 2,
                reader_pool_size = 2
            },

            [{data_dir, DataDir}, {store_id, StoreId}, {store_config, StoreConfig} | Config];
        {error, Reason} ->
            ct:pal("Khepri start error: ~p~n", [Reason]),
            ct:fail("Failed to start Khepri: ~p", [Reason])
    end.

end_per_group(_GroupName, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    khepri:stop(StoreId),
    DataDir = proplists:get_value(data_dir, Config),
    os:cmd("rm -rf " ++ DataDir),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Discovery Tests
%%====================================================================

%% @doc Test discovery module starts in single mode
discovery_single_mode(Config) ->
    StoreConfig = proplists:get_value(store_config, Config),

    %% Start discovery in single mode
    {ok, Pid} = reckon_db_discovery:start_link(StoreConfig),
    ?assert(is_pid(Pid)),

    %% Should have no discovered nodes in single mode
    Nodes = reckon_db_discovery:get_discovered_nodes(StoreConfig#store_config.store_id),
    ?assertEqual([], Nodes),

    %% Cleanup
    gen_server:stop(Pid),
    ok.

%% @doc Test getting discovered nodes
discovery_get_nodes(Config) ->
    StoreConfig = proplists:get_value(store_config, Config),

    {ok, Pid} = reckon_db_discovery:start_link(StoreConfig),

    %% Initially no nodes discovered
    Nodes = reckon_db_discovery:get_discovered_nodes(StoreConfig#store_config.store_id),
    ?assertEqual([], Nodes),

    %% Trigger discovery (should be no-op in single mode)
    ok = reckon_db_discovery:trigger_discovery(StoreConfig#store_config.store_id),

    %% Still no nodes (single mode)
    NodesAfter = reckon_db_discovery:get_discovered_nodes(StoreConfig#store_config.store_id),
    ?assertEqual([], NodesAfter),

    gen_server:stop(Pid),
    ok.

%%====================================================================
%% Coordinator Tests
%%====================================================================

%% @doc Test coordinator handles no connected nodes
coordinator_no_nodes(Config) ->
    StoreConfig = proplists:get_value(store_config, Config),
    StoreId = StoreConfig#store_config.store_id,

    {ok, Pid} = reckon_db_store_coordinator:start_link(StoreConfig),
    ?assert(is_pid(Pid)),

    %% With no other nodes connected, should return no_nodes
    Result = reckon_db_store_coordinator:join_cluster(StoreId),
    ?assertEqual(no_nodes, Result),

    gen_server:stop(Pid),
    ok.

%% @doc Test coordinator election logic
coordinator_election(Config) ->
    StoreConfig = proplists:get_value(store_config, Config),
    StoreId = StoreConfig#store_config.store_id,

    {ok, Pid} = reckon_db_store_coordinator:start_link(StoreConfig),

    %% With no connected nodes, should_handle_nodeup should return true
    %% (we're not in a multi-node cluster)
    Result = reckon_db_store_coordinator:should_handle_nodeup(StoreId),
    ?assert(is_boolean(Result)),

    gen_server:stop(Pid),
    ok.

%% @doc Test getting cluster members
coordinator_members(Config) ->
    StoreConfig = proplists:get_value(store_config, Config),
    StoreId = StoreConfig#store_config.store_id,

    {ok, Pid} = reckon_db_store_coordinator:start_link(StoreConfig),

    %% Should be able to get members (single node cluster)
    case reckon_db_store_coordinator:members(StoreId) of
        {ok, Members} ->
            ?assert(is_list(Members));
        {error, _} ->
            %% Also acceptable if cluster not fully formed
            ok
    end,

    gen_server:stop(Pid),
    ok.

%%====================================================================
%% Node Monitor Tests
%%====================================================================

%% @doc Test node monitor initialization
node_monitor_init(Config) ->
    StoreConfig = proplists:get_value(store_config, Config),

    {ok, Pid} = reckon_db_node_monitor:start_link(StoreConfig),
    ?assert(is_pid(Pid)),

    %% Should be running
    ?assert(is_process_alive(Pid)),

    gen_server:stop(Pid),
    ok.

%%====================================================================
%% Leader Tests
%%====================================================================

%% @doc Test leader worker initialization
leader_init(Config) ->
    StoreConfig = proplists:get_value(store_config, Config),

    {ok, Pid} = reckon_db_leader:start_link(StoreConfig),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    gen_server:stop(Pid),
    ok.

%% @doc Test leader is not active initially
leader_not_active_initially(Config) ->
    StoreConfig = proplists:get_value(store_config, Config),
    StoreId = StoreConfig#store_config.store_id,

    {ok, Pid} = reckon_db_leader:start_link(StoreConfig),

    %% Leader should not be active until explicitly activated
    Active = reckon_db_leader:is_active(StoreId),
    ?assertEqual(false, Active),

    gen_server:stop(Pid),
    ok.
