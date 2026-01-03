%% @doc EUnit tests for reckon_db_config module
%% @author rgfaber

-module(reckon_db_config_tests).

-include_lib("eunit/include/eunit.hrl").
-include("reckon_db.hrl").

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    %% Set up test configuration
    application:set_env(reckon_db, stores, [
        {test_store, [
            {data_dir, "/tmp/test_store"},
            {mode, single},
            {timeout, 10000},
            {writer_pool_size, 5},
            {reader_pool_size, 8}
        ]},
        {cluster_store, [
            {data_dir, "/tmp/cluster_store"},
            {mode, cluster}
        ]}
    ]),
    application:set_env(reckon_db, writer_pool_size, 10),
    application:set_env(reckon_db, reader_pool_size, 10),
    ok.

cleanup(_) ->
    application:unset_env(reckon_db, stores),
    application:unset_env(reckon_db, writer_pool_size),
    application:unset_env(reckon_db, reader_pool_size),
    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

config_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
        {"get_store_config returns configured store", fun() ->
            {ok, Config} = reckon_db_config:get_store_config(test_store),
            ?assertEqual(test_store, Config#store_config.store_id)
        end},
        {"get_store_config returns error for unknown store", fun() ->
            ?assertEqual({error, not_found}, reckon_db_config:get_store_config(unknown_store))
        end},
        {"get_all_store_configs returns all stores", fun() ->
            Configs = reckon_db_config:get_all_store_configs(),
            ?assertEqual(2, length(Configs)),
            StoreIds = [C#store_config.store_id || C <- Configs],
            ?assert(lists:member(test_store, StoreIds)),
            ?assert(lists:member(cluster_store, StoreIds))
        end},
        {"store config has correct fields", fun() ->
            {ok, Config} = reckon_db_config:get_store_config(test_store),
            ?assertEqual(test_store, Config#store_config.store_id),
            ?assertEqual("/tmp/test_store", Config#store_config.data_dir),
            ?assertEqual(single, Config#store_config.mode),
            ?assertEqual(10000, Config#store_config.timeout),
            ?assertEqual(5, Config#store_config.writer_pool_size),
            ?assertEqual(8, Config#store_config.reader_pool_size)
        end},
        {"cluster mode is parsed correctly", fun() ->
            {ok, Config} = reckon_db_config:get_store_config(cluster_store),
            ?assertEqual(cluster, Config#store_config.mode)
        end},
        {"default pool sizes are used when not specified", fun() ->
            %% cluster_store doesn't specify pool sizes, should use defaults
            {ok, Config} = reckon_db_config:get_store_config(cluster_store),
            ?assertEqual(10, Config#store_config.writer_pool_size),
            ?assertEqual(10, Config#store_config.reader_pool_size)
        end}
     ]}.

%%====================================================================
%% get_env Tests
%%====================================================================

get_env_test_() ->
    {setup,
     fun() -> application:set_env(reckon_db, test_key, test_value) end,
     fun(_) -> application:unset_env(reckon_db, test_key) end,
     [
        {"get_env returns value when set", fun() ->
            ?assertEqual(test_value, reckon_db_config:get_env(test_key, default))
        end},
        {"get_env returns default when not set", fun() ->
            ?assertEqual(default, reckon_db_config:get_env(unknown_key, default))
        end}
     ]}.
