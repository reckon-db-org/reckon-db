%% @doc EUnit tests for reckon_db_store_registry module
%% @author rgfaber

-module(reckon_db_store_registry_tests).

-include_lib("eunit/include/eunit.hrl").
-include("reckon_db.hrl").

-define(PG_SCOPE, ?RECKON_DB_PG_SCOPE).
-define(TEST_TIMEOUT, 5000).

%%====================================================================
%% Test Setup/Teardown
%%====================================================================

%% @doc Setup function for test groups that need the registry running
setup() ->
    %% Start required apps
    application:ensure_all_started(crypto),
    %% Start pg scope
    case pg:start(?PG_SCOPE) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok
    end,
    %% Start the registry
    {ok, RegistryPid} = reckon_db_store_registry:start_link(),
    RegistryPid.

cleanup(RegistryPid) ->
    case is_process_alive(RegistryPid) of
        true ->
            unlink(RegistryPid),
            exit(RegistryPid, shutdown),
            timer:sleep(50);
        false ->
            ok
    end.

%%====================================================================
%% Test Generator
%%====================================================================

store_registry_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun test_start_link/1,
         fun test_announce_store/1,
         fun test_list_stores_empty/1,
         fun test_list_stores_with_store/1,
         fun test_get_store_info_found/1,
         fun test_get_store_info_not_found/1,
         fun test_unannounce_store/1,
         fun test_list_stores_on_node/1,
         fun test_announce_duplicate_replaces/1,
         fun test_remote_announce/1,
         fun test_remote_unannounce/1
     ]}.

%%====================================================================
%% Basic Tests
%%====================================================================

test_start_link(_RegistryPid) ->
    fun() ->
        %% Registry is already started by setup, verify it's registered
        ?assertNotEqual(undefined, whereis(reckon_db_store_registry))
    end.

test_announce_store(_RegistryPid) ->
    fun() ->
        Config = test_store_config(test_store_1),
        Result = reckon_db_store_registry:announce_store(test_store_1, Config),
        ?assertEqual(ok, Result)
    end.

test_list_stores_empty(_RegistryPid) ->
    fun() ->
        {ok, Stores} = reckon_db_store_registry:list_stores(),
        ?assertEqual([], Stores)
    end.

test_list_stores_with_store(_RegistryPid) ->
    fun() ->
        Config = test_store_config(test_store_2),
        ok = reckon_db_store_registry:announce_store(test_store_2, Config),
        {ok, Stores} = reckon_db_store_registry:list_stores(),
        ?assertEqual(1, length(Stores)),
        [Store] = Stores,
        ?assertEqual(test_store_2, maps:get(store_id, Store)),
        ?assertEqual(node(), maps:get(node, Store)),
        ?assertEqual(single, maps:get(mode, Store))
    end.

test_get_store_info_found(_RegistryPid) ->
    fun() ->
        Config = test_store_config(test_store_3),
        ok = reckon_db_store_registry:announce_store(test_store_3, Config),
        {ok, Info} = reckon_db_store_registry:get_store_info(test_store_3),
        ?assertEqual(test_store_3, maps:get(store_id, Info)),
        ?assertEqual(node(), maps:get(node, Info)),
        ?assertEqual(single, maps:get(mode, Info)),
        ?assertEqual("/tmp/test_store_3", maps:get(data_dir, Info)),
        ?assert(is_integer(maps:get(registered_at, Info)))
    end.

test_get_store_info_not_found(_RegistryPid) ->
    fun() ->
        Result = reckon_db_store_registry:get_store_info(nonexistent_store),
        ?assertEqual({error, not_found}, Result)
    end.

test_unannounce_store(_RegistryPid) ->
    fun() ->
        Config = test_store_config(test_store_4),
        ok = reckon_db_store_registry:announce_store(test_store_4, Config),
        {ok, [_]} = reckon_db_store_registry:list_stores(),

        ok = reckon_db_store_registry:unannounce_store(test_store_4),
        {ok, []} = reckon_db_store_registry:list_stores()
    end.

test_list_stores_on_node(_RegistryPid) ->
    fun() ->
        Config1 = test_store_config(test_store_5),
        Config2 = test_store_config(test_store_6),
        ok = reckon_db_store_registry:announce_store(test_store_5, Config1),
        ok = reckon_db_store_registry:announce_store(test_store_6, Config2),

        {ok, LocalStores} = reckon_db_store_registry:list_stores_on_node(node()),
        ?assertEqual(2, length(LocalStores)),

        {ok, RemoteStores} = reckon_db_store_registry:list_stores_on_node('nonexistent@node'),
        ?assertEqual([], RemoteStores)
    end.

test_announce_duplicate_replaces(_RegistryPid) ->
    fun() ->
        Config1 = test_store_config(test_store_7),
        ok = reckon_db_store_registry:announce_store(test_store_7, Config1),

        %% Announce again with same store_id (simulates restart)
        Config2 = (test_store_config(test_store_7))#store_config{
            data_dir = "/tmp/test_store_7_new"
        },
        ok = reckon_db_store_registry:announce_store(test_store_7, Config2),

        %% Should still have only one entry
        {ok, Stores} = reckon_db_store_registry:list_stores(),
        ?assertEqual(1, length(Stores)),

        %% Should have the new data_dir
        {ok, Info} = reckon_db_store_registry:get_store_info(test_store_7),
        ?assertEqual("/tmp/test_store_7_new", maps:get(data_dir, Info))
    end.

%%====================================================================
%% Remote Announcement Tests (via gen_server:cast)
%%====================================================================

test_remote_announce(_RegistryPid) ->
    fun() ->
        Config = test_store_config(remote_store_1),
        RemoteNode = 'remote@host',

        %% Simulate receiving a remote announcement
        gen_server:cast(reckon_db_store_registry,
                        {remote_announce, remote_store_1, Config, RemoteNode}),

        %% Give it a moment to process
        timer:sleep(50),

        {ok, Stores} = reckon_db_store_registry:list_stores(),
        ?assertEqual(1, length(Stores)),

        {ok, Info} = reckon_db_store_registry:get_store_info(remote_store_1),
        ?assertEqual(RemoteNode, maps:get(node, Info))
    end.

test_remote_unannounce(_RegistryPid) ->
    fun() ->
        Config = test_store_config(remote_store_2),
        RemoteNode = 'remote2@host',

        %% Add a remote store first
        gen_server:cast(reckon_db_store_registry,
                        {remote_announce, remote_store_2, Config, RemoteNode}),
        timer:sleep(50),

        {ok, [_]} = reckon_db_store_registry:list_stores(),

        %% Now unannounce it
        gen_server:cast(reckon_db_store_registry,
                        {remote_unannounce, remote_store_2, RemoteNode}),
        timer:sleep(50),

        {ok, []} = reckon_db_store_registry:list_stores()
    end.

%%====================================================================
%% Helper Functions
%%====================================================================

%% @private Create a test store config
-spec test_store_config(atom()) -> store_config().
test_store_config(StoreId) ->
    DataDir = "/tmp/" ++ atom_to_list(StoreId),
    #store_config{
        store_id = StoreId,
        data_dir = DataDir,
        mode = single,
        timeout = 5000
    }.
