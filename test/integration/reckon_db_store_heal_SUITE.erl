%%% @doc reckon_db_store:ensure_khepri_started/1 self-heal.
%%%
%%% Regression for the cluster join-race: khepri_cluster:join resets the
%%% local store as part of joining, so a join interrupted mid-reset can
%%% leave the local Ra server gone. The coordinator then needs to restart
%%% the local store before it can retry the join, instead of looping
%%% forever on "not registered". This exercises that restart primitive on
%%% a single node by tearing down the Ra server and healing it.
-module(reckon_db_store_heal_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

%% Poll a predicate for up to ~5s.
-define(assertTimeout(Fun), assert_timeout(Fun, 50)).

-export([suite/0, all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([ensure_started_is_noop_when_up/1,
         ensure_started_heals_torn_down_store/1]).

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [ensure_started_is_noop_when_up,
     ensure_started_heals_torn_down_store].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    RaDataDir = "/tmp/reckon_db_store_heal_test_ra",
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
    RaDataDir = proplists:get_value(ra_data_dir, Config, "/tmp/reckon_db_store_heal_test_ra"),
    os:cmd("rm -rf " ++ RaDataDir),
    ok.

init_per_testcase(TC, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_store_heal_" ++ atom_to_list(TC) ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom("heal_" ++ atom_to_list(TC) ++ "_" ++ Rand),
    StoreConfig = #store_config{
        store_id = StoreId,
        data_dir = DataDir,
        mode = single,
        writer_pool_size = 1,
        reader_pool_size = 1,
        gateway_pool_size = 1,
        options = #{}
    },
    {ok, _} = reckon_db_sup:start_store(StoreConfig),
    ?assertTimeout(fun() -> is_pid(erlang:whereis(StoreId)) end),
    [{store_id, StoreId}, {data_dir, DataDir} | Config].

end_per_testcase(_TC, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    catch reckon_db_sup:stop_store(StoreId),
    os:cmd("rm -rf " ++ proplists:get_value(data_dir, Config)),
    ok.

%% A store whose Ra server is already registered is left untouched.
ensure_started_is_noop_when_up(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    Before = erlang:whereis(StoreId),
    ?assert(is_pid(Before)),
    ?assertEqual(ok, reckon_db_store:ensure_khepri_started(StoreId)),
    ?assert(is_pid(erlang:whereis(StoreId))).

%% Tear down the Ra server (as an interrupted join reset would) and heal
%% it: the store worker survives, and ensure_khepri_started re-registers
%% the local Ra server.
ensure_started_heals_torn_down_store(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    ?assert(is_pid(erlang:whereis(StoreId))),

    %% Simulate the reset: stop the local Khepri/Ra server. The
    %% reckon_db_store worker (registered under store_worker_name) stays up.
    ok = khepri:stop(StoreId),
    ?assertTimeout(fun() -> erlang:whereis(StoreId) =:= undefined end),
    ?assert(is_pid(erlang:whereis(reckon_db_naming:store_worker_name(StoreId)))),

    %% Heal.
    ?assertEqual(ok, reckon_db_store:ensure_khepri_started(StoreId)),
    ?assertTimeout(fun() -> is_pid(erlang:whereis(StoreId)) end),

    %% And it is a working store again — a Khepri read round-trips.
    ?assertMatch({ok, N} when is_integer(N),
                 reckon_db_streams:global_event_count(StoreId)).

assert_timeout(_Fun, 0) -> ct:fail("condition not met within timeout");
assert_timeout(Fun, N) ->
    case catch Fun() of
        true -> ok;
        _    -> timer:sleep(100), assert_timeout(Fun, N - 1)
    end.
