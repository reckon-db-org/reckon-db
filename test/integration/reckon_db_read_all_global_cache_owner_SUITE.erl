%% @doc Integration test for read-all-global cache table ownership
%%
%% Verifies that `reckon_db_read_all_global_cache' (the ETS table
%% `reckon_db_streams:read_all_global/3' uses to avoid rescanning the
%% whole store on every global read) is owned by `reckon_db_sup' itself,
%% not by whichever gateway worker happens to call it first.
%%
%% Root cause of the bug: `ensure_cache_table/0' created the table
%% lazily, from whatever process first called `read_all_global/3'.
%% A `public' ETS table dies with its owner regardless of how many
%% other processes reference it by name — so a table created by a
%% short-lived gateway worker vanished the instant that worker exited
%% for ANY reason (including a routine one-off supervised restart
%% unrelated to this table), and the next reader's `ets:lookup/2' in
%% `cached_or_rebuilt/2' crashed with `{badarg, "the table identifier
%% does not refer to an existing ETS table"}'. Reproduced on every
%% single app boot in practice.
%%
%% The fix: `reckon_db_sup:init/1' now calls
%% `reckon_db_streams:ensure_cache_table/0' itself, before starting any
%% children — the table's owner becomes the supervisor, which outlives
%% every worker under it and only resets when the whole app does.
%%
%% @author rgfaber

-module(reckon_db_read_all_global_cache_owner_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(CACHE_TABLE, reckon_db_read_all_global_cache).

%% CT callbacks
-export([
    all/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1
]).

%% Test cases
-export([
    cache_table_exists_after_app_start/1,
    cache_table_owned_by_supervisor/1,
    cache_table_survives_transient_process_exit/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {seconds, 30}}].

all() ->
    [
        cache_table_exists_after_app_start,
        cache_table_owned_by_supervisor,
        cache_table_survives_transient_process_exit
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    RaDataDir = "/tmp/reckon_db_read_all_global_cache_owner_test_ra",
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

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc GIVEN reckon_db has started
%%      WHEN we look for the cache table
%%      THEN it already exists, without anyone ever calling
%%           read_all_global/3
cache_table_exists_after_app_start(_Config) ->
    ?assertNotEqual(undefined, ets:whereis(?CACHE_TABLE)),
    ok.

%% @doc GIVEN reckon_db has started
%%      WHEN we check who owns the cache table
%%      THEN it is reckon_db_sup, not some transient worker
%%
%%      This is the core regression assertion for the fix: the table
%%      must be anchored to a process that outlives every gateway
%%      worker, since those restart routinely during a store's own
%%      startup churn.
cache_table_owned_by_supervisor(_Config) ->
    Tid = ets:whereis(?CACHE_TABLE),
    ?assertNotEqual(undefined, Tid),

    SupPid = erlang:whereis(reckon_db_sup),
    ?assertNotEqual(undefined, SupPid),

    ?assertEqual(SupPid, ets:info(Tid, owner)),
    ok.

%% @doc GIVEN the cache table already exists, owned by reckon_db_sup
%%      WHEN a short-lived process calls ensure_cache_table/0 (as
%%           read_all_global/3 does on every call) and then exits
%%      THEN the table survives, still owned by reckon_db_sup
%%
%%      Before the fix, whichever process's ensure_cache_table/0 call
%%      won the creation race became the owner — so a transient
%%      worker's OWN routine exit could take the table down. This
%%      proves a transient caller's exit is now a non-event: it never
%%      had ownership to lose.
cache_table_survives_transient_process_exit(_Config) ->
    Tid = ets:whereis(?CACHE_TABLE),
    ?assertNotEqual(undefined, Tid),
    SupPid = erlang:whereis(reckon_db_sup),

    {Pid, Ref} = spawn_monitor(fun() ->
        ok = reckon_db_streams:ensure_cache_table()
    end),

    receive
        {'DOWN', Ref, process, Pid, _Reason} -> ok
    after 5000 ->
        ct:fail("transient process did not exit")
    end,

    ?assertNotEqual(undefined, ets:whereis(?CACHE_TABLE)),
    ?assertEqual(SupPid, ets:info(ets:whereis(?CACHE_TABLE), owner)),
    ok.
