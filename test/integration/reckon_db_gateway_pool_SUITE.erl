%% @doc Version-less appends through the gateway pool must not be refused.
%%
%% `reckon_gater_api:append_events/3' leaves the version out on purpose: the
%% caller wants the event appended after whatever is there. The gateway
%% worker used to read the stream's version and pass it back as an EXPLICIT
%% expected version, which is a different promise, and a stale one: a second
%% caller on another worker of the same pool can take that version between
%% the read and the append. The append is then refused with
%% `wrong_expected_version' although the caller never named a version, and
%% reckon-gater 3.11.2 reads that as permanent and does not retry it.
%%
%% Measured before the fix: a four-worker pool with eight callers answered
%% wrong_expected_version to 32 of 240 version-less appends. The worker now
%% appends with `ANY_VERSION' and the store's transaction decides.
-module(reckon_db_gateway_pool_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

-export([
    all/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([versionless_appends_through_the_pool_are_never_refused/1]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [versionless_appends_through_the_pool_are_never_refused].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    %% reckon_gater first: a gateway worker registers itself on start, and
    %% the pool half of this test routes through it.
    {ok, _} = application:ensure_all_started(reckon_gater),
    {ok, _} = application:ensure_all_started(reckon_db),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    TCStr = atom_to_list(TestCase),
    DataDir = "/tmp/reckon_db_gateway_pool_" ++ TCStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom("gateway_pool_" ++ TCStr ++ "_" ++ Rand),
    StoreConfig = #store_config{store_id = StoreId,
                                data_dir = DataDir,
                                mode = single,
                                writer_pool_size = 5,
                                reader_pool_size = 5,
                                gateway_pool_size = 4,
                                options = #{}},
    {ok, _} = reckon_db_sup:start_store(StoreConfig),
    [{data_dir, DataDir}, {store_id, StoreId} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = reckon_db_sup:stop_store(proplists:get_value(store_id, Config)),
    ok = file:del_dir_r(proplists:get_value(data_dir, Config)),
    ok.

%% Eight callers, thirty version-less appends each, through a four-worker
%% pool: every one is appended and the versions are contiguous.
versionless_appends_through_the_pool_are_never_refused(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"gateway-pool">>),
    Callers = 8,
    PerCaller = 30,

    Self = self(),
    Pids = [spawn_link(fun() ->
                           Self ! {done, self(),
                                   [reckon_gater_api:append_events(
                                      StoreId, StreamId, events(1))
                                    || _ <- lists:seq(1, PerCaller)]}
                       end)
            || _ <- lists:seq(1, Callers)],
    Results = lists:append([receive {done, Pid, R} -> R end || Pid <- Pids]),

    Total = Callers * PerCaller,
    Refused = [{error, Reason} || {error, Reason} <- Results],
    ct:pal("~p appends, ~p refused: ~p", [Total, length(Refused), Refused]),
    ?assertEqual([], Refused),
    ?assertEqual(Total, length([ok || {ok, _} <- Results])),

    {ok, Stored} = reckon_db_streams:read(StoreId, StreamId, 0, Total + 1, forward),
    ?assertEqual(lists:seq(0, Total - 1), [E#event.version || E <- Stored]),
    ok.

events(Count) ->
    [#{event_type => <<"gateway_pool_probe">>, data => #{<<"n">> => N}}
     || N <- lists:seq(1, Count)].
