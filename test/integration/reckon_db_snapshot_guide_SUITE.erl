%% @doc The snapshot pattern reckon-gater's guides/snapshots.md publishes,
%% run end to end: a client module in the guide's shape, a real store, and
%% the store's own gateway worker.
%%
%% The guide had been compiled and never run, and every one of its defects
%% was invisible to a compiler: record_snapshot is a CAST, so a payload the
%% worker cannot unwrap kills the worker and still answers the caller `ok';
%% read_snapshot takes an integer, so the atom `latest' makes
%% integer_to_list/1 badarg inside the worker; a stream id has a shape the
%% store enforces, so `<<"account-123">>' is refused. None of that fails a
%% compile and none of it failed the guides' own examples.
%%
%% So this suite does what a reader does: append through reckon_gater_api,
%% save a snapshot the way the guide's maybe_save_snapshot/4 does (a map with
%% a `data' field), load through the snapshot, and keep writing past the
%% threshold. It is the check that makes a broken guide red before a
%% reviewer has to find it by hand.
-module(reckon_db_snapshot_guide_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("reckon_db/include/reckon_db.hrl").

-export([all/0, suite/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

%% The guide's aggregate, in its shape.
-record(account, {id, balance = 0, status = active, version = -1}).

-export([the_guides_snapshot_pattern_round_trips/1,
         a_snapshot_taken_at_the_threshold_is_used_by_the_next_load/1]).

-define(SNAPSHOT_THRESHOLD, 10).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [the_guides_snapshot_pattern_round_trips,
     a_snapshot_taken_at_the_threshold_is_used_by_the_next_load].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    {ok, _} = application:ensure_all_started(reckon_gater),
    {ok, _} = application:ensure_all_started(reckon_db),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    TCStr = atom_to_list(TestCase),
    DataDir = "/tmp/reckon_db_snapshot_guide_" ++ TCStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom("snapshot_guide_" ++ TCStr ++ "_" ++ Rand),
    StoreConfig = #store_config{store_id = StoreId, data_dir = DataDir,
                                mode = single, writer_pool_size = 1,
                                reader_pool_size = 1, gateway_pool_size = 1,
                                options = #{}},
    {ok, _} = reckon_db_sup:start_store(StoreConfig),
    [{data_dir, DataDir}, {store_id, StoreId} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = reckon_db_sup:stop_store(proplists:get_value(store_id, Config)),
    ok = file:del_dir_r(proplists:get_value(data_dir, Config)),
    ok.

%% @doc Deposit, load, deposit again - the guide's execute/3 as written,
%% including its expected version coming from the record the load returned.
the_guides_snapshot_pattern_round_trips(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    AccountId = <<"019d7a4f3c2a7d8c9e0f1234567890ab">>,
    ?assertEqual({ok, #account{id = AccountId, balance = 0, version = -1}},
                 load(StoreId, AccountId)),
    ?assertMatch({ok, 0, #account{balance = 100, version = 0}},
                 execute(StoreId, AccountId, {deposit, 100})),
    ?assertMatch({ok, 1, #account{balance = 150, version = 1}},
                 execute(StoreId, AccountId, {deposit, 50})),
    ?assertEqual({ok, #account{id = AccountId, balance = 150, version = 1}},
                 load(StoreId, AccountId)),
    %% The snapshot calls the guide shows, on a stream that exists.
    StreamId = <<"account-", AccountId/binary>>,
    ok = reckon_gater_api:record_snapshot(StoreId, snapshot_guide, StreamId, 1,
                                         #{data => #account{id = AccountId,
                                                            balance = 150,
                                                            version = 1}}),
    %% record_snapshot is a cast: give the worker a bounded moment to land it.
    ?assertMatch({ok, [#snapshot{version = 1}]},
                 wait_for_snapshots(StoreId, StreamId, 1)),
    ?assertMatch({ok, #snapshot{version = 1}},
                 reckon_gater_api:read_snapshot(StoreId, snapshot_guide, StreamId, 0)),
    ok.

%% @doc Past the threshold: the client saves a snapshot on its own, the next
%% load starts from it rather than from event 0, and appending continues from
%% the version the snapshot carried - never from a version it re-used.
a_snapshot_taken_at_the_threshold_is_used_by_the_next_load(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    AccountId = <<"019d7a4f3c2a7d8c9e0f1234567890ab">>,
    Last = lists:foldl(
             fun(_, _) ->
                     {ok, Version, _State} = execute(StoreId, AccountId, {deposit, 1}),
                     Version
             end, -1, lists:seq(1, ?SNAPSHOT_THRESHOLD + 2)),
    ?assertEqual(?SNAPSHOT_THRESHOLD + 1, Last),
    {ok, #account{version = Version, balance = Balance}} = load(StoreId, AccountId),
    ?assertEqual(?SNAPSHOT_THRESHOLD + 1, Version),
    ?assertEqual(?SNAPSHOT_THRESHOLD + 2, Balance),
    %% A snapshot was saved, and the version it holds is its own - the defect
    %% the guide had was a state carrying the version of the event BEFORE it.
    StreamId = <<"account-", AccountId/binary>>,
    {ok, Snapshots} = wait_for_snapshots(StoreId, StreamId, 1),
    ?assert(length(Snapshots) >= 1),
    ?assertEqual([?SNAPSHOT_THRESHOLD],
                 [V || #snapshot{version = V,
                                 data = #account{version = V}} <- Snapshots]),
    %% And the stream keeps writing after the snapshot, from the version the
    %% snapshot carried - not from a version it re-used.
    {ok, AfterVersion, _} = execute(StoreId, AccountId, {deposit, 1}),
    ?assertEqual(?SNAPSHOT_THRESHOLD + 2, AfterVersion),
    ok.

%% @private Wait - briefly, and with a cap - for the snapshots a cast
%% recorded to be readable. A test that read straight after the cast would be
%% racing the gateway worker, not checking the guide.
wait_for_snapshots(_StoreId, _StreamId, 0) ->
    {ok, []};
wait_for_snapshots(StoreId, StreamId, Attempts) ->
    case reckon_gater_api:list_snapshots(StoreId, snapshot_guide, StreamId) of
        {ok, []} = Empty when Attempts =< 1 ->
            Empty;
        {ok, []} when Attempts > 1 ->
            timer:sleep(50),
            wait_for_snapshots(StoreId, StreamId, Attempts - 1);
        Other ->
            Other
    end.

%%====================================================================
%% The guide's client module, in its shape
%%====================================================================

load(StoreId, AccountId) ->
    StreamId = <<"account-", AccountId/binary>>,
    {InitialState, StartVersion} =
        case reckon_gater_api:read_snapshot(StoreId, snapshot_guide, StreamId, 0) of
            {ok, #snapshot{data = State, version = V}} -> {State, V + 1};
            {error, not_found} -> {#account{id = AccountId}, 0}
        end,
    case reckon_gater_api:stream_forward(StoreId, StreamId, StartVersion, 10000) of
        {ok, Events} ->
            FinalState = lists:foldl(fun apply_event/2, InitialState, Events),
            NewVersion = case Events of
                             [] -> StartVersion - 1;
                             _ -> (lists:last(Events))#event.version
                         end,
            {ok, FinalState#account{version = NewVersion}};
        {error, {stream_not_found, _}} when StartVersion =:= 0 ->
            {ok, InitialState};
        {error, Reason} ->
            {error, Reason}
    end.

execute(StoreId, AccountId, Command) ->
    StreamId = <<"account-", AccountId/binary>>,
    {ok, Account} = load(StoreId, AccountId),
    {ok, Events} = handle_command(Command, Account),
    {ok, NewVersion} = reckon_gater_api:append_events(
                         StoreId, StreamId, Account#account.version, Events),
    NewState = (lists:foldl(fun apply_event/2, Account, Events))#account{
                 version = NewVersion
               },
    maybe_save_snapshot(StoreId, StreamId, NewState, NewVersion),
    {ok, NewVersion, NewState}.

maybe_save_snapshot(StoreId, StreamId, State, Version)
  when Version rem ?SNAPSHOT_THRESHOLD =:= 0, Version > 0 ->
    ok = reckon_gater_api:record_snapshot(StoreId, snapshot_guide, StreamId,
                                         Version, #{data => State}),
    ok;
maybe_save_snapshot(_StoreId, _StreamId, _State, _Version) ->
    ok.

handle_command({deposit, Amount}, #account{status = active} = Account)
  when Amount > 0 ->
    {ok, [#{event_type => <<"money_deposited_v1">>,
            data => #{amount => Amount,
                      balance_after => Account#account.balance + Amount}}]};
handle_command({deposit, _Amount}, #account{status = frozen}) ->
    {error, account_frozen}.

apply_event(#{event_type := EventType, data := Data}, Account) ->
    apply_event(#event{event_type = EventType, data = Data}, Account);
apply_event(#event{event_type = <<"money_deposited_v1">>, data = Data}, Account) ->
    Amount = maps:get(amount, Data),
    Account#account{balance = Account#account.balance + Amount}.
