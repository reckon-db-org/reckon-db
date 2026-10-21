%% @doc A store written by 5.11.x opens on this release with its data and its
%% subscription intact.
%%
%% The fixture is a real Ra directory, written by reckon-db 5.11.13 (khepri
%% 0.17.2, Ra 2.16.12): a stream of three events and one stream subscription,
%% whose trigger is registered in the log. Nothing here is synthesised - it
%% is what an operator's box already holds.
%%
%% This release runs the same khepri line (0.17.x, Ra machine version 2), so
%% opening the fixture converts nothing and this suite is green today. It
%% exists for the bump that is now deferred: khepri 0.18/0.19 move the Ra
%% machine version and convert the triggers in the log, and a replayed
%% legacy trigger in the wrong shape took the whole store down when that
%% upgrade was reviewed. The day the bump lands, this suite is the one that
%% has to stay green, unchanged, against this same fixture.
-module(reckon_db_upgrade_store_SUITE).

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

-export([a_5_11_store_opens_with_its_data_and_subscription/1,
         a_stream_written_past_the_counter_is_reconciled/1,
         a_stream_re_created_under_a_stale_counter_is_reconciled/1]).

%% The store id is the fixture's own: khepri stores it as the Ra node name,
%% and a store opened under a different one refuses to start. Each fixture
%% therefore carries its own id, and the case that opens it uses that one -
%% two stores cannot share an id in one VM, and the Ra system for an id keeps
%% the directory it was first started with.
-define(STREAM, <<"chat-0123456789abcdef0123456789abcdef">>).
-define(SUB_NAME, <<"fixture_stream_subscription">>).

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [a_5_11_store_opens_with_its_data_and_subscription,
     a_stream_written_past_the_counter_is_reconciled,
     a_stream_re_created_under_a_stale_counter_is_reconciled].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    {ok, _} = application:ensure_all_started(reckon_db),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    {StoreId, Fixture} = fixture_for(TestCase),
    Source = filename:join(?config(data_dir, Config), Fixture),
    DataDir = filename:join(?config(priv_dir, Config), "store_" ++
                            integer_to_list(erlang:unique_integer([positive]))),
    ok = copy_tree(Source, DataDir),
    StoreConfig = #store_config{store_id = StoreId,
                                data_dir = DataDir,
                                mode = single,
                                writer_pool_size = 5,
                                reader_pool_size = 5,
                                gateway_pool_size = 1,
                                options = #{}},
    {ok, _} = reckon_db_sup:start_store(StoreConfig),
    [{data_dir, DataDir}, {store_id, StoreId} | Config].

end_per_testcase(TestCase, _Config) ->
    {StoreId, _Fixture} = fixture_for(TestCase),
    ok = reckon_db_sup:stop_store(StoreId),
    ok.

%% The store must come up - not crash the Ra machine on a machine-version
%% conversion, and not answer noproc afterwards - with the three events it
%% was written with, its subscription still listed, and able to take a new
%% append after the three.
a_5_11_store_opens_with_its_data_and_subscription(Config) ->
    Store = store(Config),
    ?assertEqual(2, reckon_db_streams:get_version(Store, ?STREAM)),

    {ok, Events} = reckon_db_streams:read(Store, ?STREAM, 0, 100, forward),
    ?assertEqual([0, 1, 2], [E#event.version || E <- Events]),

    {ok, Subscriptions} = reckon_db_subscriptions:list(Store),
    Names = [S#subscription.subscription_name || S <- Subscriptions],
    ?assert(lists:member(?SUB_NAME, Names)),

    ?assertEqual({ok, 3}, reckon_db_streams:append(
                            Store, ?STREAM, ?ANY_VERSION, [next_event()])),
    {ok, After} = reckon_db_streams:read(Store, ?STREAM, 0, 100, forward),
    ?assertEqual([0, 1, 2, 3], [E#event.version || E <- After]),
    ok.

%% A store this release wrote to, that 5.11.x then appended to as well: the
%% counter was left at 9 while the version nodes moved on to 15.
%%
%% Without reconciling the counter the stream is stuck for every caller -
%% every append is built from version 10, which an event holds, and loses
%% sixty races against an event the counter cannot see. The fixture is that
%% exact history: written here, appended to by 5.11.13, reopened here.
a_stream_written_past_the_counter_is_reconciled(Config) ->
    Store = store(Config),
    %% The counter still says 9, which is what makes this shape dangerous.
    ?assertEqual(9, reckon_db_streams:get_version(Store, ?STREAM)),

    ?assertEqual({ok, 16}, reckon_db_streams:append(
                            Store, ?STREAM, ?ANY_VERSION, [next_event()])),

    {ok, Events} = reckon_db_streams:read(Store, ?STREAM, 0, 100, forward),
    ?assertEqual(lists:seq(0, 16), [E#event.version || E <- Events]),
    ?assertEqual(16, reckon_db_streams:get_version(Store, ?STREAM)),
    ok.

%% A stream 5.11.x deleted and re-created under the same id: it knows nothing
%% about the counter, so a fresh tree (versions 0, 1, 2) sits under a counter
%% of 4.
%%
%% Appending from 4 would land at 5 and leave a hole at 3 and 4, and refuse an
%% explicit version 3 that the tree plainly offers. The counter comes down to
%% the tree instead.
a_stream_re_created_under_a_stale_counter_is_reconciled(Config) ->
    Store = store(Config),
    ?assertEqual(4, reckon_db_streams:get_version(Store, ?STREAM)),

    %% Version 2 is what the tree has. Before the counter was reconciled this
    %% was refused with wrong_expected_version 2, 4.
    ?assertEqual({ok, 3}, reckon_db_streams:append(
                           Store, ?STREAM, 2, [next_event()])),

    {ok, Events} = reckon_db_streams:read(Store, ?STREAM, 0, 100, forward),
    ?assertEqual([0, 1, 2, 3], [E#event.version || E <- Events]),
    ?assertEqual(3, reckon_db_streams:get_version(Store, ?STREAM)),
    ok.

next_event() ->
    #{event_type => <<"fixture_event">>, data => #{<<"n">> => 4}}.

%% @private The store id the case opened.
store(Config) ->
    proplists:get_value(store_id, Config).

%% @private Which committed Ra directory a case opens, and the store id that
%% directory was written under.
fixture_for(a_5_11_store_opens_with_its_data_and_subscription) ->
    {fixture_store, "store_5_11_with_subscription"};
fixture_for(a_stream_written_past_the_counter_is_reconciled) ->
    {fixture_appends, "store_5_12_then_5_11_appends"};
fixture_for(a_stream_re_created_under_a_stale_counter_is_reconciled) ->
    {fixture_recreates, "store_5_12_then_5_11_recreates"}.

%% @private Copy a directory tree, files and subdirectories.
copy_tree(Source, Dest) ->
    case filelib:is_dir(Source) of
        true ->
            Entries = element(2, file:list_dir(Source)),
            ok = filelib:ensure_dir(filename:join(Dest, "dummy")),
            [ok = copy_tree(filename:join(Source, E), filename:join(Dest, E))
             || E <- Entries],
            ok;
        false ->
            {ok, _} = file:copy(Source, Dest),
            ok
    end.
