%% @doc Common Test suite for the generic write-maintained secondary index.
%%
%% Exercises the full write→read round-trip for all three index kinds
%% (tags / event_type / {meta, Key}), the compound `all' tag intersection,
%% index/scan parity, and the un-indexed scan fallback.
%%
%% @end
-module(reckon_db_index_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([
    tags_index_returns_matches_in_order/1,
    tags_index_all_intersects/1,
    event_type_index_returns_matches/1,
    meta_index_returns_matches/1,
    meta_index_absent_value_empty/1,
    index_matches_scan_parity/1,
    unindexed_store_falls_back_to_scan/1,
    multi_event_batch_fully_indexed/1
]).

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [tags_index_returns_matches_in_order,
     tags_index_all_intersects,
     event_type_index_returns_matches,
     meta_index_returns_matches,
     meta_index_absent_value_empty,
     index_matches_scan_parity,
     unindexed_store_falls_back_to_scan,
     multi_event_batch_fully_indexed].

%%====================================================================
%% CT boilerplate
%%====================================================================

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    RaDataDir = "/tmp/reckon_db_index_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ proplists:get_value(ra_data_dir, Config)),
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_index_" ++ atom_to_list(TestCase) ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom("index_test_" ++ atom_to_list(TestCase) ++ "_" ++ Rand),
    {ok, _} = khepri:start(DataDir, StoreId),
    khepri:put(StoreId, [streams], #{}),
    khepri:put(StoreId, [metadata], #{}),
    [{data_dir, DataDir}, {store_id, StoreId} | Config].

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    catch khepri:stop(StoreId),
    reckon_db_index_config:clear(StoreId),
    os:cmd("rm -rf " ++ proplists:get_value(data_dir, Config)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

declare(StoreId, Indexes) ->
    ok = reckon_db_index_config:load(
        #store_config{store_id = StoreId, data_dir = "/tmp", indexes = Indexes}).

sid(Label) -> reckon_db_test_helpers:sid(Label).

append(StoreId, StreamId, Event) ->
    {ok, V} = reckon_db_streams:append(StoreId, StreamId, ?ANY_VERSION, [Event]),
    V.

types(Events) -> [T || #event{event_type = T} <- Events].

%%====================================================================
%% Tests
%%====================================================================

%% tags index: read_by_tags returns exactly the tagged events, in epoch order.
tags_index_returns_matches_in_order(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    declare(StoreId, [tags]),
    S1 = sid(<<"ride-a">>), S2 = sid(<<"ride-b">>),
    append(StoreId, S1, #{event_type => <<"e1">>, data => #{}, tags => [<<"hot">>]}),
    append(StoreId, S2, #{event_type => <<"e2">>, data => #{}, tags => [<<"cold">>]}),
    append(StoreId, S1, #{event_type => <<"e3">>, data => #{}, tags => [<<"hot">>]}),

    {ok, Hot} = reckon_db_streams:read_by_tags(StoreId, [<<"hot">>], any, 100),
    ?assertEqual([<<"e1">>, <<"e3">>], types(Hot)),   %% epoch order

    {ok, Cold} = reckon_db_streams:read_by_tags(StoreId, [<<"cold">>], any, 100),
    ?assertEqual([<<"e2">>], types(Cold)),

    {ok, None} = reckon_db_streams:read_by_tags(StoreId, [<<"missing">>], any, 100),
    ?assertEqual([], None).

%% tags `all`: only events carrying every requested tag.
tags_index_all_intersects(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    declare(StoreId, [tags]),
    S = sid(<<"order-x">>),
    append(StoreId, S, #{event_type => <<"both">>, data => #{},
                         tags => [<<"a">>, <<"b">>]}),
    append(StoreId, S, #{event_type => <<"justa">>, data => #{},
                         tags => [<<"a">>]}),

    {ok, All} = reckon_db_streams:read_by_tags(StoreId, [<<"a">>, <<"b">>], all, 100),
    ?assertEqual([<<"both">>], types(All)),

    {ok, Any} = reckon_db_streams:read_by_tags(StoreId, [<<"a">>, <<"b">>], any, 100),
    ?assertEqual([<<"both">>, <<"justa">>], types(Any)).

%% event_type index.
event_type_index_returns_matches(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    declare(StoreId, [event_type]),
    S1 = sid(<<"acc-a">>), S2 = sid(<<"acc-b">>),
    append(StoreId, S1, #{event_type => <<"opened">>, data => #{}}),
    append(StoreId, S2, #{event_type => <<"closed">>, data => #{}}),
    append(StoreId, S1, #{event_type => <<"opened">>, data => #{}}),

    {ok, Opened} = reckon_db_streams:read_by_event_types(StoreId, [<<"opened">>], 100),
    ?assertEqual([<<"opened">>, <<"opened">>], types(Opened)),

    {ok, Both} = reckon_db_streams:read_by_event_types(
        StoreId, [<<"opened">>, <<"closed">>], 100),
    ?assertEqual(3, length(Both)).

%% {meta, Key} index: read_by_metadata returns events whose metadata key=value.
meta_index_returns_matches(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    declare(StoreId, [{meta, <<"causation_id">>}]),
    S = sid(<<"saga-x">>),
    append(StoreId, S, #{event_type => <<"a">>, data => #{},
                         metadata => #{<<"causation_id">> => <<"evt-1">>}}),
    append(StoreId, S, #{event_type => <<"b">>, data => #{},
                         metadata => #{<<"causation_id">> => <<"evt-2">>}}),
    append(StoreId, S, #{event_type => <<"c">>, data => #{},
                         metadata => #{<<"causation_id">> => <<"evt-1">>}}),

    {ok, Caused} = reckon_db_streams:read_by_metadata(
        StoreId, <<"causation_id">>, <<"evt-1">>),
    ?assertEqual([<<"a">>, <<"c">>], types(Caused)).

%% A value with no matching events returns [].
meta_index_absent_value_empty(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    declare(StoreId, [{meta, <<"k">>}]),
    S = sid(<<"x-y">>),
    append(StoreId, S, #{event_type => <<"a">>, data => #{},
                         metadata => #{<<"k">> => <<"present">>}}),
    {ok, Events} = reckon_db_streams:read_by_metadata(StoreId, <<"k">>, <<"absent">>),
    ?assertEqual([], Events).

%% The indexed read and the scan fallback return identical results.
index_matches_scan_parity(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    declare(StoreId, [tags, event_type]),
    S1 = sid(<<"p-a">>), S2 = sid(<<"p-b">>),
    append(StoreId, S1, #{event_type => <<"t">>, data => #{}, tags => [<<"x">>]}),
    append(StoreId, S2, #{event_type => <<"t">>, data => #{}, tags => [<<"y">>]}),
    append(StoreId, S1, #{event_type => <<"u">>, data => #{}, tags => [<<"x">>]}),

    %% Indexed reads (store declared tags + event_type)
    {ok, IdxTag} = reckon_db_streams:read_by_tags(StoreId, [<<"x">>], any, 100),
    {ok, IdxType} = reckon_db_streams:read_by_event_types(StoreId, [<<"t">>], 100),

    %% Force the scan path by clearing the declaration, then compare.
    reckon_db_index_config:clear(StoreId),
    {ok, ScanTag} = reckon_db_streams:read_by_tags(StoreId, [<<"x">>], any, 100),
    {ok, ScanType} = reckon_db_streams:read_by_event_types(StoreId, [<<"t">>], 100),

    ?assertEqual(types(ScanTag), types(IdxTag)),
    ?assertEqual(types(ScanType), types(IdxType)).

%% A store with no declared indexes still answers cross-cutting queries
%% (via scan) and read_by_metadata.
unindexed_store_falls_back_to_scan(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    declare(StoreId, []),
    S = sid(<<"u-a">>),
    append(StoreId, S, #{event_type => <<"e">>, data => #{}, tags => [<<"t">>],
                         metadata => #{<<"cid">> => <<"c1">>}}),

    {ok, ByTag} = reckon_db_streams:read_by_tags(StoreId, [<<"t">>], any, 100),
    ?assertEqual([<<"e">>], types(ByTag)),
    {ok, ByMeta} = reckon_db_streams:read_by_metadata(StoreId, <<"cid">>, <<"c1">>),
    ?assertEqual([<<"e">>], types(ByMeta)).

%% A multi-event batch writes every event's index entries atomically —
%% all are present and resolvable after the append.
multi_event_batch_fully_indexed(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    declare(StoreId, [tags]),
    S = sid(<<"batch-x">>),
    {ok, _} = reckon_db_streams:append(StoreId, S, ?ANY_VERSION, [
        #{event_type => <<"a">>, data => #{}, tags => [<<"g">>]},
        #{event_type => <<"b">>, data => #{}, tags => [<<"g">>]},
        #{event_type => <<"c">>, data => #{}, tags => [<<"g">>]}
    ]),
    {ok, Tagged} = reckon_db_streams:read_by_tags(StoreId, [<<"g">>], any, 100),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], types(Tagged)).
