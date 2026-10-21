%% @doc The inspector against a real store.
%%
%% test/unit/reckon_db_store_inspector_tests.erl mocks reckon_db_streams,
%% and its stubs of get_version/2 returned {ok, Version} while the real
%% function returns the version itself. The mocks and the code agreed
%% with each other and neither agreed with the store: stream_info/2 was
%% a case_clause on every real call and stream_event_count/2 answered 0
%% for every stream (reckon-db#6). These cases use a real Khepri store,
%% so the same divergence fails here.
-module(reckon_db_store_inspector_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

-export([
    all/0,
    groups/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

-export([
    stream_info_reports_a_real_stream/1,
    stream_info_answers_not_found/1,
    store_stats_counts_real_events/1,
    a_scavenged_stream_reports_the_events_it_still_has/1
]).

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [{group, store}].

groups() ->
    [{store, [sequence], [
        stream_info_reports_a_real_stream,
        stream_info_answers_not_found,
        store_stats_counts_real_events,
        a_scavenged_stream_reports_the_events_it_still_has
    ]}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    RaDataDir = "/tmp/reckon_db_inspector_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),

    PgScope = reckon_db_test_helpers:start_pg_scope(),

    [{pg_scope, PgScope}, {ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    reckon_db_test_helpers:stop_pg_scope(proplists:get_value(pg_scope, Config, already_running)),
    os:cmd("rm -rf " ++ proplists:get_value(ra_data_dir, Config, "/tmp/reckon_db_inspector_test_ra")),
    ok.

init_per_group(_GroupName, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_inspector_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom("inspector_test_" ++ Rand),
    {ok, _} = khepri:start(DataDir, StoreId),
    khepri:put(StoreId, [streams], #{}),
    [{data_dir, DataDir}, {store_id, StoreId} | Config].

end_per_group(_GroupName, Config) ->
    khepri:stop(proplists:get_value(store_id, Config)),
    os:cmd("rm -rf " ++ proplists:get_value(data_dir, Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

stream_info_reports_a_real_stream(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = new_stream_id(),
    {ok, 2} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(3)),

    %% Three events at versions 0, 1, 2.
    {ok, Info} = reckon_db_store_inspector:stream_info(StoreId, StreamId),

    ?assertEqual(StreamId, maps:get(stream_id, Info)),
    ?assertEqual(2, maps:get(version, Info)),
    ?assertEqual(3, maps:get(event_count, Info)),
    ?assert(is_integer(maps:get(first_event_at, Info))),
    ?assert(is_integer(maps:get(last_event_at, Info))),
    ok.

stream_info_answers_not_found(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    Missing = new_stream_id(),

    ?assertEqual({error, {stream_not_found, Missing}},
        reckon_db_store_inspector:stream_info(StoreId, Missing)),
    ok.

store_stats_counts_real_events(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    %% The group's store is shared and sequential, so measure the delta
    %% rather than an absolute count.
    {ok, Before} = reckon_db_store_inspector:store_stats(StoreId),

    First = new_stream_id(),
    Second = new_stream_id(),
    {ok, _} = reckon_db_streams:append(StoreId, First, ?NO_STREAM, events(3)),
    {ok, _} = reckon_db_streams:append(StoreId, Second, ?NO_STREAM, events(2)),

    %% Five events across two streams. This total was 0 for every store
    %% while stream_event_count/2 matched a shape get_version/2 never
    %% returns, and subscription_lag is built on the same total.
    {ok, After} = reckon_db_store_inspector:store_stats(StoreId),

    ?assertEqual(maps:get(stream_count, Before) + 2, maps:get(stream_count, After)),
    ?assertEqual(maps:get(total_events, Before) + 5, maps:get(total_events, After)),
    ?assertEqual(true, maps:get(has_events, After)),
    ok.

%% A scavenge deletes version nodes without renumbering, so the newest
%% version stops being the event count. `Version + 1' answered the versions
%% ever assigned: five events, two scavenged, three live -> 5. stream_info and
%% store_stats both read this, and subscription_lag is built on the total.
a_scavenged_stream_reports_the_events_it_still_has(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = new_stream_id(),
    {ok, 4} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(5)),
    {ok, Before} = reckon_db_store_inspector:store_stats(StoreId),

    {ok, #{deleted_count := 2}} =
        reckon_db_scavenge:scavenge(StoreId, StreamId,
            #{before_version => 2, require_snapshot => false}),

    {ok, Info} = reckon_db_store_inspector:stream_info(StoreId, StreamId),
    ?assertEqual(4, maps:get(version, Info)),
    ?assertEqual(3, maps:get(event_count, Info)),

    {ok, After} = reckon_db_store_inspector:store_stats(StoreId),
    ?assertEqual(maps:get(total_events, Before) - 2, maps:get(total_events, After)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

new_stream_id() ->
    reckon_gater_stream_id:new(<<"test">>).

events(Count) ->
    [#{
        event_type => <<"inspector_event">>,
        data => #{<<"n">> => N},
        metadata => #{<<"correlation_id">> => uuid()}
     } || N <- lists:seq(1, Count)].

uuid() ->
    Bytes = crypto:strong_rand_bytes(16),
    <<A:32, B:16, C:16, D:16, E:48>> = Bytes,
    iolist_to_binary(io_lib:format(
        "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [A, B, C, D, E])).
