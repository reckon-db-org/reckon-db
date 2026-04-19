%% @doc Unit tests for reckon_db_store_inspector.
%%
%% Mocks the underlying facade modules (reckon_db_streams,
%% reckon_db_snapshots_store, reckon_db_subscriptions_store)
%% to test the inspector's aggregation and data transformation logic.
-module(reckon_db_store_inspector_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("reckon_gater/include/reckon_gater_types.hrl").

-define(STORE, test_inspector_store).

%%====================================================================
%% Test generators
%%====================================================================

store_stats_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun store_stats_empty_store/0,
        fun store_stats_with_data/0
    ]}.

list_all_snapshots_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun snapshots_empty_store/0,
        fun snapshots_with_data/0,
        fun snapshots_stream_error_skipped/0
    ]}.

list_subscriptions_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun subscriptions_empty/0,
        fun subscriptions_with_record/0,
        fun subscriptions_with_undefined_pid/0,
        fun subscriptions_with_map/0
    ]}.

subscription_lag_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun lag_calculation/0,
        fun lag_not_found/0
    ]}.

event_type_summary_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun event_types_empty/0,
        fun event_types_with_events/0
    ]}.

stream_info_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun stream_info_found/0,
        fun stream_info_not_found/0
    ]}.

%%====================================================================
%% Setup / Cleanup
%%====================================================================

setup() ->
    meck:new(reckon_db_streams, [non_strict]),
    meck:new(reckon_db_snapshots_store, [non_strict]),
    meck:new(reckon_db_subscriptions_store, [non_strict]),
    ok.

cleanup(_) ->
    meck:unload(reckon_db_streams),
    meck:unload(reckon_db_snapshots_store),
    meck:unload(reckon_db_subscriptions_store),
    ok.

%%====================================================================
%% store_stats tests
%%====================================================================

store_stats_empty_store() ->
    mock_streams([]),
    mock_subs([]),
    {ok, Stats} = reckon_db_store_inspector:store_stats(?STORE),
    ?assertEqual(0, maps:get(stream_count, Stats)),
    ?assertEqual(0, maps:get(total_events, Stats)),
    ?assertEqual(0, maps:get(snapshot_count, Stats)),
    ?assertEqual(0, maps:get(subscription_count, Stats)),
    ?assertEqual(false, maps:get(has_events, Stats)).

store_stats_with_data() ->
    mock_streams([<<"stream-a">>, <<"stream-b">>]),
    mock_version(<<"stream-a">>, 5),
    mock_version(<<"stream-b">>, 2),
    mock_snapshots(<<"stream-a">>, [make_snapshot(<<"stream-a">>, 3)]),
    mock_snapshots(<<"stream-b">>, []),
    mock_subs([make_sub(<<"prj_test">>)]),
    {ok, Stats} = reckon_db_store_inspector:store_stats(?STORE),
    ?assertEqual(2, maps:get(stream_count, Stats)),
    ?assertEqual(9, maps:get(total_events, Stats)),  %% (5+1) + (2+1) = 9
    ?assertEqual(1, maps:get(snapshot_count, Stats)),
    ?assertEqual(1, maps:get(subscription_count, Stats)),
    ?assertEqual(true, maps:get(has_events, Stats)).

%%====================================================================
%% list_all_snapshots tests
%%====================================================================

snapshots_empty_store() ->
    mock_streams([]),
    {ok, Snaps} = reckon_db_store_inspector:list_all_snapshots(?STORE),
    ?assertEqual([], Snaps).

snapshots_with_data() ->
    mock_streams([<<"s1">>, <<"s2">>]),
    mock_snapshots(<<"s1">>, [make_snapshot(<<"s1">>, 10)]),
    mock_snapshots(<<"s2">>, [make_snapshot(<<"s2">>, 5)]),
    {ok, Snaps} = reckon_db_store_inspector:list_all_snapshots(?STORE),
    ?assertEqual(2, length(Snaps)),
    %% Sorted by timestamp descending
    First = hd(Snaps),
    ?assertEqual(<<"s1">>, maps:get(stream_id, First)).

snapshots_stream_error_skipped() ->
    mock_streams([<<"good">>, <<"bad">>]),
    mock_snapshots(<<"good">>, [make_snapshot(<<"good">>, 1)]),
    %% Override the snapshot mock to return error for <<"bad">>
    meck:expect(reckon_db_snapshots_store, list,
        fun(?STORE, <<"good">>) -> {ok, [make_snapshot(<<"good">>, 1)]};
           (?STORE, <<"bad">>) -> {error, broken};
           (?STORE, _) -> {ok, []}
        end),
    {ok, Snaps} = reckon_db_store_inspector:list_all_snapshots(?STORE),
    ?assertEqual(1, length(Snaps)),
    ?assertEqual(<<"good">>, maps:get(stream_id, hd(Snaps))).

%%====================================================================
%% list_subscriptions tests
%%====================================================================

subscriptions_empty() ->
    mock_subs([]),
    {ok, Subs} = reckon_db_store_inspector:list_subscriptions(?STORE),
    ?assertEqual([], Subs).

subscriptions_with_record() ->
    Sub = make_sub(<<"prj_docs">>),
    mock_subs([Sub]),
    {ok, [Map]} = reckon_db_store_inspector:list_subscriptions(?STORE),
    ?assertEqual(<<"prj_docs">>, maps:get(subscription_name, Map)),
    ?assert(is_binary(maps:get(subscriber_pid, Map))).

subscriptions_with_undefined_pid() ->
    Sub = #subscription{
        id = <<"sub1">>,
        subscription_name = <<"prj_orphan">>,
        type = event_type,
        selector = <<"test_event">>,
        subscriber_pid = undefined,
        checkpoint = 0,
        pool_size = 1,
        created_at = 1000
    },
    mock_subs([Sub]),
    {ok, [Map]} = reckon_db_store_inspector:list_subscriptions(?STORE),
    ?assertEqual(<<"undefined">>, maps:get(subscriber_pid, Map)).

subscriptions_with_map() ->
    Sub = #{subscription_name => <<"prj_map">>, subscriber_pid => undefined, type => stream},
    mock_subs([Sub]),
    {ok, [Map]} = reckon_db_store_inspector:list_subscriptions(?STORE),
    ?assertEqual(<<"prj_map">>, maps:get(subscription_name, Map)).

%%====================================================================
%% subscription_lag tests
%%====================================================================

lag_calculation() ->
    Sub = make_sub(<<"prj_test">>),
    meck:expect(reckon_db_subscriptions_store, find_by_name,
        fun(?STORE, <<"prj_test">>) -> {ok, Sub} end),
    mock_streams([<<"s1">>, <<"s2">>]),
    mock_version(<<"s1">>, 9),
    mock_version(<<"s2">>, 4),
    {ok, Lag} = reckon_db_store_inspector:subscription_lag(?STORE, <<"prj_test">>),
    ?assertEqual(42, maps:get(checkpoint, Lag)),
    ?assertEqual(15, maps:get(latest_position, Lag)).  %% (9+1)+(4+1)=15

lag_not_found() ->
    meck:expect(reckon_db_subscriptions_store, find_by_name,
        fun(?STORE, <<"missing">>) -> {error, not_found} end),
    ?assertEqual({error, not_found},
        reckon_db_store_inspector:subscription_lag(?STORE, <<"missing">>)).

%%====================================================================
%% event_type_summary tests
%%====================================================================

event_types_empty() ->
    mock_streams([]),
    {ok, Types} = reckon_db_store_inspector:event_type_summary(?STORE),
    ?assertEqual([], Types).

event_types_with_events() ->
    mock_streams([<<"s1">>]),
    Events = [
        #event{event_type = <<"user_registered_v1">>},
        #event{event_type = <<"user_registered_v1">>},
        #event{event_type = <<"user_archived_v1">>}
    ],
    meck:expect(reckon_db_streams, read_all,
        fun(?STORE, <<"s1">>, forward, _) -> {ok, Events} end),
    {ok, Types} = reckon_db_store_inspector:event_type_summary(?STORE),
    ?assertEqual(2, length(Types)),
    %% Sorted by count descending
    First = hd(Types),
    ?assertEqual(<<"user_registered_v1">>, maps:get(event_type, First)),
    ?assertEqual(2, maps:get(count, First)).

%%====================================================================
%% stream_info tests
%%====================================================================

stream_info_found() ->
    meck:expect(reckon_db_streams, get_version,
        fun(?STORE, <<"s1">>) -> {ok, 10} end),
    meck:expect(reckon_db_streams, read,
        fun(?STORE, <<"s1">>, 0, 1, forward) ->
                {ok, [#event{timestamp = 1000}]};
           (?STORE, <<"s1">>, 10, 1, forward) ->
                {ok, [#event{timestamp = 5000}]}
        end),
    meck:expect(reckon_db_snapshots_store, list,
        fun(?STORE, <<"s1">>) -> {ok, [make_snapshot(<<"s1">>, 5)]} end),
    {ok, Info} = reckon_db_store_inspector:stream_info(?STORE, <<"s1">>),
    ?assertEqual(<<"s1">>, maps:get(stream_id, Info)),
    ?assertEqual(10, maps:get(version, Info)),
    ?assertEqual(11, maps:get(event_count, Info)),
    ?assertEqual(1000, maps:get(first_event_at, Info)),
    ?assertEqual(5000, maps:get(last_event_at, Info)),
    ?assertEqual(1, maps:get(count, maps:get(snapshots, Info))).

stream_info_not_found() ->
    meck:expect(reckon_db_streams, get_version,
        fun(?STORE, <<"missing">>) -> {error, not_found} end),
    ?assertEqual({error, not_found},
        reckon_db_store_inspector:stream_info(?STORE, <<"missing">>)).

%%====================================================================
%% Mock helpers
%%====================================================================

mock_streams(StreamIds) ->
    meck:expect(reckon_db_streams, list_streams,
        fun(?STORE) -> {ok, StreamIds} end),
    put(mock_versions, #{}),
    put(mock_snapshots, #{}),
    meck:expect(reckon_db_streams, get_version,
        fun(?STORE, Id) ->
            case maps:find(Id, get(mock_versions)) of
                {ok, V} -> {ok, V};
                error -> {ok, 0}
            end
        end),
    meck:expect(reckon_db_snapshots_store, list,
        fun(?STORE, Id) ->
            case maps:find(Id, get(mock_snapshots)) of
                {ok, S} -> {ok, S};
                error -> {ok, []}
            end
        end).

mock_version(StreamId, Version) ->
    put(mock_versions, maps:put(StreamId, Version, get(mock_versions))).

mock_snapshots(StreamId, Snapshots) ->
    put(mock_snapshots, maps:put(StreamId, Snapshots, get(mock_snapshots))).

mock_subs(Subs) ->
    meck:expect(reckon_db_subscriptions_store, list,
        fun(?STORE) -> {ok, Subs} end).

make_snapshot(StreamId, Version) ->
    #snapshot{
        stream_id = StreamId,
        version = Version,
        data = #{},
        metadata = #{},
        timestamp = Version * 1000
    }.

make_sub(Name) ->
    #subscription{
        id = Name,
        subscription_name = Name,
        type = event_type,
        selector = <<"test_event_v1">>,
        subscriber_pid = list_to_pid("<0.500.0>"),
        checkpoint = 42,
        pool_size = 1,
        created_at = 1000
    }.
