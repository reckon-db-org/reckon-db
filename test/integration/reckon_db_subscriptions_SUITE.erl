%% @doc Common Test suite for reckon_db_subscriptions module
%%
%% Integration tests for subscription operations including:
%% - Subscribe/unsubscribe operations
%% - Subscription types (stream, event_type, event_pattern, event_payload)
%% - Event delivery via emitters
%% - Subscription persistence
%%
%% @author R. Lefever

-module(reckon_db_subscriptions_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    %% Subscribe/Unsubscribe tests
    subscribe_to_stream/1,
    subscribe_to_event_type/1,
    subscribe_to_event_pattern/1,
    subscribe_with_pool_size/1,
    subscribe_duplicate_fails/1,
    unsubscribe_by_key/1,
    unsubscribe_nonexistent/1,

    %% Subscription store tests
    subscription_persisted/1,
    list_subscriptions/1,
    subscription_exists/1,

    %% Emitter group tests
    emitter_group_join_leave/1,
    emitter_group_members/1,
    emitter_group_broadcast/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [
        {group, subscribe_tests},
        {group, store_tests},
        {group, emitter_group_tests}
    ].

groups() ->
    [
        {subscribe_tests, [sequence], [
            subscribe_to_stream,
            subscribe_to_event_type,
            subscribe_to_event_pattern,
            subscribe_with_pool_size,
            subscribe_duplicate_fails,
            unsubscribe_by_key,
            unsubscribe_nonexistent
        ]},
        {store_tests, [sequence], [
            subscription_persisted,
            list_subscriptions,
            subscription_exists
        ]},
        {emitter_group_tests, [sequence], [
            emitter_group_join_leave,
            emitter_group_members,
            emitter_group_broadcast
        ]}
    ].

init_per_suite(Config) ->
    %% Start required applications
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    %% Configure Ra data directory before starting Ra
    RaDataDir = "/tmp/reckon_db_subscriptions_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),

    %% Start Ra first, then Khepri
    {ok, _} = application:ensure_all_started(ra),

    %% Now start the default Ra system
    ok = ra:start(),

    %% Start Khepri
    {ok, _} = application:ensure_all_started(khepri),

    %% Start pg scope
    case pg:start(?RECKON_DB_PG_SCOPE) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,

    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    %% Clean up Ra data directory
    RaDataDir = proplists:get_value(ra_data_dir, Config, "/tmp/reckon_db_subscriptions_test_ra"),
    os:cmd("rm -rf " ++ RaDataDir),
    ok.

init_per_group(GroupName, Config) ->
    %% Use unique data directory for each group
    GroupStr = atom_to_list(GroupName),
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_subscriptions_test_" ++ GroupStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    %% Generate unique store ID for each group to avoid conflicts
    StoreId = list_to_atom("subscriptions_test_" ++ GroupStr ++ "_" ++ Rand),

    %% Start Khepri store
    case khepri:start(DataDir, StoreId) of
        {ok, _} ->
            %% Initialize base paths
            khepri:put(StoreId, [streams], #{}),
            khepri:put(StoreId, [subscriptions], #{}),
            khepri:put(StoreId, [procs], #{}),
            [{data_dir, DataDir}, {store_id, StoreId} | Config];
        {error, Reason} ->
            ct:pal("Khepri start error: ~p~n", [Reason]),
            ct:fail("Failed to start Khepri: ~p", [Reason])
    end.

end_per_group(_GroupName, Config) ->
    %% Stop and clean up store
    StoreId = proplists:get_value(store_id, Config),
    khepri:stop(StoreId),
    DataDir = proplists:get_value(data_dir, Config),
    os:cmd("rm -rf " ++ DataDir),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Subscribe/Unsubscribe Tests
%%====================================================================

%% @doc Test subscribing to a stream
subscribe_to_stream(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$stream-sub">>,

    {ok, Key} = reckon_db_subscriptions:subscribe(
        StoreId,
        stream,
        StreamId,
        <<"stream_subscription">>
    ),

    ?assert(is_binary(Key)),
    ?assertEqual(true, reckon_db_subscriptions:exists(StoreId, Key)),

    %% Cleanup
    ok = reckon_db_subscriptions:unsubscribe(StoreId, Key),
    ok.

%% @doc Test subscribing to an event type
subscribe_to_event_type(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    EventType = <<"user_created">>,

    {ok, Key} = reckon_db_subscriptions:subscribe(
        StoreId,
        event_type,
        EventType,
        <<"event_type_subscription">>
    ),

    ?assert(is_binary(Key)),
    ?assertEqual(true, reckon_db_subscriptions:exists(StoreId, Key)),

    %% Cleanup
    ok = reckon_db_subscriptions:unsubscribe(StoreId, Key),
    ok.

%% @doc Test subscribing to an event pattern
subscribe_to_event_pattern(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    Pattern = #{event_type => <<"order_placed">>},

    {ok, Key} = reckon_db_subscriptions:subscribe(
        StoreId,
        event_pattern,
        Pattern,
        <<"pattern_subscription">>
    ),

    ?assert(is_binary(Key)),
    ?assertEqual(true, reckon_db_subscriptions:exists(StoreId, Key)),

    %% Cleanup
    ok = reckon_db_subscriptions:unsubscribe(StoreId, Key),
    ok.

%% @doc Test subscribing with custom pool size
subscribe_with_pool_size(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$pooled-stream">>,

    {ok, Key} = reckon_db_subscriptions:subscribe(
        StoreId,
        stream,
        StreamId,
        <<"pooled_subscription">>,
        #{pool_size => 4}
    ),

    ?assert(is_binary(Key)),

    %% Verify pool size is stored
    {ok, Sub} = reckon_db_subscriptions:get(StoreId, Key),
    ?assertEqual(4, Sub#subscription.pool_size),

    %% Cleanup
    ok = reckon_db_subscriptions:unsubscribe(StoreId, Key),
    ok.

%% @doc Test that duplicate subscriptions fail
subscribe_duplicate_fails(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$dup-stream">>,
    SubName = <<"duplicate_test">>,

    %% First subscription should succeed
    {ok, Key} = reckon_db_subscriptions:subscribe(
        StoreId,
        stream,
        StreamId,
        SubName
    ),

    %% Second subscription with same name should fail
    Result = reckon_db_subscriptions:subscribe(
        StoreId,
        stream,
        StreamId,
        SubName
    ),

    ?assertMatch({error, {already_exists, SubName}}, Result),

    %% Cleanup
    ok = reckon_db_subscriptions:unsubscribe(StoreId, Key),
    ok.

%% @doc Test unsubscribing by key
unsubscribe_by_key(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$unsub-stream">>,

    {ok, Key} = reckon_db_subscriptions:subscribe(
        StoreId,
        stream,
        StreamId,
        <<"unsub_test">>
    ),

    ?assertEqual(true, reckon_db_subscriptions:exists(StoreId, Key)),

    ok = reckon_db_subscriptions:unsubscribe(StoreId, Key),

    ?assertEqual(false, reckon_db_subscriptions:exists(StoreId, Key)),
    ok.

%% @doc Test unsubscribing non-existent subscription
unsubscribe_nonexistent(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    Result = reckon_db_subscriptions:unsubscribe(StoreId, <<"nonexistent_key">>),

    ?assertEqual({error, not_found}, Result),
    ok.

%%====================================================================
%% Store Tests
%%====================================================================

%% @doc Test that subscriptions are persisted
subscription_persisted(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$persist-stream">>,

    {ok, Key} = reckon_db_subscriptions:subscribe(
        StoreId,
        stream,
        StreamId,
        <<"persist_test">>
    ),

    %% Verify subscription can be retrieved
    {ok, Sub} = reckon_db_subscriptions:get(StoreId, Key),

    ?assertEqual(stream, Sub#subscription.type),
    ?assertEqual(StreamId, Sub#subscription.selector),
    ?assertEqual(<<"persist_test">>, Sub#subscription.subscription_name),

    %% Cleanup
    ok = reckon_db_subscriptions:unsubscribe(StoreId, Key),
    ok.

%% @doc Test listing subscriptions
list_subscriptions(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    %% Create multiple subscriptions
    {ok, Key1} = reckon_db_subscriptions:subscribe(
        StoreId, stream, <<"test$list1">>, <<"list_test_1">>
    ),
    {ok, Key2} = reckon_db_subscriptions:subscribe(
        StoreId, stream, <<"test$list2">>, <<"list_test_2">>
    ),

    %% List all subscriptions
    {ok, Subs} = reckon_db_subscriptions:list(StoreId),

    ?assert(length(Subs) >= 2),

    %% Cleanup
    ok = reckon_db_subscriptions:unsubscribe(StoreId, Key1),
    ok = reckon_db_subscriptions:unsubscribe(StoreId, Key2),
    ok.

%% @doc Test subscription exists check
subscription_exists(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    {ok, Key} = reckon_db_subscriptions:subscribe(
        StoreId, stream, <<"test$exists">>, <<"exists_test">>
    ),

    ?assertEqual(true, reckon_db_subscriptions:exists(StoreId, Key)),
    ?assertEqual(false, reckon_db_subscriptions:exists(StoreId, <<"nonexistent">>)),

    %% Cleanup
    ok = reckon_db_subscriptions:unsubscribe(StoreId, Key),
    ok.

%%====================================================================
%% Emitter Group Tests
%%====================================================================

%% @doc Test joining and leaving emitter groups
emitter_group_join_leave(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    SubscriptionKey = <<"join_leave_test">>,

    %% Initially no members
    Members0 = reckon_db_emitter_group:members(StoreId, SubscriptionKey),
    ?assertEqual([], Members0),

    %% Join the group
    ok = reckon_db_emitter_group:join(StoreId, SubscriptionKey, self()),

    %% Now we should be a member
    Members1 = reckon_db_emitter_group:members(StoreId, SubscriptionKey),
    ?assert(lists:member(self(), Members1)),

    %% Leave the group
    ok = reckon_db_emitter_group:leave(StoreId, SubscriptionKey, self()),

    %% No longer a member
    Members2 = reckon_db_emitter_group:members(StoreId, SubscriptionKey),
    ?assertEqual(false, lists:member(self(), Members2)),
    ok.

%% @doc Test getting emitter group members
emitter_group_members(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    SubscriptionKey = <<"members_test">>,

    %% Spawn a few test processes
    Pids = [spawn(fun() -> receive stop -> ok end end) || _ <- lists:seq(1, 3)],

    %% Join all to the group
    [ok = reckon_db_emitter_group:join(StoreId, SubscriptionKey, Pid) || Pid <- Pids],

    %% Get members
    Members = reckon_db_emitter_group:members(StoreId, SubscriptionKey),
    ?assertEqual(3, length(Members)),

    %% Cleanup
    [begin
        reckon_db_emitter_group:leave(StoreId, SubscriptionKey, Pid),
        Pid ! stop
     end || Pid <- Pids],
    ok.

%% @doc Test broadcasting to emitter group
emitter_group_broadcast(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    SubscriptionKey = <<"broadcast_test">>,

    %% Join the group
    ok = reckon_db_emitter_group:join(StoreId, SubscriptionKey, self()),

    %% Create a test event
    TestEvent = #event{
        event_id = <<"test-event-1">>,
        event_type = <<"test_event">>,
        stream_id = <<"test$stream">>,
        version = 0,
        data = #{<<"key">> => <<"value">>},
        metadata = #{},
        timestamp = erlang:system_time(millisecond),
        epoch_us = erlang:system_time(microsecond)
    },

    %% Broadcast the event
    ok = reckon_db_emitter_group:broadcast(StoreId, SubscriptionKey, TestEvent),

    %% We should receive the broadcast (as local forward)
    receive
        {forward_to_local, _Topic, ReceivedEvent} ->
            ?assertEqual(<<"test-event-1">>, ReceivedEvent#event.event_id)
    after 1000 ->
        ct:fail("Did not receive broadcast message")
    end,

    %% Cleanup
    ok = reckon_db_emitter_group:leave(StoreId, SubscriptionKey, self()),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================
