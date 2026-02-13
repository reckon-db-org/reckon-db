%% @doc Integration test for end-to-end subscription event delivery
%%
%% Verifies that when a gen_server subscribes to a stream and an event
%% is written to that stream, the subscribing process receives the event.
%%
%% This tests the full pipeline:
%%   append → Khepri trigger → emitter_group:broadcast → emitter → subscriber
%%
%% @author rgfaber

-module(reckon_db_subscription_delivery_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

%% CT callbacks
-export([
    all/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    subscribe_then_append_delivers_event/1,
    subscribe_then_append_multiple_delivers_all/1,
    subscribe_by_event_type_delivers_matching/1,
    unsubscribe_stops_delivery/1,
    subscriber_receives_correct_event_data/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {seconds, 30}}].

all() ->
    [
        subscribe_then_append_delivers_event,
        subscribe_then_append_multiple_delivers_all,
        subscribe_by_event_type_delivers_matching,
        unsubscribe_stops_delivery,
        subscriber_receives_correct_event_data
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    RaDataDir = "/tmp/reckon_db_delivery_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),

    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),

    case pg:start(?RECKON_DB_PG_SCOPE) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,

    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    RaDataDir = proplists:get_value(ra_data_dir, Config),
    os:cmd("rm -rf " ++ RaDataDir),
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    TCStr = atom_to_list(TestCase),
    DataDir = "/tmp/reckon_db_delivery_" ++ TCStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    StoreId = list_to_atom("delivery_test_" ++ TCStr ++ "_" ++ Rand),

    case khepri:start(DataDir, StoreId) of
        {ok, _} ->
            khepri:put(StoreId, [streams], #{}),
            khepri:put(StoreId, [subscriptions], #{}),
            khepri:put(StoreId, [procs], #{}),
            [{data_dir, DataDir}, {store_id, StoreId} | Config];
        {error, Reason} ->
            ct:fail("Failed to start Khepri: ~p", [Reason])
    end.

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    khepri:stop(StoreId),
    DataDir = proplists:get_value(data_dir, Config),
    os:cmd("rm -rf " ++ DataDir),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc GIVEN a process subscribes to a stream
%%      AND an emitter worker is running for that subscription
%%      WHEN an event is appended to the stream
%%      THEN the subscribing process receives the event
subscribe_then_append_delivers_event(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$delivery-001">>,
    SubName = <<"delivery_test_sub">>,

    %% Subscribe with self() as subscriber
    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => self()}
    ),

    %% Start an emitter worker that joins the group for this subscription
    EmitterName = reckon_db_emitter_group:emitter_name(StoreId, SubKey),
    {ok, EmitterPid} = reckon_db_emitter:start_link(
        StoreId, SubKey, self(), EmitterName
    ),

    %% Append an event to the stream
    Event = #{
        event_type => <<"thing_happened_v1">>,
        data => #{<<"key">> => <<"value">>}
    },
    {ok, _Version} = reckon_db_streams:append(StoreId, StreamId, -2, [Event]),

    %% Wait for the event to be delivered
    receive
        {events, [ReceivedEvent]} ->
            ?assertEqual(<<"thing_happened_v1">>, ReceivedEvent#event.event_type),
            ?assertEqual(StreamId, ReceivedEvent#event.stream_id)
    after 5000 ->
        ct:fail("Did not receive event within 5 seconds")
    end,

    %% Cleanup
    gen_server:stop(EmitterPid),
    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%% @doc GIVEN a subscriber
%%      WHEN multiple events are appended
%%      THEN all events are received
subscribe_then_append_multiple_delivers_all(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$delivery-multi">>,
    SubName = <<"multi_delivery_sub">>,

    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => self()}
    ),

    EmitterName = reckon_db_emitter_group:emitter_name(StoreId, SubKey),
    {ok, EmitterPid} = reckon_db_emitter:start_link(
        StoreId, SubKey, self(), EmitterName
    ),

    %% Append 3 events
    Events = [
        #{event_type => <<"first_v1">>, data => #{<<"n">> => 1}},
        #{event_type => <<"second_v1">>, data => #{<<"n">> => 2}},
        #{event_type => <<"third_v1">>, data => #{<<"n">> => 3}}
    ],
    {ok, _} = reckon_db_streams:append(StoreId, StreamId, -2, Events),

    %% Collect all delivered events (each may arrive as separate {events, [E]} message)
    Received = collect_events(3, 5000),
    ?assertEqual(3, length(Received)),

    ReceivedTypes = [E#event.event_type || E <- Received],
    ?assert(lists:member(<<"first_v1">>, ReceivedTypes)),
    ?assert(lists:member(<<"second_v1">>, ReceivedTypes)),
    ?assert(lists:member(<<"third_v1">>, ReceivedTypes)),

    gen_server:stop(EmitterPid),
    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%% @doc GIVEN a subscriber listening for a specific event type
%%      WHEN events of different types are appended
%%      THEN only the matching event type is delivered
subscribe_by_event_type_delivers_matching(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    TargetType = <<"order_placed_v1">>,
    SubName = <<"type_filter_sub">>,

    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, event_type, TargetType, SubName,
        #{subscriber => self()}
    ),

    EmitterName = reckon_db_emitter_group:emitter_name(StoreId, SubKey),
    {ok, EmitterPid} = reckon_db_emitter:start_link(
        StoreId, SubKey, self(), EmitterName
    ),

    %% Append a matching event
    StreamId = <<"order$order-123">>,
    MatchingEvent = #{
        event_type => <<"order_placed_v1">>,
        data => #{<<"amount">> => 42}
    },
    {ok, _} = reckon_db_streams:append(StoreId, StreamId, -2, [MatchingEvent]),

    %% Append a non-matching event to a different stream
    StreamId2 = <<"user$user-456">>,
    NonMatchingEvent = #{
        event_type => <<"user_registered_v1">>,
        data => #{<<"name">> => <<"alice">>}
    },
    {ok, _} = reckon_db_streams:append(StoreId, StreamId2, -2, [NonMatchingEvent]),

    %% Should receive the matching event
    receive
        {events, [ReceivedEvent]} ->
            ?assertEqual(<<"order_placed_v1">>, ReceivedEvent#event.event_type)
    after 5000 ->
        ct:fail("Did not receive matching event")
    end,

    %% Should NOT receive the non-matching event
    receive
        {events, [UnexpectedEvent]} ->
            ct:fail("Received unexpected event: ~p", [UnexpectedEvent#event.event_type])
    after 1000 ->
        ok  %% Expected: no more events
    end,

    gen_server:stop(EmitterPid),
    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%% @doc GIVEN a subscriber that unsubscribes
%%      WHEN an event is appended after unsubscribe
%%      THEN no event is delivered
unsubscribe_stops_delivery(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$unsub-delivery">>,
    SubName = <<"unsub_delivery_sub">>,

    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => self()}
    ),

    EmitterName = reckon_db_emitter_group:emitter_name(StoreId, SubKey),
    {ok, EmitterPid} = reckon_db_emitter:start_link(
        StoreId, SubKey, self(), EmitterName
    ),

    %% First event should be delivered
    {ok, _} = reckon_db_streams:append(StoreId, StreamId, -2,
        [#{event_type => <<"before_unsub_v1">>, data => #{}}]),

    receive
        {events, [_]} -> ok
    after 5000 ->
        ct:fail("First event not delivered")
    end,

    %% Unsubscribe and stop emitter
    gen_server:stop(EmitterPid),
    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),

    %% Append another event
    {ok, _} = reckon_db_streams:append(StoreId, StreamId, -2,
        [#{event_type => <<"after_unsub_v1">>, data => #{}}]),

    %% Should NOT receive it
    receive
        {events, _} ->
            ct:fail("Received event after unsubscribe")
    after 1000 ->
        ok  %% Expected: nothing
    end,

    ok.

%% @doc GIVEN a subscriber
%%      WHEN an event with specific data is appended
%%      THEN the received event contains the correct data, type, and stream_id
subscriber_receives_correct_event_data(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"venture$venture-abc123">>,
    SubName = <<"data_verify_sub">>,

    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName,
        #{subscriber => self()}
    ),

    EmitterName = reckon_db_emitter_group:emitter_name(StoreId, SubKey),
    {ok, EmitterPid} = reckon_db_emitter:start_link(
        StoreId, SubKey, self(), EmitterName
    ),

    EventData = #{
        <<"venture_id">> => <<"venture-abc123">>,
        <<"name">> => <<"Test Venture">>,
        <<"brief">> => <<"A test venture">>
    },
    Event = #{
        event_type => <<"venture_initiated_v1">>,
        data => EventData,
        metadata => #{<<"correlation_id">> => <<"req-999">>}
    },
    {ok, NewVersion} = reckon_db_streams:append(StoreId, StreamId, -2, [Event]),

    receive
        {events, [ReceivedEvent]} ->
            %% Verify all fields
            ?assertEqual(<<"venture_initiated_v1">>, ReceivedEvent#event.event_type),
            ?assertEqual(StreamId, ReceivedEvent#event.stream_id),
            ?assertEqual(0, ReceivedEvent#event.version),
            ?assert(is_binary(ReceivedEvent#event.event_id)),
            ?assert(ReceivedEvent#event.timestamp > 0),

            %% Verify the data payload is preserved
            ReceivedData = ReceivedEvent#event.data,
            ?assertEqual(EventData, ReceivedData),

            %% Verify metadata
            ReceivedMeta = ReceivedEvent#event.metadata,
            ?assertEqual(<<"req-999">>, maps:get(<<"correlation_id">>, ReceivedMeta)),

            %% Verify version matches what append returned
            %% append returns the version of the last written event (0-based)
            ?assertEqual(NewVersion, ReceivedEvent#event.version)
    after 5000 ->
        ct:fail("Did not receive event with correct data")
    end,

    gen_server:stop(EmitterPid),
    reckon_db_subscriptions:unsubscribe(StoreId, SubKey),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% @private Collect N events from the mailbox, each arriving as {events, [E]}
collect_events(N, Timeout) ->
    collect_events(N, Timeout, []).

collect_events(0, _Timeout, Acc) ->
    lists:reverse(Acc);
collect_events(N, Timeout, Acc) ->
    receive
        {events, Events} when is_list(Events) ->
            collect_events(N - length(Events), Timeout, lists:reverse(Events) ++ Acc)
    after Timeout ->
        lists:reverse(Acc)
    end.
