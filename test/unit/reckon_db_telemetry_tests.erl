%% @doc EUnit tests for reckon_db_telemetry module
%% @author Macula.io

-module(reckon_db_telemetry_tests).

-include_lib("eunit/include/eunit.hrl").
-include("reckon_db_telemetry.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

telemetry_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
        {"attach and detach default handler",
         fun attach_detach_default_handler_test/0},
        {"attach and detach custom handler",
         fun attach_detach_custom_handler_test/0},
        {"emit stream write events",
         fun emit_stream_write_events_test/0},
        {"emit subscription events",
         fun emit_subscription_events_test/0},
        {"emit snapshot events",
         fun emit_snapshot_events_test/0},
        {"emit cluster events",
         fun emit_cluster_events_test/0},
        {"emit store events",
         fun emit_store_events_test/0},
        {"emit emitter events",
         fun emit_emitter_events_test/0},
        {"custom handler receives events",
         fun custom_handler_receives_events_test/0}
     ]}.

setup() ->
    %% Ensure telemetry app is started
    application:ensure_all_started(telemetry),
    ok.

cleanup(_) ->
    %% Detach any handlers
    catch reckon_db_telemetry:detach_default_handler(),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

attach_detach_default_handler_test() ->
    %% Should be able to attach
    Result1 = reckon_db_telemetry:attach_default_handler(),
    ?assertEqual(ok, Result1),

    %% Should fail when already attached
    Result2 = reckon_db_telemetry:attach_default_handler(),
    ?assertEqual({error, already_exists}, Result2),

    %% Should be able to detach
    Result3 = reckon_db_telemetry:detach_default_handler(),
    ?assertEqual(ok, Result3),

    %% Should be able to attach again
    Result4 = reckon_db_telemetry:attach_default_handler(),
    ?assertEqual(ok, Result4),

    %% Cleanup
    reckon_db_telemetry:detach_default_handler().

attach_detach_custom_handler_test() ->
    Handler = fun(_Event, _Measurements, _Meta, _Config) -> ok end,

    %% Should be able to attach
    Result1 = reckon_db_telemetry:attach(test_handler, Handler, #{}),
    ?assertEqual(ok, Result1),

    %% Should fail when already attached
    Result2 = reckon_db_telemetry:attach(test_handler, Handler, #{}),
    ?assertEqual({error, already_exists}, Result2),

    %% Should be able to detach
    Result3 = reckon_db_telemetry:detach(test_handler),
    ?assertEqual(ok, Result3).

emit_stream_write_events_test() ->
    ok = reckon_db_telemetry:attach_default_handler(),

    %% Should not crash when emitting stream events
    ok = reckon_db_telemetry:emit(
        ?STREAM_WRITE_START,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => test_store, stream_id => <<"test-stream">>, event_count => 5}
    ),

    ok = reckon_db_telemetry:emit(
        ?STREAM_WRITE_STOP,
        #{duration => 1000, event_count => 5},
        #{store_id => test_store, stream_id => <<"test-stream">>, new_version => 5}
    ),

    ok = reckon_db_telemetry:emit(
        ?STREAM_WRITE_ERROR,
        #{duration => 500},
        #{store_id => test_store, stream_id => <<"test-stream">>, reason => version_mismatch}
    ),

    ok = reckon_db_telemetry:emit(
        ?STREAM_READ_START,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => test_store, stream_id => <<"test-stream">>}
    ),

    ok = reckon_db_telemetry:emit(
        ?STREAM_READ_STOP,
        #{duration => 200, event_count => 10},
        #{store_id => test_store, stream_id => <<"test-stream">>}
    ),

    reckon_db_telemetry:detach_default_handler().

emit_subscription_events_test() ->
    ok = reckon_db_telemetry:attach_default_handler(),

    ok = reckon_db_telemetry:emit(
        ?SUBSCRIPTION_CREATED,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => test_store, subscription_id => <<"sub-1">>, type => by_stream}
    ),

    ok = reckon_db_telemetry:emit(
        ?SUBSCRIPTION_DELETED,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => test_store, subscription_id => <<"sub-1">>}
    ),

    ok = reckon_db_telemetry:emit(
        ?SUBSCRIPTION_EVENT_DELIVERED,
        #{duration => 100},
        #{store_id => test_store, subscription_id => <<"sub-1">>, event_id => <<"evt-1">>}
    ),

    reckon_db_telemetry:detach_default_handler().

emit_snapshot_events_test() ->
    ok = reckon_db_telemetry:attach_default_handler(),

    ok = reckon_db_telemetry:emit(
        ?SNAPSHOT_CREATED,
        #{system_time => erlang:system_time(millisecond), size_bytes => 1024, duration => 50},
        #{store_id => test_store, stream_id => <<"test-stream">>, version => 10}
    ),

    ok = reckon_db_telemetry:emit(
        ?SNAPSHOT_READ,
        #{duration => 20, size_bytes => 1024},
        #{store_id => test_store, stream_id => <<"test-stream">>, version => 10}
    ),

    reckon_db_telemetry:detach_default_handler().

emit_cluster_events_test() ->
    ok = reckon_db_telemetry:attach_default_handler(),

    ok = reckon_db_telemetry:emit(
        ?CLUSTER_NODE_UP,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => test_store, node => 'node1@localhost', member_count => 2}
    ),

    ok = reckon_db_telemetry:emit(
        ?CLUSTER_NODE_DOWN,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => test_store, node => 'node1@localhost', reason => nodedown, member_count => 1}
    ),

    %% Test with 'leader' key
    ok = reckon_db_telemetry:emit(
        ?CLUSTER_LEADER_ELECTED,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => test_store, leader => 'node1@localhost'}
    ),

    %% Test with 'leader_node' key (backward compatibility)
    ok = reckon_db_telemetry:emit(
        ?CLUSTER_LEADER_ELECTED,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => test_store, leader_node => 'node2@localhost', previous_leader => 'node1@localhost'}
    ),

    reckon_db_telemetry:detach_default_handler().

emit_store_events_test() ->
    ok = reckon_db_telemetry:attach_default_handler(),

    ok = reckon_db_telemetry:emit(
        ?STORE_STARTED,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => test_store, mode => single, data_dir => <<"/tmp/test">>}
    ),

    ok = reckon_db_telemetry:emit(
        ?STORE_STOPPED,
        #{uptime_ms => 60000},
        #{store_id => test_store, reason => normal}
    ),

    reckon_db_telemetry:detach_default_handler().

emit_emitter_events_test() ->
    ok = reckon_db_telemetry:attach_default_handler(),

    ok = reckon_db_telemetry:emit(
        ?EMITTER_BROADCAST,
        #{duration => 50},
        #{store_id => test_store, subscription_id => <<"sub-1">>, recipient_count => 3}
    ),

    ok = reckon_db_telemetry:emit(
        ?EMITTER_POOL_CREATED,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => test_store, subscription_id => <<"sub-1">>, pool_size => 4}
    ),

    reckon_db_telemetry:detach_default_handler().

custom_handler_receives_events_test() ->
    Self = self(),

    %% Create a handler that sends events to the test process
    Handler = fun(Event, Measurements, Meta, _Config) ->
        Self ! {telemetry_event, Event, Measurements, Meta},
        ok
    end,

    ok = reckon_db_telemetry:attach(custom_test_handler, Handler, #{}),

    %% Emit an event
    ok = reckon_db_telemetry:emit(
        ?STREAM_WRITE_STOP,
        #{duration => 1000, event_count => 5},
        #{store_id => test_store, stream_id => <<"test-stream">>}
    ),

    %% Verify we received the event
    receive
        {telemetry_event, Event, Measurements, Meta} ->
            ?assertEqual(?STREAM_WRITE_STOP, Event),
            ?assertEqual(#{duration => 1000, event_count => 5}, Measurements),
            ?assertEqual(test_store, maps:get(store_id, Meta)),
            ?assertEqual(<<"test-stream">>, maps:get(stream_id, Meta))
    after 1000 ->
        ?assert(false, "Did not receive telemetry event")
    end,

    reckon_db_telemetry:detach(custom_test_handler).
