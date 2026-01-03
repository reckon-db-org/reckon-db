%% @doc End-to-end test suite for reckon-db with reckon-db-gater integration
%%
%% Tests the full integration between the event store (reckon-db) and the
%% gateway (reckon-db-gater), including:
%%
%% - Gateway worker registration and discovery
%% - Stream operations via gater API
%% - Subscription operations via gater API
%% - Snapshot operations via gater API
%% - Load balancing across multiple workers
%% - Retry and failover behavior
%%
%% @author rgfaber

-module(reckon_db_gater_e2e_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% Include gater first to avoid DEFAULT_TIMEOUT conflict
-include_lib("reckon_gater/include/esdb_gater.hrl").
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

%% Test cases - Worker registration
-export([
    register_worker_test/1,
    unregister_worker_test/1,
    get_workers_test/1,
    multiple_workers_test/1
]).

%% Test cases - Stream operations via gater
-export([
    gater_append_events_test/1,
    gater_append_with_version_test/1,
    gater_get_events_forward_test/1,
    gater_get_events_backward_test/1,
    gater_stream_forward_test/1,
    gater_stream_backward_test/1,
    gater_get_version_test/1,
    gater_get_streams_test/1,
    gater_wrong_version_test/1
]).

%% Test cases - Subscription operations via gater
-export([
    gater_save_subscription_test/1,
    gater_get_subscriptions_test/1,
    gater_remove_subscription_test/1,
    gater_ack_event_test/1
]).

%% Test cases - Snapshot operations via gater
-export([
    gater_record_snapshot_test/1,
    gater_read_snapshot_test/1,
    gater_list_snapshots_test/1,
    gater_delete_snapshot_test/1
]).

%% Test cases - Load balancing and health
-export([
    gater_load_balance_test/1,
    gater_health_check_test/1,
    gater_no_workers_error_test/1
]).

-define(STORE_ID, e2e_test_store).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
        {group, worker_registration},
        {group, stream_operations},
        {group, subscription_operations},
        {group, snapshot_operations},
        {group, load_balancing}
    ].

groups() ->
    [
        {worker_registration, [sequence], [
            register_worker_test,
            unregister_worker_test,
            get_workers_test,
            multiple_workers_test
        ]},
        {stream_operations, [sequence], [
            gater_append_events_test,
            gater_append_with_version_test,
            gater_get_events_forward_test,
            gater_get_events_backward_test,
            gater_stream_forward_test,
            gater_stream_backward_test,
            gater_get_version_test,
            gater_get_streams_test,
            gater_wrong_version_test
        ]},
        {subscription_operations, [sequence], [
            gater_save_subscription_test,
            gater_get_subscriptions_test,
            gater_remove_subscription_test,
            gater_ack_event_test
        ]},
        {snapshot_operations, [sequence], [
            gater_record_snapshot_test,
            gater_read_snapshot_test,
            gater_list_snapshots_test,
            gater_delete_snapshot_test
        ]},
        {load_balancing, [sequence], [
            gater_load_balance_test,
            gater_health_check_test,
            gater_no_workers_error_test
        ]}
    ].

init_per_suite(Config) ->
    %% Start required applications
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    %% Configure Ra data directory
    RaDataDir = "/tmp/reckon_db_e2e_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),

    %% Start Ra
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),

    %% Start Khepri
    {ok, _} = application:ensure_all_started(khepri),

    %% Start pg scope for reckon_db
    case pg:start(?RECKON_DB_PG_SCOPE) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,

    %% Start pg scope for reckon_gater
    case pg:start(reckon_gater_pg) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,

    %% Start reckon_gater application (worker registry)
    {ok, _} = application:ensure_all_started(reckon_gater),

    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    %% Stop applications
    application:stop(reckon_gater),

    %% Clean up Ra data directory
    RaDataDir = proplists:get_value(ra_data_dir, Config, "/tmp/reckon_db_e2e_test_ra"),
    os:cmd("rm -rf " ++ RaDataDir),
    ok.

init_per_group(GroupName, Config) ->
    %% Use unique data directory for each group
    GroupStr = atom_to_list(GroupName),
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_e2e_test_" ++ GroupStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    %% Generate unique store ID
    StoreId = list_to_atom("e2e_test_" ++ GroupStr ++ "_" ++ Rand),

    %% Start Khepri store
    case khepri:start(DataDir, StoreId) of
        {ok, _} ->
            %% Initialize base paths
            khepri:put(StoreId, [streams], #{}),
            khepri:put(StoreId, [subscriptions], #{}),
            khepri:put(StoreId, [snapshots], #{}),
            [{data_dir, DataDir}, {store_id, StoreId} | Config];
        {error, Reason} ->
            ct:fail("Failed to start Khepri: ~p", [Reason])
    end.

end_per_group(_GroupName, Config) ->
    StoreId = proplists:get_value(store_id, Config),

    %% Unregister any remaining workers
    case esdb_gater_api:get_workers(StoreId) of
        {ok, Workers} ->
            lists:foreach(fun(#worker_entry{pid = Pid}) ->
                esdb_gater_api:unregister_worker(StoreId, Pid)
            end, Workers);
        _ ->
            ok
    end,

    %% Stop and clean up store
    khepri:stop(StoreId),
    DataDir = proplists:get_value(data_dir, Config),
    os:cmd("rm -rf " ++ DataDir),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Worker Registration Tests
%%====================================================================

%% @doc Test registering a gateway worker
register_worker_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    %% Start a mock worker process
    Worker = spawn_link(fun() -> mock_worker_loop(StoreId) end),

    %% Register worker
    ok = esdb_gater_api:register_worker(StoreId, Worker),

    %% Verify worker is registered
    {ok, Workers} = esdb_gater_api:get_workers(StoreId),
    ?assertEqual(1, length(Workers)),

    %% Cleanup
    exit(Worker, normal),
    timer:sleep(100),
    ok.

%% @doc Test unregistering a gateway worker
unregister_worker_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    %% Start and register a worker
    Worker = spawn_link(fun() -> mock_worker_loop(StoreId) end),
    ok = esdb_gater_api:register_worker(StoreId, Worker),

    %% Verify registered
    {ok, Workers1} = esdb_gater_api:get_workers(StoreId),
    ?assertEqual(1, length(Workers1)),

    %% Unregister
    ok = esdb_gater_api:unregister_worker(StoreId, Worker),

    %% Verify unregistered
    {ok, Workers2} = esdb_gater_api:get_workers(StoreId),
    ?assertEqual(0, length(Workers2)),

    exit(Worker, normal),
    ok.

%% @doc Test getting registered workers
get_workers_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    %% No workers initially
    {ok, Workers0} = esdb_gater_api:get_workers(StoreId),
    ?assertEqual(0, length(Workers0)),

    %% Register 3 workers
    Worker1 = spawn_link(fun() -> mock_worker_loop(StoreId) end),
    Worker2 = spawn_link(fun() -> mock_worker_loop(StoreId) end),
    Worker3 = spawn_link(fun() -> mock_worker_loop(StoreId) end),

    ok = esdb_gater_api:register_worker(StoreId, Worker1),
    ok = esdb_gater_api:register_worker(StoreId, Worker2),
    ok = esdb_gater_api:register_worker(StoreId, Worker3),

    %% Verify 3 workers
    {ok, Workers} = esdb_gater_api:get_workers(StoreId),
    ?assertEqual(3, length(Workers)),

    %% Cleanup
    exit(Worker1, normal),
    exit(Worker2, normal),
    exit(Worker3, normal),
    timer:sleep(100),
    ok.

%% @doc Test multiple workers handling requests
multiple_workers_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    %% Start a real gateway worker using the gateway worker module
    Worker = start_gateway_worker(StoreId, Config),

    %% Give time for registration
    timer:sleep(200),

    %% Verify worker is registered
    {ok, Workers} = esdb_gater_api:get_workers(StoreId),
    ?assert(length(Workers) >= 1),

    %% Stop worker
    gen_server:stop(Worker),
    timer:sleep(100),
    ok.

%%====================================================================
%% Stream Operations via Gater Tests
%%====================================================================

%% @doc Test appending events via gater
gater_append_events_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"gater_event">>, 3),

    %% Start a gateway worker
    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    %% Append via gater API
    {ok, Result} = esdb_gater_api:append_events(StoreId, StreamId, Events),
    {ok, Version} = Result,

    ?assertEqual(2, Version),

    gen_server:stop(Worker),
    ok.

%% @doc Test appending with expected version
gater_append_with_version_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events1 = generate_events(<<"event1">>, 2),
    Events2 = generate_events(<<"event2">>, 2),

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    %% First append
    {ok, {ok, V1}} = esdb_gater_api:append_events(StoreId, StreamId, -1, Events1),
    ?assertEqual(1, V1),

    %% Second append with correct version
    {ok, {ok, V2}} = esdb_gater_api:append_events(StoreId, StreamId, V1, Events2),
    ?assertEqual(3, V2),

    gen_server:stop(Worker),
    ok.

%% @doc Test reading events forward via gater
gater_get_events_forward_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"forward_event">>, 5),

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    %% Append events
    {ok, _} = esdb_gater_api:append_events(StoreId, StreamId, Events),

    %% Read forward
    {ok, {ok, ReadEvents}} = esdb_gater_api:get_events(StoreId, StreamId, 0, 10, forward),

    ?assertEqual(5, length(ReadEvents)),

    %% Verify ordering (versions should be 0,1,2,3,4)
    Versions = [E#event.version || E <- ReadEvents],
    ?assertEqual([0, 1, 2, 3, 4], Versions),

    gen_server:stop(Worker),
    ok.

%% @doc Test reading events backward via gater
gater_get_events_backward_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"backward_event">>, 5),

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    {ok, _} = esdb_gater_api:append_events(StoreId, StreamId, Events),

    %% Read backward from version 4
    {ok, {ok, ReadEvents}} = esdb_gater_api:get_events(StoreId, StreamId, 4, 5, backward),

    ?assertEqual(5, length(ReadEvents)),

    %% Verify reverse order
    Versions = [E#event.version || E <- ReadEvents],
    ?assertEqual([4, 3, 2, 1, 0], Versions),

    gen_server:stop(Worker),
    ok.

%% @doc Test stream_forward via gater
gater_stream_forward_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"stream_fwd">>, 10),

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    {ok, _} = esdb_gater_api:append_events(StoreId, StreamId, Events),

    %% Stream forward from version 5, count 3
    {ok, {ok, ReadEvents}} = esdb_gater_api:stream_forward(StoreId, StreamId, 5, 3),

    ?assertEqual(3, length(ReadEvents)),
    Versions = [E#event.version || E <- ReadEvents],
    ?assertEqual([5, 6, 7], Versions),

    gen_server:stop(Worker),
    ok.

%% @doc Test stream_backward via gater
gater_stream_backward_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"stream_bwd">>, 10),

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    {ok, _} = esdb_gater_api:append_events(StoreId, StreamId, Events),

    %% Stream backward from version 7, count 3
    {ok, {ok, ReadEvents}} = esdb_gater_api:stream_backward(StoreId, StreamId, 7, 3),

    ?assertEqual(3, length(ReadEvents)),
    Versions = [E#event.version || E <- ReadEvents],
    ?assertEqual([7, 6, 5], Versions),

    gen_server:stop(Worker),
    ok.

%% @doc Test getting stream version via gater
gater_get_version_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"version_event">>, 7),

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    {ok, _} = esdb_gater_api:append_events(StoreId, StreamId, Events),

    %% get_version returns integer directly wrapped in {ok, ...}
    {ok, Version} = esdb_gater_api:get_version(StoreId, StreamId),

    ?assertEqual(6, Version),

    gen_server:stop(Worker),
    ok.

%% @doc Test getting all streams via gater
gater_get_streams_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    Stream1 = generate_stream_id(),
    Stream2 = generate_stream_id(),
    Events = generate_events(<<"streams_event">>, 1),

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    {ok, _} = esdb_gater_api:append_events(StoreId, Stream1, Events),
    {ok, _} = esdb_gater_api:append_events(StoreId, Stream2, Events),

    {ok, {ok, Streams}} = esdb_gater_api:get_streams(StoreId),

    ?assert(lists:member(Stream1, Streams)),
    ?assert(lists:member(Stream2, Streams)),

    gen_server:stop(Worker),
    ok.

%% @doc Test wrong expected version error via gater
gater_wrong_version_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"wrong_ver">>, 3),

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    %% Create stream
    {ok, _} = esdb_gater_api:append_events(StoreId, StreamId, Events),

    %% Try to append with wrong version
    {ok, Result} = esdb_gater_api:append_events(StoreId, StreamId, 99, Events),
    ?assertMatch({error, {wrong_expected_version, _}}, Result),

    gen_server:stop(Worker),
    ok.

%%====================================================================
%% Subscription Operations via Gater Tests
%%====================================================================

%% @doc Test saving a subscription via gater
gater_save_subscription_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    SubName = <<"test_subscription_1">>,

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    %% Save subscription (cast - always returns ok)
    ok = esdb_gater_api:save_subscription(StoreId, stream, StreamId, SubName, 0, self()),

    %% Give time for cast to complete
    timer:sleep(100),

    gen_server:stop(Worker),
    ok.

%% @doc Test getting subscriptions via gater
gater_get_subscriptions_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    SubName = <<"test_subscription_2">>,

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    %% Save subscription
    ok = esdb_gater_api:save_subscription(StoreId, stream, StreamId, SubName, 0, self()),
    timer:sleep(100),

    %% Get subscriptions
    {ok, _Result} = esdb_gater_api:get_subscriptions(StoreId),

    gen_server:stop(Worker),
    ok.

%% @doc Test removing a subscription via gater
gater_remove_subscription_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    SubName = <<"test_subscription_3">>,

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    %% Save subscription
    ok = esdb_gater_api:save_subscription(StoreId, stream, StreamId, SubName, 0, self()),
    timer:sleep(100),

    %% Remove subscription
    ok = esdb_gater_api:remove_subscription(StoreId, stream, StreamId, SubName),
    timer:sleep(100),

    gen_server:stop(Worker),
    ok.

%% @doc Test acknowledging an event via gater
gater_ack_event_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    SubName = <<"test_subscription_ack">>,

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    %% Create a mock event
    Event = #{
        stream_id => <<"test_stream">>,
        version => 5,
        event_type => <<"test_event">>
    },

    %% Ack event (cast - always returns ok)
    ok = esdb_gater_api:ack_event(StoreId, SubName, self(), Event),

    gen_server:stop(Worker),
    ok.

%%====================================================================
%% Snapshot Operations via Gater Tests
%%====================================================================

%% @doc Test recording a snapshot via gater
gater_record_snapshot_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    SourceId = generate_uuid(),
    StreamId = generate_stream_id(),
    Version = 10,
    SnapshotData = #{
        state => #{balance => 1000},
        version => Version
    },

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    %% Record snapshot (cast)
    ok = esdb_gater_api:record_snapshot(StoreId, SourceId, StreamId, Version, SnapshotData),
    timer:sleep(100),

    gen_server:stop(Worker),
    ok.

%% @doc Test reading a snapshot via gater
gater_read_snapshot_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    SourceId = generate_uuid(),
    StreamId = generate_stream_id(),
    Version = 10,
    SnapshotData = #{
        state => #{balance => 2000},
        version => Version
    },

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    %% Record snapshot first
    ok = esdb_gater_api:record_snapshot(StoreId, SourceId, StreamId, Version, SnapshotData),
    timer:sleep(100),

    %% Read snapshot
    {ok, _Result} = esdb_gater_api:read_snapshot(StoreId, SourceId, StreamId, Version),

    gen_server:stop(Worker),
    ok.

%% @doc Test listing snapshots via gater
gater_list_snapshots_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    SourceId = generate_uuid(),
    StreamId = generate_stream_id(),

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    %% Record multiple snapshots
    ok = esdb_gater_api:record_snapshot(StoreId, SourceId, StreamId, 5, #{v => 5}),
    ok = esdb_gater_api:record_snapshot(StoreId, SourceId, StreamId, 10, #{v => 10}),
    ok = esdb_gater_api:record_snapshot(StoreId, SourceId, StreamId, 15, #{v => 15}),
    timer:sleep(100),

    %% List snapshots
    {ok, _Result} = esdb_gater_api:list_snapshots(StoreId, SourceId, StreamId),

    gen_server:stop(Worker),
    ok.

%% @doc Test deleting a snapshot via gater
gater_delete_snapshot_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    SourceId = generate_uuid(),
    StreamId = generate_stream_id(),
    Version = 20,

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    %% Record snapshot
    ok = esdb_gater_api:record_snapshot(StoreId, SourceId, StreamId, Version, #{v => 20}),
    timer:sleep(100),

    %% Delete snapshot (cast)
    ok = esdb_gater_api:delete_snapshot(StoreId, SourceId, StreamId, Version),
    timer:sleep(100),

    gen_server:stop(Worker),
    ok.

%%====================================================================
%% Load Balancing and Health Tests
%%====================================================================

%% @doc Test load balancing across multiple workers
gater_load_balance_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),

    %% Start 3 gateway workers
    Worker1 = start_gateway_worker(StoreId, Config),
    Worker2 = start_gateway_worker(StoreId, Config),
    Worker3 = start_gateway_worker(StoreId, Config),
    timer:sleep(300),

    %% Verify 3 workers registered
    {ok, Workers} = esdb_gater_api:get_workers(StoreId),
    ?assertEqual(3, length(Workers)),

    %% Make multiple requests - should be load balanced
    Events = generate_events(<<"lb_event">>, 1),
    lists:foreach(fun(_) ->
        {ok, _} = esdb_gater_api:append_events(StoreId, StreamId, Events)
    end, lists:seq(1, 10)),

    %% Verify all appends succeeded by reading
    {ok, {ok, ReadEvents}} = esdb_gater_api:get_events(StoreId, StreamId, 0, 100, forward),
    ?assertEqual(10, length(ReadEvents)),

    gen_server:stop(Worker1),
    gen_server:stop(Worker2),
    gen_server:stop(Worker3),
    ok.

%% @doc Test gateway health check
gater_health_check_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    Worker = start_gateway_worker(StoreId, Config),
    timer:sleep(200),

    %% Get health status
    {ok, Health} = esdb_gater_api:health(),

    ?assertEqual(healthy, maps:get(status, Health)),
    ?assert(is_map(maps:get(stores, Health))),
    ?assert(maps:get(total_workers, Health) >= 1),

    gen_server:stop(Worker),
    ok.

%% @doc Test error when no workers available
gater_no_workers_error_test(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"no_worker">>, 1),

    %% Ensure no workers registered
    {ok, Workers} = esdb_gater_api:get_workers(StoreId),
    ?assertEqual(0, length(Workers)),

    %% Try to append - should fail after retry exhaustion
    Result = esdb_gater_api:append_events(StoreId, StreamId, Events),
    ?assertMatch({error, {retries_exhausted, no_workers}}, Result),

    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

%% @private Start a real gateway worker
-spec start_gateway_worker(atom(), list()) -> pid().
start_gateway_worker(StoreId, Config) ->
    DataDir = proplists:get_value(data_dir, Config),
    GatewayConfig = #store_config{
        store_id = StoreId,
        data_dir = DataDir,
        mode = single,
        timeout = 5000,
        writer_pool_size = 1,
        reader_pool_size = 1,
        gateway_pool_size = 1,
        options = #{}
    },
    {ok, Pid} = reckon_db_gateway_worker:start_link(GatewayConfig),
    Pid.

%% @private Mock worker process for basic registration tests
mock_worker_loop(StoreId) ->
    receive
        {From, {get_version, _StoreId, _StreamId}} ->
            From ! {self(), {ok, 0}},
            mock_worker_loop(StoreId);
        stop ->
            ok;
        _ ->
            mock_worker_loop(StoreId)
    after
        60000 ->
            ok
    end.

%% @private Generate a unique stream ID
generate_stream_id() ->
    Uuid = generate_uuid(),
    <<"e2e_test$", Uuid/binary>>.

%% @private Generate a test event
generate_event(EventType) ->
    #{
        event_type => EventType,
        data => #{
            <<"key">> => <<"value">>,
            <<"timestamp">> => erlang:system_time(millisecond)
        },
        metadata => #{
            <<"correlation_id">> => generate_uuid(),
            <<"source">> => <<"e2e_test">>
        }
    }.

%% @private Generate multiple events
generate_events(EventType, Count) ->
    [generate_event(EventType) || _ <- lists:seq(1, Count)].

%% @private Generate a UUID
generate_uuid() ->
    Bytes = crypto:strong_rand_bytes(16),
    <<A:32, B:16, C:16, D:16, E:48>> = Bytes,
    iolist_to_binary(
        io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
                      [A, B, C, D, E])
    ).
