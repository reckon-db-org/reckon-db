%% @doc Common Test suite for reckon_db_snapshots module
%%
%% Integration tests for snapshot operations including:
%% - Save/load operations
%% - Version management
%% - Delete operations
%% - Integration with streams and aggregator
%%
%% @author Macula.io

-module(reckon_db_snapshots_SUITE).

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
    %% Basic snapshot tests
    save_and_load_snapshot/1,
    load_nonexistent_snapshot/1,
    save_with_metadata/1,
    overwrite_snapshot/1,

    %% Version tests
    save_multiple_versions/1,
    load_specific_version/1,
    load_latest_version/1,

    %% List and delete tests
    list_snapshots/1,
    delete_all_snapshots/1,
    delete_specific_version/1,
    exists_check/1,

    %% Integration with streams
    snapshot_with_stream_events/1,
    aggregate_from_snapshot/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [
        {group, basic_tests},
        {group, version_tests},
        {group, delete_tests},
        {group, integration_tests}
    ].

groups() ->
    [
        {basic_tests, [sequence], [
            save_and_load_snapshot,
            load_nonexistent_snapshot,
            save_with_metadata,
            overwrite_snapshot
        ]},
        {version_tests, [sequence], [
            save_multiple_versions,
            load_specific_version,
            load_latest_version
        ]},
        {delete_tests, [sequence], [
            list_snapshots,
            delete_all_snapshots,
            delete_specific_version,
            exists_check
        ]},
        {integration_tests, [sequence], [
            snapshot_with_stream_events,
            aggregate_from_snapshot
        ]}
    ].

init_per_suite(Config) ->
    %% Start required applications
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    %% Configure Ra data directory before starting Ra
    RaDataDir = "/tmp/reckon_db_snapshots_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),

    %% Start Ra first, then Khepri
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),

    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    RaDataDir = proplists:get_value(ra_data_dir, Config, "/tmp/reckon_db_snapshots_test_ra"),
    os:cmd("rm -rf " ++ RaDataDir),
    ok.

init_per_group(GroupName, Config) ->
    GroupStr = atom_to_list(GroupName),
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_snapshots_test_" ++ GroupStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    StoreId = list_to_atom("snapshots_test_" ++ GroupStr ++ "_" ++ Rand),

    case khepri:start(DataDir, StoreId) of
        {ok, _} ->
            khepri:put(StoreId, [streams], #{}),
            khepri:put(StoreId, [snapshots], #{}),
            [{data_dir, DataDir}, {store_id, StoreId} | Config];
        {error, Reason} ->
            ct:pal("Khepri start error: ~p~n", [Reason]),
            ct:fail("Failed to start Khepri: ~p", [Reason])
    end.

end_per_group(_GroupName, Config) ->
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
%% Basic Snapshot Tests
%%====================================================================

%% @doc Test saving and loading a snapshot
save_and_load_snapshot(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$basic-stream">>,
    Data = #{name => <<"Alice">>, balance => 100},

    %% Save snapshot
    ok = reckon_db_snapshots:save(StoreId, StreamId, 5, Data),

    %% Load snapshot
    {ok, Snapshot} = reckon_db_snapshots:load(StoreId, StreamId),

    ?assertEqual(StreamId, Snapshot#snapshot.stream_id),
    ?assertEqual(5, Snapshot#snapshot.version),
    ?assertEqual(Data, Snapshot#snapshot.data),
    ok.

%% @doc Test loading non-existent snapshot returns error
load_nonexistent_snapshot(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$nonexistent">>,

    Result = reckon_db_snapshots:load(StoreId, StreamId),

    ?assertEqual({error, not_found}, Result),
    ok.

%% @doc Test saving snapshot with metadata
save_with_metadata(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$metadata-stream">>,
    Data = #{count => 42},
    Metadata = #{source => <<"test">>, version => 1},

    ok = reckon_db_snapshots:save(StoreId, StreamId, 10, Data, Metadata),

    {ok, Snapshot} = reckon_db_snapshots:load(StoreId, StreamId),

    ?assertEqual(Data, Snapshot#snapshot.data),
    ?assertEqual(Metadata, Snapshot#snapshot.metadata),
    ok.

%% @doc Test overwriting snapshot at same version
overwrite_snapshot(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$overwrite-stream">>,

    %% Save first version
    ok = reckon_db_snapshots:save(StoreId, StreamId, 5, #{value => 1}),

    %% Overwrite with new data
    ok = reckon_db_snapshots:save(StoreId, StreamId, 5, #{value => 2}),

    %% Load should return the new data
    {ok, Snapshot} = reckon_db_snapshots:load_at(StoreId, StreamId, 5),
    ?assertEqual(#{value => 2}, Snapshot#snapshot.data),
    ok.

%%====================================================================
%% Version Tests
%%====================================================================

%% @doc Test saving multiple snapshot versions
save_multiple_versions(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$multi-version">>,

    %% Save multiple versions
    ok = reckon_db_snapshots:save(StoreId, StreamId, 10, #{version => 10}),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 20, #{version => 20}),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 30, #{version => 30}),

    %% List all versions
    {ok, Snapshots} = reckon_db_snapshots:list(StoreId, StreamId),

    ?assertEqual(3, length(Snapshots)),
    Versions = [S#snapshot.version || S <- Snapshots],
    ?assertEqual([10, 20, 30], Versions),
    ok.

%% @doc Test loading a specific version
load_specific_version(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$specific-version">>,

    ok = reckon_db_snapshots:save(StoreId, StreamId, 5, #{v => 5}),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 10, #{v => 10}),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 15, #{v => 15}),

    %% Load specific version
    {ok, Snapshot} = reckon_db_snapshots:load_at(StoreId, StreamId, 10),
    ?assertEqual(#{v => 10}, Snapshot#snapshot.data),
    ?assertEqual(10, Snapshot#snapshot.version),
    ok.

%% @doc Test loading latest version
load_latest_version(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$latest-version">>,

    ok = reckon_db_snapshots:save(StoreId, StreamId, 5, #{v => 5}),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 15, #{v => 15}),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 10, #{v => 10}),

    %% Load should return highest version (15)
    {ok, Snapshot} = reckon_db_snapshots:load(StoreId, StreamId),
    ?assertEqual(#{v => 15}, Snapshot#snapshot.data),
    ?assertEqual(15, Snapshot#snapshot.version),
    ok.

%%====================================================================
%% Delete Tests
%%====================================================================

%% @doc Test listing snapshots
list_snapshots(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$list-stream">>,

    ok = reckon_db_snapshots:save(StoreId, StreamId, 1, #{a => 1}),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 2, #{a => 2}),

    {ok, Snapshots} = reckon_db_snapshots:list(StoreId, StreamId),
    ?assertEqual(2, length(Snapshots)),
    ok.

%% @doc Test deleting all snapshots for a stream
delete_all_snapshots(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$delete-all">>,

    ok = reckon_db_snapshots:save(StoreId, StreamId, 1, #{a => 1}),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 2, #{a => 2}),

    ?assertEqual(true, reckon_db_snapshots:exists(StoreId, StreamId)),

    ok = reckon_db_snapshots:delete(StoreId, StreamId),

    ?assertEqual(false, reckon_db_snapshots:exists(StoreId, StreamId)),
    {ok, []} = reckon_db_snapshots:list(StoreId, StreamId),
    ok.

%% @doc Test deleting a specific snapshot version
delete_specific_version(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$delete-specific">>,

    ok = reckon_db_snapshots:save(StoreId, StreamId, 1, #{a => 1}),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 2, #{a => 2}),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 3, #{a => 3}),

    %% Delete version 2
    ok = reckon_db_snapshots:delete_at(StoreId, StreamId, 2),

    {ok, Snapshots} = reckon_db_snapshots:list(StoreId, StreamId),
    Versions = [S#snapshot.version || S <- Snapshots],
    ?assertEqual([1, 3], Versions),
    ok.

%% @doc Test exists check
exists_check(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$exists">>,

    ?assertEqual(false, reckon_db_snapshots:exists(StoreId, StreamId)),
    ?assertEqual(false, reckon_db_snapshots:exists_at(StoreId, StreamId, 5)),

    ok = reckon_db_snapshots:save(StoreId, StreamId, 5, #{test => true}),

    ?assertEqual(true, reckon_db_snapshots:exists(StoreId, StreamId)),
    ?assertEqual(true, reckon_db_snapshots:exists_at(StoreId, StreamId, 5)),
    ?assertEqual(false, reckon_db_snapshots:exists_at(StoreId, StreamId, 10)),
    ok.

%%====================================================================
%% Integration Tests
%%====================================================================

%% @doc Test snapshot with stream events
snapshot_with_stream_events(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"test$integration">>,

    %% Write some events
    Events = [
        #{event_type => <<"balance_credited">>, data => #{amount => {sum, 100}}},
        #{event_type => <<"balance_credited">>, data => #{amount => {sum, 50}}}
    ],
    {ok, _} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events),

    %% Create a snapshot at version 1
    SnapshotData = #{balance => 150},
    ok = reckon_db_snapshots:save(StoreId, StreamId, 1, SnapshotData),

    %% Verify both exist
    ?assertEqual(true, reckon_db_snapshots:exists(StoreId, StreamId)),
    {ok, ReadEvents} = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),
    ?assertEqual(2, length(ReadEvents)),
    ok.

%% @doc Test aggregating from a snapshot
aggregate_from_snapshot(_Config) ->
    StreamId = <<"test$aggregate">>,

    %% Create initial state via snapshot
    Snapshot = #snapshot{
        stream_id = StreamId,
        version = 5,
        data = #{name => <<"Bob">>, balance => 500},
        metadata = #{},
        timestamp = erlang:system_time(millisecond)
    },

    %% Create events to apply on top
    NewEvents = [
        #{data => #{balance => {sum, 100}}},
        #{data => #{balance => {sum, -50}}},
        #{data => #{status => <<"active">>}}
    ],

    %% Aggregate from snapshot
    Result = reckon_db_aggregator:aggregate(NewEvents, Snapshot, #{}),

    Expected = #{
        name => <<"Bob">>,
        balance => 550,
        status => <<"active">>
    },
    ?assertEqual(Expected, Result),
    ok.
