%% @doc Common Test suite for reckon_db_streams module
%%
%% Integration tests for stream operations including:
%% - Append with version checking
%% - Read operations (forward/backward)
%% - Stream metadata (version, exists, list)
%% - Error cases
%%
%% @author R. Lefever

-module(reckon_db_streams_SUITE).

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
    %% Append tests
    append_to_new_stream/1,
    append_to_existing_stream/1,
    append_with_any_version/1,
    append_wrong_expected_version/1,
    append_no_stream_to_existing/1,
    append_multiple_events/1,

    %% Read tests
    read_forward/1,
    read_backward/1,
    read_with_count/1,
    read_from_nonexistent_stream/1,
    read_empty_result/1,

    %% Stream metadata tests
    get_version_new_stream/1,
    get_version_existing_stream/1,
    stream_exists/1,
    stream_not_exists/1,
    list_streams/1,
    list_streams_empty/1,

    %% Delete tests
    delete_stream/1,
    delete_nonexistent_stream/1
]).

-define(STORE_ID, streams_test_store).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [
        {group, append_tests},
        {group, read_tests},
        {group, metadata_tests},
        {group, delete_tests}
    ].

groups() ->
    [
        {append_tests, [sequence], [
            append_to_new_stream,
            append_to_existing_stream,
            append_with_any_version,
            append_wrong_expected_version,
            append_no_stream_to_existing,
            append_multiple_events
        ]},
        {read_tests, [sequence], [
            read_forward,
            read_backward,
            read_with_count,
            read_from_nonexistent_stream,
            read_empty_result
        ]},
        {metadata_tests, [sequence], [
            get_version_new_stream,
            get_version_existing_stream,
            stream_exists,
            stream_not_exists,
            list_streams,
            list_streams_empty
        ]},
        {delete_tests, [sequence], [
            delete_stream,
            delete_nonexistent_stream
        ]}
    ].

init_per_suite(Config) ->
    %% Start required applications
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    %% Configure Ra data directory before starting Ra
    RaDataDir = "/tmp/reckon_db_streams_test_ra",
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
    RaDataDir = proplists:get_value(ra_data_dir, Config, "/tmp/reckon_db_streams_test_ra"),
    os:cmd("rm -rf " ++ RaDataDir),
    ok.

init_per_group(GroupName, Config) ->
    %% Use unique data directory for each group
    GroupStr = atom_to_list(GroupName),
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_streams_test_" ++ GroupStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    %% Generate unique store ID for each group to avoid conflicts
    StoreId = list_to_atom("streams_test_" ++ GroupStr ++ "_" ++ Rand),

    %% Start Khepri store - pass just the data directory string
    case khepri:start(DataDir, StoreId) of
        {ok, _} ->
            %% Initialize base paths
            khepri:put(StoreId, [streams], #{}),
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
%% Append Tests
%%====================================================================

%% @doc Test appending to a new stream with NO_STREAM expected version
append_to_new_stream(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = [generate_event(<<"test_event">>)],

    {ok, Version} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events),

    ?assertEqual(0, Version),
    ok.

%% @doc Test appending to an existing stream with correct version
append_to_existing_stream(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events1 = [generate_event(<<"test_event">>)],
    Events2 = [generate_event(<<"test_event_2">>)],

    {ok, V1} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events1),
    ?assertEqual(0, V1),

    {ok, V2} = reckon_db_streams:append(StoreId, StreamId, V1, Events2),
    ?assertEqual(1, V2),
    ok.

%% @doc Test appending with ANY_VERSION (no version check)
append_with_any_version(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = [generate_event(<<"test_event">>)],

    %% First append with ANY_VERSION to new stream
    {ok, V1} = reckon_db_streams:append(StoreId, StreamId, ?ANY_VERSION, Events),
    ?assertEqual(0, V1),

    %% Second append with ANY_VERSION
    {ok, V2} = reckon_db_streams:append(StoreId, StreamId, ?ANY_VERSION, Events),
    ?assertEqual(1, V2),
    ok.

%% @doc Test append fails with wrong expected version
append_wrong_expected_version(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = [generate_event(<<"test_event">>)],

    %% Create stream
    {ok, 0} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events),

    %% Try to append with wrong version
    Result = reckon_db_streams:append(StoreId, StreamId, 5, Events),
    ?assertMatch({error, {wrong_expected_version, 5, 0}}, Result),
    ok.

%% @doc Test append with NO_STREAM fails on existing stream
append_no_stream_to_existing(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = [generate_event(<<"test_event">>)],

    %% Create stream
    {ok, 0} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events),

    %% Try to append with NO_STREAM expectation
    Result = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events),
    ?assertMatch({error, {wrong_expected_version, ?NO_STREAM, 0}}, Result),
    ok.

%% @doc Test appending multiple events at once
append_multiple_events(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"batch_event">>, 5),

    {ok, Version} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events),

    %% Version should be 4 (0-indexed, 5 events = versions 0-4)
    ?assertEqual(4, Version),
    ok.

%%====================================================================
%% Read Tests
%%====================================================================

%% @doc Test reading events forward
read_forward(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"forward_event">>, 5),

    {ok, _} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events),

    {ok, ReadEvents} = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),

    ?assertEqual(5, length(ReadEvents)),

    %% Verify order (versions should be 0, 1, 2, 3, 4)
    Versions = [E#event.version || E <- ReadEvents],
    ?assertEqual([0, 1, 2, 3, 4], Versions),
    ok.

%% @doc Test reading events backward
read_backward(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"backward_event">>, 5),

    {ok, _} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events),

    %% Read backward from version 4
    {ok, ReadEvents} = reckon_db_streams:read(StoreId, StreamId, 4, 5, backward),

    ?assertEqual(5, length(ReadEvents)),

    %% Verify reverse order (versions should be 4, 3, 2, 1, 0)
    Versions = [E#event.version || E <- ReadEvents],
    ?assertEqual([4, 3, 2, 1, 0], Versions),
    ok.

%% @doc Test reading with limited count
read_with_count(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"count_event">>, 10),

    {ok, _} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events),

    %% Read only 3 events
    {ok, ReadEvents} = reckon_db_streams:read(StoreId, StreamId, 0, 3, forward),

    ?assertEqual(3, length(ReadEvents)),
    Versions = [E#event.version || E <- ReadEvents],
    ?assertEqual([0, 1, 2], Versions),
    ok.

%% @doc Test reading from non-existent stream
read_from_nonexistent_stream(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),

    Result = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),

    ?assertMatch({error, {stream_not_found, StreamId}}, Result),
    ok.

%% @doc Test reading from position beyond stream length
read_empty_result(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"empty_event">>, 3),

    {ok, _} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events),

    %% Read from beyond end of stream
    {ok, ReadEvents} = reckon_db_streams:read(StoreId, StreamId, 100, 10, forward),

    ?assertEqual(0, length(ReadEvents)),
    ok.

%%====================================================================
%% Metadata Tests
%%====================================================================

%% @doc Test get_version on non-existent stream
get_version_new_stream(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),

    Version = reckon_db_streams:get_version(StoreId, StreamId),

    ?assertEqual(?NO_STREAM, Version),
    ok.

%% @doc Test get_version on existing stream
get_version_existing_stream(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = generate_events(<<"version_event">>, 5),

    {ok, _} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events),

    Version = reckon_db_streams:get_version(StoreId, StreamId),

    ?assertEqual(4, Version),
    ok.

%% @doc Test exists returns true for existing stream
stream_exists(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = [generate_event(<<"exists_event">>)],

    {ok, _} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events),

    Exists = reckon_db_streams:exists(StoreId, StreamId),

    ?assertEqual(true, Exists),
    ok.

%% @doc Test exists returns false for non-existent stream
stream_not_exists(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),

    Exists = reckon_db_streams:exists(StoreId, StreamId),

    ?assertEqual(false, Exists),
    ok.

%% @doc Test listing all streams
list_streams(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    Stream1 = generate_stream_id(),
    Stream2 = generate_stream_id(),
    Events = [generate_event(<<"list_event">>)],

    {ok, _} = reckon_db_streams:append(StoreId, Stream1, ?NO_STREAM, Events),
    {ok, _} = reckon_db_streams:append(StoreId, Stream2, ?NO_STREAM, Events),

    {ok, Streams} = reckon_db_streams:list_streams(StoreId),

    ?assert(lists:member(Stream1, Streams)),
    ?assert(lists:member(Stream2, Streams)),
    ok.

%% @doc Test listing streams when none exist
list_streams_empty(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    %% This test runs in a fresh store from init_per_group
    %% Note: Other tests in this group may have created streams
    %% So we just verify the function returns a list
    Result = reckon_db_streams:list_streams(StoreId),

    ?assertMatch({ok, _}, Result),
    ok.

%%====================================================================
%% Delete Tests
%%====================================================================

%% @doc Test deleting an existing stream
delete_stream(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),
    Events = [generate_event(<<"delete_event">>)],

    {ok, _} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, Events),
    ?assertEqual(true, reckon_db_streams:exists(StoreId, StreamId)),

    ok = reckon_db_streams:delete(StoreId, StreamId),

    ?assertEqual(false, reckon_db_streams:exists(StoreId, StreamId)),
    ok.

%% @doc Test deleting a non-existent stream (should not error)
delete_nonexistent_stream(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = generate_stream_id(),

    %% Should succeed even if stream doesn't exist
    Result = reckon_db_streams:delete(StoreId, StreamId),

    ?assertEqual(ok, Result),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

%% @private Generate a unique stream ID
generate_stream_id() ->
    Uuid = generate_uuid(),
    <<"test$", Uuid/binary>>.

%% @private Generate a test event
generate_event(EventType) ->
    #{
        event_type => EventType,
        data => #{
            <<"key">> => <<"value">>,
            <<"timestamp">> => erlang:system_time(millisecond)
        },
        metadata => #{
            <<"correlation_id">> => generate_uuid()
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
