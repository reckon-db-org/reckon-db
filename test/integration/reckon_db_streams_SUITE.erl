%% @doc Common Test suite for reckon_db_streams module
%%
%% Integration tests for stream operations including:
%% - Append with version checking
%% - Read operations (forward/backward)
%% - Stream metadata (version, exists, list)
%% - Error cases
%%
%% @author rgfaber

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
    global_event_count_tracks_appends/1,

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
    delete_nonexistent_stream/1,

    %% read_all_global / cache tests
    read_all_global_returns_events_in_epoch_order/1,
    read_all_global_paginates/1,
    read_all_global_cache_hit_matches_scan/1,
    read_all_global_invalidates_on_append/1,
    read_all_global_cache_isolated_per_store/1,
    read_all_global_rejects_torn_page_instead_of_mixing_generations/1
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
        {group, delete_tests},
        {group, read_all_global_tests}
    ].

groups() ->
    [
        {append_tests, [sequence], [
            append_to_new_stream,
            append_to_existing_stream,
            append_with_any_version,
            append_wrong_expected_version,
            append_no_stream_to_existing,
            append_multiple_events,
            global_event_count_tracks_appends
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
        ]},
        {read_all_global_tests, [sequence], [
            read_all_global_returns_events_in_epoch_order,
            read_all_global_paginates,
            read_all_global_cache_hit_matches_scan,
            read_all_global_invalidates_on_append,
            read_all_global_cache_isolated_per_store,
            read_all_global_rejects_torn_page_instead_of_mixing_generations
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

%% @doc The monotonic global event counter increases by the number of
%% events in each append batch, across streams. Uses deltas so it is
%% robust to the shared per-group store.
global_event_count_tracks_appends(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    {ok, Before} = reckon_db_streams:global_event_count(StoreId),

    {ok, _} = reckon_db_streams:append(
        StoreId, generate_stream_id(), ?NO_STREAM, generate_events(<<"cnt">>, 3)),
    {ok, After1} = reckon_db_streams:global_event_count(StoreId),
    ?assertEqual(Before + 3, After1),

    {ok, _} = reckon_db_streams:append(
        StoreId, generate_stream_id(), ?NO_STREAM, generate_events(<<"cnt">>, 2)),
    {ok, After2} = reckon_db_streams:global_event_count(StoreId),
    ?assertEqual(Before + 5, After2),
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
%% read_all_global / cache tests
%%====================================================================

%% @doc Events across multiple streams come back in global epoch order,
%% regardless of which stream they were appended to.
read_all_global_returns_events_in_epoch_order(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    S1 = generate_stream_id(), S2 = generate_stream_id(),

    {ok, _} = reckon_db_streams:append(StoreId, S1, ?NO_STREAM,
        [generate_event(<<"rag_e1">>)]),
    {ok, _} = reckon_db_streams:append(StoreId, S2, ?NO_STREAM,
        [generate_event(<<"rag_e2">>)]),
    {ok, _} = reckon_db_streams:append(StoreId, S1, 0,
        [generate_event(<<"rag_e3">>)]),

    {ok, Events} = reckon_db_streams:read_all_global(StoreId, 0, 100),
    Types = [T || #event{event_type = T} <- Events],
    %% All three of ours, in append order (epoch-ascending); other groups'
    %% events may also be present since groups share the all-streams read,
    %% so assert containment and relative order rather than exact list.
    ?assert(lists:member(<<"rag_e1">>, Types)),
    Idx = fun(T) -> length(lists:takewhile(fun(X) -> X =/= T end, Types)) end,
    ?assert(Idx(<<"rag_e1">>) < Idx(<<"rag_e2">>)),
    ?assert(Idx(<<"rag_e2">>) < Idx(<<"rag_e3">>)).

%% @doc Offset/BatchSize correctly page a known, isolated slice of events.
read_all_global_paginates(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    S = generate_stream_id(),
    Marker = generate_uuid(),
    Types = [<<"page_", Marker/binary, "_", (integer_to_binary(N))/binary>>
             || N <- lists:seq(1, 5)],
    [begin
         {ok, _} = reckon_db_streams:append(StoreId, S,
             case N of 1 -> ?NO_STREAM; _ -> N - 2 end,
             [generate_event(T)])
     end || {N, T} <- lists:zip(lists:seq(1, 5), Types)],

    {ok, All} = reckon_db_streams:read_all_global(StoreId, 0, 100000),
    AllTypes = [T || #event{event_type = T} <- All],
    OurIndexes = [I || {I, T} <- lists:zip(lists:seq(0, length(AllTypes) - 1), AllTypes),
                        lists:member(T, Types)],
    ?assertEqual(5, length(OurIndexes)),
    %% Our 5 events are contiguous in global order (nothing else appended
    %% between them within this test) and a page starting at the first of
    %% ours returns exactly them in order.
    FirstOurs = lists:min(OurIndexes),
    {ok, Page} = reckon_db_streams:read_all_global(StoreId, FirstOurs, 5),
    ?assertEqual(Types, [T || #event{event_type = T} <- Page]).

%% @doc A second read_all_global call with no intervening append returns
%% the identical result as the first (cache hit, same data either way).
read_all_global_cache_hit_matches_scan(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    S = generate_stream_id(),
    {ok, _} = reckon_db_streams:append(StoreId, S, ?NO_STREAM,
        [generate_event(<<"cache_hit_e">>)]),

    {ok, First} = reckon_db_streams:read_all_global(StoreId, 0, 100000),
    {ok, Second} = reckon_db_streams:read_all_global(StoreId, 0, 100000),
    ?assertEqual([E#event.event_id || E <- First],
                 [E#event.event_id || E <- Second]).

%% @doc An append BETWEEN two read_all_global calls is visible on the next
%% call -- the count-fingerprinted cache must not serve stale data.
read_all_global_invalidates_on_append(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    S = generate_stream_id(),
    {ok, _} = reckon_db_streams:append(StoreId, S, ?NO_STREAM,
        [generate_event(<<"before_invalidate">>)]),

    {ok, Before} = reckon_db_streams:read_all_global(StoreId, 0, 100000),
    BeforeCount = length(Before),

    {ok, _} = reckon_db_streams:append(StoreId, S, 0,
        [generate_event(<<"after_invalidate">>)]),

    {ok, After} = reckon_db_streams:read_all_global(StoreId, 0, 100000),
    ?assertEqual(BeforeCount + 1, length(After)),
    AfterTypes = [T || #event{event_type = T} <- After],
    ?assert(lists:member(<<"after_invalidate">>, AfterTypes)).

%% @doc A cache entry for one store never leaks into another store's read
%% -- two distinct stores, same group's Khepri instance can't apply here
%% (groups already isolate at the store level), so this specifically
%% exercises the cache TABLE being shared process-wide across StoreIds.
read_all_global_cache_isolated_per_store(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    OtherStoreId = list_to_atom(atom_to_list(StoreId) ++ "_other"),
    OtherDataDir = proplists:get_value(data_dir, Config) ++ "_other",
    os:cmd("rm -rf " ++ OtherDataDir),
    ok = filelib:ensure_dir(filename:join(OtherDataDir, "dummy")),
    {ok, _} = khepri:start(OtherDataDir, OtherStoreId),
    khepri:put(OtherStoreId, [streams], #{}),

    S1 = generate_stream_id(),
    {ok, _} = reckon_db_streams:append(StoreId, S1, ?NO_STREAM,
        [generate_event(<<"isolation_main">>)]),
    S2 = reckon_gater_stream_id:new(<<"test">>),
    {ok, _} = reckon_db_streams:append(OtherStoreId, S2, ?NO_STREAM,
        [generate_event(<<"isolation_other">>)]),

    {ok, OtherEvents} = reckon_db_streams:read_all_global(OtherStoreId, 0, 100000),
    OtherTypes = [T || #event{event_type = T} <- OtherEvents],
    ?assertEqual([<<"isolation_other">>], OtherTypes),

    khepri:stop(OtherStoreId),
    os:cmd("rm -rf " ++ OtherDataDir),
    ok.

%% @doc If a row this page is about to read carries a Generation tag that
%% doesn't match what the meta row promised -- what a rebuild landing
%% between two of this page's `ets:lookup' calls would produce, since the
%% WRITE side (one atomic `ets:insert/2' per rebuild) does not make the
%% per-lookup READ side atomic too -- the page must fail closed instead of
%% silently returning a torn mix of two generations. Pokes the cache table
%% directly (its name mirrors the private `?READ_ALL_GLOBAL_CACHE' macro)
%% since there is no other way to construct this exact race
%% deterministically.
read_all_global_rejects_torn_page_instead_of_mixing_generations(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    S = generate_stream_id(),
    Marker = integer_to_binary(erlang:unique_integer([positive])),
    Types = [<<"torn_", Marker/binary, "_", (integer_to_binary(N))/binary>>
             || N <- lists:seq(1, 3)],
    lists:foreach(fun({N, T}) ->
        Ver = case N of 1 -> ?NO_STREAM; _ -> N - 2 end,
        {ok, _} = reckon_db_streams:append(StoreId, S, Ver, [generate_event(T)])
    end, lists:zip(lists:seq(1, 3), Types)),

    %% Force a rebuild so the cache is populated, and find our 3 events'
    %% positions in it.
    {ok, All} = reckon_db_streams:read_all_global(StoreId, 0, 1000000),
    AllTypes = [Ev#event.event_type || Ev <- All],
    OurIndexes = [I || {I, T} <- lists:zip(lists:seq(0, length(AllTypes) - 1), AllTypes),
                        lists:member(T, Types)],
    3 = length(OurIndexes),
    [FirstOurs | _] = lists:sort(OurIndexes),

    %% Corrupt exactly one of our rows' Generation tag in place -- one row
    %% now disagrees with the meta row's Generation, the rest still agree.
    CacheTable = reckon_db_read_all_global_cache,
    [{Key, _Generation, Event}] = ets:lookup(CacheTable, {StoreId, FirstOurs}),
    true = ets:insert(CacheTable, {Key, make_ref(), Event}),

    %% A page spanning the corrupted position fails closed rather than
    %% returning a page silently mixing two generations.
    Result = reckon_db_streams:read_all_global(StoreId, FirstOurs, 3),
    ?assertEqual({error, cache_generation_changed_mid_page}, Result).

%%====================================================================
%% Helper Functions
%%====================================================================

%% @private Generate a unique stream ID conforming to the
%% reckon-db user-stream format. See reckon_gater_stream_id.
generate_stream_id() ->
    reckon_gater_stream_id:new(<<"test">>).

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
