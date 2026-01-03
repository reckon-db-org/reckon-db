%% @doc Test helpers for reckon-db integration tests
%% @author rgfaber

-module(reckon_db_test_helpers).

-include("reckon_db.hrl").

-export([
    start_test_app/0,
    stop_test_app/0,
    ensure_store/1,
    stop_store/1,
    cleanup_data_dir/1,
    generate_stream_id/0,
    generate_event/1,
    generate_events/2,
    wait_for/2,
    wait_for/3
]).

%%====================================================================
%% Application Lifecycle
%%====================================================================

%% @doc Start the test application and all dependencies
-spec start_test_app() -> ok | {error, term()}.
start_test_app() ->
    %% Start required applications
    application:ensure_all_started(crypto),
    application:ensure_all_started(telemetry),
    application:ensure_all_started(ra),
    application:ensure_all_started(khepri),

    %% Start pg scope for process groups
    case pg:start(?RECKON_DB_PG_SCOPE) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,

    %% Start reckon_db
    case application:ensure_all_started(reckon_db) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%% @doc Stop the test application
-spec stop_test_app() -> ok.
stop_test_app() ->
    application:stop(reckon_db),
    ok.

%%====================================================================
%% Store Management
%%====================================================================

%% @doc Ensure a test store is running
-spec ensure_store(atom()) -> ok | {error, term()}.
ensure_store(StoreId) ->
    DataDir = data_dir_for(StoreId),
    cleanup_data_dir(DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    %% Start the Khepri store with proper options map
    KhepriOpts = #{
        data_dir => DataDir,
        store_id => StoreId
    },
    case khepri:start(StoreId, KhepriOpts) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%% @doc Stop a test store
-spec stop_store(atom()) -> ok.
stop_store(StoreId) ->
    khepri:stop(StoreId),
    DataDir = data_dir_for(StoreId),
    cleanup_data_dir(DataDir),
    ok.

%% @doc Clean up the data directory
-spec cleanup_data_dir(string()) -> ok.
cleanup_data_dir(DataDir) ->
    os:cmd("rm -rf " ++ DataDir),
    ok.

%% @private
-spec data_dir_for(atom()) -> string().
data_dir_for(StoreId) ->
    "/tmp/reckon_db_test_" ++ atom_to_list(StoreId).

%%====================================================================
%% Test Data Generation
%%====================================================================

%% @doc Generate a unique stream ID
-spec generate_stream_id() -> binary().
generate_stream_id() ->
    Uuid = generate_uuid(),
    <<"test$", Uuid/binary>>.

%% @doc Generate a single test event
-spec generate_event(binary()) -> map().
generate_event(EventType) ->
    #{
        event_type => EventType,
        data => #{
            <<"key">> => <<"value">>,
            <<"timestamp">> => erlang:system_time(millisecond)
        },
        metadata => #{
            <<"correlation_id">> => generate_uuid(),
            <<"source">> => <<"test">>
        }
    }.

%% @doc Generate multiple test events
-spec generate_events(binary(), pos_integer()) -> [map()].
generate_events(EventType, Count) ->
    [generate_event(EventType) || _ <- lists:seq(1, Count)].

%% @private
-spec generate_uuid() -> binary().
generate_uuid() ->
    Bytes = crypto:strong_rand_bytes(16),
    <<A:32, B:16, C:16, D:16, E:48>> = Bytes,
    iolist_to_binary(
        io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
                      [A, B, C, D, E])
    ).

%%====================================================================
%% Wait Utilities
%%====================================================================

%% @doc Wait for a condition to become true
-spec wait_for(fun(() -> boolean()), pos_integer()) -> ok | timeout.
wait_for(Fun, TimeoutMs) ->
    wait_for(Fun, TimeoutMs, 100).

%% @doc Wait for a condition with custom interval
-spec wait_for(fun(() -> boolean()), pos_integer(), pos_integer()) -> ok | timeout.
wait_for(Fun, TimeoutMs, IntervalMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    wait_for_loop(Fun, Deadline, IntervalMs).

%% @private
wait_for_loop(Fun, Deadline, IntervalMs) ->
    case Fun() of
        true ->
            ok;
        false ->
            Now = erlang:monotonic_time(millisecond),
            case Now >= Deadline of
                true ->
                    timeout;
                false ->
                    timer:sleep(IntervalMs),
                    wait_for_loop(Fun, Deadline, IntervalMs)
            end
    end.
