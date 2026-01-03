%% @doc Snapshots API facade for reckon-db
%%
%% Provides the public API for snapshot operations:
%% - save: Save aggregate state as a snapshot
%% - load: Load the latest snapshot for a stream
%% - load_at: Load a specific snapshot version
%% - list: List all snapshots for a stream
%% - delete: Delete snapshots for a stream
%% - exists: Check if a snapshot exists
%%
%% Snapshots are used to optimize event replay by storing
%% aggregate state at specific versions.
%%
%% @author rgfaber

-module(reckon_db_snapshots).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([
    save/4,
    save/5,
    load/2,
    load_at/3,
    list/2,
    delete/2,
    delete_at/3,
    exists/2,
    exists_at/3
]).

%%====================================================================
%% Types
%%====================================================================

-type snapshot_data() :: map() | binary().
-type snapshot_metadata() :: map().

-export_type([snapshot_data/0, snapshot_metadata/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Save a snapshot with default empty metadata
%%
%% Parameters:
%%   StoreId  - The store identifier
%%   StreamId - The stream this snapshot belongs to
%%   Version  - The event version this snapshot represents
%%   Data     - The aggregate state to snapshot
%%
%% Returns ok on success or {error, Reason} on failure.
-spec save(atom(), binary(), non_neg_integer(), snapshot_data()) ->
    ok | {error, term()}.
save(StoreId, StreamId, Version, Data) ->
    save(StoreId, StreamId, Version, Data, #{}).

%% @doc Save a snapshot with metadata
-spec save(atom(), binary(), non_neg_integer(), snapshot_data(), snapshot_metadata()) ->
    ok | {error, term()}.
save(StoreId, StreamId, Version, Data, Metadata) ->
    StartTime = erlang:monotonic_time(),

    Snapshot = #snapshot{
        stream_id = StreamId,
        version = Version,
        data = Data,
        metadata = Metadata,
        timestamp = erlang:system_time(millisecond)
    },

    Result = reckon_db_snapshots_store:put(StoreId, Snapshot),

    %% Emit telemetry
    case Result of
        ok ->
            DataSize = estimate_size(Data),
            Duration = erlang:monotonic_time() - StartTime,
            telemetry:execute(
                ?SNAPSHOT_CREATED,
                #{system_time => erlang:system_time(millisecond),
                  size_bytes => DataSize, duration => Duration},
                #{store_id => StoreId, stream_id => StreamId, version => Version}
            ),
            logger:debug("Snapshot saved: store=~p, stream=~s, version=~p",
                        [StoreId, StreamId, Version]);
        {error, Reason} ->
            logger:warning("Failed to save snapshot: store=~p, stream=~s, version=~p, reason=~p",
                          [StoreId, StreamId, Version, Reason])
    end,

    Result.

%% @doc Load the latest snapshot for a stream
%%
%% Returns {ok, Snapshot} if found, {error, not_found} otherwise.
-spec load(atom(), binary()) -> {ok, snapshot()} | {error, not_found}.
load(StoreId, StreamId) ->
    StartTime = erlang:monotonic_time(),

    Result = case reckon_db_snapshots_store:get_latest(StoreId, StreamId) of
        undefined -> {error, not_found};
        Snapshot -> {ok, Snapshot}
    end,

    Duration = erlang:monotonic_time() - StartTime,

    %% Emit telemetry for successful reads
    case Result of
        {ok, #snapshot{version = Version, data = Data}} ->
            DataSize = estimate_size(Data),
            telemetry:execute(
                ?SNAPSHOT_READ,
                #{duration => Duration, size_bytes => DataSize},
                #{store_id => StoreId, stream_id => StreamId, version => Version}
            );
        _ ->
            ok
    end,

    Result.

%% @doc Load a specific snapshot version
-spec load_at(atom(), binary(), non_neg_integer()) -> {ok, snapshot()} | {error, not_found}.
load_at(StoreId, StreamId, Version) ->
    case reckon_db_snapshots_store:get(StoreId, StreamId, Version) of
        undefined -> {error, not_found};
        Snapshot -> {ok, Snapshot}
    end.

%% @doc List all snapshots for a stream
-spec list(atom(), binary()) -> {ok, [snapshot()]} | {error, term()}.
list(StoreId, StreamId) ->
    reckon_db_snapshots_store:list(StoreId, StreamId).

%% @doc Delete all snapshots for a stream
-spec delete(atom(), binary()) -> ok | {error, term()}.
delete(StoreId, StreamId) ->
    reckon_db_snapshots_store:delete(StoreId, StreamId).

%% @doc Delete a specific snapshot version
-spec delete_at(atom(), binary(), non_neg_integer()) -> ok | {error, term()}.
delete_at(StoreId, StreamId, Version) ->
    reckon_db_snapshots_store:delete(StoreId, StreamId, Version).

%% @doc Check if any snapshot exists for a stream
-spec exists(atom(), binary()) -> boolean().
exists(StoreId, StreamId) ->
    reckon_db_snapshots_store:exists(StoreId, StreamId).

%% @doc Check if a specific snapshot version exists
-spec exists_at(atom(), binary(), non_neg_integer()) -> boolean().
exists_at(StoreId, StreamId, Version) ->
    reckon_db_snapshots_store:exists(StoreId, StreamId, Version).

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Estimate the size of data in bytes
-spec estimate_size(term()) -> non_neg_integer().
estimate_size(Data) when is_binary(Data) ->
    byte_size(Data);
estimate_size(Data) when is_map(Data) ->
    %% Rough estimate based on term_to_binary size
    byte_size(term_to_binary(Data));
estimate_size(Data) ->
    byte_size(term_to_binary(Data)).
