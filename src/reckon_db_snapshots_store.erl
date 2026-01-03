%% @doc Snapshots store for reckon-db
%%
%% Manages snapshot persistence and retrieval directly via Khepri.
%% Snapshots are stored at path [snapshots, StreamId, PaddedVersion].
%%
%% @author rgfaber

-module(reckon_db_snapshots_store).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

%% API
-export([
    put/2,
    get/2,
    get/3,
    get_latest/2,
    delete/2,
    delete/3,
    list/2,
    exists/2,
    exists/3
]).

%%====================================================================
%% Types
%%====================================================================

-type store_id() :: atom().

%%====================================================================
%% API
%%====================================================================

%% @doc Store a snapshot
-spec put(store_id(), snapshot()) -> ok | {error, term()}.
put(StoreId, #snapshot{stream_id = StreamId, version = Version} = Snapshot) ->
    PaddedVersion = pad_version(Version),
    Path = ?SNAPSHOTS_PATH ++ [StreamId, PaddedVersion],
    case khepri:put(StoreId, Path, Snapshot) of
        ok -> ok;
        {error, _} = Error -> Error
    end.

%% @doc Get the latest snapshot for a stream
-spec get(store_id(), binary()) -> snapshot() | undefined.
get(StoreId, StreamId) ->
    get_latest(StoreId, StreamId).

%% @doc Get a specific snapshot version for a stream
-spec get(store_id(), binary(), non_neg_integer()) -> snapshot() | undefined.
get(StoreId, StreamId, Version) ->
    PaddedVersion = pad_version(Version),
    Path = ?SNAPSHOTS_PATH ++ [StreamId, PaddedVersion],
    case khepri:get(StoreId, Path) of
        {ok, Snapshot} when is_record(Snapshot, snapshot) ->
            Snapshot;
        {ok, SnapshotMap} when is_map(SnapshotMap) ->
            map_to_snapshot(SnapshotMap);
        _ ->
            undefined
    end.

%% @doc Get the latest snapshot for a stream
-spec get_latest(store_id(), binary()) -> snapshot() | undefined.
get_latest(StoreId, StreamId) ->
    case list(StoreId, StreamId) of
        {ok, []} ->
            undefined;
        {ok, Snapshots} ->
            %% Snapshots are already sorted by version (padded string sort)
            lists:last(Snapshots);
        {error, _} ->
            undefined
    end.

%% @doc Delete all snapshots for a stream
-spec delete(store_id(), binary()) -> ok | {error, term()}.
delete(StoreId, StreamId) ->
    Path = ?SNAPSHOTS_PATH ++ [StreamId],
    case khepri:delete(StoreId, Path) of
        ok -> ok;
        {error, _} = Error -> Error
    end.

%% @doc Delete a specific snapshot version
-spec delete(store_id(), binary(), non_neg_integer()) -> ok | {error, term()}.
delete(StoreId, StreamId, Version) ->
    PaddedVersion = pad_version(Version),
    Path = ?SNAPSHOTS_PATH ++ [StreamId, PaddedVersion],
    case khepri:delete(StoreId, Path) of
        ok -> ok;
        {error, _} = Error -> Error
    end.

%% @doc List all snapshots for a stream
-spec list(store_id(), binary()) -> {ok, [snapshot()]} | {error, term()}.
list(StoreId, StreamId) ->
    Path = ?SNAPSHOTS_PATH ++ [StreamId, ?KHEPRI_WILDCARD_STAR],
    case khepri:get_many(StoreId, Path) of
        {ok, Results} when is_map(Results) ->
            Snapshots = [convert_to_snapshot(V) || {_, V} <- maps:to_list(Results)],
            ValidSnapshots = [S || S <- Snapshots, S =/= undefined],
            %% Sort by version
            Sorted = lists:sort(
                fun(#snapshot{version = V1}, #snapshot{version = V2}) -> V1 =< V2 end,
                ValidSnapshots
            ),
            {ok, Sorted};
        {ok, _} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

%% @doc Check if any snapshot exists for a stream
-spec exists(store_id(), binary()) -> boolean().
exists(StoreId, StreamId) ->
    Path = ?SNAPSHOTS_PATH ++ [StreamId],
    case khepri:exists(StoreId, Path) of
        true -> true;
        false -> false;
        {error, _} -> false
    end.

%% @doc Check if a specific snapshot version exists
-spec exists(store_id(), binary(), non_neg_integer()) -> boolean().
exists(StoreId, StreamId, Version) ->
    PaddedVersion = pad_version(Version),
    Path = ?SNAPSHOTS_PATH ++ [StreamId, PaddedVersion],
    case khepri:exists(StoreId, Path) of
        true -> true;
        false -> false;
        {error, _} -> false
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Pad version number to 10 digits for proper string sorting
-spec pad_version(non_neg_integer()) -> binary().
pad_version(Version) ->
    VersionStr = integer_to_list(Version),
    Padding = 10 - length(VersionStr),
    PaddedStr = lists:duplicate(Padding, $0) ++ VersionStr,
    list_to_binary(PaddedStr).

%% @private Convert value to snapshot record
-spec convert_to_snapshot(term()) -> snapshot() | undefined.
convert_to_snapshot(V) when is_record(V, snapshot) ->
    V;
convert_to_snapshot(V) when is_map(V) ->
    map_to_snapshot(V);
convert_to_snapshot(_) ->
    undefined.

%% @private Convert map to snapshot record
-spec map_to_snapshot(map()) -> snapshot().
map_to_snapshot(Map) ->
    #snapshot{
        stream_id = maps:get(stream_id, Map, undefined),
        version = maps:get(version, Map, 0),
        data = maps:get(data, Map, #{}),
        metadata = maps:get(metadata, Map, #{}),
        timestamp = maps:get(timestamp, Map, 0)
    }.
