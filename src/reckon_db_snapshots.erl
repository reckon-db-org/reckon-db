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

    Snapshot0 = #snapshot{
        stream_id = StreamId,
        version = Version,
        data = Data,
        metadata = Metadata,
        timestamp = erlang:system_time(millisecond)
    },

    %% Apply integrity if enabled on this store. For an
    %% integrity-disabled store this is a pass-through; for an
    %% integrity-enabled store this populates anchor_hash + mac
    %% so a subsequent load can verify the snapshot AND that the
    %% underlying stream's chain at this version is unchanged.
    case apply_snapshot_integrity(StoreId, StreamId, Version, Snapshot0) of
        {ok, Snapshot} ->
            Result = reckon_db_snapshots_store:put(StoreId, Snapshot),
            emit_save_telemetry(Result, StartTime, StoreId, StreamId, Version, Data),
            Result;
        {error, _} = Err ->
            logger:warning("Refusing to save snapshot: store=~p, stream=~s, "
                           "version=~p, reason=~p",
                           [StoreId, StreamId, Version, Err]),
            Err
    end.

%% @doc Load the latest snapshot for a stream.
%%
%% Returns {ok, Snapshot} if found and integrity-valid.
%% Returns {error, not_found} if no snapshot exists.
%% Returns {error, {integrity_violation, _}} if the store has
%% integrity enabled and the loaded snapshot fails verification.
%% Callers (typically aggregate rebuild) should treat an integrity
%% violation as a directive to fall back to full event replay from
%% the stream's chain_start_version watermark.
-spec load(atom(), binary()) ->
    {ok, snapshot()} | {error, not_found} | {error, term()}.
load(StoreId, StreamId) ->
    StartTime = erlang:monotonic_time(),

    Result =
        case reckon_db_snapshots_store:get_latest(StoreId, StreamId) of
            undefined ->
                {error, not_found};
            Snapshot ->
                maybe_verify_snapshot_on_load(StoreId, StreamId, Snapshot)
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

%% @doc Load a specific snapshot version.
%%
%% Same verification semantics as load/2.
-spec load_at(atom(), binary(), non_neg_integer()) ->
    {ok, snapshot()} | {error, not_found} | {error, term()}.
load_at(StoreId, StreamId, Version) ->
    case reckon_db_snapshots_store:get(StoreId, StreamId, Version) of
        undefined ->
            {error, not_found};
        Snapshot ->
            maybe_verify_snapshot_on_load(StoreId, StreamId, Snapshot)
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

%% @private Emit save-side telemetry, mirroring the original
%% telemetry/logging behaviour now that the save path branches on
%% integrity outcome.
emit_save_telemetry(ok, StartTime, StoreId, StreamId, Version, Data) ->
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
emit_save_telemetry({error, Reason}, _StartTime, StoreId, StreamId, Version, _Data) ->
    logger:warning(
        "Failed to save snapshot: store=~p, stream=~s, version=~p, reason=~p",
        [StoreId, StreamId, Version, Reason]).

%%====================================================================
%% Tamper-resistance — save side
%%====================================================================

%% @private Apply snapshot integrity if enabled on the store.
%%
%% For integrity-disabled stores: returns the snapshot unchanged.
%%
%% For integrity-enabled stores: computes the anchor_hash (the chain
%% hash of the event at the snapshot's version) by reading that event
%% from storage, then computes the snapshot MAC over the snapshot
%% record with both fields populated. The resulting snapshot is what
%% gets persisted.
%%
%% If integrity is enabled but the event at the snapshot version is
%% not present or not integrity-bearing, the save is refused: a
%% snapshot whose anchor cannot be established would be unverifiable
%% at load time and is strictly worse than no snapshot at all.
-spec apply_snapshot_integrity(
    atom(), binary(), non_neg_integer(), snapshot()
) -> {ok, snapshot()} | {error, term()}.
apply_snapshot_integrity(StoreId, StreamId, Version, Snapshot) ->
    seal_if_enabled(reckon_db_integrity_key:is_enabled(StoreId),
                    StoreId, StreamId, Version, Snapshot).

seal_if_enabled(false, _StoreId, _StreamId, _Version, Snapshot) ->
    {ok, Snapshot};
seal_if_enabled(true, StoreId, StreamId, Version, Snapshot) ->
    seal_with_anchor(compute_event_chain_hash(StoreId, StreamId, Version),
                     StoreId, Snapshot).

seal_with_anchor({ok, AnchorHash}, StoreId, Snapshot) ->
    Key = reckon_db_integrity_key:get(StoreId),
    Snapshot1 = Snapshot#snapshot{anchor_hash = AnchorHash},
    Mac = reckon_gater_integrity:compute_snapshot_mac(Snapshot1, Key),
    {ok, Snapshot1#snapshot{mac = Mac}};
seal_with_anchor({error, _} = Err, _StoreId, _Snapshot) ->
    Err.

%%====================================================================
%% Tamper-resistance — load side
%%====================================================================

%% @private Run snapshot verification if integrity is enabled on the
%% store AND the snapshot itself carries integrity fields. Returns
%% {ok, Snapshot} unchanged in all other cases (the snapshot is
%% treated as legacy data).
maybe_verify_snapshot_on_load(StoreId, StreamId,
                              #snapshot{version = Version} = Snapshot) ->
    verify_if_needed(should_verify_snapshot(StoreId, Snapshot),
                     StoreId, StreamId, Version, Snapshot).

verify_if_needed(false, _StoreId, _StreamId, _Version, Snapshot) ->
    {ok, Snapshot};
verify_if_needed(true, StoreId, StreamId, Version, Snapshot) ->
    verify_against_anchor(compute_event_chain_hash(StoreId, StreamId, Version),
                          StoreId, StreamId, Version, Snapshot).

verify_against_anchor({ok, ActualAnchor}, StoreId, StreamId, Version, Snapshot) ->
    Key = reckon_db_integrity_key:get(StoreId),
    check_snapshot_mac(reckon_gater_integrity:verify_snapshot(Snapshot, ActualAnchor, Key),
                       StoreId, StreamId, Version, Snapshot);
verify_against_anchor({error, _} = Err, _StoreId, _StreamId, _Version, _Snapshot) ->
    Err.

check_snapshot_mac(ok, _StoreId, _StreamId, _Version, Snapshot) ->
    {ok, Snapshot};
check_snapshot_mac({integrity_violation, _} = Violation, StoreId, StreamId, Version, _Snapshot) ->
    emit_load_violation_telemetry(StoreId, StreamId, Version, Violation),
    {error, Violation}.

should_verify_snapshot(StoreId, Snapshot) ->
    reckon_db_integrity_key:is_enabled(StoreId)
        andalso (not reckon_gater_integrity:is_legacy_snapshot(Snapshot)).

emit_load_violation_telemetry(StoreId, StreamId, Version, _Violation) ->
    telemetry:execute(
        [reckon, db, snapshot, integrity, violation],
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, stream_id => StreamId, version => Version}
    ).

%%====================================================================
%% Tamper-resistance — shared helper
%%====================================================================

%% @private Read the event at (StreamId, Version) and compute its
%% chain hash. Returns {ok, Hash32} on success.
%%
%% Used by both save (to capture the anchor at snapshot time) and
%% load (to verify that the anchor still matches). Bypasses the
%% verifying read in reckon_db_streams because we want the raw
%% record - if the stream itself has been tampered, the anchor
%% comparison will catch it.
compute_event_chain_hash(StoreId, StreamId, Version) ->
    PaddedVersion = pad_version_for_event(Version),
    Path = reckon_db_stream_path:event_path(StreamId, PaddedVersion),
    case khepri:get(StoreId, Path) of
        {ok, #event{prev_event_hash = PrevHash} = Event}
                when is_binary(PrevHash) ->
            {ok, reckon_gater_integrity:compute_chain_hash(Event, PrevHash)};
        {ok, #event{prev_event_hash = undefined}} ->
            {error, {snapshot_anchor_unavailable,
                     #{stream_id => StreamId,
                       version => Version,
                       reason => event_is_legacy}}};
        _Other ->
            {error, {snapshot_anchor_unavailable,
                     #{stream_id => StreamId,
                       version => Version,
                       reason => event_not_found}}}
    end.

pad_version_for_event(Version) ->
    VersionStr = integer_to_list(Version),
    Padding = ?VERSION_PADDING - length(VersionStr),
    list_to_binary(lists:duplicate(Padding, $0) ++ VersionStr).
