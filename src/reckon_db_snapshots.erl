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

%% The life token an unanchored snapshot's scheme-2 MAC binds. "$" keeps it
%% apart from every life a stream can be minted with (reckon-db#25).
-define(UNANCHORED_LIFE, <<"$unanchored">>).

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

%% A snapshot version is guarded by ?IS_SNAPSHOT_VERSION (reckon_db.hrl):
%% the bound is the store key's padded width, so a version that cannot be
%% keyed is refused here rather than killing the worker in pad_version/1.

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
%%
%% A version that is not a non-negative integer is answered
%% {error, {invalid_version, Version}}, as by load_at/3 and delete_at/3:
%% it reaches them from gateway clients as any term (reckon-db#14).
-spec save(atom(), binary(), non_neg_integer() | term(), snapshot_data(), snapshot_metadata()) ->
    ok | {error, term()}.
save(_StoreId, _StreamId, Version, _Data, _Metadata) when not ?IS_SNAPSHOT_VERSION(Version) ->
    {error, {invalid_version, Version}};
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
    {ok, snapshot()} | {error, not_found | store_integrity_violation() | {snapshot_anchor_unavailable, map()}}.
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
-spec load_at(atom(), binary(), non_neg_integer() | term()) ->
    {ok, snapshot()} | {error, not_found | {invalid_version, term()}
                              | store_integrity_violation() | {snapshot_anchor_unavailable, map()}}.
load_at(_StoreId, _StreamId, Version) when not ?IS_SNAPSHOT_VERSION(Version) ->
    {error, {invalid_version, Version}};
load_at(StoreId, StreamId, Version) ->
    case reckon_db_snapshots_store:get(StoreId, StreamId, Version) of
        undefined ->
            {error, not_found};
        Snapshot ->
            maybe_verify_snapshot_on_load(StoreId, StreamId, Snapshot)
    end.

%% @doc List all snapshots for a stream, in version order.
%%
%% Same verification semantics as load/2, for every snapshot listed: if one
%% fails, the list is refused with its {integrity_violation, _} (or
%% {snapshot_anchor_unavailable, _}), never served with it in or silently
%% left out. A caller that picks the latest from this list (reckon-evoq's
%% checkpoint read) gets the same protection load/2 gives.
-spec list(atom(), binary()) ->
    {ok, [snapshot()]} | {error, store_integrity_violation() | {snapshot_anchor_unavailable, map()} | term()}.
list(StoreId, StreamId) ->
    verify_listed(reckon_db_snapshots_store:list(StoreId, StreamId), StoreId, StreamId).

verify_listed({ok, Snapshots}, StoreId, StreamId) ->
    Kind = snapshot_kind(StoreId, StreamId),
    lists:foldl(fun(Snapshot, Acc) -> keep_verified(Acc, Kind, StoreId, StreamId, Snapshot) end,
                {ok, []}, lists:reverse(Snapshots));
verify_listed({error, _} = Error, _StoreId, _StreamId) ->
    Error.

keep_verified({ok, Kept}, Kind, StoreId, StreamId, Snapshot) ->
    prepend_verified(verify_by_kind(Kind, StoreId, StreamId, Snapshot), Kept);
keep_verified({error, _} = Error, _Kind, _StoreId, _StreamId, _Snapshot) ->
    Error.

prepend_verified({ok, Snapshot}, Kept) -> {ok, [Snapshot | Kept]};
prepend_verified({error, _} = Error, _Kept) -> Error.

%% @doc Delete all snapshots for a stream
-spec delete(atom(), binary()) -> ok | {error, term()}.
delete(StoreId, StreamId) ->
    reckon_db_snapshots_store:delete(StoreId, StreamId).

%% @doc Delete a specific snapshot version
-spec delete_at(atom(), binary(), non_neg_integer() | term()) -> ok | {error, term()}.
delete_at(_StoreId, _StreamId, Version) when not ?IS_SNAPSHOT_VERSION(Version) ->
    {error, {invalid_version, Version}};
delete_at(StoreId, StreamId, Version) ->
    reckon_db_snapshots_store:delete(StoreId, StreamId, Version).

%% @doc Check if any snapshot exists for a stream
-spec exists(atom(), binary()) -> boolean().
exists(StoreId, StreamId) ->
    reckon_db_snapshots_store:exists(StoreId, StreamId).

%% @doc Check if a specific snapshot version exists
%%
%% A version that cannot be keyed is refused with
%% {error, {invalid_version, Version}} rather than answered false: `latest'
%% and every other non-version used to reach the padding and raise badarg in
%% the caller's process (reckon-db#14). It is the one predicate here that
%% takes a version, so it is the only one whose answer is not a bare boolean.
-spec exists_at(atom(), binary(), non_neg_integer() | term()) ->
    boolean() | {error, {invalid_version, term()}}.
exists_at(_StoreId, _StreamId, Version) when not ?IS_SNAPSHOT_VERSION(Version) ->
    {error, {invalid_version, Version}};
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
%% For integrity-enabled stores the stream decides how the snapshot is
%% sealed (see snapshot_kind/2):
%%
%% anchored: computes the anchor_hash (the chain hash of the event at the
%% snapshot's version) by reading that event from storage, then computes
%% the snapshot MAC over the snapshot record with both fields populated.
%% If the event at the snapshot version is not present or not
%% integrity-bearing, the save is refused: a snapshot whose anchor cannot
%% be established would be unverifiable at load time and is strictly worse
%% than no snapshot at all.
%%
%% unanchored: a system stream that holds no events (a projection
%% checkpoint) has no chain to anchor into. It is sealed with a scheme-2
%% MAC bound to this store and the life token <<"$unanchored">>, and no
%% anchor (reckon-db#25).
-spec apply_snapshot_integrity(
    atom(), binary(), non_neg_integer(), snapshot()
) -> {ok, snapshot()} | {error, {snapshot_anchor_unavailable, map()}}.
apply_snapshot_integrity(StoreId, StreamId, Version, Snapshot) ->
    seal(snapshot_kind(StoreId, StreamId), StoreId, StreamId, Version, Snapshot).

seal(plain, _StoreId, _StreamId, _Version, Snapshot) ->
    {ok, Snapshot};
seal(anchored, StoreId, StreamId, Version, Snapshot) ->
    seal_with_anchor(compute_event_chain_hash(StoreId, StreamId, Version),
                     StoreId, Snapshot);
seal(unanchored, StoreId, _StreamId, _Version, Snapshot) ->
    Key = reckon_db_integrity_key:get(StoreId),
    Snapshot1 = Snapshot#snapshot{anchor_hash = undefined},
    Mac = reckon_gater_integrity:compute_snapshot_mac(
            Snapshot1, Key, #{store_id => StoreId, life => ?UNANCHORED_LIFE}),
    {ok, Snapshot1#snapshot{mac = Mac}}.

seal_with_anchor({ok, AnchorHash}, StoreId, Snapshot) ->
    Key = reckon_db_integrity_key:get(StoreId),
    Snapshot1 = Snapshot#snapshot{anchor_hash = AnchorHash},
    Mac = reckon_gater_integrity:compute_snapshot_mac(Snapshot1, Key),
    {ok, Snapshot1#snapshot{mac = Mac}};
seal_with_anchor({error, _} = Err, _StoreId, _Snapshot) ->
    Err.

%% @private How a snapshot on this stream is sealed and verified.
%%
%% plain: the store has integrity off.
%% unanchored: a system ("$") stream with no events stored.
%% anchored: every other stream, as before.
%%
%% Decided by the stream at the time of the call, never by what the stored
%% snapshot claims: a seal that does not fit the stream is refused on load.
-spec snapshot_kind(atom(), binary()) -> plain | anchored | unanchored.
snapshot_kind(StoreId, StreamId) ->
    kind_on(reckon_db_integrity_key:is_enabled(StoreId), StoreId, StreamId).

kind_on(false, _StoreId, _StreamId) ->
    plain;
kind_on(true, StoreId, StreamId) ->
    anchoring(reckon_gater_stream_id:is_system(StreamId)
                  andalso reckon_db_streams:get_version(StoreId, StreamId) =:= ?NO_STREAM).

anchoring(true)  -> unanchored;
anchoring(false) -> anchored.

%%====================================================================
%% Tamper-resistance — load side
%%====================================================================

%% @private Verify a loaded snapshot the way its stream says it is sealed
%% (see snapshot_kind/2).
%%
%% plain: returned unchanged.
%% unanchored: the scheme-2 MAC must verify against this store and the
%% <<"$unanchored">> life; a scheme-1 MAC is refused (v2_from 0), and a
%% bare value with no MAC fails closed: on a system stream it is not
%% believed.
%% anchored: a snapshot with integrity fields is verified against the
%% stream's chain; one with neither field is legacy data and returned
%% unchanged; an unanchored seal (MAC, no anchor) is refused, since the
%% stream now holds events and an anchor is what it must carry.
maybe_verify_snapshot_on_load(StoreId, StreamId, Snapshot) ->
    verify_by_kind(snapshot_kind(StoreId, StreamId), StoreId, StreamId, Snapshot).

verify_by_kind(plain, _StoreId, _StreamId, Snapshot) ->
    {ok, Snapshot};
verify_by_kind(unanchored, StoreId, StreamId, #snapshot{mac = undefined} = Snapshot) ->
    refuse(unsealed_system_snapshot, StoreId, StreamId, Snapshot);
verify_by_kind(unanchored, StoreId, StreamId, #snapshot{mac = Mac} = Snapshot) ->
    Key = reckon_db_integrity_key:get(StoreId),
    Ctx = #{store_id => StoreId, life => ?UNANCHORED_LIFE, v2_from => 0},
    check_snapshot_mac(reckon_gater_integrity:verify_snapshot_mac_shape(Mac, Snapshot, Key, Ctx),
                       StoreId, StreamId, Snapshot);
verify_by_kind(anchored, StoreId, StreamId,
               #snapshot{anchor_hash = undefined, mac = Mac} = Snapshot) when Mac =/= undefined ->
    refuse(unanchored_seal_on_stream_with_events, StoreId, StreamId, Snapshot);
verify_by_kind(anchored, StoreId, StreamId, Snapshot) ->
    verify_if_needed(not reckon_gater_integrity:is_legacy_snapshot(Snapshot),
                     StoreId, StreamId, Snapshot).

verify_if_needed(false, _StoreId, _StreamId, Snapshot) ->
    {ok, Snapshot};
verify_if_needed(true, StoreId, StreamId, #snapshot{version = Version} = Snapshot) ->
    verify_against_anchor(compute_event_chain_hash(StoreId, StreamId, Version),
                          StoreId, StreamId, Snapshot).

verify_against_anchor({ok, ActualAnchor}, StoreId, StreamId, Snapshot) ->
    Key = reckon_db_integrity_key:get(StoreId),
    check_snapshot_mac(reckon_gater_integrity:verify_snapshot(Snapshot, ActualAnchor, Key),
                       StoreId, StreamId, Snapshot);
verify_against_anchor({error, _} = Err, _StoreId, _StreamId, _Snapshot) ->
    Err.

refuse(Reason, StoreId, StreamId, Snapshot) ->
    check_snapshot_mac({integrity_violation, Reason}, StoreId, StreamId, Snapshot).

check_snapshot_mac(ok, _StoreId, _StreamId, Snapshot) ->
    {ok, Snapshot};
check_snapshot_mac({integrity_violation, _} = Violation, StoreId, StreamId,
                   #snapshot{version = Version}) ->
    emit_load_violation_telemetry(StoreId, StreamId, Version, Violation),
    {error, Violation}.

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
