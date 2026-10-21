%% @doc Per-stream chain-start watermark for the tamper-resistance
%% migration story.
%%
%% Each stream that has ever held integrity-bearing events records the
%% version at which integrity began. Events with version below the
%% watermark are pre-integrity legacy; events at or above must carry
%% prev_event_hash + mac.
%%
%% == Storage ==
%%
%% Watermark lives in Khepri under
%% [metadata, integrity, chain_start, StreamId] (the macro
%% ?INTEGRITY_CHAIN_START_PATH from reckon_db.hrl, plus the stream ID).
%% Value is a single non_neg_integer().
%%
%% == Lazy enablement ==
%%
%% The watermark is created on the first integrity-bearing append to a
%% stream. There is no explicit "enable integrity on this stream"
%% operation - the act of appending under a store with integrity
%% enabled implicitly sets the watermark if it does not already exist.
%%
%% For a fresh stream (never had any events) this means the watermark
%% is set to 0 when the first event is written.
%%
%% For an existing legacy stream (events 0..N-1 already stored) being
%% appended to for the first time AFTER integrity was enabled on the
%% store, the watermark is set to N - the version that the new event
%% is about to take. The first N legacy events stay legacy.
%%
%% == Read-side use ==
%%
%% The read path consults the watermark to decide whether to verify
%% integrity on an event. This module's responsibility ends at
%% read/write of the watermark itself; verification logic lives in
%% reckon_gater_integrity.
%%
%% @end
-module(reckon_db_chain_watermark).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([
    lookup/2,
    set_if_absent/3,
    delete/2,
    seal/2,
    seal_all/1
]).

-export_type([watermark/0]).

%% What lookup/2 answers about a stream's chain start.
%%
%% `unsealed' is the shape a store written before the watermark was
%% authenticated has: a bare integer, which anybody who can write the tree can
%% set. It is not trusted to make an event legacy.
-type watermark() :: {sealed, non_neg_integer()} |
                     {unsealed, non_neg_integer()} |
                     undefined.

%%====================================================================
%% Public API
%%====================================================================

%% @doc Read the chain-start version for a stream.
%%
%% Returns the version at which integrity began for the stream, or
%% 'undefined' if no watermark has been recorded (i.e., the stream
%% has never had an integrity-bearing append).
-spec lookup(StoreId :: atom(), StreamId :: binary()) ->
    {ok, watermark()}.
lookup(StoreId, StreamId) ->
    Path = ?INTEGRITY_CHAIN_START_PATH ++ [StreamId],
    case khepri:get(StoreId, Path) of
        {ok, #{version := Version, mac := Mac}} when is_integer(Version) ->
            {ok, sealed_shape(StoreId, StreamId, Version, Mac)};
        {ok, Version} when is_integer(Version), Version >= 0 ->
            %% The pre-authentication shape. Reported as what it is.
            {ok, {unsealed, Version}};
        _ ->
            {ok, undefined}
    end.

%% @private
-spec sealed_shape(atom(), binary(), non_neg_integer(), term()) ->
    {sealed, non_neg_integer()} | {unsealed, non_neg_integer()}.
sealed_shape(StoreId, StreamId, Version, Mac) ->
    case seals(StoreId, StreamId, Version, Mac) of
        true -> {sealed, Version};
        false -> {unsealed, Version}
    end.

%% @doc Set the chain-start watermark for a stream if not already set.
%%
%% Returns the version that ended up recorded - either the value
%% passed in (this call set it), or the value that was already
%% present (a concurrent call set it first).
%%
%% This function intentionally tolerates write-write races: multiple
%% writers attempting to set the same watermark all converge on the
%% same outcome, because the writer's "next version about to write"
%% is itself serialized by reckon-db's per-stream append ordering.
%% If two concurrent writers somehow both reach this code path with
%% the same NextVersion, the watermark ends up equal to NextVersion
%% regardless of who won.
-spec set_if_absent(
    StoreId :: atom(),
    StreamId :: binary(),
    Version :: non_neg_integer()
) -> {ok, RecordedVersion :: non_neg_integer()}.
set_if_absent(StoreId, StreamId, Version)
        when is_integer(Version), Version >= 0 ->
    case lookup(StoreId, StreamId) of
        {ok, {sealed, Existing}} ->
            {ok, Existing};
        {ok, {unsealed, Existing}} ->
            %% Written before watermarks were authenticated: seal it at the
            %% value it already has rather than minting a second one.
            ok = khepri:put(StoreId, ?INTEGRITY_CHAIN_START_PATH ++ [StreamId],
                            sealed_record(StoreId, StreamId, Existing)),
            {ok, Existing};
        {ok, undefined} ->
            Path = ?INTEGRITY_CHAIN_START_PATH ++ [StreamId],
            ok = khepri:put(StoreId, Path, sealed_record(StoreId, StreamId, Version)),
            %% Re-read to handle the rare race where two writers both
            %% saw 'undefined' and both wrote. Whoever's write
            %% Khepri/Ra serialised last wins; we return that value.
            {ok, Recorded} = lookup(StoreId, StreamId),
            recorded_or_version(Recorded, Version)
    end.

%% @private Prefer the re-read value; fall back to our own write (defensive;
%% lookup should not be undefined immediately after a put).
recorded_or_version(undefined, Version) -> {ok, Version};
recorded_or_version({sealed, Recorded}, _Version) -> {ok, Recorded};
recorded_or_version({unsealed, Recorded}, _Version) -> {ok, Recorded}.

%% @doc Delete a stream's watermark.
%%
%% Used only when deleting the entire stream - the watermark is part
%% of the stream's metadata and should not outlive it. Best-effort:
%% any error is swallowed since this should never block stream
%% deletion itself.
-spec delete(StoreId :: atom(), StreamId :: binary()) -> ok.
delete(StoreId, StreamId) ->
    Path = ?INTEGRITY_CHAIN_START_PATH ++ [StreamId],
    _ = khepri:delete(StoreId, Path),
    ok.

%%====================================================================
%% Sealing
%%====================================================================

%% @doc Seal a watermark written before watermarks were authenticated.
%%
%% An upgrade step, run once per store before this version serves reads of an
%% existing store: the plain integer carries no proof that the store wrote it,
%% and a read must not treat it as the point where the chain begins. Sealing
%% rewrites it in the authenticated shape with the value it already had.
-spec seal(atom(), binary()) -> ok | {error, term()}.
seal(StoreId, StreamId) ->
    case lookup(StoreId, StreamId) of
        {ok, {unsealed, Version}} ->
            khepri:put(StoreId, ?INTEGRITY_CHAIN_START_PATH ++ [StreamId],
                       sealed_record(StoreId, StreamId, Version));
        _ ->
            ok
    end.

%% @doc Seal every watermark in a store. See seal/2.
%%
%% Returns how many were sealed. A store with an integrity key and no
%% watermarks answers zero.
-spec seal_all(atom()) -> {ok, non_neg_integer()} | {error, term()}.
seal_all(StoreId) ->
    Path = ?INTEGRITY_CHAIN_START_PATH ++ [?KHEPRI_WILDCARD_STAR],
    case khepri:get_many(StoreId, Path) of
        {ok, Watermarks} when is_map(Watermarks) ->
            {ok, maps:fold(seal_fold(StoreId), 0, Watermarks)};
        {error, _} = Error ->
            Error
    end.

%% @private
seal_fold(StoreId) ->
    fun(Path, Value, Count) -> seal_entry(StoreId, Path, Value, Count) end.

%% @private
seal_entry(StoreId, Path, Value, Count) ->
    StreamId = lists:last(Path),
    case unsealed_version(StoreId, StreamId, Value) of
        undefined ->
            Count;
        Version ->
            ok = khepri:put(StoreId, Path, sealed_record(StoreId, StreamId, Version)),
            Count + 1
    end.

%% @private The version of a watermark that needs sealing, or undefined when
%% it is already sealed (or is not a watermark at all).
-spec unsealed_version(atom(), binary(), term()) -> non_neg_integer() | undefined.
unsealed_version(StoreId, StreamId, #{version := Version, mac := Mac})
        when is_integer(Version) ->
    case seals(StoreId, StreamId, Version, Mac) of
        true -> undefined;
        false -> Version
    end;
unsealed_version(_StoreId, _StreamId, Version)
        when is_integer(Version), Version >= 0 ->
    Version;
unsealed_version(_StoreId, _StreamId, _Other) ->
    undefined.

%% @private The authenticated record: the version, and an HMAC over it under
%% the store's integrity key. Without the MAC the node is just another tree
%% node, and moving it is how a reader is talked into skipping the chain.
-spec sealed_record(atom(), binary(), non_neg_integer()) ->
    #{version := non_neg_integer(), mac := binary() | undefined}.
sealed_record(StoreId, StreamId, Version) ->
    #{version => Version, mac => mac(StoreId, StreamId, Version)}.

%% @private
-spec seals(atom(), binary(), non_neg_integer(), term()) -> boolean().
seals(StoreId, StreamId, Version, Mac) when is_binary(Mac) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            crypto:hash_equals(Mac, mac_with(Key, StreamId, Version));
        _ ->
            false
    end;
seals(_StoreId, _StreamId, _Version, _Mac) ->
    false.

%% @private
-spec mac(atom(), binary(), non_neg_integer()) -> binary() | undefined.
mac(StoreId, StreamId, Version) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) -> mac_with(Key, StreamId, Version);
        _ -> undefined
    end.

%% @private
-spec mac_with(binary(), binary(), non_neg_integer()) -> binary().
mac_with(Key, StreamId, Version) ->
    crypto:mac(hmac, sha256, Key,
               term_to_binary({?MODULE, StreamId, Version}, [deterministic])).
