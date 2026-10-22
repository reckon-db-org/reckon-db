%% @doc Where a scavenge cut a stream's chain, recorded so a read can tell a
%% scavenge from a deletion.
%%
%% A scavenge removes a PREFIX of a stream - versions 0..k - and the oldest
%% event that survives carries, in its `prev_event_hash', the chain hash of
%% the last event that was removed. That hash is the only thing that can
%% anchor a read of the survivor: the predecessor it names is gone, so
%% nothing in the tree can be recomputed to check it.
%%
%% The danger of "anchor on whatever the first event carries" is that an
%% attacker who deletes the same prefix by hand looks exactly like a
%% scavenge. This module records the one place where a missing predecessor is
%% expected - the boundary the scavenge itself wrote - and the read path
%% anchors there and nowhere else. Everywhere else a missing predecessor is
%% an integrity violation.
%%
%% == Storage ==
%%
%% `[metadata, integrity, scavenge_boundary, StreamId]', next to the
%% chain-start watermark: a map of `#{version, prev_hash, mac}' where
%% `version' is the oldest surviving version and `prev_hash' is the chain
%% hash of the last removed event.
%%
%% The MAC is an HMAC-SHA256 under the store's integrity key over the STORE,
%% the stream, the stream's LIFE TOKEN, the version and the hash. Without it,
%% an attacker who can delete events could simply write the boundary record
%% too, and the anchor would prove nothing. A store with integrity disabled
%% records no boundary at all: there is no key to bind it with and no chain
%% being verified.
%%
%% The store id and the life token are what stop a boundary being used where
%% it was never written. Without them a boundary verifies in any store under
%% the same key, and in any later life of the same stream: delete the stream,
%% re-create it, paste the old boundary back over a stripped prefix, and the
%% anchor that is supposed to explain the region below it explains a region
%% nothing can vouch for (Jupiter r16 JR6). The life token is minted fresh for
%% every new life, so a re-created stream cannot inherit one.
%%
%% == One boundary per stream ==
%%
%% A scavenge always deletes from version 0, so the boundaries of successive
%% scavenges nest. Only the deepest one can ever be the first missing
%% predecessor of a read, so only the deepest one is kept, and a boundary
%% never moves backwards.
%%
%% @end
-module(reckon_db_scavenge_boundary).

-include("reckon_db.hrl").

-export([
    record/4,
    boundary_record/5,
    boundary_path/1,
    lookup/2,
    verify/3,
    life/2
]).

-export_type([boundary/0]).

-type boundary() :: #{version := non_neg_integer(),
                      prev_hash := binary(),
                      mac := binary()}.

-define(BOUNDARY_PATH, [metadata, integrity, scavenge_boundary]).

%%====================================================================
%% Public API
%%====================================================================

%% @doc Record the boundary a scavenge leaves in `StreamId'.
%%
%% `Version' is the oldest version that survives (the first one that was not
%% deleted) and `PrevHash' the chain hash of the last event that was. A
%% boundary already recorded deeper than this one is left alone; the same one
%% is rewritten with a fresh MAC (the key may have been rotated).
-spec record(atom(), binary(), non_neg_integer(), binary()) -> ok | {error, term()}.
record(StoreId, StreamId, Version, PrevHash)
        when is_integer(Version), Version >= 0, is_binary(PrevHash) ->
    case reckon_db_integrity_key:is_enabled(StoreId) of
        false ->
            %% Nothing verifies a chain on this store, so an anchor here
            %% would be a claim nothing can check.
            ok;
        true ->
            record_if_deepest(StoreId, StreamId, Version, PrevHash)
    end.

%% @doc The boundary recorded for a stream, if any.
-spec lookup(atom(), binary()) -> {ok, boundary() | undefined}.
lookup(StoreId, StreamId) ->
    Path = ?BOUNDARY_PATH ++ [StreamId],
    case khepri:get(StoreId, Path) of
        {ok, #{} = Boundary} -> {ok, Boundary};
        {error, _} -> {ok, undefined}
    end.

%% @doc Is this recorded boundary the authentic one for this store?
%%
%% Checks the MAC under the store's current integrity key and this stream's
%% current life. A boundary whose MAC does not verify is treated as absent: an
%% anchor an attacker wrote, or one lifted from another store or from an
%% earlier life of this stream, is not an anchor.
-spec verify(atom(), binary(), boundary()) -> boolean().
verify(StoreId, StreamId, #{version := Version, prev_hash := PrevHash,
                            mac := Mac})
        when is_integer(Version), is_binary(PrevHash), is_binary(Mac) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            Expected = mac(Key, StoreId, StreamId, life(StoreId, StreamId),
                           Version, PrevHash),
            %% A malformed MAC in the tree is not a verified boundary, and
            %% must not raise: crypto:hash_equals refuses differing lengths.
            byte_size(Mac) =:= byte_size(Expected) andalso
                crypto:hash_equals(Mac, Expected);
        _ ->
            false
    end;
verify(_StoreId, _StreamId, _Boundary) ->
    false.

%% @doc The stream's current life token, which the MAC binds.
%%
%% Read at verify time, so a boundary recorded in an earlier life stops
%% verifying the moment the stream is re-created, whatever an attacker pastes
%% back afterwards.
-spec life(atom(), binary()) -> binary() | undefined.
life(StoreId, StreamId) ->
    case reckon_db_chain_watermark:life(StoreId, StreamId) of
        {ok, Life} -> Life;
        {error, Reason} ->
            %% A boundary MAC that cannot bind the life is not written and does
            %% not verify, which is the fail-closed answer: the caller's
            %% comparison against a real MAC cannot succeed on `undefined'
            %% unless the life really is absent.
            logger:error("scavenge boundary for ~p: the stream's life could not "
                         "be read: ~p", [StreamId, Reason]),
            undefined
    end.

%%====================================================================
%% Internal
%%====================================================================

%% @private
-spec record_if_deepest(atom(), binary(), non_neg_integer(), binary())
        -> ok | {error, term()}.
record_if_deepest(StoreId, StreamId, Version, PrevHash) ->
    case lookup(StoreId, StreamId) of
        {ok, #{version := Existing}} when Existing > Version ->
            %% A deeper cut is already recorded, and it is the one a read can
            %% meet first. Nothing to write.
            ok;
        _ ->
            write(StoreId, StreamId, Version, PrevHash)
    end.

%% @doc The record a boundary write stores, MAC'd and ready to be put inside
%% a caller's transaction (the migration mints a boundary where a snapshot
%% authenticated a cut that no scavenge recorded).
%%
%% The life is the life the caller's transaction will LEAVE on the stream, not
%% necessarily the one it holds now: the migration seals a stream and writes
%% its life token in the same transaction, so a boundary minted beside that
%% seal has to name the new life or it stops verifying the moment the seal
%% lands.
-spec boundary_record(atom(), binary(), binary() | undefined,
                      non_neg_integer(), binary()) ->
        #{version := non_neg_integer(), prev_hash := binary(),
          mac := binary()}.
boundary_record(StoreId, StreamId, Life, Version, PrevHash) ->
    Key = reckon_db_integrity_key:get(StoreId),
    true = is_binary(Key),
    #{version => Version,
      prev_hash => PrevHash,
      mac => mac(Key, StoreId, StreamId, Life, Version, PrevHash)}.

%% @doc The tree path a stream's boundary lives at.
-spec boundary_path(binary()) -> [atom() | binary()].
boundary_path(StreamId) ->
    ?BOUNDARY_PATH ++ [StreamId].

%% @private
-spec write(atom(), binary(), non_neg_integer(), binary())
        -> ok | {error, term()}.
write(StoreId, StreamId, Version, PrevHash) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            put_boundary(StoreId, StreamId, life(StoreId, StreamId), Version,
                         PrevHash, Key);
        _ ->
            ok
    end.

%% @private A boundary that cannot be written is handed back to the caller,
%% and the caller must not delete anything: a stream whose predecessors are
%% gone and whose boundary is not recorded refuses every integrity read until
%% somebody scavenges it again.
-spec put_boundary(atom(), binary(), binary() | undefined, non_neg_integer(),
                   binary(), binary()) -> ok | {error, term()}.
put_boundary(StoreId, StreamId, Life, Version, PrevHash, Key) ->
    Boundary = #{version => Version,
                 prev_hash => PrevHash,
                 mac => mac(Key, StoreId, StreamId, Life, Version, PrevHash)},
    %% A transaction, not a bare put: it is the one write a scavenge must not
    %% get past unnoticed, and a transaction answers an error instead of
    %% taking the calling worker down with a badmatch (reckon-db#4).
    Fun = fun() -> khepri_tx:put(?BOUNDARY_PATH ++ [StreamId], Boundary) end,
    case khepri:transaction(StoreId, Fun) of
        {ok, ok} -> ok;
        {ok, {error, Reason}} -> {error, Reason};
        {error, Reason} -> {error, Reason}
    end.

%% @private A MAC over exactly what the anchor claims: WHICH STORE wrote it,
%% for WHICH stream, under WHICH life, at which version, naming which hash.
-spec mac(binary(), atom(), binary(), binary() | undefined, non_neg_integer(),
          binary()) -> binary().
mac(Key, StoreId, StreamId, Life, Version, PrevHash) ->
    crypto:mac(hmac, sha256, Key,
               term_to_binary({?MODULE, StoreId, StreamId, Life, Version,
                               PrevHash},
                              [deterministic])).
