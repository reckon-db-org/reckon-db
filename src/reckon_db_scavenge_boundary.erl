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
%% The MAC is an HMAC-SHA256 under the store's integrity key over the stream,
%% the version and the hash. Without it, an attacker who can delete events
%% could simply write the boundary record too, and the anchor would prove
%% nothing. A store with integrity disabled records no boundary at all: there
%% is no key to bind it with and no chain being verified.
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
    lookup/2,
    verify/3,
    delete/2
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
%% Checks the MAC under the store's current integrity key. A boundary whose
%% MAC does not verify is treated as absent: an anchor an attacker wrote is
%% not an anchor.
-spec verify(atom(), binary(), boundary()) -> boolean().
verify(StoreId, StreamId, #{version := Version, prev_hash := PrevHash,
                            mac := Mac})
        when is_integer(Version), is_binary(PrevHash), is_binary(Mac) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            crypto:hash_equals(Mac, mac(Key, StreamId, Version, PrevHash));
        _ ->
            false
    end;
verify(_StoreId, _StreamId, _Boundary) ->
    false.

%% @doc Forget a stream's boundary, when the stream itself is deleted.
-spec delete(atom(), binary()) -> ok.
delete(StoreId, StreamId) ->
    _ = khepri:delete(StoreId, ?BOUNDARY_PATH ++ [StreamId]),
    ok.

%%====================================================================
%% Internal
%%====================================================================

%% @private
-spec record_if_deepest(atom(), binary(), non_neg_integer(), binary()) -> ok.
record_if_deepest(StoreId, StreamId, Version, PrevHash) ->
    case lookup(StoreId, StreamId) of
        {ok, #{version := Existing}} when Existing > Version ->
            %% A deeper cut is already recorded, and it is the one a read can
            %% meet first. Nothing to write.
            ok;
        _ ->
            write(StoreId, StreamId, Version, PrevHash)
    end.

%% @private
-spec write(atom(), binary(), non_neg_integer(), binary()) -> ok.
write(StoreId, StreamId, Version, PrevHash) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            put_boundary(StoreId, StreamId, Version, PrevHash, Key);
        _ ->
            ok
    end.

%% @private A boundary that cannot be written is handed back to the caller,
%% and the caller must not delete anything: a stream whose predecessors are
%% gone and whose boundary is not recorded refuses every integrity read until
%% somebody scavenges it again.
-spec put_boundary(atom(), binary(), non_neg_integer(), binary(), binary())
        -> ok | {error, term()}.
put_boundary(StoreId, StreamId, Version, PrevHash, Key) ->
    Boundary = #{version => Version,
                 prev_hash => PrevHash,
                 mac => mac(Key, StreamId, Version, PrevHash)},
    khepri:put(StoreId, ?BOUNDARY_PATH ++ [StreamId], Boundary).

%% @private A MAC over exactly what the anchor claims: this stream, this
%% version, this hash.
-spec mac(binary(), binary(), non_neg_integer(), binary()) -> binary().
mac(Key, StreamId, Version, PrevHash) ->
    crypto:mac(hmac, sha256, Key,
               term_to_binary({?MODULE, StreamId, Version, PrevHash},
                              [deterministic])).
