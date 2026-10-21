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
    seal_all/1,
    migrate/1
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

%% @doc Read or mint a stream's chain-start watermark for the append that is
%% about to run at `Version'.
%%
%% Returns the version the store vouches integrity begins at - either the one
%% it minted for this brand new stream, or the one already recorded.
%%
%% This function intentionally tolerates write-write races: multiple
%% writers attempting to set the same watermark all converge on
%% the same outcome, because the writer's "next version about to write"
%% is itself serialized by reckon-db's per-stream append ordering.
%% If two concurrent writers somehow both reach this code path with
%% the same NextVersion, the watermark ends up equal to NextVersion
%% regardless of who won.
%%
%% == When it mints, and when it refuses ==
%%
%% Only for a NEW stream, at version 0. That is the one case where the value
%% is not a claim about existing data: the stream has no events, so nothing
%% is being excused by it.
%%
%% An existing stream with no watermark is refused with
%% `{chain_start_missing, StreamId}'. It is not minted, because minting one
%% would vouch for a chain start the store never chose: delete a stream's
%% watermark, append once, and the next read of that stream would come back
%% clean no matter what had been rewritten underneath. A stream that predates
%% integrity gets its watermark from the migration at store start, which is
%% the only place the value can be decided against the stream's contents.
%%
%% An UNSEALED watermark is refused the same way, as
%% `{chain_start_unauthenticated, Value}': sealing is the migration's job,
%% and an append that sealed a bare integer would authenticate whatever is
%% written there.
-spec set_if_absent(
    StoreId :: atom(),
    StreamId :: binary(),
    Version :: non_neg_integer()
) -> {ok, RecordedVersion :: non_neg_integer()}
   | {error, {chain_start_unauthenticated, non_neg_integer()}
            | {chain_start_missing, binary()}}.
set_if_absent(StoreId, StreamId, Version)
        when is_integer(Version), Version >= 0 ->
    case lookup(StoreId, StreamId) of
        {ok, {sealed, Existing}} ->
            {ok, Existing};
        {ok, {unsealed, Existing}} ->
            {error, {chain_start_unauthenticated, Existing}};
        {ok, undefined} when Version =:= 0 ->
            mint_watermark(StoreId, StreamId);
        {ok, undefined} ->
            %% An existing stream has a watermark, or the migration gave it
            %% one. No watermark here means somebody removed it.
            {error, {chain_start_missing, StreamId}}
    end.

%% @private The first append of a new stream, the only place a watermark is
%% minted at runtime.
mint_watermark(StoreId, StreamId) ->
    Path = ?INTEGRITY_CHAIN_START_PATH ++ [StreamId],
    ok = khepri:put(StoreId, Path, sealed_record(StoreId, StreamId, 0)),
    %% Re-read to handle the rare race where two writers both saw
    %% 'undefined' and both wrote. Whoever's write Khepri/Ra serialised
    %% last wins; we return that value.
    {ok, Recorded} = lookup(StoreId, StreamId),
    recorded_or_version(Recorded, 0).

%% @private Prefer the re-read value; fall back to our own write (defensive;
%% lookup should not be undefined immediately after a put).
recorded_or_version(undefined, Version) -> {ok, Version};
recorded_or_version({sealed, Recorded}, _Version) -> {ok, Recorded};
recorded_or_version({unsealed, Recorded}, _Version) ->
    %% A concurrent writer put a bare integer between our write and our
    %% read: it is not ours to seal, and it is not to be believed.
    {error, {chain_start_unauthenticated, Recorded}}.

%% @doc The upgrade, run at store start: seal the watermarks an earlier
%% version wrote as bare integers.
%%
%% An earlier version stored a watermark as a plain integer. This version
%% refuses one for the good reason that anybody who can write the tree can
%% write an integer, but refusing it means a store upgraded in place stops
%% serving: every append to those streams fails and every read is a
%% violation until somebody runs the sealing step by hand. Downtime that
%% waits for a human act is not an acceptable shape for a security fix, so
%% the sealing happens by itself, once, here.
%%
%% == When it runs ==
%%
%% At store start, before the store serves anything. It runs only on a store
%% that has an integrity key and no authenticated migration marker. Once the
%% marker is written it never runs again: after that a bare integer is not a
%% leftover, it is a forgery, and nothing seals it.
%%
%% == What it will and will not seal ==
%%
%% A bare integer is not evidence. Before sealing one at value W, the stream
%% is checked: W may not sit above the stream's newest version, and no event
%% below W may carry a chain. A genuine legacy watermark satisfies both by
%% construction (it was written at the version integrity began), so an
%% integer moved past the stream, or moved over events that still carry
%% hashes, is left alone and reported instead of being authenticated. A
%% stream with no watermark at all is sealed at the first chain-bearing
%% version, or, if it has none, at newest + 1 (the whole stream predates
%% integrity).
%%
%% What it cannot decide: a bare integer at or below the newest version over
%% a stream whose integrity fields were ALL cleared is indistinguishable from
%% a genuine pre-integrity watermark, because the events themselves carry no
%% evidence either way. Sealing it preserves exactly what the previous
%% version did with that store; it does not widen it, and the marker is what
%% stops it from ever happening again. Say this in the docs rather than
%% implying the migration is tamper-proof.
%%
%% A failure here is logged and reported, never fatal: the store still
%% starts, the affected streams still refuse (fail closed, one stream at a
%% time), and the next start retries because the marker is only written on
%% success.
-spec migrate(atom()) -> ok.
migrate(StoreId) ->
    case reckon_db_integrity_key:is_enabled(StoreId) of
        false -> ok;
        true -> migrate_store(StoreId)
    end.

%% @private
migrate_store(StoreId) ->
    case migration_state(StoreId) of
        migrated ->
            ok;
        unauthorised ->
            migration_violation(StoreId);
        not_migrated ->
            _ = run_migration(StoreId),
            ok
    end.

%% @private What the store says about having been migrated.
%%
%% An authenticated marker is the whole answer. Without one, a store whose
%% watermarks are already sealed has been migrated (or has written integrity
%% data since), and a missing marker there is an anomaly, not an invitation
%% to seal whatever bare integers are lying around: re-running would let a
%% writer strip a stream, delete the marker and be sealed back to a clean
%% read.
-spec migration_state(atom()) -> migrated | unauthorised | not_migrated.
migration_state(StoreId) ->
    case marker_verified(StoreId) of
        true -> migrated;
        false -> marker_missing_state(StoreId)
    end.

%% @private
marker_missing_state(StoreId) ->
    case any_sealed(StoreId) of
        true -> unauthorised;
        false -> not_migrated
    end.

%% @private
marker_verified(StoreId) ->
    case khepri:get(StoreId, ?INTEGRITY_MIGRATION_PATH) of
        {ok, #{version := Version, mac := Mac}} when is_binary(Mac) ->
            marker_matches(StoreId, Version, Mac);
        _ ->
            false
    end.

%% @private
marker_matches(StoreId, Version, Mac) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            crypto:hash_equals(Mac, migration_mac_with(Key, Version));
        _ ->
            false
    end.

%% @private Whether any watermark in the store verifies in the sealed shape.
any_sealed(StoreId) ->
    Path = ?INTEGRITY_CHAIN_START_PATH ++ [?KHEPRI_WILDCARD_STAR],
    case khepri:get_many(StoreId, Path) of
        {ok, Watermarks} when is_map(Watermarks) ->
            any_sealed_entry(StoreId, maps:to_list(Watermarks));
        _ ->
            false
    end.

%% @private
any_sealed_entry(StoreId, Entries) ->
    lists:any(fun({Path, Value}) -> watermark_verifies(StoreId, Path, Value) end,
              Entries).

%% @private
watermark_verifies(StoreId, Path, Value) ->
    StreamId = lists:last(Path),
    case unsealed_version(StoreId, StreamId, Value) of
        undefined -> true;
        _ -> false
    end.

%% @private
run_migration(StoreId) ->
    case reckon_db_streams:list_streams(StoreId) of
        {ok, StreamIds} ->
            finish_migration(StoreId, StreamIds);
        {error, Reason} ->
            migration_failed(StoreId, Reason),
            {error, Reason}
    end.

%% @private
finish_migration(StoreId, StreamIds) ->
    Sealed = length([S || S <- StreamIds, migrate_stream(StoreId, S) =:= sealed]),
    case write_marker(StoreId) of
        ok ->
            logger:info("integrity migration on store ~p: sealed ~b "
                        "watermark(s) over ~b stream(s)",
                        [StoreId, Sealed, length(StreamIds)]),
            telemetry:execute([reckon, db, integrity, migrated],
                              #{system_time => erlang:system_time(millisecond)},
                              #{store_id => StoreId, sealed => Sealed,
                                streams => length(StreamIds)}),
            {ok, Sealed};
        {error, _} = Error ->
            migration_failed(StoreId, Error),
            {ok, Sealed}
    end.

%% @private
migration_failed(StoreId, Reason) ->
    logger:error("integrity migration on store ~p did not finish: ~p; the "
                 "affected streams stay refused and the next start retries",
                 [StoreId, Reason]),
    ok.

%% @private Seal one stream's watermark if it is a bare integer the stream
%% can still vouch for.
-spec migrate_stream(atom(), binary()) -> sealed | skipped.
migrate_stream(StoreId, StreamId) ->
    case lookup(StoreId, StreamId) of
        {ok, {sealed, _Version}} ->
            skipped;
        {ok, {unsealed, Version}} ->
            migrate_bare_watermark(StoreId, StreamId, Version);
        {ok, undefined} ->
            migrate_missing_watermark(StoreId, StreamId)
    end.

%% @private
migrate_bare_watermark(StoreId, StreamId, Version) ->
    case newest_version(StoreId, StreamId) of
        {ok, Newest} when Version =< Newest + 1 ->
            migrate_bare_within_stream(StoreId, StreamId, Version);
        {ok, Newest} ->
            inconsistent_watermark(StoreId, StreamId, Version,
                                   {above_newest, Newest}),
            skipped;
        {error, Reason} ->
            stream_unreadable(StoreId, StreamId, Reason),
            skipped
    end.

%% @private A watermark may not excuse an event that still carries a chain:
%% a genuine one was written at the version integrity began, so nothing
%% below it has a hash.
migrate_bare_within_stream(StoreId, StreamId, Version) ->
    case any_chain_bearing_below(StoreId, StreamId, Version) of
        false ->
            ok = khepri:put(StoreId, ?INTEGRITY_CHAIN_START_PATH ++ [StreamId],
                            sealed_record(StoreId, StreamId, Version)),
            sealed;
        true ->
            inconsistent_watermark(StoreId, StreamId, Version, chain_below),
            skipped
    end.

%% @private A stream with no watermark at all: seal at the version the chain
%% begins, so its integrity reads have a starting point.
migrate_missing_watermark(StoreId, StreamId) ->
    case lowest_chain_bearing(StoreId, StreamId) of
        {ok, {version, Version}} ->
            seal_at(StoreId, StreamId, Version);
        {ok, none} ->
            seal_whole_stream_legacy(StoreId, StreamId);
        {error, Reason} ->
            stream_unreadable(StoreId, StreamId, Reason),
            skipped
    end.

%% @private
seal_whole_stream_legacy(StoreId, StreamId) ->
    case newest_version(StoreId, StreamId) of
        {ok, Newest} when is_integer(Newest), Newest >= 0 ->
            seal_at(StoreId, StreamId, Newest + 1);
        _ ->
            skipped
    end.

%% @private
seal_at(StoreId, StreamId, Version) ->
    ok = khepri:put(StoreId, ?INTEGRITY_CHAIN_START_PATH ++ [StreamId],
                    sealed_record(StoreId, StreamId, Version)),
    sealed.

%% @private
inconsistent_watermark(StoreId, StreamId, Version, Why) ->
    logger:warning("integrity migration left stream ~p on store ~p unsealed: "
                   "watermark ~b is not one the store can vouch for (~p); "
                   "its reads stay refused", [StreamId, StoreId, Version, Why]),
    telemetry:execute([reckon, db, integrity, migration_skipped],
                      #{system_time => erlang:system_time(millisecond)},
                      #{store_id => StoreId, stream_id => StreamId,
                        watermark => Version, reason => Why}).

%% @private
stream_unreadable(StoreId, StreamId, Reason) ->
    logger:warning("integrity migration could not read stream ~p on store ~p: ~p",
                   [StreamId, StoreId, Reason]).

%% @private Write the marker. Authenticated under the integrity key, so it
%% cannot be fabricated, and its absence after a migration is the anomaly
%% migration_state/1 reports rather than acts on.
write_marker(StoreId) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            khepri:put(StoreId, ?INTEGRITY_MIGRATION_PATH,
                       #{version => ?INTEGRITY_MIGRATION_VERSION,
                         mac => migration_mac_with(Key,
                                                   ?INTEGRITY_MIGRATION_VERSION)});
        _ ->
            {error, no_integrity_key}
    end.

%% @private
migration_violation(StoreId) ->
    logger:error("store ~p has sealed watermarks but no authenticated migration "
                 "marker; refusing to seal anything", [StoreId]),
    telemetry:execute([reckon, db, integrity, migration_marker_missing],
                      #{system_time => erlang:system_time(millisecond)},
                      #{store_id => StoreId}),
    ok.

%% @private
-spec migration_mac_with(binary(), non_neg_integer()) -> binary().
migration_mac_with(Key, Version) ->
    crypto:mac(hmac, sha256, Key,
               term_to_binary({?MODULE, migration, Version}, [deterministic])).

%% @private
-spec newest_version(atom(), binary()) -> {ok, integer()} | {error, term()}.
newest_version(StoreId, StreamId) ->
    case reckon_db_streams:newest_event_version(StoreId, StreamId) of
        {ok, ?NO_STREAM} -> {ok, -1};
        {ok, Version} -> {ok, Version};
        {error, _} = Error -> Error
    end.

%% @private Versions below the watermark that still carry a chain. Reads here
%% are for metadata, not for serving, so verification is off.
-spec any_chain_bearing_below(atom(), binary(), non_neg_integer()) -> boolean().
any_chain_bearing_below(_StoreId, _StreamId, 0) ->
    false;
any_chain_bearing_below(StoreId, StreamId, Version) ->
    case scan_forward(StoreId, StreamId, 0, Version - 1) of
        {ok, Events} -> lists:any(fun chain_bearing/1, Events);
        {error, _} -> true
    end.

%% @private
-spec lowest_chain_bearing(atom(), binary()) -> {ok, {version, non_neg_integer()} | none}
                                              | {error, term()}.
lowest_chain_bearing(StoreId, StreamId) ->
    case newest_version(StoreId, StreamId) of
        {ok, Newest} when Newest >= 0 ->
            scan_for_chain(StoreId, StreamId, Newest);
        {ok, _} ->
            {ok, none};
        {error, _} = Error ->
            Error
    end.

%% @private
scan_for_chain(StoreId, StreamId, Newest) ->
    case scan_forward(StoreId, StreamId, 0, Newest) of
        {ok, Events} -> {ok, first_chain_bearing(Events)};
        {error, _} = Error -> Error
    end.

%% @private
first_chain_bearing(Events) ->
    case [E || E <- Events, chain_bearing(E)] of
        [] -> none;
        [#event{version = V} | _] -> {version, V}
    end.

%% @private
chain_bearing(#event{prev_event_hash = PrevHash}) -> is_binary(PrevHash).

%% @private Read every version in a range, unverified.
-spec scan_forward(atom(), binary(), non_neg_integer(), non_neg_integer()) ->
    {ok, [event()]} | {error, term()}.
scan_forward(StoreId, StreamId, From, To) ->
    Count = To - From + 1,
    reckon_db_streams:read(StoreId, StreamId, From, Count, forward,
                           #{verify => skip_all}).

%% @private Delete a stream's watermark.
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

%% @doc Seal one stream's bare-integer watermark.
%%
%% Rarely needed: `migrate/1' runs this shape automatically at store start.
%% It refuses once that migration has run, because a bare integer on a
%% migrated store is not a leftover from an earlier version, it is a forgery,
%% and this is exactly the call that would bless it.
-spec seal(atom(), binary()) -> ok | {error, term()}.
seal(StoreId, StreamId) ->
    case migration_state(StoreId) of
        migrated ->
            {error, already_migrated};
        unauthorised ->
            {error, migration_marker_missing};
        not_migrated ->
            seal_manually(StoreId, StreamId)
    end.

%% @private
seal_manually(StoreId, StreamId) ->
    case lookup(StoreId, StreamId) of
        {ok, {unsealed, Version}} ->
            migrate_bare_watermark(StoreId, StreamId, Version),
            ok;
        _ ->
            ok
    end.

%% @doc Run the watermark migration, as store start does.
%%
%% Returns how many watermarks it sealed, zero on a store already migrated,
%% and `{error, migration_marker_missing}' on a store whose watermarks are
%% sealed but whose marker is absent or does not verify. It never re-seals on
%% that anomaly: re-sealing is precisely what a writer who stripped a stream
%% and deleted the marker would want.
-spec seal_all(atom()) -> {ok, non_neg_integer()} | {error, term()}.
seal_all(StoreId) ->
    case migration_state(StoreId) of
        migrated -> {ok, 0};
        unauthorised -> {error, migration_marker_missing};
        not_migrated -> run_migration(StoreId)
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
