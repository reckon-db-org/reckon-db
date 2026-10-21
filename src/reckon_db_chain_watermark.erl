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
%% The watermark lives in Khepri under
%% `[metadata, integrity, chain_start, StreamId]' and the stream's life token
%% under `[metadata, integrity, stream_life, StreamId]'. A sealed watermark is
%% `#{version := V, v2_from := N, life := Life, mac := Mac}', MAC'd under the
%% store's integrity key over the store id, the stream id, the life token,
%% the version and `v2_from'. A release before 5.12.0 wrote a bare integer.
%%
%% The life token is a random value minted with the watermark and deleted
%% with the stream. It is what makes a sealed record from an earlier life of
%% the same stream id stop verifying after the stream is deleted and
%% re-created, and it is covered by every scheme-2 event MAC as well.
%%
%% `v2_from' is the version from which this stream's events are MAC'd under
%% scheme 2 (store- and life-bound). A scheme-1 MAC at or above it is refused,
%% so an old-scheme writer on the same key cannot put a store-blind MAC above
%% the point where the stream began writing scheme 2.
%%
%% == Read-side use ==
%%
%% `lookup/2' answers the shape; `binding/2' answers the recorded life and
%% `v2_from' that the read and write paths need. Verification itself lives in
%% gater's integrity module and in `reckon_db_streams'.
%%
%% == The migration ==
%%
%% See the threat model, reckon-db-org/reckon-db#18. In short: the scan reads
%% every stream outside the tree; one Khepri transaction compare-and-swaps
%% each watermark it seals against the raw value the scan saw, writes the
%% life token and a scavenge boundary where one is needed, and writes the
%% authenticated done marker with them. There is no in-progress record.
%% @end
-module(reckon_db_chain_watermark).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([
    lookup/2,
    binding/2,
    life/2,
    path/1,
    life_path/1,
    set_if_absent/3,
    delete/2,
    migrate/1,
    migrate/2
]).

-export_type([watermark/0, binding/0]).

%% Events read per page while the migration scans a stream.
-define(SCAN_PAGE, 500).

%% The node-local anchor, beside the store's data directory.
-define(ANCHOR_FILE, "integrity_migration.done").
-define(ANCHOR_VERSION, 1).

%% What lookup/2 answers about a stream's chain start.
-type watermark() :: {sealed, non_neg_integer()} |
                     {unsealed, term()} |
                     undefined.

%% What binding/2 answers: everything a verifier needs to check this
%% stream's scheme-2 MACs.
-type binding() :: #{version := non_neg_integer(),
                     v2_from := non_neg_integer(),
                     life := binary()}.

%% One stream as the migration scan found it.
-type scan() :: #{store_id := atom(),
                  stream_id := binary(),
                  watermark := watermark(),
                  raw := term(),
                  life := binary() | undefined,
                  newest := integer(),
                  chain_start := {ok, non_neg_integer()} | none,
                  genesis := {ok, non_neg_integer()} | none,
                  cut := {ok, non_neg_integer()} | none,
                  error => term()}.

%% A decision the scan made, ready to commit.
-type plan() :: #{kind := seal,
                  stream_id := binary(),
                  version := non_neg_integer(),
                  expected := term(),
                  writes := [{[atom() | binary()], term()}]} |
                #{kind := refuse, stream_id := binary(), reason := term()} |
                #{kind := skip}.

%%====================================================================
%% Public API
%%====================================================================

%% @doc Read the chain-start version for a stream.
-spec lookup(StoreId :: atom(), StreamId :: binary()) -> {ok, watermark()}.
lookup(StoreId, StreamId) ->
    case khepri:get(StoreId, path(StreamId)) of
        {ok, #{version := Version} = Record} ->
            {ok, sealed_or_bare(StoreId, StreamId, Version, Record)};
        {ok, Version} when is_integer(Version), Version >= 0 ->
            {ok, {unsealed, Version}};
        {ok, Other} ->
            {ok, {unsealed, Other}};
        _ ->
            {ok, undefined}
    end.

%% @private
sealed_or_bare(StoreId, StreamId, Version, #{mac := Mac} = Record)
        when is_integer(Version), is_binary(Mac) ->
    case seal_verifies(StoreId, StreamId, Record) of
        true -> {sealed, Version};
        false -> {unsealed, Record}
    end;
sealed_or_bare(_StoreId, _StreamId, _Version, Other) ->
    {unsealed, Other}.

%% @doc The life token, `v2_from' and version of a stream's sealed
%% watermark, or `undefined' when the stream has none the store can vouch
%% for. This is what a verifier needs; `lookup/2' is the shape.
-spec binding(atom(), binary()) -> {ok, binding() | undefined}.
binding(StoreId, StreamId) ->
    case khepri:get(StoreId, path(StreamId)) of
        {ok, #{version := Version, v2_from := V2From, life := Life} = Record}
                when is_integer(Version), is_integer(V2From),
                     is_binary(Life) ->
            binding_or_undefined(StoreId, StreamId,
                                 #{version => Version, v2_from => V2From,
                                   life => Life}, Record);
        _ ->
            {ok, undefined}
    end.

%% @private
binding_or_undefined(StoreId, StreamId, Binding, Record) ->
    case seal_verifies(StoreId, StreamId, Record) of
        true -> {ok, Binding};
        false -> {ok, undefined}
    end.

%% @doc A stream's life token, or `undefined'.
-spec life(atom(), binary()) -> {ok, binary() | undefined}.
life(StoreId, StreamId) ->
    case khepri:get(StoreId, life_path(StreamId)) of
        {ok, Life} when is_binary(Life) -> {ok, Life};
        _ -> {ok, undefined}
    end.

%% @doc The tree path a stream's watermark lives at.
-spec path(binary()) -> [atom() | binary()].
path(StreamId) ->
    ?INTEGRITY_CHAIN_START_PATH ++ [StreamId].

%% @doc The tree path a stream's life token lives at.
-spec life_path(binary()) -> [atom() | binary()].
life_path(StreamId) ->
    ?INTEGRITY_STREAM_LIFE_PATH ++ [StreamId].

%% @doc Read or mint a stream's chain-start watermark for the append that is
%% about to run at `Version'.
%%
%% Mints only for a NEW stream, at version 0: that is the one case where the
%% value is not a claim about existing data. An existing stream with no
%% watermark is refused with `{chain_start_missing, StreamId}', and an
%% unauthenticated one with `{chain_start_unauthenticated, Value}'. Only the
%% migration seals an existing stream's chain start.
-spec set_if_absent(
    StoreId :: atom(),
    StreamId :: binary(),
    Version :: non_neg_integer()
) -> {ok, RecordedVersion :: non_neg_integer()}
   | {error, {chain_start_unauthenticated, term()}
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
            {error, {chain_start_missing, StreamId}}
    end.

%% @private The first append of a new stream: mint its life token and its
%% watermark at version 0, under scheme 2 from the first event.
mint_watermark(StoreId, StreamId) ->
    Life = new_life(),
    ok = khepri:put(StoreId, life_path(StreamId), Life),
    ok = khepri:put(StoreId, path(StreamId),
                    sealed_record(StoreId, StreamId, Life, 0, 0)),
    {ok, Recorded} = lookup(StoreId, StreamId),
    recorded_or_version(Recorded, 0).

%% @private
recorded_or_version(undefined, Version) -> {ok, Version};
recorded_or_version({sealed, Recorded}, _Version) -> {ok, Recorded};
recorded_or_version({unsealed, Recorded}, _Version) ->
    {error, {chain_start_unauthenticated, Recorded}}.

%% @doc The upgrade, run at store start.
-spec migrate(atom()) -> ok.
migrate(StoreId) ->
    _ = migrate(StoreId, #{}),
    ok.

%% @doc The upgrade with options: `data_dir' (the anchor), `accept_missing'
%% (the operator act) and `operator' (a name recorded in the log).
-spec migrate(atom(), map()) -> ok | {ok, non_neg_integer()} | {error, term()}.
migrate(StoreId, Opts) when is_map(Opts) ->
    case maps:find(accept_missing, Opts) of
        {ok, StreamIds} when is_list(StreamIds) ->
            accept_missing(StoreId, StreamIds, Opts);
        _ ->
            migrate_at_start(StoreId, Opts)
    end.

%% @private
-spec migrate_at_start(atom(), map()) -> {ok, non_neg_integer()} | {error, term()}.
migrate_at_start(StoreId, Opts) ->
    case reckon_db_integrity_key:is_enabled(StoreId) of
        false -> {ok, 0};
        true -> migrate_store(StoreId, anchor_path(Opts))
    end.

%% @doc Delete a stream's integrity metadata (watermark and life token).
-spec delete(StoreId :: atom(), StreamId :: binary()) -> ok.
delete(StoreId, StreamId) ->
    _ = khepri:delete(StoreId, path(StreamId)),
    _ = khepri:delete(StoreId, life_path(StreamId)),
    ok.

%%====================================================================
%% The migration
%%====================================================================

%% @private
migrate_store(StoreId, AnchorPath) ->
    case migration_state(StoreId, AnchorPath) of
        migrated ->
            _ = ensure_anchor(StoreId, AnchorPath),
            {ok, 0};
        unauthorised ->
            migration_violation(StoreId),
            {error, migration_marker_missing};
        not_migrated ->
            run_migration(StoreId, AnchorPath)
    end.

%% @private
migration_state(StoreId, AnchorPath) ->
    case read_marker(StoreId) of
        {ok, _Refused} -> migrated;
        none -> migration_state_without_marker(StoreId, AnchorPath)
    end.

%% @private
migration_state_without_marker(StoreId, AnchorPath) ->
    case anchor_verified(StoreId, AnchorPath) of
        true -> unauthorised;
        false -> watermarks_state(StoreId)
    end.

%% @private
watermarks_state(StoreId) ->
    case any_sealed(StoreId) of
        true -> unauthorised;
        false -> not_migrated
    end.

%% @private
-spec read_marker(atom()) -> {ok, [binary()]} | none.
read_marker(StoreId) ->
    case khepri:get(StoreId, ?INTEGRITY_MIGRATION_PATH) of
        {ok, #{version := Version, refused := Refused, mac := Mac}}
                when is_binary(Mac), is_list(Refused) ->
            marker_matches(StoreId, Version, Refused, Mac);
        _ ->
            none
    end.

%% @private
marker_matches(StoreId, Version, Refused, Mac) ->
    case marker_mac_verifies(StoreId, Version, Refused, Mac) of
        true -> {ok, Refused};
        false -> none
    end.

%% @private
marker_mac_verifies(StoreId, Version, Refused, Mac) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            crypto:hash_equals(Mac, migration_mac(StoreId, Key, Version, Refused));
        _ ->
            false
    end.

%% @private
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
    lists:any(fun({Path, _Value}) -> sealed_of(StoreId, lists:last(Path)) end,
              Entries).

%% @private
sealed_of(StoreId, StreamId) ->
    case lookup(StoreId, StreamId) of
        {ok, {sealed, _}} -> true;
        {ok, _} -> false
    end.

%% @private
run_migration(StoreId, AnchorPath) ->
    case reckon_db_streams:list_streams(StoreId) of
        {ok, StreamIds} ->
            commit_migration(StoreId, StreamIds, AnchorPath);
        {error, Reason} ->
            migration_failed(StoreId, Reason),
            {error, Reason}
    end.

%% @private
commit_migration(StoreId, StreamIds, AnchorPath) ->
    Scans = [scan_stream(StoreId, StreamId) || StreamId <- StreamIds],
    Evidence = deletion_evidence(StoreId, Scans),
    Plans = [plan_stream(StoreId, Scan, Evidence) || Scan <- Scans],
    case commit_plans(StoreId, Plans) of
        {ok, committed} ->
            _ = ensure_anchor(StoreId, AnchorPath),
            report_migration(StoreId, StreamIds, Plans, Evidence),
            {ok, committed_seals(Plans)};
        {ok, already_migrated} ->
            cluster_loser(StoreId, AnchorPath);
        {error, {marker_present, _}} ->
            cluster_loser(StoreId, AnchorPath);
        {error, Reason} ->
            migration_failed(StoreId, Reason),
            {error, Reason}
    end.

%% @private The store already carries a marker: after verifying it, this
%% node writes its own anchor, so every node in a cluster ends up with one.
cluster_loser(StoreId, AnchorPath) ->
    case read_marker(StoreId) of
        {ok, _Refused} ->
            _ = ensure_anchor(StoreId, AnchorPath),
            {ok, 0};
        none ->
            logger:error("integrity migration on store ~p found a marker that "
                         "does not verify; sealing nothing", [StoreId]),
            telemetry:execute([reckon, db, integrity, migration_marker_unverified],
                              #{system_time => erlang:system_time(millisecond)},
                              #{store_id => StoreId}),
            {error, migration_marker_unverified}
    end.

%% @private
committed_seals(Plans) ->
    length([P || #{kind := seal} = P <- Plans]).

%% @private A stream that shows a watermark was deleted: it carries a chain,
%% and it has no authenticated watermark at the version its chain begins -
%% unless an authenticated cut explains the missing chain start.
-spec deletion_evidence(atom(), [scan()]) -> boolean().
deletion_evidence(_StoreId, Scans) ->
    lists:any(fun shows_deletion/1, Scans).

%% @private
shows_deletion(#{chain_start := {ok, ChainStart}, watermark := Watermark,
                 cut := Cut} = Scan) ->
    _ = Scan,
    not sealed_at(Watermark, ChainStart) andalso Cut =:= none;
shows_deletion(_Scan) ->
    false.

%% @private
sealed_at({sealed, Version}, Version) -> true;
sealed_at(_Watermark, _ChainStart) -> false.

%% @private
-spec plan_stream(atom(), scan(), boolean()) -> plan().
plan_stream(_StoreId, #{error := Reason} = Scan, _Evidence) ->
    refuse(Scan, {unreadable, Reason});
plan_stream(_StoreId, #{watermark := {sealed, _}}, _Evidence) ->
    #{kind => skip};
plan_stream(StoreId, #{chain_start := none} = Scan, Evidence) ->
    plan_chainless(StoreId, Scan, Evidence);
plan_stream(_StoreId, #{chain_start := {ok, ChainStart}} = Scan, _Evidence) ->
    plan_chain_bearing(Scan, ChainStart).

%% @private A stream that carries no chain at all.
plan_chainless(_StoreId, #{watermark := undefined, newest := Newest} = Scan, false) ->
    seal_at(Scan, Newest + 1);
plan_chainless(_StoreId, #{watermark := {unsealed, Watermark},
                           newest := Newest} = Scan, false) ->
    plan_bare_chainless(Scan, Watermark, Newest);
plan_chainless(_StoreId, Scan, true) ->
    refuse(Scan, needs_accept_missing).

%% @private A bare watermark over a chain-less stream: at or below newest + 1
%% is the shape the earlier version wrote; above it is a claim nothing can
%% check.
plan_bare_chainless(Scan, Watermark, Newest) when is_integer(Watermark),
                                                  Watermark =< Newest + 1 ->
    seal_at(Scan, Watermark);
plan_bare_chainless(Scan, Watermark, _Newest) ->
    refuse(Scan, {above_newest, Watermark}).

%% @private A stream that carries a chain is sealed at a version the store
%% can PROVE: the event at the chain start is the genesis-prev event whose
%% MAC verifies, or an authenticated cut explains why it is gone.
plan_chain_bearing(Scan, ChainStart) ->
    case maps:get(genesis, Scan) of
        {ok, ChainStart} -> seal_at(Scan, ChainStart);
        {ok, Other} -> refuse(Scan, {genesis_not_at_chain_start, Other, ChainStart});
        none -> plan_unproven_chain_start(Scan, ChainStart)
    end.

%% @private
plan_unproven_chain_start(#{cut := {ok, ChainStart}} = Scan, ChainStart) ->
    seal_at(Scan, ChainStart);
plan_unproven_chain_start(Scan, ChainStart) ->
    refuse(Scan, {chain_start_unproven, ChainStart}).

%% @private
refuse(Scan, Reason) ->
    #{kind => refuse, stream_id => maps:get(stream_id, Scan), reason => Reason}.

%% @private Build a seal plan: the version, the raw value the CAS compares
%% against, and the writes it carries (a life token when the stream has none,
%% and a boundary where a cut needs recording).
seal_at(Scan, Version) ->
    StoreId = maps:get(store_id, Scan),
    StreamId = maps:get(stream_id, Scan),
    Life = plan_life(Scan),
    V2From = maps:get(newest, Scan) + 1,
    Seal = sealed_record(StoreId, StreamId, Life, Version, V2From),
    Writes = [{life_path(StreamId), Life} | boundary_writes(StoreId, Scan)],
    #{kind => seal, stream_id => StreamId, version => Version,
      expected => maps:get(raw, Scan),
      writes => Writes ++ [{path(StreamId), Seal}]}.

%% @private
plan_life(#{life := Life}) when is_binary(Life) -> Life;
plan_life(_Scan) -> new_life().

%% @private Where a cut was authenticated by something other than a boundary
%% the store already holds, the migration mints the boundary in the same
%% transaction, so later reads anchor at the cut.
boundary_writes(StoreId, #{stream_id := StreamId, cut := {ok, ChainStart},
                           chain_start := {ok, ChainStart}}) ->
    case reckon_db_scavenge_boundary:lookup(StoreId, StreamId) of
        {ok, Boundary} when is_map(Boundary) -> [];
        _ -> boundary_write(StoreId, StreamId, ChainStart)
    end;
boundary_writes(_StoreId, _Scan) ->
    [].

%% @private
boundary_write(StoreId, StreamId, ChainStart) ->
    case carried_hash(StoreId, StreamId, ChainStart) of
        undefined ->
            [];
        Carried ->
            [{reckon_db_scavenge_boundary:boundary_path(StreamId),
              reckon_db_scavenge_boundary:boundary_record(
                StoreId, StreamId, ChainStart, Carried)}]
    end.

%% @private The hash the event at `Version' carries in its predecessor field.
carried_hash(StoreId, StreamId, Version) ->
    case reckon_db_streams:read(StoreId, StreamId, Version, 1, forward,
                                #{verify => skip_all}) of
        {ok, [#event{prev_event_hash = Prev}]} when is_binary(Prev) -> Prev;
        _ -> undefined
    end.

%%====================================================================
%% Committing
%%====================================================================

%% @private One transaction: every seal compare-and-swapped, the life tokens
%% and boundaries written with them, and the marker.
commit_plans(StoreId, Plans) ->
    Seals = [P || #{kind := seal} = P <- Plans],
    Refused = [maps:get(stream_id, P) || #{kind := refuse} = P <- Plans],
    Marker = marker_record(StoreId, Refused),
    Fun = fun() -> migration_tx(Seals, Marker) end,
    case khepri:transaction(StoreId, Fun) of
        {ok, committed} -> {ok, committed};
        {ok, already_migrated} -> {ok, already_migrated};
        {ok, {error, Reason}} -> {error, Reason};
        {error, Reason} -> {error, Reason}
    end.

%% @private
migration_tx(Seals, Marker) ->
    case marker_present() of
        true ->
            already_migrated;
        false ->
            lists:foreach(fun write_plan/1, Seals),
            ok = khepri_tx:put(?INTEGRITY_MIGRATION_PATH, Marker),
            committed
    end.

%% @private
marker_present() ->
    case khepri_tx:get(?INTEGRITY_MIGRATION_PATH) of
        {ok, _} -> true;
        _ -> false
    end.

%% @private Compare-and-swap on the RAW value the scan saw, then write the
%% plan's other nodes. A value that moved aborts the whole migration.
write_plan(#{stream_id := StreamId, expected := Expected, writes := Writes}) ->
    Path = path(StreamId),
    case current_raw(Path) of
        Expected -> write_nodes(Writes);
        Other -> khepri_tx:abort({watermark_changed, StreamId, Expected, Other})
    end.

%% @private
write_nodes(Writes) ->
    lists:foreach(fun({P, V}) -> ok = khepri_tx:put(P, V) end, Writes),
    ok.

%% @private
current_raw(Path) ->
    case khepri_tx:get(Path) of
        {ok, Value} -> Value;
        _ -> undefined
    end.

%% @private
marker_record(StoreId, Refused) ->
    Sorted = lists:usort(Refused),
    Key = reckon_db_integrity_key:get(StoreId),
    true = is_binary(Key),
    #{version => ?INTEGRITY_MIGRATION_VERSION,
      refused => Sorted,
      mac => migration_mac(StoreId, Key, ?INTEGRITY_MIGRATION_VERSION, Sorted)}.

%% @private
-spec migration_mac(atom(), binary(), non_neg_integer(), list()) -> binary().
migration_mac(StoreId, Key, Version, Refused) ->
    crypto:mac(hmac, sha256, Key,
               term_to_binary({?MODULE, StoreId, migration, Version, Refused},
                              [deterministic])).

%% @private
migration_failed(StoreId, Reason) ->
    logger:error("integrity migration on store ~p did not finish: ~p; the "
                 "affected streams stay refused and the next start retries",
                 [StoreId, Reason]),
    ok.

%% @private
migration_violation(StoreId) ->
    logger:error("store ~p says it was migrated (marker: no; sealed "
                 "watermarks or a node-local anchor: yes) but has no "
                 "authenticated migration marker; refusing to seal anything",
                 [StoreId]),
    telemetry:execute([reckon, db, integrity, migration_marker_missing],
                      #{system_time => erlang:system_time(millisecond)},
                      #{store_id => StoreId}),
    ok.

%% @private
report_migration(StoreId, StreamIds, Plans, Evidence) ->
    Sealed = committed_seals(Plans),
    Refused = [{maps:get(stream_id, P), maps:get(reason, P)}
               || #{kind := refuse} = P <- Plans],
    logger:info("integrity migration on store ~p: sealed ~b watermark(s), "
                "refused ~b of ~b stream(s), deletion evidence ~p",
                [StoreId, Sealed, length(Refused), length(StreamIds), Evidence]),
    telemetry:execute([reckon, db, integrity, migrated],
                      #{system_time => erlang:system_time(millisecond)},
                      #{store_id => StoreId, sealed => Sealed,
                        refused => length(Refused),
                        streams => length(StreamIds),
                        deletion_evidence => Evidence}),
    lists:foreach(fun({StreamId, Why}) ->
                          report_refused(StoreId, StreamId, Why)
                  end, Refused).

%% @private
report_refused(StoreId, StreamId, Why) ->
    logger:warning("integrity migration refused stream ~p on store ~p (~p); "
                   "it stays refused until an operator vouches for it with "
                   "migrate(~p, #{accept_missing => [~p]})",
                   [StreamId, StoreId, Why, StoreId, StreamId]),
    telemetry:execute([reckon, db, integrity, migration_skipped],
                      #{system_time => erlang:system_time(millisecond)},
                      #{store_id => StoreId, stream_id => StreamId,
                        reason => Why}).

%%====================================================================
%% The operator act
%%====================================================================

%% @private The operator act: seal the streams the migration refused, and
%% only those.
accept_missing(StoreId, StreamIds, Opts) ->
    case read_marker(StoreId) of
        {ok, Refused} -> accept_refused(StoreId, StreamIds, Refused, Opts);
        none -> {error, migration_marker_missing}
    end.

%% @private
accept_refused(StoreId, StreamIds, Refused, Opts) ->
    case [S || S <- StreamIds, not lists:member(S, Refused)] of
        [] -> accept_scanned(StoreId, StreamIds, Refused, Opts);
        Unknown -> {error, {not_refused, Unknown}}
    end.

%% @private
accept_scanned(StoreId, StreamIds, Refused, Opts) ->
    Scans = [scan_stream(StoreId, StreamId) || StreamId <- StreamIds],
    case plans_for_accepted(Scans) of
        {ok, Plans} -> commit_accept(StoreId, StreamIds, Refused, Plans, Opts);
        {error, Reason} -> {error, Reason}
    end.

%% @private
plans_for_accepted(Scans) ->
    plans_for_accepted(Scans, []).

%% @private
plans_for_accepted([], Acc) ->
    {ok, lists:reverse(Acc)};
plans_for_accepted([#{error := Reason} | _Rest], _Acc) ->
    {error, {unreadable, Reason}};
plans_for_accepted([#{chain_start := {ok, ChainStart}} = Scan | Rest], Acc) ->
    case maps:get(genesis, Scan) of
        {ok, _} -> plans_for_accepted(Rest, [seal_at(Scan, ChainStart) | Acc]);
        none -> accept_unproven(Scan, ChainStart, Rest, Acc)
    end;
plans_for_accepted([#{chain_start := none} = Scan | Rest], Acc) ->
    plans_for_accepted(Rest, [seal_at(Scan, maps:get(newest, Scan) + 1) | Acc]).

%% @private
accept_unproven(#{cut := {ok, ChainStart}} = Scan, ChainStart, Rest, Acc) ->
    plans_for_accepted(Rest, [seal_at(Scan, ChainStart) | Acc]);
accept_unproven(Scan, ChainStart, _Rest, _Acc) ->
    {error, {cannot_vouch, maps:get(stream_id, Scan),
             {chain_start_unproven, ChainStart}}}.

%% @private
commit_accept(StoreId, StreamIds, Refused, Plans, Opts) ->
    Remaining = [S || S <- Refused, not lists:member(S, StreamIds)],
    Marker = marker_record(StoreId, Remaining),
    Fun = fun() -> accept_tx(Plans, Marker) end,
    case khepri:transaction(StoreId, Fun) of
        {ok, committed} ->
            report_accept(StoreId, StreamIds, Opts),
            ok;
        {ok, {error, Reason}} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
accept_tx(Plans, Marker) ->
    lists:foreach(fun write_plan/1, Plans),
    ok = khepri_tx:put(?INTEGRITY_MIGRATION_PATH, Marker),
    committed.

%% @private
report_accept(StoreId, StreamIds, Opts) ->
    Operator = maps:get(operator, Opts, node()),
    logger:notice("operator ~p accepted ~b stream(s) as pre-integrity on "
                  "store ~p at ~p: ~p",
                  [Operator, length(StreamIds), StoreId,
                   erlang:system_time(millisecond), StreamIds]),
    telemetry:execute([reckon, db, integrity, operator_accepted],
                      #{system_time => erlang:system_time(millisecond)},
                      #{store_id => StoreId, operator => Operator,
                        streams => StreamIds}),
    ok.

%%====================================================================
%% Scanning
%%====================================================================

%% @private
-spec scan_stream(atom(), binary()) -> scan().
scan_stream(StoreId, StreamId) ->
    {ok, Watermark} = lookup(StoreId, StreamId),
    Base = #{store_id => StoreId,
             stream_id => StreamId,
             watermark => Watermark,
             raw => raw_watermark(StoreId, StreamId),
             life => current_life(StoreId, StreamId),
             newest => -1,
             chain_start => none,
             genesis => none,
             cut => none},
    case newest_version(StoreId, StreamId) of
        {ok, Newest} -> scan_versions(StoreId, Base#{newest => Newest});
        {error, Reason} -> Base#{error => Reason}
    end.

%% @private
current_life(StoreId, StreamId) ->
    {ok, Life} = life(StoreId, StreamId),
    Life.

%% @private The RAW tree value, which is what the compare-and-swap compares:
%% a bare integer, a map (authentic or not), or undefined.
raw_watermark(StoreId, StreamId) ->
    case khepri:get(StoreId, path(StreamId)) of
        {ok, Value} -> Value;
        _ -> undefined
    end.

%% @private
scan_versions(StoreId, #{stream_id := StreamId, newest := Newest} = Scan) ->
    case lowest_chain_event(StoreId, StreamId, 0, Newest) of
        {ok, none} ->
            Scan;
        {ok, Event} ->
            ChainStart = Event#event.version,
            Scan#{chain_start => {ok, ChainStart},
                  genesis => genesis_version(StoreId, Scan, Event),
                  cut => authenticated_cut(StoreId, StreamId, ChainStart,
                                            maps:get(life, Scan))};
        {error, Reason} ->
            Scan#{error => Reason}
    end.

%% @private
genesis_version(StoreId, Scan, Event) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) -> verify_genesis(StoreId, Scan, Event, Key);
        _ -> none
    end.

%% @private The chain-start event is verified against the store and life it
%% was written in, so a scheme-2 event verifies and a scheme-1 one still does
%% (v2_from is set above its own version, which is where an old-line event
%% necessarily sits).
verify_genesis(StoreId, Scan, Event, Key) ->
    Version = Event#event.version,
    Ctx = genesis_context(StoreId, Scan, Key, Version),
    case reckon_gater_integrity:verify_event(
           Event, reckon_gater_integrity:genesis_prev_hash(), Key, Ctx) of
        ok -> {ok, Version};
        _ -> none
    end.

%% @private
genesis_context(StoreId, Scan, Key, Version) ->
    Base = #{store_id => StoreId, key => Key, v2_from => Version + 1},
    case maps:get(life, Scan) of
        Life when is_binary(Life) -> Base#{life => Life};
        _ -> Base
    end.

%% @private An authenticated cut at exactly this chain start: a scavenge
%% boundary the store wrote, or a snapshot of this stream whose anchor_hash
%% is the hash the surviving event carries and whose version is one below it.
authenticated_cut(StoreId, StreamId, ChainStart, Life) ->
    case boundary_cut(StoreId, StreamId, ChainStart) of
        {ok, V} -> {ok, V};
        none -> snapshot_cut(StoreId, StreamId, ChainStart, Life)
    end.

%% @private
boundary_cut(StoreId, StreamId, ChainStart) ->
    case reckon_db_scavenge_boundary:lookup(StoreId, StreamId) of
        {ok, #{version := ChainStart} = Boundary} ->
            boundary_cut_verified(StoreId, StreamId, Boundary, ChainStart);
        _ ->
            none
    end.

%% @private
boundary_cut_verified(StoreId, StreamId, Boundary, ChainStart) ->
    case reckon_db_scavenge_boundary:verify(StoreId, StreamId, Boundary) of
        true -> {ok, ChainStart};
        false -> none
    end.

%% @private
snapshot_cut(StoreId, StreamId, ChainStart, Life) ->
    case reckon_db_snapshots_store:get_latest(StoreId, StreamId) of
        undefined ->
            none;
        Snapshot ->
            snapshot_cut_matches(StoreId, StreamId, ChainStart, Snapshot, Life)
    end.

%% @private
snapshot_cut_matches(StoreId, StreamId, ChainStart, Snapshot, Life) ->
    Carried = carried_hash(StoreId, StreamId, ChainStart),
    Vouched = snapshot_mac_ok(StoreId, Life, Snapshot),
    Anchored = snapshot_anchor_matches(Snapshot, ChainStart, Carried),
    case Vouched andalso Anchored of
        true -> {ok, ChainStart};
        false -> none
    end.

%% @private A snapshot's MAC, verified on its own: the load path also checks
%% that the event at the snapshot's version still exists, which is exactly
%% what a cut has removed. The shape of the stored MAC says which scheme
%% wrote it, so a scheme-2 snapshot is verified against the life token the
%% stream still holds even after its watermark is gone.
snapshot_mac_ok(StoreId, Life, Snapshot) ->
    Key = reckon_db_integrity_key:get(StoreId),
    case {is_binary(Key), Snapshot#snapshot.mac, is_binary(Life)} of
        {true, {v2, _, _}, true} ->
            Ctx = #{store_id => StoreId, life => Life},
            reckon_gater_integrity:verify_snapshot(
              Snapshot, Snapshot#snapshot.anchor_hash, Key, Ctx) =:= ok;
        {true, {_, _}, _} ->
            reckon_gater_integrity:verify_snapshot(
              Snapshot, Snapshot#snapshot.anchor_hash, Key) =:= ok;
        _ ->
            false
    end.

%% @private
snapshot_anchor_matches(#snapshot{version = Version, anchor_hash = Anchor},
                        ChainStart, Carried)
        when is_binary(Carried) ->
    Version =:= ChainStart - 1 andalso Anchor =:= Carried;
snapshot_anchor_matches(_Snapshot, _ChainStart, _Carried) ->
    false.

%% @private
lowest_chain_event(_StoreId, _StreamId, From, To) when From > To ->
    {ok, none};
lowest_chain_event(StoreId, StreamId, From, To) ->
    Count = min(?SCAN_PAGE, To - From + 1),
    case read_page(StoreId, StreamId, From, Count) of
        {ok, Events} -> first_chain_event(Events, StoreId, StreamId, From, Count, To);
        {error, _} = Error -> Error
    end.

%% @private
first_chain_event(Events, StoreId, StreamId, From, Count, To) ->
    case [E || E <- Events, chain_bearing(E)] of
        [#event{} = First | _] -> {ok, First};
        [] -> lowest_chain_event(StoreId, StreamId, From + Count, To)
    end.

%% @private
read_page(StoreId, StreamId, From, Count) ->
    reckon_db_streams:read(StoreId, StreamId, From, Count, forward,
                           #{verify => skip_all}).

%% @private
chain_bearing(#event{prev_event_hash = PrevHash}) -> is_binary(PrevHash).

%%====================================================================
%% The node-local anchor
%%====================================================================

%% @private
-spec anchor_path(map()) -> file:filename_all() | undefined.
anchor_path(#{data_dir := DataDir}) when is_list(DataDir) ->
    filename:join(DataDir, ?ANCHOR_FILE);
anchor_path(_) ->
    undefined.

%% @private
ensure_anchor(_StoreId, undefined) ->
    ok;
ensure_anchor(StoreId, Path) ->
    case anchor_verified(StoreId, Path) of
        true -> ok;
        false -> write_anchor(StoreId, Path)
    end.

%% @private
anchor_verified(_StoreId, undefined) ->
    false;
anchor_verified(StoreId, Path) ->
    case file:read_file(Path) of
        {ok, Bin} -> anchor_bin_matches(StoreId, Bin);
        _ -> false
    end.

%% @private
anchor_bin_matches(StoreId, Bin) ->
    case safe_term(Bin) of
        #{version := ?ANCHOR_VERSION, store_id := StoreId, mac := Mac}
                when is_binary(Mac) ->
            anchor_mac_matches(StoreId, Mac);
        _ ->
            false
    end.

%% @private
safe_term(Bin) ->
    try binary_to_term(Bin, [safe]) of
        Term -> Term
    catch
        _:_ -> undefined
    end.

%% @private
anchor_mac_matches(StoreId, Mac) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            crypto:hash_equals(Mac, anchor_mac(Key, StoreId));
        _ ->
            false
    end.

%% @private
write_anchor(StoreId, Path) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            Bin = term_to_binary(#{version => ?ANCHOR_VERSION,
                                   store_id => StoreId,
                                   mac => anchor_mac(Key, StoreId)},
                                 [deterministic]),
            write_file_atomically(Path, Bin);
        _ ->
            ok
    end.

%% @private
write_file_atomically(Path, Bin) ->
    Temp = Path ++ ".tmp",
    case filelib:ensure_dir(Path) of
        ok -> write_and_rename(Temp, Path, Bin);
        {error, Reason} -> anchor_write_failed(Path, Reason)
    end.

%% @private
write_and_rename(Temp, Path, Bin) ->
    case file:write_file(Temp, Bin) of
        ok -> rename_anchor(Temp, Path);
        {error, Reason} -> anchor_write_failed(Path, Reason)
    end.

%% @private
rename_anchor(Temp, Path) ->
    case file:rename(Temp, Path) of
        ok -> ok;
        {error, Reason} -> anchor_write_failed(Path, Reason)
    end.

%% @private
anchor_write_failed(Path, Reason) ->
    logger:warning("integrity migration could not write its anchor ~ts: ~p",
                   [Path, Reason]),
    ok.

%% @private
-spec anchor_mac(binary(), atom()) -> binary().
anchor_mac(Key, StoreId) ->
    crypto:mac(hmac, sha256, Key,
               term_to_binary({?MODULE, anchor, ?ANCHOR_VERSION, StoreId},
                              [deterministic])).

%%====================================================================
%% Watermark helpers
%%====================================================================

%% @private
-spec newest_version(atom(), binary()) -> {ok, integer()} | {error, term()}.
newest_version(StoreId, StreamId) ->
    case reckon_db_streams:newest_event_version(StoreId, StreamId) of
        {ok, ?NO_STREAM} -> {ok, -1};
        {ok, Version} -> {ok, Version};
        {error, _} = Error -> Error
    end.

%% @private A fresh life token. Random, so a record sealed under one life
%% cannot be made to fit another.
-spec new_life() -> binary().
new_life() ->
    crypto:strong_rand_bytes(16).

%% @private The authenticated record: the version, the version scheme 2
%% begins at, the life token, and an HMAC over all of it under the store's
%% integrity key - store id and stream id included, so the record does not
%% fit another store or another life of the same stream.
-spec sealed_record(atom(), binary(), binary(), non_neg_integer(),
                    non_neg_integer()) -> map().
sealed_record(StoreId, StreamId, Life, Version, V2From) ->
    Key = reckon_db_integrity_key:get(StoreId),
    true = is_binary(Key),
    #{version => Version,
      v2_from => V2From,
      life => Life,
      mac => seal_mac(StoreId, Key, StreamId, Life, Version, V2From)}.

%% @private
seal_verifies(StoreId, StreamId,
              #{version := Version, v2_from := V2From, life := Life,
                mac := Mac})
        when is_integer(Version), is_integer(V2From), is_binary(Life),
             is_binary(Mac) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            crypto:hash_equals(
              Mac, seal_mac(StoreId, Key, StreamId, Life, Version, V2From));
        _ ->
            false
    end;
seal_verifies(_StoreId, _StreamId, _Record) ->
    false.

%% @private
-spec seal_mac(atom(), binary(), binary(), binary(), non_neg_integer(),
               non_neg_integer()) -> binary().
seal_mac(StoreId, Key, StreamId, Life, Version, V2From) ->
    crypto:mac(hmac, sha256, Key,
               term_to_binary({?MODULE, seal, StoreId, StreamId, Life,
                               Version, V2From}, [deterministic])).
