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
%% `[metadata, integrity, chain_start, StreamId]' (the macro
%% `?INTEGRITY_CHAIN_START_PATH' from `reckon_db.hrl', plus the stream ID).
%% The value is either a bare `non_neg_integer()', which is what releases
%% before 5.12.0 wrote and which anybody who can write the tree can write,
%% or a sealed record `#{version := V, mac := Mac}' authenticated under the
%% store's integrity key.
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
%% gater's integrity module and in `reckon_db_streams'.
%%
%% == The migration ==
%%
%% Releases before 5.12.0 wrote a bare integer. 5.12.0 refuses a bare
%% integer, because anybody who can write the tree can write one, so an
%% upgraded store seals what the earlier version wrote, once, at store
%% start (`migrate/1') - before the store serves anything, on a store with
%% an integrity key and no authenticated marker.
%%
%% The scan runs outside the tree. Every decision it makes is then
%% committed by ONE `khepri:transaction': each seal compare-and-swaps the
%% value the scan saw, and the authenticated done marker is written with
%% them. There is no in-progress record, so a crash leaves either a
%% migrated store or an untouched one - and nothing with a constant MAC
%% that a writer could replay to make the migration seal a claim nobody
%% made. A value that changed between the scan and the transaction aborts
%% the whole migration, and the next start runs it again.
%%
%% What the scan decides, per stream:
%%
%% <ul>
%% <li>Already sealed: nothing to do.</li>
%% <li>Bare watermark at the version the chain begins: seal it. That is
%% the value the earlier version wrote, and nothing below it carries a
%% hash.</li>
%% <li>Bare watermark over a chain-bearing event, or over a chain-bearing
%% stream at any version other than where its chain begins: refuse. A
%% genuine watermark sits exactly where integrity began, so one that is
%% anywhere else was moved to excuse events whose chain somebody
%% removed.</li>
%% <li>No watermark and no chain-bearing event: seal at `newest + 1'. The
%% whole stream predates integrity, or the earlier version crashed between
%% minting the watermark and appending the event that would have carried a
%% chain.</li>
%% <li>No watermark and a chain-bearing event: this is the state the
%% earlier version cannot produce - it minted the watermark before it
%% stamped a chain - so it is evidence that a watermark was deleted. How
%% the stream is treated depends on whether the store shows that evidence
%% anywhere (see below).</li>
%% </ul>
%%
%% == Deletion evidence ==
%%
%% A stream that carries a chain and has no authenticated watermark at the
%% version its chain begins is evidence that a watermark was deleted.
%%
%% Once one stream shows that evidence, the store is telling the truth
%% about nothing: for every OTHER stream, "no watermark over chain-less
%% events" is no longer the benign shape it is on an untouched store, it is
%% a stream whose watermark was deleted and whose events were stripped.
%% Sealing those would vouch for exactly the data the attacker rewrote. So:
%%
%% <ul>
%% <li>A chain-bearing stream with no watermark is rebuilt from its own
%% evidence: the lowest event whose `prev_event_hash' is the genesis value
%% AND whose MAC verifies is where the store began that chain - nothing
%% else can carry a genesis predecessor with a valid MAC - and the
%% watermark is sealed there. If no such event survives (the genesis event
%% itself was stripped or deleted), that stream is refused.</li>
%% <li>A chain-less stream with a missing or bare watermark is refused and
%% named. `migrate(StoreId, #{accept_missing => [StreamId]})' is the
%% operator act that vouches for the named streams and only those; it
%% reconstructs the same way and cannot excuse a hash-less event at or
%% above the chain start.</li>
%% </ul>
%%
%% On a store with no such evidence everything is sealed as above, and an
%% interrupted upgrade costs nothing: nothing was written.
%%
%% == The node-local anchor ==
%%
%% Sealed watermarks and the marker are all tree nodes, so a writer with
%% tree access can delete every one of them and leave a store that looks
%% like it was never migrated. When the store is given a `data_dir', the
%% migration also writes `<data_dir>/integrity_migration.done', an HMAC over
%% the store id, and any later start that verifies the marker but finds no
%% anchor writes one too, so every node in a cluster ends up with one. A
%% start that finds the anchor and no verified marker reports a violation
%% and seals nothing. What that does not cover: a data directory that is
%% wiped, and a node that never held the store. Both are named in the
%% threat model rather than hidden - see reckon-db-org/reckon-db#18.
%% @end
-module(reckon_db_chain_watermark).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([
    lookup/2,
    path/1,
    set_if_absent/3,
    delete/2,
    seal/2,
    seal_all/1,
    migrate/1,
    migrate/2
]).

-export_type([watermark/0]).

%% Events read per page while the migration scans a stream. The scan is one
%% pass over each stream at boot, and this is what keeps it from reading a
%% whole stream into memory in one call.
-define(SCAN_PAGE, 500).

%% The node-local anchor, beside the store's data directory.
-define(ANCHOR_FILE, "integrity_migration.done").
-define(ANCHOR_VERSION, 1).

%% What lookup/2 answers about a stream's chain start.
%%
%% `unsealed' is the shape a store written before the watermark was
%% authenticated has: a bare integer, which anybody who can write the tree can
%% set. It is not trusted to make an event legacy.
-type watermark() :: {sealed, non_neg_integer()} |
                     {unsealed, non_neg_integer()} |
                     undefined.

%% One stream as the migration scan found it.
-type scan() :: #{store_id := atom(),
                  stream_id := binary(),
                  watermark := watermark(),
                  newest := integer(),
                  chain_start := {ok, non_neg_integer()} | none,
                  genesis := {ok, non_neg_integer()} | none,
                  boundary := {ok, non_neg_integer()} | none,
                  error => term()}.

%% A decision the scan made, ready to commit.
-type plan() :: {seal, binary(), non_neg_integer(), term(), map()} |
                {refuse, binary(), term()} |
                skip.

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
    case khepri:get(StoreId, path(StreamId)) of
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
%% Never fails the store start: a store that cannot be migrated stays
%% fail-closed on the affected streams, reports them, and retries on its
%% next boot.
-spec migrate(atom()) -> ok.
migrate(StoreId) ->
    _ = migrate(StoreId, #{}),
    ok.

%% @doc The upgrade with options.
%%
%% Options:
%% <ul>
%% <li>`data_dir' - where to write the node-local anchor. Store start passes
%% it; without it no anchor is written or read.</li>
%% <li>`accept_missing' - the operator act. A list of stream ids the human
%% running it vouches for. It seals only those streams, and only when the
%% store's migration already ran; see `accept_missing/3'.</li>
%% <li>`operator' - an optional name recorded in the log line, so the log
%% says who vouched for them.</li>
%% </ul>
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

%% @doc Run the watermark migration, as store start does.
%%
%% Returns how many watermarks it sealed, zero on a store already migrated,
%% and `{error, migration_marker_missing}' on a store whose watermarks are
%% sealed but whose marker is absent or does not verify. It never re-seals on
%% that anomaly: re-sealing is precisely what a writer who stripped a stream
%% and deleted the marker would want.
-spec seal_all(atom()) -> {ok, non_neg_integer()} | {error, term()}.
seal_all(StoreId) ->
    migrate_at_start(StoreId, #{}).

%% @doc Seal one stream's bare-integer watermark.
%%
%% Rarely needed: `migrate/1' runs this shape automatically at store start.
%% It refuses once that migration has run, because a bare integer on a
%% migrated store is not a leftover from an earlier version, it is a forgery,
%% and this is exactly the call that would bless it.
-spec seal(atom(), binary()) -> ok | {error, term()}.
seal(StoreId, StreamId) ->
    case migration_state(StoreId, undefined) of
        migrated ->
            {error, already_migrated};
        unauthorised ->
            {error, migration_marker_missing};
        not_migrated ->
            seal_one(StoreId, StreamId)
    end.

%% @private
seal_one(StoreId, StreamId) ->
    case plan_for_one(StoreId, StreamId) of
        {seal, _, _, _, _} = Seal ->
            commit_seals(StoreId, [Seal]);
        {refuse, _, _} ->
            ok;
        skip ->
            ok
    end.

%% @private Plan a single stream outside the migration, for seal/2.
plan_for_one(StoreId, StreamId) ->
    case scan_stream(StoreId, StreamId) of
        #{error := Reason} ->
            {refuse, StreamId, {unreadable, Reason}};
        Scan ->
            plan_stream(Scan, deletion_evidence([Scan]))
    end.

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
            migration_violation(StoreId, AnchorPath),
            {error, migration_marker_missing};
        not_migrated ->
            run_migration(StoreId, AnchorPath)
    end.

%% @private What the store says about having been migrated.
%%
%% An authenticated marker is the whole answer. Without one, a store that has
%% already been migrated says so in two other places: the node-local anchor,
%% and its sealed watermarks. Either is a refusal to migrate again, not an
%% invitation to seal whatever bare integers are lying around: re-running
%% would let a writer strip a stream, delete the marker and be sealed back to
%% a clean read.
-spec migration_state(atom(), file:filename_all() | undefined) ->
    migrated | unauthorised | not_migrated.
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
            crypto:hash_equals(Mac, migration_mac(StoreId, Version, Refused));
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
    Evidence = deletion_evidence(Scans),
    Plans = [plan_stream(Scan, Evidence) || Scan <- Scans],
    Refused = [{StreamId, Why} || {refuse, StreamId, Why} <- Plans],
    case commit_plans(StoreId, Plans, Refused) of
        committed ->
            _ = ensure_anchor(StoreId, AnchorPath),
            report_migration(StoreId, length(StreamIds), Plans, Evidence),
            {ok, length([S || {seal, _, _, _, _} = S <- Plans])};
        {error, Reason} ->
            migration_failed(StoreId, Reason),
            {error, Reason}
    end.

%% @private Whether any stream shows that a watermark was deleted: it carries
%% a chain, and it has no authenticated watermark at the version its chain
%% begins. Nothing else in the tree can put a stream in that state, because
%% the earlier version minted the watermark before it stamped the chain.
-spec deletion_evidence([scan()]) -> boolean().
deletion_evidence(Scans) ->
    lists:any(fun shows_deletion/1, Scans).

%% @private
shows_deletion(#{chain_start := {ok, ChainStart}} = Scan) ->
    case maps:get(watermark, Scan) of
        {sealed, _} -> false;
        {unsealed, ChainStart} -> false;
        _ -> true
    end;
shows_deletion(_Scan) ->
    false.

%% @private
-spec plan_stream(scan(), boolean()) -> plan().
plan_stream(#{error := Reason} = Scan, _Evidence) ->
    {refuse, maps:get(stream_id, Scan), {unreadable, Reason}};
plan_stream(#{watermark := {sealed, _}}, _Evidence) ->
    skip;
plan_stream(#{chain_start := none} = Scan, Evidence) ->
    plan_chainless(Scan, Evidence);
plan_stream(#{chain_start := {ok, ChainStart}} = Scan, Evidence) ->
    plan_chain_bearing(Scan, ChainStart, Evidence).

%% @private A stream that carries no chain at all. Its watermark cannot be
%% checked against anything, so it is sealed on the shape alone - unless the
%% store shows that watermarks have been deleted, in which case a chain-less
%% stream with no authenticated watermark is refused rather than vouched for.
plan_chainless(#{watermark := Watermark, newest := Newest} = Scan, Evidence) ->
    StreamId = maps:get(stream_id, Scan),
    case {Watermark, Evidence} of
        {undefined, true} -> {refuse, StreamId, needs_accept_missing};
        {{unsealed, _}, true} -> {refuse, StreamId, needs_accept_missing};
        {undefined, false} -> seal_plan(Scan, Newest + 1, undefined);
        {{unsealed, Version}, false} -> plan_bare_chainless(Scan, Version, Newest);
        {_, _} -> skip
    end.

%% @private A bare watermark over a chain-less stream. At or below `newest +
%% 1' is the shape the earlier version wrote. ABOVE it is refused: the one
%% way it arises honestly needs a crash between the earlier version's mint
%% and its append, and then a scavenge, and a chain-less tail records no
%% MAC'd boundary that could account for the gap - so a bare number above
%% the stream is a claim nobody can check, and the operator act is the way
%% back for the honest store that does hit it.
plan_bare_chainless(Scan, Version, Newest) when Version =< Newest + 1 ->
    seal_plan(Scan, Version, Version);
plan_bare_chainless(Scan, Version, _Newest) ->
    {refuse, maps:get(stream_id, Scan), {above_newest, Version}}.

%% @private A stream that carries a chain is sealed at the version the store
%% can PROVE the chain began, never at the value it happens to hold. The proof
%% is the event that only a store can write at a chain start: its predecessor
%% is the genesis value and its MAC verifies under the store's key. A bare
%% watermark is not proof of anything - delete the chain fields off the first
%% three chain-bearing events and move the bare watermark up three, and the
%% new "first chain-bearing event" sits at a version nobody can account for,
%% which is why sealing at whatever value is on the node is the attack.
plan_chain_bearing(Scan, ChainStart, _Evidence) ->
    case maps:get(genesis, Scan) of
        {ok, ChainStart} ->
            seal_plan(Scan, ChainStart, raw_watermark(Scan));
        {ok, Other} ->
            {refuse, maps:get(stream_id, Scan),
             {genesis_not_at_chain_start, Other, ChainStart}};
        none ->
            plan_unproven_chain_start(Scan, ChainStart)
    end.

%% @private The chain-start event does not verify: either somebody stripped
%% it, or a scavenge removed it. A verified scavenge boundary is the one
%% thing that can tell those apart - only a scavenge can write one - and it
%% answers only when it sits exactly where the surviving chain begins, which
%% is the shape a scavenge leaves when it cuts through the chain start. Any
%% other version is refused: with the marker gone, a stream the store cannot
%% account for is exactly what an attacker who stripped a prefix leaves.
plan_unproven_chain_start(#{boundary := {ok, ChainStart}} = Scan, ChainStart) ->
    seal_plan(Scan, ChainStart, raw_watermark(Scan));
plan_unproven_chain_start(Scan, ChainStart) ->
    {refuse, maps:get(stream_id, Scan), {chain_start_unproven, ChainStart}}.

%% @private
seal_plan(Scan, Version, Expected) ->
    StoreId = maps:get(store_id, Scan),
    StreamId = maps:get(stream_id, Scan),
    {seal, StreamId, Version, Expected, sealed_record(StoreId, StreamId, Version)}.

%%====================================================================
%% Committing
%%====================================================================

%% @private Commit every plan in one transaction: seals compare-and-swapped,
%% marker written with them, refused streams named in its payload.
commit_plans(StoreId, Plans, Refused) ->
    Seals = [Seal || {seal, _, _, _, _} = Seal <- Plans],
    Marker = marker_record(StoreId, [StreamId || {StreamId, _Why} <- Refused]),
    Fun = fun() -> migration_tx(Seals, Marker) end,
    case khepri:transaction(StoreId, Fun) of
        {ok, committed} -> committed;
        {ok, already_migrated} -> committed;
        %% Khepri 0.17.x wraps a process_command error as {ok, Err} when Ra
        %% is not ready yet (the same shape the DCB write path unwraps).
        {ok, {error, Reason}} -> {error, Reason};
        {error, Reason} -> {error, Reason}
    end.

%% @private The migration transaction. Pure khepri_tx operations only:
%% everything it writes was computed outside. It is the whole migration -
%% either every seal and the marker land together, or the transaction aborts
%% and the next start runs the migration again from scratch.
migration_tx(Seals, Marker) ->
    case marker_present() of
        true ->
            already_migrated;
        false ->
            lists:foreach(fun cas_seal/1, Seals),
            ok = khepri_tx:put(?INTEGRITY_MIGRATION_PATH, Marker),
            committed
    end.

%% @private
marker_present() ->
    case khepri_tx:get(?INTEGRITY_MIGRATION_PATH) of
        {ok, _} -> true;
        _ -> false
    end.

%% @private Seal one watermark only if it still holds the value the scan
%% saw. Anything else means another writer (or another node's migration)
%% moved it while this one was scanning, and sealing a claim nobody made is
%% exactly what this refuses to do.
cas_seal({seal, StreamId, _Version, Expected, Record}) ->
    Path = ?INTEGRITY_CHAIN_START_PATH ++ [StreamId],
    case current_raw(Path) of
        Expected ->
            ok = khepri_tx:put(Path, Record);
        Other ->
            khepri_tx:abort({watermark_changed, StreamId, Expected, Other})
    end.

%% @private What the path holds right now, with an absent node and a node
%% with no data both reported as undefined - which is what the scan recorded
%% for a stream it found no watermark on.
current_raw(Path) ->
    case khepri_tx:get(Path) of
        {ok, Value} -> Value;
        _ -> undefined
    end.

%% @private Seal without a marker, for seal/2. Same compare-and-swap, so a
%% value that moved is refused rather than blessed.
commit_seals(StoreId, Seals) ->
    Fun = fun() -> tx_seals(Seals) end,
    case khepri:transaction(StoreId, Fun) of
        {ok, committed} -> ok;
        {ok, {error, Reason}} -> {error, Reason};
        {error, Reason} -> {error, Reason}
    end.

%% @private
tx_seals(Seals) ->
    lists:foreach(fun cas_seal/1, Seals),
    committed.

%% @private Write the authenticated migration marker. Its payload names the
%% streams the migration refused, so a later operator act knows what was
%% left behind without re-deriving it.
marker_record(StoreId, Refused) ->
    Sorted = lists:usort(Refused),
    #{version => ?INTEGRITY_MIGRATION_VERSION,
      refused => Sorted,
      mac => migration_mac(StoreId, ?INTEGRITY_MIGRATION_VERSION, Sorted)}.

%% @private
-spec migration_mac(atom(), non_neg_integer(), list()) -> binary().
migration_mac(StoreId, Version, Refused) ->
    Key = reckon_db_integrity_key:get(StoreId),
    true = is_binary(Key),
    crypto:mac(hmac, sha256, Key,
               term_to_binary({?MODULE, migration, Version, Refused},
                              [deterministic])).

%% @private
migration_failed(StoreId, Reason) ->
    logger:error("integrity migration on store ~p did not finish: ~p; the "
                 "affected streams stay refused and the next start retries",
                 [StoreId, Reason]),
    ok.

%% @private
migration_violation(StoreId, AnchorPath) ->
    logger:error("store ~p says it was migrated (marker: no; sealed "
                 "watermarks or a node-local anchor: yes) but has no "
                 "authenticated migration marker; refusing to seal anything "
                 "(anchor: ~ts)", [StoreId, anchor_label(AnchorPath)]),
    telemetry:execute([reckon, db, integrity, migration_marker_missing],
                      #{system_time => erlang:system_time(millisecond)},
                      #{store_id => StoreId}),
    ok.

%% @private
anchor_label(undefined) -> "not used";
anchor_label(Path) -> Path.

%% @private
report_migration(StoreId, StreamCount, Plans, Evidence) ->
    Sealed = length([S || {seal, _, _, _, _} = S <- Plans]),
    Refused = [{StreamId, Why} || {refuse, StreamId, Why} <- Plans],
    logger:info("integrity migration on store ~p: sealed ~b watermark(s), "
                "refused ~b of ~b stream(s), deletion evidence ~p",
                [StoreId, Sealed, length(Refused), StreamCount, Evidence]),
    telemetry:execute([reckon, db, integrity, migrated],
                      #{system_time => erlang:system_time(millisecond)},
                      #{store_id => StoreId, sealed => Sealed,
                        refused => length(Refused),
                        streams => StreamCount,
                        deletion_evidence => Evidence}),
    lists:foreach(fun(RefusedStream) -> report_refused(StoreId, RefusedStream) end,
                  Refused).

%% @private
report_refused(StoreId, {StreamId, Why}) ->
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
%% only those. The streams are named one by one, never a store-wide yes, and
%% the log line says who ran it and when.
%%
%% It recomputes each stream's chain start from the stream's own evidence,
%% so it cannot launder a stripped stream: a hash-less event at or above the
%% version it seals at still fails `missing_integrity'.
accept_missing(StoreId, StreamIds, Opts) ->
    case read_marker(StoreId) of
        {ok, Refused} ->
            accept_refused(StoreId, StreamIds, Refused, Opts);
        none ->
            {error, migration_marker_missing}
    end.

%% @private
accept_refused(StoreId, StreamIds, Refused, Opts) ->
    case [S || S <- StreamIds, not lists:member(S, Refused)] of
        [] -> accept_scanned(StoreId, StreamIds, Refused, Opts);
        Unknown -> {error, {not_refused, Unknown}}
    end.

%% @private
accept_scanned(StoreId, StreamIds, Refused, Opts) ->
    Scans = [{StreamId, scan_stream(StoreId, StreamId)} || StreamId <- StreamIds],
    case plans_for_accepted(Scans) of
        {ok, Plans} ->
            commit_accept(StoreId, StreamIds, Refused, Plans, Opts);
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Under the operator act, a stream's chain start comes from its own
%% evidence: the genesis-prev event with a valid MAC where one survives, and
%% `newest + 1' for a stream that carries no chain at all (the operator is
%% vouching that the whole stream predates integrity).
plans_for_accepted(Scans) ->
    plans_for_accepted(Scans, []).

%% @private
plans_for_accepted([], Acc) ->
    {ok, lists:reverse(Acc)};
plans_for_accepted([{_StreamId, #{error := Reason}} | _Rest], _Acc) ->
    {error, {unreadable, Reason}};
plans_for_accepted([{StreamId, #{chain_start := {ok, ChainStart}} = Scan} | Rest], Acc) ->
    case maps:get(genesis, Scan) of
        {ok, Version} ->
            plans_for_accepted(Rest, [seal_plan(Scan, Version, raw_watermark(Scan)) | Acc]);
        none ->
            accept_unproven(Scan, StreamId, ChainStart, Rest, Acc)
    end;
plans_for_accepted([{_StreamId, #{chain_start := none} = Scan} | Rest], Acc) ->
    Version = maps:get(newest, Scan) + 1,
    plans_for_accepted(Rest, [seal_plan(Scan, Version, raw_watermark(Scan)) | Acc]).

%% @private The operator act vouches for a stream the migration could not
%% prove, but only where an authenticated scavenge boundary already accounts
%% for the gap: the human is vouching for the data, not for the cut.
accept_unproven(#{boundary := {ok, ChainStart}} = Scan, _StreamId, ChainStart, Rest, Acc) ->
    plans_for_accepted(Rest, [seal_plan(Scan, ChainStart, raw_watermark(Scan)) | Acc]);
accept_unproven(_Scan, StreamId, ChainStart, _Rest, _Acc) ->
    {error, {cannot_vouch, StreamId, {chain_start_unproven, ChainStart}}}.

%% @private The raw value the transaction compares against: what the scan
%% found at the watermark path, or undefined when it found nothing.
raw_watermark(#{watermark := {unsealed, Version}}) -> Version;
raw_watermark(#{watermark := undefined}) -> undefined;
raw_watermark(#{watermark := {sealed, Version}}) -> {sealed, Version}.

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
    lists:foreach(fun cas_seal/1, Plans),
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

%% @private Everything the migration needs to know about one stream, read
%% outside the tree: its watermark, its newest version, where its chain
%% begins, and whether that chain-start event is one only the store could
%% have written.
-spec scan_stream(atom(), binary()) -> scan().
scan_stream(StoreId, StreamId) ->
    {ok, Watermark} = lookup(StoreId, StreamId),
    Base = #{store_id => StoreId,
             stream_id => StreamId,
             watermark => Watermark,
             newest => -1,
             chain_start => none,
             genesis => none,
             boundary => boundary_version(StoreId, StreamId)},
    case newest_version(StoreId, StreamId) of
        {ok, Newest} ->
            scan_versions(StoreId, Base#{newest => Newest});
        {error, Reason} ->
            Base#{error => Reason}
    end.

%% @private
scan_versions(StoreId, #{stream_id := StreamId, newest := Newest} = Scan) ->
    case lowest_chain_event(StoreId, StreamId, 0, Newest) of
        {ok, none} ->
            Scan;
        {ok, Event} ->
            Scan#{chain_start => {ok, Event#event.version},
                  genesis => genesis_version(StoreId, Event)};
        {error, Reason} ->
            Scan#{error => Reason}
    end.

%% @private Whether this event is one only the store could have written at a
%% chain start: its predecessor is the genesis value, and its MAC verifies
%% under the store's key. A copied or moved event fails the MAC, because the
%% MAC covers the stream and the version it was written at.
genesis_version(StoreId, Event) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) -> verify_genesis(Event, Key);
        _ -> none
    end.

%% @private The version a verified scavenge boundary names, if the store has
%% one for this stream. Only a scavenge can write one: it is MAC'd under the
%% store's key over the stream, the version and the hash it cut at.
boundary_version(StoreId, StreamId) ->
    case reckon_db_scavenge_boundary:lookup(StoreId, StreamId) of
        {ok, Boundary} when is_map(Boundary) -> vetted_boundary(StoreId, StreamId, Boundary);
        _ -> none
    end.

%% @private
vetted_boundary(StoreId, StreamId, Boundary) ->
    case reckon_db_scavenge_boundary:verify(StoreId, StreamId, Boundary) of
        true -> {ok, maps:get(version, Boundary)};
        false -> none
    end.

%% @private
verify_genesis(Event, Key) ->
    case reckon_gater_integrity:verify_event(
           Event, reckon_gater_integrity:genesis_prev_hash(), Key) of
        ok -> {ok, Event#event.version};
        _ -> none
    end.

%% @private The lowest version whose event carries a chain, found in pages
%% and stopped at the first hit: one call per stream over its whole range is
%% what this replaces.
-spec lowest_chain_event(atom(), binary(), non_neg_integer(), integer()) ->
    {ok, event() | none} | {error, term()}.
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
        [#event{} = First | _] ->
            {ok, First};
        [] ->
            lowest_chain_event(StoreId, StreamId, From + Count, To)
    end.

%% @private Read a page of a stream, unverified: this is metadata for the
%% migration, not a read the store serves.
-spec read_page(atom(), binary(), non_neg_integer(), pos_integer()) ->
    {ok, [event()]} | {error, term()}.
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

%% @private Write the anchor when it is missing or does not verify: any start
%% that verifies the in-tree marker gets one, so every node in a cluster ends
%% up with its own.
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

%% @private An anchor that cannot be written never fails the store start: it
%% is what bounds a tree-only writer, not what makes the store work.
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

%% @doc The tree path a stream's watermark lives at.
-spec path(binary()) -> [atom() | binary()].
path(StreamId) ->
    ?INTEGRITY_CHAIN_START_PATH ++ [StreamId].

%% @doc Delete a stream's watermark.
%%
%% Called when deleting the entire stream, in the same transaction: the
%% watermark is part of the stream's metadata and must not outlive it, or a
%% stream re-created under the same id inherits a chain start it never had.
-spec delete(StoreId :: atom(), StreamId :: binary()) -> ok.
delete(StoreId, StreamId) ->
    _ = khepri:delete(StoreId, path(StreamId)),
    ok.

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
