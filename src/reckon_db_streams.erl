%% @doc Streams API facade for reckon-db
%%
%% Provides the public API for stream operations:
%% - append: Write events to a stream with optimistic concurrency
%% - read: Read events from a stream
%% - get_version: Get current stream version
%% - exists: Check if stream exists
%% - list_streams: List all streams in the store
%%
%% @author rgfaber

-module(reckon_db_streams).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").
-include_lib("khepri/include/khepri.hrl").

%% API
-export([
    append/4,
    append/5,
    append_if_no_tag_matches/4,
    read/5,
    read/6,
    read_all/4,
    fold_event_batches/6,
    read_all_global/3,
    read_by_event_types/3,
    read_by_tags/4,
    read_by_metadata/3,
    get_version/2,
    newest_event_version/2,
    ensure_version_counter/3,
    live_event_count/2,
    exists/2,
    has_events/1,
    list_streams/1,
    global_event_count/1,
    delete/2
]).

%% Internal exports for workers
-export([
    do_append/4,
    do_read/5,
    ensure_cache_table/0
]).

%%====================================================================
%% Types
%%====================================================================

-type new_event() :: #{
    event_type := binary(),
    data := map() | binary(),
    metadata => map(),
    tags => [binary()],
    event_id => binary()
}.

-type direction() :: forward | backward.

-export_type([new_event/0, direction/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Append events to a stream with expected version check
%%
%% Expected version semantics:
%%   -1 (NO_STREAM)    - Stream must not exist (first write)
%%   -2 (ANY_VERSION)  - Land after whatever is already there
%%   N >= 0            - Stream version must equal N
%%
%% Returns {ok, NewVersion} on success or {error, Reason} on failure.
%%
%% An append never writes over an event that is already stored. Two appends
%% racing for the same version no longer lose one of them: the loser re-reads
%% the version and tries again with a growing, capped delay, and answers
%% {error, {version_conflict, ExpectedVersion, CurrentVersion}} when it has
%% tried 60 times. An explicit expected version answers
%% {error, {wrong_expected_version, Expected, Current}} when another writer
%% got there first. An append of no events writes nothing and answers the
%% version it read.
%%
%% The version comes from a per-stream counter kept in the same Ra command as
%% the events (5.12.0), so it survives a scavenge: deleting old versions does
%% not move it, and an append after a scavenge lands after the newest event
%% instead of writing over it. Before 5.12.0 it was a count of the live
%% version nodes, which a scavenge made wrong; 5.11.13 refused the append in
%% that state rather than overwrite, and this release makes the version exact
%% again. A counter that a 5.11.x writer left behind - a rollback - is
%% reconciled against the newest version node by the append that finds it.
%%
%% The rejected append answers with the version the last attempt read.
%%
%% What a gateway caller sees of version_conflict depends on the gater it
%% runs: 3.11.x recognises neither error, reads an unknown one as transient,
%% and burns ten retries - 81 to 89 seconds measured - before answering
%% {error, {retries_exhausted, Reason}}. A gater with a clause per error
%% returns it on the first call; that change is written but UNRELEASED
%% (reckon-db-org/evoq#22), not a release to depend on. reckon-evoq and evoq
%% do not recognise it either: it reaches the caller as an error and never
%% triggers a rebuild.
-spec append(atom(), binary(), integer(), [new_event()]) ->
    {ok, non_neg_integer()} | {error, term()}.
append(StoreId, StreamId, ExpectedVersion, Events) ->
    append(StoreId, StreamId, ExpectedVersion, Events, #{}).

%% @doc Conditionally append events under the DCB pseudo-stream
%% (Dynamic Consistency Boundary, Phase 3, 2.4.0+).
%%
%% Unlike append/4,5, the precondition is NOT a stream-version check;
%% it is a tag-filter context query. Returns
%% {error, {context_changed, MaxSeq}} when any event matching
%% TagFilter has seq above SeqCutoff.
%%
%% v1 refuses on stores with integrity enabled (DCB v1 lacks HMAC
%% chain). Returns {error, integrity_not_supported_in_dcb_v1} in
%% that case.
%%
%% See: plans/PLAN_DCB_IMPLEMENTATION.md
%%
%% NOTE: This facade calls reckon_db_dcb directly. P3.4 will route via
%% reckon_db_gateway_worker for transport-layer consistency with
%% append/4,5.
-spec append_if_no_tag_matches(
    StoreId   :: atom(),
    TagFilter :: tag_filter(),
    SeqCutoff :: seq_cutoff(),
    Events    :: [reckon_db_log_backend:new_event()]
) ->
      {ok, LastSeq :: non_neg_integer()}
    | {error, {context_changed, non_neg_integer()}}
    | {error, no_events}
    | {error, integrity_not_supported_in_dcb_v1}
    | {error, term()}.
append_if_no_tag_matches(StoreId, TagFilter, SeqCutoff, Events) ->
    reckon_db_dcb:append_if_no_tag_matches(StoreId, TagFilter, SeqCutoff, Events).

-spec append(atom(), binary(), integer(), [new_event()], map()) ->
    {ok, non_neg_integer()} | {error, term()}.
append(StoreId, StreamId, ExpectedVersion, Events, _Opts) ->
    %% Stream-id format gate. Rejecting at the head of append/4
    %% means no malformed id reaches Khepri, so the store can't
    %% accumulate polluted paths from misbehaving tests / clients.
    %% See reckon_gater_stream_id for the format rules (moved out
    %% of reckon-db in 3.0.0 — protocol contract belongs in the
    %% gateway layer, shared with reckon-evoq).
    case reckon_gater_stream_id:validate(StreamId) of
        ok ->
            do_append_with_telemetry(StoreId, StreamId, ExpectedVersion, Events);
        {error, Reason} ->
            {error, {invalid_stream_id, Reason, StreamId}}
    end.

%% @private validation-gated wrapper around do_append that also
%% emits the write-lifecycle telemetry.
do_append_with_telemetry(StoreId, StreamId, ExpectedVersion, Events) ->
    StartTime = erlang:monotonic_time(),

    %% Emit start telemetry
    telemetry:execute(
        ?STREAM_WRITE_START,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, stream_id => StreamId,
          event_count => length(Events), expected_version => ExpectedVersion}
    ),

    Result = do_append(StoreId, StreamId, ExpectedVersion, Events),

    Duration = erlang:monotonic_time() - StartTime,

    %% Emit stop/error telemetry
    case Result of
        {ok, NewVersion} ->
            telemetry:execute(
                ?STREAM_WRITE_STOP,
                #{duration => Duration, event_count => length(Events)},
                #{store_id => StoreId, stream_id => StreamId, new_version => NewVersion}
            ),
            Result;
        {error, Reason} ->
            telemetry:execute(
                ?STREAM_WRITE_ERROR,
                #{duration => Duration},
                #{store_id => StoreId, stream_id => StreamId, reason => Reason}
            ),
            Result
    end.

%% @doc Read events from a stream
%%
%% Parameters:
%%   StoreId      - The store identifier
%%   StreamId     - The stream identifier
%%   StartVersion - Starting version (0-based)
%%   Count        - Maximum number of events to read
%%   Direction    - forward or backward
%%
%% Returns {ok, [Event]} or {error, Reason}
-spec read(atom(), binary(), non_neg_integer(), pos_integer(), direction()) ->
    {ok, [event()]} | {error, term()}.
read(StoreId, StreamId, StartVersion, Count, Direction) ->
    read(StoreId, StreamId, StartVersion, Count, Direction, #{}).

%% @doc Read events from a stream with explicit options.
%%
%% Currently supported options:
%%
%%   `verify' :: `skip_legacy' | `strict' | `skip_all'
%%       Tamper-resistance enforcement mode. Default: `skip_legacy'.
%%       - `skip_legacy' (default): events with version below the
%%         per-stream chain_start watermark are returned untouched
%%         (legacy data); events at or above the watermark are
%%         verified strictly and an integrity_violation is returned
%%         on any failure.
%%       - `strict': every event must carry integrity fields and
%%         verify; legacy events surface as missing_integrity.
%%       - `skip_all': no verification (dangerous; intended for
%%         migration tooling only).
%%
%% Backward-direction reads always bypass chain verification in 2.1.0;
%% the MAC alone could still be checked but is not in this release.
%% Forward reads receive full chain + MAC verification.
-type verify_mode() :: skip_legacy | strict | skip_all.
-type read_opts() :: #{verify => verify_mode()}.

-spec read(
    atom(), binary(), non_neg_integer(), pos_integer(), direction(), read_opts()
) -> {ok, [event()]} | {error, term()}.
read(StoreId, StreamId, StartVersion, Count, Direction, Opts) ->
    %% Stream-id format gate, the read-side twin of append/5's. A
    %% malformed id is answered here rather than raised from a path
    %% builder deep inside a gateway worker, where the raise killed the
    %% worker and the retry layer re-dispatched the same hopeless read
    %% until the caller's own timeout (reckon-db#4).
    case reckon_db_stream_path:validate(StreamId) of
        ok -> read_validated(StoreId, StreamId, StartVersion, Count, Direction, Opts);
        {error, _} = Invalid -> Invalid
    end.

%% @private The read itself, past the stream-id gate.
read_validated(StoreId, StreamId, StartVersion, Count, Direction, Opts) ->
    StartTime = erlang:monotonic_time(),

    %% Emit start telemetry
    telemetry:execute(
        ?STREAM_READ_START,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, stream_id => StreamId,
          start_version => StartVersion, count => Count, direction => Direction}
    ),

    Result = do_read_with_verify(
        StoreId, StreamId, StartVersion, Count, Direction, Opts),

    Duration = erlang:monotonic_time() - StartTime,

    %% Emit stop telemetry
    case Result of
        {ok, Events} ->
            telemetry:execute(
                ?STREAM_READ_STOP,
                #{duration => Duration, event_count => length(Events)},
                #{store_id => StoreId, stream_id => StreamId}
            ),
            Result;
        Error ->
            Error
    end.

%% @doc Read a batch of a stream's live events, from the oldest or newest end.
%%
%% `read/5' is positional: it asks for `StartVersion' and the `Count'
%% positions above it, and answers the ones that hold an event. That is right
%% for a caller holding a cursor and wrong for this one, because a scavenge
%% removes a prefix of a stream without renumbering the rest. Asking from 0
%% for a stream whose first thirteen versions were scavenged answers `[]'
%% over a stream that holds three events, and asking backwards from 0 answers
%% at most version 0 - the newest events, which is what a backward caller
%% wants, were never in the window.
%%
%% So this starts at the live end: the oldest version for `forward', the
%% newest for `backward'. The first read is the same positional window this
%% always did, and answers in full for a stream that holds at least
%% `BatchSize' live events from that end; only when it comes back short does
%% this ask the tree which versions are live and read from the first of them.
-spec read_all(atom(), binary(), pos_integer(), direction()) ->
    {ok, [event()]} | {error, term()}.
read_all(StoreId, StreamId, BatchSize, Direction) ->
    %% The same stream-id gate as read/5, and before the backward direction
    %% builds a counter path from the id.
    case reckon_db_stream_path:validate(StreamId) of
        ok -> read_live(StoreId, StreamId, BatchSize, Direction);
        {error, _} = Invalid -> Invalid
    end.

%% @private
read_live(StoreId, StreamId, BatchSize, forward) ->
    shortened_window(StoreId, StreamId, BatchSize, forward,
                     read(StoreId, StreamId, 0, BatchSize, forward));
read_live(StoreId, StreamId, BatchSize, backward) ->
    case newest_event_version(StoreId, StreamId) of
        {ok, ?NO_STREAM} ->
            {ok, []};
        {ok, Newest} ->
            shortened_window(StoreId, StreamId, BatchSize, backward,
                             read(StoreId, StreamId, Newest, BatchSize, backward));
        {error, _} = Error ->
            Error
    end.

%% @private The window was not filled from the live end, so the stream is
%% either shorter than the batch or hollow at that end: read again from the
%% oldest (or newest) version the tree actually holds.
shortened_window(_StoreId, _StreamId, BatchSize, _Direction, {ok, Events})
        when length(Events) =:= BatchSize ->
    {ok, Events};
shortened_window(StoreId, StreamId, BatchSize, forward, {ok, _Short}) ->
    from_the_live_end(StoreId, StreamId, BatchSize, forward, fun lists:min/1);
shortened_window(StoreId, StreamId, BatchSize, backward, {ok, _Short}) ->
    from_the_live_end(StoreId, StreamId, BatchSize, backward, fun lists:max/1);
shortened_window(_StoreId, _StreamId, _BatchSize, _Direction, {error, _} = Error) ->
    Error.

%% @private
from_the_live_end(StoreId, StreamId, BatchSize, Direction, End) ->
    case version_nodes(StoreId, StreamId, #{favor => consistency}) of
        {ok, []} ->
            {ok, []};
        {ok, Versions} ->
            read(StoreId, StreamId, End(Versions), BatchSize, Direction);
        {error, _} = Error ->
            Error
    end.

%% @doc Fold a function over a stream's live events, one batch at a time.
%%
%% For the callers that want every event rather than a page: a bulk backfill
%% or an operator report. One batch is held at a time, so a stream of any size
%% costs one `BatchSize' read rather than a list of everything, and the cursor
%% is a version: each batch after the first starts at the version one step
%% past the previous one's far end.
%%
%% Where it stops is the newest version the stream had when the fold started,
%% not the first batch that came back short. A scavenge deletes a prefix
%% without renumbering, and it can run while this does: the fold reads
%% 0..9999 in full, a scavenge removes 0..15000, and the next window answers
%% 15001..19999 - a short batch over a stream that goes on to 25000. Ending
%% there would hand back a partial answer that looks complete, which for a
%% link backfill is worse than not running at all. So the cursor is compared
%% against the newest version, an empty window inside the stream is read as a
%% hole and jumped, and only a cursor past the end stops the fold.
%%
%% Events appended after the fold started are not visited: this is a pass over
%% the stream as it was, and a caller that needs those wants a subscription.
%%
%% Direction is the order the batches are visited in, and each batch is
%% returned in that direction too, as `read/5' returns it.
-spec fold_event_batches(
    atom(), binary(), pos_integer(), direction(),
    fun(([event()], Acc) -> Acc), Acc) -> {ok, Acc} | {error, term()} when Acc :: term().
fold_event_batches(StoreId, StreamId, BatchSize, Direction, Fun, Acc0) ->
    case newest_event_version(StoreId, StreamId) of
        {ok, Newest} when Newest < 0 ->
            {ok, Acc0};
        {ok, Newest} ->
            fold_from(StoreId, StreamId, BatchSize, Direction, Newest, Fun,
                      Acc0, live_end);
        {error, _} = Error ->
            Error
    end.

%% @private The first batch comes from the live end, so a stream whose prefix
%% was scavenged before the fold started begins at the first event still there.
%% After that the cursor is a version, and the fold goes on while it is still
%% inside the stream.
fold_from(StoreId, StreamId, BatchSize, Direction, Newest, Fun, Acc, live_end) ->
    %% The first batch is the one read_all/4 answers, and it is not bounded by
    %% Newest: an append can land between the newest-version read above and
    %% this one. Every batch the fold visits stays at or below Newest, the
    %% number it was given, so a caller fencing on that number sees the same
    %% stream it was told about.
    fold_batch(StoreId, StreamId, BatchSize, Direction, Newest, Fun, Acc, live_end,
               bounded(read_all(StoreId, StreamId, BatchSize, Direction),
                       Newest, Direction));
fold_from(StoreId, StreamId, BatchSize, Direction, Newest, Fun, Acc, {at, Start}) ->
    case inside_the_stream(Start, Newest, Direction) of
        true ->
            fold_batch(StoreId, StreamId, BatchSize, Direction, Newest, Fun, Acc,
                       {at, Start},
                       read(StoreId, StreamId, Start, BatchSize, Direction));
        false ->
            {ok, Acc}
    end;
fold_from(StoreId, StreamId, BatchSize, Direction, Newest, Fun, Acc,
          {consistent, Start}) ->
    case inside_the_stream(Start, Newest, Direction) of
        true ->
            fold_batch(StoreId, StreamId, BatchSize, Direction, Newest, Fun, Acc,
                       {at, Start},
                       read_consistent(StoreId, StreamId, Start, BatchSize,
                                       Direction));
        false ->
            {ok, Acc}
    end.

%% @private
fold_batch(StoreId, StreamId, BatchSize, Direction, Newest, Fun, Acc, _Cursor,
           {ok, [_ | _] = Events}) ->
    fold_from(StoreId, StreamId, BatchSize, Direction, Newest, Fun,
              Fun(Events, Acc), {at, next_start(Events, Direction)});
fold_batch(_StoreId, _StreamId, _BatchSize, _Direction, _Newest, _Fun, Acc,
           live_end, {ok, []}) ->
    %% No event anywhere: there is nothing to fold.
    {ok, Acc};
fold_batch(StoreId, StreamId, BatchSize, Direction, Newest, Fun, Acc,
           {at, Start}, {ok, []}) ->
    %% Still inside the stream and the window holds nothing: a scavenge took
    %% these versions while the fold was running. Ask the tree where the live
    %% events are now and jump there.
    reanchor(StoreId, StreamId, BatchSize, Direction, Newest, Fun, Acc,
             next_live_version(StoreId, StreamId, Direction, Start));
fold_batch(_StoreId, _StreamId, _BatchSize, _Direction, _Newest, _Fun, _Acc,
           _Cursor, {error, _} = Error) ->
    Error.

%% @private
reanchor(_StoreId, _StreamId, _BatchSize, _Direction, _Newest, _Fun, Acc, none) ->
    {ok, Acc};
reanchor(StoreId, StreamId, BatchSize, Direction, Newest, Fun, Acc, {ok, Start}) ->
    fold_from(StoreId, StreamId, BatchSize, Direction, Newest, Fun, Acc,
              {consistent, Start});
reanchor(_StoreId, _StreamId, _BatchSize, _Direction, _Newest, _Fun, _Acc,
         {error, _} = Error) ->
    Error.

%% @private A window read asked of the leader rather than this node.
%%
%% The fold only pays this after a re-anchor: the tree said the version is
%% live, and the local read said the window was empty. On a follower that has
%% not caught up those two answers disagree, and re-anchoring again would land
%% on the same version for ever. One consistent read settles it.
read_consistent(StoreId, StreamId, Start, Count, Direction) ->
    {ok, Events} = read_events(StoreId, StreamId, Start, Count, Direction,
                               read_options(consistent)),
    verify_read_result({ok, Events}, StoreId, StreamId, Start, Direction, #{}).

%% @private Whether a cursor is still inside the stream, in the direction the
%% fold is travelling.
inside_the_stream(Start, Newest, forward) -> Start =< Newest;
inside_the_stream(Start, _Newest, backward) -> Start >= 0.

%% @private Drop a batch's events above the fold's fence. Going backward the
%% window starts at Newest, so there is never anything above it.
bounded({ok, Events}, Newest, forward) ->
    {ok, [E || #event{version = V} = E <- Events, V =< Newest]};
bounded(Result, _Newest, _Direction) ->
    Result.

%% @private The version one step past the batch just visited.
next_start(Events, forward) ->
    %% Read forward, the batch ends at its highest version.
    (lists:last(Events))#event.version + 1;
next_start(Events, backward) ->
    %% Read backward, the batch begins at its highest version and ends at its
    %% lowest, which is where the next batch below it starts.
    (lists:last(Events))#event.version - 1.

%% @private The nearest live version on the cursor's side of a hole: the
%% smallest at or after it going forward, the largest at or before it going
%% backward. `none' when the hole runs to the end of the stream.
next_live_version(StoreId, StreamId, Direction, Start) ->
    case version_nodes(StoreId, StreamId, #{favor => consistency}) of
        {ok, Versions} -> live_version(Versions, Direction, Start);
        {error, _} = Error -> Error
    end.

%% @private
live_version(Versions, forward, Start) ->
    nearest([V || V <- Versions, V >= Start], fun lists:min/1);
live_version(Versions, backward, Start) ->
    nearest([V || V <- Versions, V =< Start], fun lists:max/1).

%% @private
nearest([], _End) -> none;
nearest(Versions, End) -> {ok, End(Versions)}.

%% @doc Read all events across all streams in global order.
%%
%% Returns events in global order (epoch_us, then stream_id, then version;
%% see `reckon_db_index:sort_in_global_order/1'), skipping `Offset' events
%% and returning up to `BatchSize' events. Used by catch-up subscriptions
%% to replay historical events to a subscriber, so the events of one append
%% replay in the order they were appended.
%%
%% Parameters:
%%   StoreId   - The store identifier
%%   Offset    - Number of events to skip (0-based)
%%   BatchSize - Maximum number of events to return
%%
%% Returns events in global order (epoch_us, stream_id, version).
%%
%% == Why this is cached, and why an index inside Khepri cannot replace it ==
%%
%% This function has no true server-side pagination: Khepri's `get_many/2'
%% has no offset/limit primitive (`if_name_matches'/`if_path_matches' are
%% regex-only, no numeric range condition), so ANY paginated design over
%% it -- indexed or not -- pays a full-matching-subtree fetch on every
%% call. Verified by building and benchmarking a secondary `all' index
%% first (a natural-looking fix, since `reckon_db_index' already does
%% exactly this for `tags'/`event_type'/`{meta,_}'): at 10k events it was
%% NOT faster than the plain scan below, because those calls are
%% ref+point-resolve (fine when a lookup matches a SMALL subset of the
%% store, wrong when -- like `read_all_global' -- it touches nearly the
%% whole store every time), and denormalizing full events into the index
%% just duplicated the same full-subtree-fetch cost under a different path.
%%
%% `evoq_store_subscription:catch_up_historical/1' (the only real caller)
%% pages through a store via a tight sequential burst of
%% `(StoreId, GrowingOffset, 1000)' calls at subscription start -- which is
%% exactly the access pattern this cache targets: pay the full scan-and-sort
%% ONCE per burst (fingerprinted by `global_event_count/1', already O(1) and
%% already exact -- it only ever increments on append), not once per page.
%% A store that grows mid-burst invalidates the cache on its own; the TTL
%% below is a memory-hygiene bound for a burst that stalls or never
%% completes, not a correctness mechanism.
%%
%% == Why the cache indexes one ETS row per event, not one row per store ==
%%
%% A first cut of this cache (5.11.1) stored the WHOLE sorted list as a
%% single ETS value and served a page via `lists:nthtail'/`sublist' on the
%% list `ets:lookup' handed back. That still copies and walks the ENTIRE
%% cached list on EVERY page: `ets:lookup' always deep-copies the full
%% stored term out to the caller, so a "cache hit" against a real
%% ~87k-event store (reproduced against a real evidence store, not a
%% synthetic one -- see hecate-sentinel's CHANGELOG) cost ~110-130ms per
%% page regardless of hit/miss, not the O(1) the fingerprinting was meant
%% to buy. Against a store that size, blocking `evoq_store_subscription's
%% synchronous catch-up for the whole burst was long enough on real
%% (weaker-than-benchmark) hardware to plausibly trip an external liveness
%% check mid-replay -- see evoq's async catch-up fix, same release train.
%%
%% Indexing each event under its own `{StoreId, Position}' key makes a page
%% read `BatchSize' independent point lookups (each O(1)/O(log N) in ETS,
%% and each copies exactly one event, not the whole store) instead of one
%% O(N) copy. The one full scan-and-sort per fingerprint change is
%% unchanged -- sorting still requires seeing every event at least once --
%% only the PER-PAGE cost after that scan is fixed.
%%
%% Two correctness properties a whole-list cache gets for free and a
%% per-row one has to earn back, both closed by a per-rebuild Generation
%% id every row (including the meta row) is tagged with:
%%
%% 1. The page bound must be the ACTUAL number of rows this generation
%%    wrote, never `global_event_count/1'. That counter is a fingerprint
%%    ("has anything changed"), not an authoritative row count -- it does
%%    not exist on stores that predate it (reads as 0, nothing backfilled),
%%    DCB appends (`reckon_db_dcb') never bump it, and deletes never
%%    decrement it. Any of those makes it diverge from the real scanned
%%    length: paging against it would silently truncate catch-up (counter
%%    too low) or let a shrunk generation's stale trailing rows leak back
%%    in (counter too high). `Len = length(SortedEvents)' from THIS
%%    rebuild is the only authoritative bound.
%% 2. A page is `BatchSize' independent `ets:lookup' calls, not one atomic
%%    read -- so while the WRITE side is atomic (the whole batch lands in
%%    one `ets:insert/2', which OTP guarantees atomic and isolated for
%%    `set' tables), the READ side is not: a rebuild can still land between
%%    two of a page's lookups. If that rebuild's fresh sort reassigns a
%%    position this page already read (only possible if two events'
%%    epoch_us values are close enough, or a delete/DCB write races the
%%    scan, to change relative order --
%%    `reckon_db_index:sort_in_global_order/1' promises a total order of
%%    whatever it saw, nothing about positions surviving a later append),
%%    the page would silently mix two generations. Tagging
%%    each row with its Generation and checking it on every lookup turns
%%    that into a detectable condition instead of a silent one: a page
%%    that spans a rebuild is retried once, against the new generation,
%%    rather than returned torn.
-define(READ_ALL_GLOBAL_CACHE, reckon_db_read_all_global_cache).
%% Long enough to cover one catch-up burst (milliseconds to low seconds in
%% practice); short enough that an entry from a burst nobody ever repeats
%% doesn't sit in memory indefinitely.
-define(READ_ALL_GLOBAL_CACHE_TTL_MS, 30000).
%% One rebuild takes low seconds even against a real ~87k-event store (see
%% the module doc above); a SECOND rebuild landing inside the handful of
%% BEAM reductions between two ets:lookup calls of the same page would
%% need back-to-back rebuilds on that timescale. One retry is generous.
-define(PAGE_RETRY_LIMIT, 1).

-spec read_all_global(atom(), non_neg_integer(), pos_integer()) ->
    {ok, [event()]} | {error, term()}.
read_all_global(StoreId, Offset, BatchSize) ->
    case ensure_cached(StoreId) of
        {ok, Len, Generation} ->
            fetch_page(StoreId, Offset, BatchSize, Len, Generation, ?PAGE_RETRY_LIMIT);
        {error, _} = Error ->
            Error
    end.

%% @private Ensure this store's event index is current (rebuilding it, one
%% scan-and-sort, when the `global_event_count/1' fingerprint has moved or
%% the entry outlived its TTL) and return `{ok, Len, Generation}' --
%% `fetch_page/5' pages against Len (the real row count) and verifies
%% Generation on every row it reads.
-spec ensure_cached(atom()) ->
    {ok, non_neg_integer(), reference()} | {error, term()}.
ensure_cached(StoreId) ->
    ensure_cache_table(),
    case global_event_count(StoreId) of
        {ok, Count} -> cached_or_rebuilt(StoreId, Count);
        {error, _} = Error -> Error
    end.

cached_or_rebuilt(StoreId, Count) ->
    Now = erlang:monotonic_time(millisecond),
    case ets:lookup(?READ_ALL_GLOBAL_CACHE, {meta, StoreId}) of
        [{_, Count, Len, Generation, InsertedAt}]
                when Now - InsertedAt < ?READ_ALL_GLOBAL_CACHE_TTL_MS ->
            {ok, Len, Generation};
        _ ->
            rebuild_cache(StoreId, Count, Now)
    end.

rebuild_cache(StoreId, Count, Now) ->
    case query_event_results(StoreId, has_data_leaf()) of
        {ok, Results} ->
            Events = [convert_result_to_event(PathKey, Value)
                      || {PathKey, Value} <- Results],
            ValidEvents = [E || E <- Events, E =/= undefined],
            SortedEvents = reckon_db_index:sort_in_global_order(ValidEvents),
            Len = length(SortedEvents),
            Generation = make_ref(),
            Rows = index_rows(StoreId, SortedEvents, Generation),
            true = ets:insert(?READ_ALL_GLOBAL_CACHE,
                               [{{meta, StoreId}, Count, Len, Generation, Now} | Rows]),
            {ok, Len, Generation};
        {error, _} = Error ->
            Error
    end.

%% @private One `{{StoreId, Position}, Generation, Event}' row per event,
%% 0-based and dense -- `fetch_page/5' relies on every position in
%% `[0, Len)' existing under the SAME Generation this call minted.
index_rows(StoreId, Events, Generation) ->
    {Rows, _} = lists:mapfoldl(
        fun(Event, Position) -> {{{StoreId, Position}, Generation, Event}, Position + 1} end,
        0, Events),
    Rows.

%% @private A page is `BatchSize' independent point lookups, not a copy of
%% the whole cached store -- cost scales with the page, not with Len. Every
%% row read must carry the SAME Generation this call started with; a
%% mismatch (or a missing row -- same cause) means a rebuild landed
%% mid-page, and the page is retried once against the fresh generation
%% rather than returned as a torn mix of two.
fetch_page(_StoreId, Offset, _BatchSize, Len, _Generation, _RetriesLeft) when Offset >= Len ->
    {ok, []};
fetch_page(StoreId, Offset, BatchSize, Len, Generation, RetriesLeft) ->
    Last = min(Offset + BatchSize, Len) - 1,
    Positions = lists:seq(Offset, Last),
    case lookup_consistent(StoreId, Positions, Generation, []) of
        {ok, _Events} = Ok -> Ok;
        stale -> retry_page(StoreId, Offset, BatchSize, RetriesLeft)
    end.

lookup_consistent(_StoreId, [], _Generation, Acc) ->
    {ok, lists:reverse(Acc)};
lookup_consistent(StoreId, [Position | Rest], Generation, Acc) ->
    case ets:lookup(?READ_ALL_GLOBAL_CACHE, {StoreId, Position}) of
        [{_, Generation, Event}] -> lookup_consistent(StoreId, Rest, Generation, [Event | Acc]);
        _ -> stale
    end.

retry_page(_StoreId, _Offset, _BatchSize, 0) ->
    {error, cache_generation_changed_mid_page};
retry_page(StoreId, Offset, BatchSize, RetriesLeft) ->
    case ensure_cached(StoreId) of
        {ok, Len, Generation} ->
            fetch_page(StoreId, Offset, BatchSize, Len, Generation, RetriesLeft - 1);
        {error, _} = Error ->
            Error
    end.

%% @doc Idempotently create the read-all-global cache table.
%%
%% Exported (not just called lazily from `ensure_cached/1') so the
%% top-level supervisor's own init callback can call this itself,
%% making the SUPERVISOR
%% the table's owner instead of whichever transient gateway worker
%% happens to call `read_all_global/3' first.
%%
%% That mattered: a `public' ETS table is owned by whoever calls
%% `ets:new/2', and the table dies with its owner regardless of how
%% many other processes still reference it by name. A table created
%% lazily by a short-lived worker vanishes the instant that worker
%% exits for ANY reason -- including a routine one-off supervised
%% restart unrelated to this table -- and the next caller's
%% `ets:lookup/2' in `cached_or_rebuilt/2' then crashes with
%% `{badarg, "the table identifier does not refer to an existing ETS
%% table"}'. Reproduced on every boot in practice: gateway workers
%% restart routinely during a store's own startup churn, and whichever
%% one happened to win the race to create this table took it down
%% with it moments later.
%%
%% Racing calls to THIS function are still safe on their own terms
%% (`ets:new/2' raises `badarg' on a name collision, meaning another
%% process already won -- nothing to do) -- the fix is giving the
%% table a caller that is never itself the process racing to use it
%% moments later.
-spec ensure_cache_table() -> ok.
ensure_cache_table() ->
    case ets:whereis(?READ_ALL_GLOBAL_CACHE) of
        undefined -> create_cache_table();
        _Tid -> ok
    end.

create_cache_table() ->
    try
        ets:new(?READ_ALL_GLOBAL_CACHE,
                [named_table, public, set, {read_concurrency, true}]),
        ok
    catch
        error:badarg -> ok
    end.

%% @private Leaf condition matching any version/seq node that carries
%% event data: `#if_all{[*, has_data]}'. Shared by the global readers.
-spec has_data_leaf() -> khepri:condition().
has_data_leaf() ->
    #if_all{conditions = [
        ?KHEPRI_WILDCARD_STAR,
        #if_has_data{has_data = true}
    ]}.

%% @private Query every event node across the store. Model C stores
%% regular events at 4 levels ([streams, Type, Id, Version]) and the DCB
%% pseudo-stream at 2 ([streams, _dcb, SeqKey]), so a single get_many
%% cannot span both depths — we run one query per depth and merge.
%%
%% The regular query is authoritative for error propagation (the
%% `[streams]' root always exists); the DCB subtree is best-effort
%% (absent when no DCB events have been written → empty, not an error).
-spec query_event_results(atom(), khepri:condition()) ->
    {ok, [{[atom() | binary()], term()}]} | {error, term()}.
query_event_results(StoreId, LeafMatch) ->
    RegularPattern = [streams,
                      ?KHEPRI_WILDCARD_STAR,
                      ?KHEPRI_WILDCARD_STAR,
                      LeafMatch],
    case khepri:get_many(StoreId, RegularPattern) of
        {ok, Regular} when is_map(Regular) ->
            DcbPattern = ?DCB_STREAM_PATH ++ [LeafMatch],
            {ok, maps:to_list(Regular) ++ get_many_list(StoreId, DcbPattern)};
        {ok, _} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

%% @private Best-effort get_many returning a plain list ([] on any miss).
-spec get_many_list(atom(), khepri_path:native_pattern()) ->
    [{[atom() | binary()], term()}].
get_many_list(StoreId, Pattern) ->
    case khepri:get_many(StoreId, Pattern) of
        {ok, Results} when is_map(Results) -> maps:to_list(Results);
        _ -> []
    end.

%% @doc Read all events of specific types from all streams using Khepri native filtering.
%%
%% This function uses Khepri's built-in #if_data_matches condition to filter
%% events by type at the database level, avoiding loading all events into memory.
%%
%% Parameters:
%%   StoreId    - The store identifier
%%   EventTypes - List of event type binaries to match
%%   BatchSize  - Maximum number of events to return (for pagination)
%%
%% Returns events in global order (epoch_us, stream_id, version).
-spec read_by_event_types(atom(), [binary()], pos_integer()) ->
    {ok, [event()]} | {error, term()}.
read_by_event_types(StoreId, EventTypes, BatchSize) when is_list(EventTypes) ->
    case reckon_db_index_config:is_indexed(StoreId, event_type) of
        true ->
            limit_index_result(
                reckon_db_index:lookup_event_types(StoreId, EventTypes),
                BatchSize);
        false ->
            warn_unindexed(StoreId, event_type),
            scan_by_event_types(StoreId, EventTypes, BatchSize)
    end.

%% @private Whole-store scan fallback for stores that did not declare the
%% `event_type' index. Pushes the type match into Khepri (DB-side
%% #if_data_matches) so non-matching events aren't loaded.
-spec scan_by_event_types(atom(), [binary()], pos_integer()) ->
    {ok, [event()]} | {error, term()}.
scan_by_event_types(StoreId, EventTypes, BatchSize) ->
    %% The leaf condition is applied at both depths by query_event_results
    %% (regular 4-level events + the 2-level DCB log).
    TypeConditions = [
        #if_data_matches{pattern = #event{event_type = ET, _ = '_'}}
        || ET <- EventTypes
    ],
    LeafMatch = case TypeConditions of
        [] ->
            has_data_leaf();
        [SingleCondition] ->
            #if_all{conditions = [
                ?KHEPRI_WILDCARD_STAR,
                #if_has_data{has_data = true},
                SingleCondition
            ]};
        _ ->
            #if_all{conditions = [
                ?KHEPRI_WILDCARD_STAR,
                #if_has_data{has_data = true},
                #if_any{conditions = TypeConditions}
            ]}
    end,

    case query_event_results(StoreId, LeafMatch) of
        {ok, Results} ->
            Events = [convert_result_to_event(PathKey, Value)
                      || {PathKey, Value} <- Results],
            ValidEvents = [E || E <- Events, E =/= undefined],
            SortedEvents = reckon_db_index:sort_in_global_order(ValidEvents),
            LimitedEvents = lists:sublist(SortedEvents, BatchSize),
            {ok, LimitedEvents};
        {error, _} = Error ->
            Error
    end.

%% @doc Read all events matching tags from all streams.
%%
%% Tags provide a mechanism for cross-stream querying without affecting
%% stream-based concurrency control. This is useful for the process-centric
%% model where you want to find all events related to specific participants.
%%
%% == Match Modes ==
%%
%% `any' (default): Returns events containing ANY of the specified tags (union).
%%   Example: `read_by_tags(Store, [<<"student:456">>, <<"student:789">>], any, 100)'
%%   Returns events for either student.
%%
%% `all': Returns events containing ALL of the specified tags (intersection).
%%   Example: `read_by_tags(Store, [<<"student:456">>, <<"course:CS101">>], all, 100)'
%%   Returns only events tagged with both student 456 AND course CS101.
%%
%% == Parameters ==
%%
%%   StoreId   - The store identifier
%%   Tags      - List of tag binaries to match
%%   Match     - `any' | `all' (matching strategy)
%%   BatchSize - Maximum number of events to return
%%
%% == Returns ==
%%
%% Events in global order (epoch_us, stream_id, version).
-spec read_by_tags(atom(), [binary()], any | all, pos_integer()) ->
    {ok, [event()]} | {error, term()}.
read_by_tags(StoreId, Tags, Match, BatchSize) when is_list(Tags), is_atom(Match) ->
    case reckon_db_index_config:is_indexed(StoreId, tags) of
        true ->
            limit_index_result(
                reckon_db_index:lookup_tags(StoreId, Tags, Match), BatchSize);
        false ->
            warn_unindexed(StoreId, tags),
            scan_by_tags(StoreId, Tags, Match, BatchSize)
    end.

%% @private Whole-store tag scan fallback for stores that did not declare
%% the `tags' index. Khepri pattern matching doesn't support list
%% membership, so events with data are fetched and filtered client-side.
%% This is exactly the cross-cutting scan the secondary index replaces.
-spec scan_by_tags(atom(), [binary()], any | all, pos_integer()) ->
    {ok, [event()]} | {error, term()}.
scan_by_tags(StoreId, Tags, Match, BatchSize) ->
    case query_event_results(StoreId, has_data_leaf()) of
        {ok, Results} ->
            AllEvents = [convert_result_to_event(PathKey, Value)
                         || {PathKey, Value} <- Results],
            ValidEvents = [E || E <- AllEvents, E =/= undefined],
            FilteredEvents = filter_events_by_tags(ValidEvents, Tags, Match),
            SortedEvents = reckon_db_index:sort_in_global_order(FilteredEvents),
            LimitedEvents = lists:sublist(SortedEvents, BatchSize),
            {ok, LimitedEvents};
        {error, _} = Error ->
            Error
    end.

%% @doc Read all events whose metadata key = value.
%%
%% This is the sanctioned primitive applications build causation /
%% correlation / saga read models on (e.g. `read_by_metadata(Store,
%% <<"causation_id">>, EventId)`). The store returns events matching a
%% metadata key=value pair — bounded and indexed when `{meta, Key}' is
%% declared — and does NOT interpret what the key means. Lineage
%% traversal, graphs, and read models are the application's job.
%%
%% Indexed (O(matches)) when the store declared `{meta, Key}'; otherwise
%% a one-time `logger:warning' is emitted and the query falls back to a
%% whole-store scan (O(total events)).
-spec read_by_metadata(atom(), binary(), binary()) ->
    {ok, [event()]} | {error, term()}.
read_by_metadata(StoreId, Key, Value) when is_binary(Key), is_binary(Value) ->
    case reckon_db_index_config:is_indexed(StoreId, {meta, Key}) of
        true ->
            reckon_db_index:lookup_meta(StoreId, Key, Value);
        false ->
            warn_unindexed(StoreId, {meta, Key}),
            scan_by_metadata(StoreId, Key, Value)
    end.

%% @private Whole-store scan fallback for an un-indexed metadata key.
-spec scan_by_metadata(atom(), binary(), binary()) ->
    {ok, [event()]} | {error, term()}.
scan_by_metadata(StoreId, Key, Value) ->
    case query_event_results(StoreId, has_data_leaf()) of
        {ok, Results} ->
            Events = [convert_result_to_event(P, V) || {P, V} <- Results],
            Matching = [E || #event{metadata = M} = E <- Events,
                             is_map(M), maps:get(Key, M, undefined) =:= Value],
            {ok, reckon_db_index:sort_in_global_order(Matching)};
        {error, _} = Error ->
            Error
    end.

%% @private Apply the BatchSize limit to an index lookup result.
-spec limit_index_result({ok, [event()]} | {error, term()}, pos_integer()) ->
    {ok, [event()]} | {error, term()}.
limit_index_result({ok, Events}, BatchSize) ->
    {ok, lists:sublist(Events, BatchSize)};
limit_index_result({error, _} = Error, _BatchSize) ->
    Error.

%% @private Warn ONCE per (store, index-kind) that a cross-cutting query
%% is hitting an un-indexed scan, so operators can declare the index. No
%% silent truncation — the query still runs, just O(store). The
%% persistent_term flag keeps it to a single line per kind.
-spec warn_unindexed(atom(), index_decl()) -> ok.
warn_unindexed(StoreId, Kind) ->
    Flag = {reckon_db, idx_unindexed_warned, StoreId, Kind},
    case persistent_term:get(Flag, false) of
        true ->
            ok;
        false ->
            persistent_term:put(Flag, true),
            logger:warning(
                "reckon_db: cross-cutting query on un-indexed ~p (store ~p) — "
                "O(store) scan; declare the index in store_config to make it "
                "O(matches)", [Kind, StoreId]),
            ok
    end.

%% @private Filter events by tags according to match mode
%% Empty search tags always returns empty (no criteria = no results)
-spec filter_events_by_tags([event()], [binary()], any | all) -> [event()].
filter_events_by_tags(_Events, [], _Match) ->
    [];
filter_events_by_tags(Events, Tags, any) ->
    %% Return events that have ANY of the tags (union)
    TagSet = sets:from_list(Tags),
    lists:filter(
        fun(#event{tags = EventTags}) when is_list(EventTags), EventTags =/= [] ->
            EventTagSet = sets:from_list(EventTags),
            sets:size(sets:intersection(TagSet, EventTagSet)) > 0;
           (_) ->
            false
        end,
        Events
    );
filter_events_by_tags(Events, Tags, all) ->
    %% Return events that have ALL of the tags (intersection)
    TagSet = sets:from_list(Tags),
    lists:filter(
        fun(#event{tags = EventTags}) when is_list(EventTags), EventTags =/= [] ->
            EventTagSet = sets:from_list(EventTags),
            sets:is_subset(TagSet, EventTagSet);
           (_) ->
            false
        end,
        Events
    ).

%% @private Convert a Khepri result to an event, deriving stream_id from
%% the path. Handles both the 4-level regular layout
%% ([streams, Type, Id, Version]) and the 2-level DCB log
%% ([streams, _dcb, SeqKey]); `stream_id_from_path/1' reconstructs the
%% opaque id for both.
-spec convert_result_to_event([atom() | binary()], term()) -> event() | undefined.
convert_result_to_event([streams | _] = Path, Event) when is_record(Event, event) ->
    Event#event{stream_id = reckon_db_stream_path:stream_id_from_path(Path)};
convert_result_to_event([streams | _] = Path, EventMap) when is_map(EventMap) ->
    Event = map_to_event(EventMap),
    Event#event{stream_id = reckon_db_stream_path:stream_id_from_path(Path)};
convert_result_to_event(_, _) ->
    undefined.

%% @doc Get current version of a stream
%%
%% Returns:
%%   -1    - if stream doesn't exist or is empty
%%   N >= 0 - representing the version of the latest event
%%
%% The answer is the stream's version COUNTER, written with the events in
%% the append transaction, never the number of version nodes. Counting was
%% wrong the moment a stream was scavenged: scavenging deletes old version
%% nodes and does not renumber the rest, so the count fell and the next
%% append reused (and silently overwrote) live versions.
%%
%% Streams written by a store that predates the counter have no counter
%% node yet; for those the newest version node is read once (names only)
%% and the next append writes the counter. That read is the expensive one -
%% the same walk, measured at 130-250 ms on a 50 000-event stream - and it is
%% paid on every version read until the stream's first append after the
%% upgrade writes the counter.
%%
%% This answers -1 for a store it could not read, and `append/4' answers the
%% store's own error for the same failure. The difference is deliberate: this
%% function has no error channel and its callers page by version, while an
%% append must not write over a tree it could not read. `newest_event_version/2'
%% answers `{ok, Version}' or the store's error from the same read, for a
%% caller that has to tell -1 apart from a store that did not answer.
-spec get_version(atom(), binary()) -> integer().
get_version(StoreId, StreamId) ->
    case current_version_for_append(StoreId, StreamId) of
        {ok, Version, _CounterExisted} -> Version;
        %% A store that cannot answer reads as "no stream" here, which is
        %% what it answered before the read errors were separated out: this
        %% function has no error channel, and an append still refuses to
        %% write over a node that is there (append_guard/6).
        {error, _} -> ?NO_STREAM
    end.

%% @doc The newest version an event node holds, read from the tree.
%%
%% `get_version/2' answers the counter, which is what an append writes from.
%% This answers what the stream is, and the two differ after a rollback:
%% 5.11.x keeps no counter, so it can scavenge a stream's newest events away,
%% or delete and re-create the stream, and the counter sits above what is
%% left. A scavenge takes its cutoff from here - measuring `keep_versions'
%% against a version that no longer exists protects nothing - and answers
%% `{ok, ?NO_STREAM}' for a stream with no event left.
%%
%% The counter is asked first, because one node read answers it: the counter
%% is written in the same Ra command as the events, so a counter whose node
%% is in the tree names the newest version. The tree is walked only when that
%% node is gone, or when there is no counter at all, which is the rollback
%% shape and the pre-counter stream. Walking is what made a dry run on a 50k
%% stream cost 130-226 ms.
-spec newest_event_version(atom(), binary()) -> {ok, integer()} | {error, term()}.
newest_event_version(StoreId, StreamId) ->
    Options = read_options(consistent),
    case counter_naming_a_live_node(StoreId, StreamId, Options) of
        {ok, Version} -> {ok, Version};
        no_counter -> get_version_from_version_nodes(StoreId, StreamId, Options)
    end.

%% @private The version an append writes from, whether it came from the
%% stream's counter, and - when nothing could be read at all - the error.
%%
%% The flag matters at write time: a stream with no counter (written before
%% this release) has no number to compare in the transaction, only the
%% version nodes, and that is what append_guard/6 checks for it.
-spec current_version_for_append(atom(), binary()) ->
    {ok, integer(), boolean()} | {error, term()}.
current_version_for_append(StoreId, StreamId) ->
    current_version_for_append(StoreId, StreamId, local).

%% @private The same read, told which answer is acceptable.
%%
%% The first read of an append takes whatever this node can serve at once
%% (`local`). A re-read after a lost race cannot: on a follower it may answer
%% the same version the previous read did, and the retry would then lose the
%% race again on a healthy stream - sixty times - and answer
%% version_conflict for a stream nobody else is writing. So the retry asks
%% for `consistent' and pays for it only on the path that has already lost a
%% race.
%%
%% `favor => consistency' is a local query fenced to the leader's last
%% applied index, not the leader's own answer: khepri asks the leader where
%% its log ends and then refuses to answer until the local server has applied
%% that far. That is what the retry needs - an answer no older than the race
%% it just lost - and it does not cost a round trip for the read itself.
-spec current_version_for_append(atom(), binary(), local | consistent) ->
    {ok, integer(), boolean()} | {error, term()}.
current_version_for_append(StoreId, StreamId, Read) ->
    Options = read_options(Read),
    case counter_value(StoreId, StreamId, Options) of
        {ok, Version} -> {ok, Version, true};
        none          -> version_from_the_tree(StoreId, StreamId, Options)
    end.

%% @private The version a stream with no counter has, taken from its nodes.
version_from_the_tree(StoreId, StreamId, Options) ->
    case get_version_from_version_nodes(StoreId, StreamId, Options) of
        {ok, Version}      -> {ok, Version, false};
        {error, _} = Error -> Error
    end.

%% @private
read_options(local) -> #{};
read_options(consistent) -> #{favor => consistency}.

%% @private The stream's version counter, as this node can serve it.
counter_value(StoreId, StreamId, Options) ->
    Path = reckon_db_stream_path:version_counter_path(StreamId),
    case khepri:get(StoreId, Path, Options) of
        {ok, Version} when is_integer(Version), Version >= 0 -> {ok, Version};
        _ -> none
    end.

%% @private The counter, when it is the newest version in the tree.
%%
%% Two node reads, where walking every version node of a 50k stream cost
%% 130-226 ms. The counter is written in the same Ra command as the events, so
%% a counter whose node is present is normally the newest version. Two shapes
%% break that, and both are named by a read of the node after it:
%%
%% - the counter's own node is gone - a counter above a tree a delete and
%%   re-create left short, or a rollback shape - and it says nothing;
%% - the node at Counter + 1 is present - a 5.11.x writer appended past the
%%   counter, which that release never maintained. Measured: counter 9 with
%%   the tree holding 0..15, where answering 9 made a keep_versions 3 scavenge
%%   delete {0,6} instead of {0,12}: it under-scavenged, and the cutoff was
%%   measured against a version that was not the newest.
%%
%% Either way the tree is what answers, and the caller walks it.
counter_naming_a_live_node(StoreId, StreamId, Options) ->
    case counter_value(StoreId, StreamId, Options) of
        {ok, Version} -> counter_over_a_live_node(StoreId, StreamId, Version, Options);
        none          -> no_counter
    end.

%% @private
counter_over_a_live_node(StoreId, StreamId, Version, Options) ->
    case version_node_exists(StoreId, StreamId, Version, Options) of
        true ->
            counter_is_newest(StoreId, StreamId, Version, Options);
        false ->
            no_counter
    end.

%% @private Whether a writer went past the counter.
counter_is_newest(StoreId, StreamId, Version, Options) ->
    case version_node_exists(StoreId, StreamId, Version + 1, Options) of
        false -> {ok, Version};
        true  -> no_counter
    end.

%% @private Latest version present in the tree, for a stream whose counter
%% node is not there yet (written before this release, or never appended
%% to since). One read of the child names: the padded version nodes sort
%% lexicographically, so the maximum name IS the latest version.
-spec get_version_from_version_nodes(atom(), binary(), #{favor => consistency}) ->
    {ok, integer()} | {error, term()}.
get_version_from_version_nodes(StoreId, StreamId, Options) ->
    case version_nodes(StoreId, StreamId, Options) of
        {ok, []}           -> {ok, ?NO_STREAM};
        {ok, Versions}     -> {ok, lists:max(Versions)};
        {error, _} = Error -> Error
    end.

%% @private Every live version number in a stream, read once.
%%
%% A read error is an error, not an empty tree: `[]' is what a fully
%% scavenged stream answers, and the reconcile reads that as "the counter is
%% the only record left, keep it". Folding a store that did not answer into
%% it would keep a counter nothing had checked.
version_nodes(StoreId, StreamId, Options) ->
    Pattern = reckon_db_stream_path:versions_pattern(StreamId),
    Query = Options#{props_to_return => []},
    case khepri_adv:get_many(StoreId, Pattern, Query) of
        {ok, Nodes}        -> {ok, version_numbers(Nodes)};
        {error, _} = Error -> Error
    end.

%% @private Version numbers of the event nodes in a get_many result.
version_numbers(Nodes) ->
    [binary_to_integer(Name) || [_, _, _, Name] <- maps:keys(Nodes),
                                 is_valid_version_name(Name)].

%% @private A version node name is the fixed-width padded version.
is_valid_version_name(Name) when is_binary(Name) ->
    byte_size(Name) =:= ?VERSION_PADDING andalso
        lists:all(fun(C) -> C >= $0 andalso C =< $9 end, binary_to_list(Name));
is_valid_version_name(_) ->
    false.

%% @doc How many events a stream actually holds.
%%
%% Not `get_version/2 + 1': a scavenge deletes version nodes without
%% renumbering, so after one the newest version stops being the count. This
%% counts the nodes that are there.
-spec live_event_count(atom(), binary()) -> non_neg_integer().
live_event_count(StoreId, StreamId) ->
    case khepri:count(StoreId, reckon_db_stream_path:versions_pattern(StreamId)) of
        {ok, Count} when is_integer(Count) -> Count;
        _ -> 0
    end.

%% @doc Check if a stream exists
-spec exists(atom(), binary()) -> boolean().
exists(StoreId, StreamId) ->
    Path = reckon_db_stream_path:stream_path(StreamId),
    case khepri:exists(StoreId, Path) of
        true -> true;
        false -> false;
        {error, _} -> false
    end.

%% @doc Check if a store contains at least one event.
%% Cannot rely on stream existence alone — streams can survive
%% after all their events are deleted (truncation, GDPR erasure).
%% Checks for actual event data by reading 1 event globally.
-spec has_events(atom()) -> boolean().
has_events(StoreId) ->
    case read_all_global(StoreId, 0, 1) of
        {ok, [_ | _]} -> true;
        _              -> false
    end.

%% @doc List all streams in the store
-spec list_streams(atom()) -> {ok, [binary()]} | {error, term()}.
list_streams(StoreId) ->
    %% Aggregate-id nodes live at depth 3: [streams, Type, Id]. One such
    %% node per stream (its children are the version leaves). Reconstruct
    %% the opaque id and drop the reserved DCB pseudo-stream — it is not a
    %% user stream.
    Path = ?STREAMS_PATH ++ [?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR],
    case khepri:get_many(StoreId, Path) of
        {ok, Results} when is_map(Results) ->
            StreamIds = lists:usort([
                reckon_db_stream_path:stream_id_from_path(P)
                || P <- maps:keys(Results),
                   not is_dcb_path(P)
            ]),
            {ok, StreamIds};
        {ok, _} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

%% @doc The store's monotonic total event count — an O(1) read of the
%% counter that every append batch maintains (see bump_global_event_count/1).
%% Absent counter (a store that predates the counter, or has never been
%% appended to) reads as 0. This is the cheap source for ingest-rate
%% dashboards, replacing a full-store scan.
-spec global_event_count(atom()) -> {ok, non_neg_integer()}.
global_event_count(StoreId) ->
    %% Anything but a stored integer (absent counter on a fresh or
    %% pre-counter store) reads as 0 — mirrors the DCB counter idiom. A
    %% genuinely unreachable store surfaces as an rpc failure at the
    %% dispatch layer before this runs.
    Count = case khepri:get(StoreId, ?GLOBAL_EVENT_COUNT_PATH) of
                {ok, V} when is_integer(V) -> V;
                _                          -> 0
            end,
    {ok, Count}.

%% @private True for the DCB pseudo-stream's 2-level nodes
%% ([streams, _dcb, SeqKey]) — excluded from user-facing stream listings.
-spec is_dcb_path([atom() | binary()]) -> boolean().
is_dcb_path([streams, ?DCB_STREAM | _]) -> true;
is_dcb_path(_) -> false.

%% @doc Delete a stream and all its events
-spec delete(atom(), binary()) -> ok | {error, term()}.
delete(StoreId, StreamId) ->
    %% Same gate as read/6: a malformed id is the caller's mistake and
    %% must come back as an error, not as a dead gateway worker
    %% (reckon-db#4).
    case reckon_db_stream_path:validate(StreamId) of
        ok -> delete_validated(StoreId, StreamId);
        {error, _} = Invalid -> Invalid
    end.

%% @private The delete itself, past the stream-id gate.
%%
%% The version counter and the chain-start watermark go with the stream, in
%% the same transaction: a counter left behind would make a stream re-created
%% under the same id start after the deleted one's last version instead of at
%% 0, and a watermark left behind would give it a chain start it never had -
%% its earliest events would read as pre-integrity legacy (unverified), and an
%% append below the leftover watermark would be refused.
delete_validated(StoreId, StreamId) ->
    Path = reckon_db_stream_path:stream_path(StreamId),
    CounterPath = reckon_db_stream_path:version_counter_path(StreamId),
    %% The scavenge boundary goes with them, INSIDE the same transaction: a
    %% crash between a delete and a boundary delete afterwards left a boundary
    %% that still verified, and a stream re-created under the same id then had
    %% an anchor its own life never wrote (Jupiter r16 JR6).
    IntegrityPaths = [reckon_db_chain_watermark:path(StreamId),
                      reckon_db_chain_watermark:life_path(StreamId),
                      reckon_db_scavenge_boundary:boundary_path(StreamId)],
    %% The tombstone INTENT goes to disk BEFORE the delete commits, so a crash
    %% between the two is resolvable at the next start (the life is still in
    %% the tree, so the intent is dropped). A crash after the commit but before
    %% the intent reached the disk leaves the anchor holding the deleted life,
    %% and a restore of that stream is accepted - the mint's crash window is
    %% the same shape and is named beside it in #18.
    %%
    %% The generation the tombstone is recorded at takes the SEAL's generation
    %% as well as the counter: the seal's is MAC'd and the counter is a plain
    %% node, so a writer who lowers the counter to 0 and then deletes would
    %% otherwise place the tombstone at 0 - at or below a restored life of
    %% generation 1 - and the restoration would be adopted (r21 review, item 6).
    Witness = delete_witness(StoreId, StreamId),
    case begin_tombstone(StoreId, StreamId, Witness) of
        ok ->
            run_delete(StoreId, StreamId,
                       [Path, CounterPath | IntegrityPaths], Witness);
        {error, _} = Refusal ->
            Refusal
    end.

%% @private The transaction itself, and what a landed delete records.
run_delete(StoreId, StreamId, Paths, Witness) ->
    Fun = fun() -> delete_stream_tx(Paths) end,
    case khepri:transaction(StoreId, Fun) of
        {ok, ok} ->
            ok = reckon_db_life_anchor:record_deleted(StoreId, StreamId,
                                                      maps:get(gen, Witness)),
            ok;
        {ok, {error, E}} ->
            delete_failed(StoreId, StreamId, {error, E});
        {error, _} = Error ->
            delete_failed(StoreId, StreamId, Error)
    end.

%% @private The intent has to be durable before the delete runs, or the delete
%% must not run: with no anchor there is nowhere for it to land, and a delete
%% that commits with neither an intent nor a tombstone leaves the deleted life
%% restorable after the next start (Jupiter J22-2). An integrity-enabled store
%% refuses the delete for the same reason a read refuses to answer - the check
%% that would catch the restore is not running. A store without integrity has
%% nothing for the anchor to protect, so it deletes as it always did.
-spec begin_tombstone(atom(), binary(),
                      #{gen := non_neg_integer(),
                        life := binary() | undefined}) ->
        ok | {error, {life_anchor_unavailable, binary()}}.
begin_tombstone(StoreId, StreamId, Witness) ->
    case reckon_db_life_anchor:record_delete_intent(
           StoreId, StreamId, maps:get(gen, Witness), maps:get(life, Witness)) of
        ok -> ok;
        {error, life_anchor_unavailable} = Refusal ->
            delete_without_anchor(StoreId, StreamId, Refusal)
    end.

%% @private
delete_without_anchor(StoreId, StreamId, _Refusal) ->
    case reckon_db_integrity_key:is_enabled(StoreId) of
        true -> {error, {life_anchor_unavailable, StreamId}};
        false -> ok
    end.

%% @private What the tombstone is recorded at, and the life the intent stands
%% for: the seal's own generation and life when it verifies (MAC'd, so neither
%% can be raised or replaced), and otherwise what THIS NODE recorded for the
%% stream, and the tree's generation counter.
%%
%% The fallback matters because a seal a tree writer broke does not verify: the
%% intent then carried no life at all, and once the seal was put back the
%% stream read as a different life at the same generation - the J22-1 shape a
%% second time (Jupiter J23-4). What this node recorded is not something a tree
%% writer can break.
-spec delete_witness(atom(), binary()) -> #{gen := non_neg_integer(),
                                           life := binary() | undefined}.
delete_witness(StoreId, StreamId) ->
    CounterGen = reckon_db_chain_watermark:life_gen(StoreId, StreamId),
    case reckon_db_chain_watermark:binding(StoreId, StreamId) of
        {ok, #{life := Life, gen := Gen}} when is_binary(Life) ->
            #{gen => max(Gen, CounterGen), life => Life};
        _ ->
            recorded_witness(StoreId, StreamId, CounterGen)
    end.

%% @private
recorded_witness(StoreId, StreamId, CounterGen) ->
    case reckon_db_life_anchor:entry(StoreId, StreamId) of
        {Gen, Life} when is_binary(Life) ->
            #{gen => max(Gen, CounterGen), life => Life};
        _ ->
            #{gen => CounterGen, life => undefined}
    end.

%% @private The transaction answered an error, and an error is AMBIGUOUS: a
%% Ra timeout can still commit. Nothing is decided here - the anchor resolves
%% the intent against the tree once the cluster has nothing in flight, in its
%% own process, so no cleanup races the command and a slow or dying anchor
%% cannot take this caller down (Terra T23-3, T23-5).
delete_failed(StoreId, StreamId, Error) ->
    ok = reckon_db_life_anchor:resolve_when_decided(StoreId, StreamId),
    Error.

%% @private Every node in one command: they go together or none does.
delete_stream_tx(Paths) ->
    Results = [delete_if_present(P) || P <- Paths],
    case [E || {error, _} = E <- Results] of
        [] -> ok;
        [FirstError | _] -> FirstError
    end.

%% @private A path that is already absent is not a failure: a stream written
%% before integrity was enabled has no watermark, and an empty stream may have
%% no counter.
delete_if_present(Path) ->
    case khepri_tx:exists(Path) of
        true -> khepri_tx:delete(Path);
        _ -> ok
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
%%
%% The version this batch writes from is read here and VERIFIED again inside
%% the write transaction (append_guard/6). Reading it here is what lets the
%% event records and their index entries be built outside Ra's transaction -
%% HMAC stamping and crypto cannot run inside one - but a read outside the
%% transaction is a snapshot: two appends to the same stream that both read
%% version N build versions N+1.. and one of them would overwrite the other.
%% So the transaction re-reads the counter, aborts if it moved, and the append
%% retries from the new version. Without that, two concurrent appends of 50
%% events to one stream lost 50 of them - with every call answering ok.
-spec do_append(atom(), binary(), integer(), [new_event()]) ->
    {ok, non_neg_integer()} | {error, term()}.
do_append(StoreId, StreamId, ExpectedVersion, Events) ->
    do_append(StoreId, StreamId, ExpectedVersion, Events, ?APPEND_ATTEMPTS, local).

%% @private
do_append(StoreId, StreamId, ExpectedVersion, Events, Attempts, Read) ->
    do_append(StoreId, StreamId, ExpectedVersion, Events, Attempts, Read, undefined).

%% @private One attempt: read the version, make sure the counter is not above
%% the tree, check what the caller asked for, then append - or retry while the
%% stream keeps moving under us.
do_append(StoreId, StreamId, ExpectedVersion, Events, Attempts, Read,
          PreviousVersion) ->
    case current_version_for_append(StoreId, StreamId, Read) of
        {ok, CurrentVersion, CounterExisted} ->
            append_from(StoreId, StreamId, CurrentVersion, CounterExisted,
                        ExpectedVersion, Events, Attempts, PreviousVersion);
        {error, _} = Error ->
            Error
    end.

%% @private One attempt, once the version is known.
append_from(StoreId, StreamId, CurrentVersion, CounterExisted, ExpectedVersion,
            Events, Attempts, PreviousVersion) ->
    case counter_sits_above_the_tree(StoreId, StreamId, CurrentVersion,
                                     CounterExisted, Attempts) of
        ok ->
            check_and_append(StoreId, StreamId, CurrentVersion, CounterExisted,
                             ExpectedVersion, Events, Attempts, PreviousVersion);
        reconciled ->
            retry_after_reconcile(StoreId, StreamId, CurrentVersion,
                                  ExpectedVersion, Events, Attempts);
        {error, _} = Error ->
            Error
    end.

%% @private Whether the counter points at an event that is there.
%%
%% One read of the version node the counter names. Missing means the counter
%% sits above the tree: 5.11.x keeps no counter, so after a rollback it can
%% have deleted and re-created a stream under the same id, or scavenged one
%% down from the top, and neither touches the counter. An ANY_VERSION append
%% built from that number writes above a gap it can never fill (a re-created
%% stream is 0..2 under a counter of 4, so it lands at 5) or in front of
%% events older than itself (a scavenged one is 13..15 under a counter of 9,
%% so it lands at 10) - and the node it writes is free in both, so no
%% collision ever brings the reconcile in.
%%
%% The other direction needs no check: a counter below the tree means the node
%% the batch would occupy is taken, and write_batch_tx/8 refuses it.
%%
%% A tree with no version node at all is not this shape. This release's own
%% scavenge deletes events without renumbering and leaves the counter as the
%% only record of how far the stream got, so reconcile_counter/3 answers
%% `stale' for it and the append goes ahead from the counter.
%%
%% The last attempt does not reconcile: there is no write left to make from
%% the repaired counter, so the caller gets the ordinary answer instead of
%% another round.
counter_sits_above_the_tree(_StoreId, _StreamId, _CurrentVersion, _CounterExisted,
                            Attempts) when Attempts =< 1 ->
    ok;
counter_sits_above_the_tree(_StoreId, _StreamId, _CurrentVersion, false,
                            _Attempts) ->
    %% No counter: the version came from the tree itself.
    ok;
counter_sits_above_the_tree(StoreId, StreamId, CurrentVersion, true, _Attempts) ->
    case version_node_exists(StoreId, StreamId, CurrentVersion,
                             read_options(local)) of
        true ->
            ok;
        false ->
            reconcile_before_append(StoreId, StreamId, CurrentVersion)
    end.

%% @private Repair the counter, or say there was nothing to repair.
%%
%% The first read took what this node could serve at once, so a follower can
%% answer "the node is gone" for a stream where it is not: the reconcile reads
%% consistently, and answers `stale' when the leader's tree has nothing to
%% repair - no version node at all, or the newest one already matching.
reconcile_before_append(StoreId, StreamId, CurrentVersion) ->
    case reconcile_counter(StoreId, StreamId, CurrentVersion) of
        ok                 -> reconciled;
        stale              -> ok;
        {error, _} = Error -> Error
    end.

%% @private
check_and_append(StoreId, StreamId, CurrentVersion, CounterExisted,
                 ExpectedVersion, Events, Attempts, PreviousVersion) ->
    case check_expected_version(ExpectedVersion, CurrentVersion) of
        ok ->
            append_checked(StoreId, StreamId, CurrentVersion, CounterExisted,
                           ExpectedVersion, Events, Attempts, PreviousVersion);
        {error, _} = Error ->
            reconcile_or_error(StoreId, StreamId, CurrentVersion,
                               ExpectedVersion, Events, Attempts, Error)
    end.

%% @private The expected version holds, so append - unless the counter did not
%% move since the attempt that just lost, which is the stale-counter shape.
append_checked(StoreId, StreamId, CurrentVersion, _CounterExisted,
               ExpectedVersion, Events, Attempts, CurrentVersion) ->
    %% The counter did not move, yet the version node this batch would occupy
    %% is taken. This release cannot produce that shape: the counter is
    %% written in the same Ra command as the event. So something else wrote
    %% the stream - 5.11.x, after a rollback - and the counter is stale.
    %% Every further attempt would lose the same race against an event the
    %% counter cannot see.
    reconcile_or_stall(StoreId, StreamId, CurrentVersion, ExpectedVersion,
                       Events, Attempts);
append_checked(StoreId, StreamId, CurrentVersion, CounterExisted,
               ExpectedVersion, Events, Attempts, _PreviousVersion) ->
    try_append(StoreId, StreamId, CurrentVersion, CounterExisted,
               ExpectedVersion, Events, Attempts).

%% @private A counter that will not move and a taken node: repair it and try
%% again, or - when there is nothing to repair - answer the stall.
reconcile_or_stall(StoreId, StreamId, CurrentVersion, ExpectedVersion, Events,
                   Attempts) ->
    case reconcile_counter(StoreId, StreamId, CurrentVersion) of
        ok -> retry_after_reconcile(StoreId, StreamId, CurrentVersion,
                                    ExpectedVersion, Events, Attempts);
        stale -> {error, {version_unverifiable, CurrentVersion, target_occupied}};
        {error, _} = Error -> Error
    end.

%% @private The caller's own check failed, and it can be answering the stale
%% counter too: an append naming the version the tree really has is refused by
%% a number 5.11.x never moved. Ask the tree before answering - and if there
%% is nothing to repair, the caller's error stands.
reconcile_or_error(StoreId, StreamId, CurrentVersion, ExpectedVersion, Events,
                   Attempts, Error) ->
    case reconcile_counter(StoreId, StreamId, CurrentVersion) of
        ok -> retry_after_reconcile(StoreId, StreamId, CurrentVersion,
                                    ExpectedVersion, Events, Attempts);
        stale -> Error;
        {error, _} = RepairError -> RepairError
    end.

%% @private Try the append again once the counter has been repaired. The
%% budget shrinks by one so a counter that keeps moving cannot loop here, and
%% it has a floor: `Attempts' counts the attempts still to be made, so
%% `Attempts =< 1' is the last one, and it answers the way a spent retry
%% budget answers anywhere else in this module rather than spending itself on
%% a re-read whose only possible outcome is the same cap one level down.
retry_after_reconcile(_StoreId, _StreamId, CurrentVersion, ExpectedVersion,
                      _Events, Attempts) when Attempts =< 1 ->
    {error, {version_conflict, ExpectedVersion, CurrentVersion}};
retry_after_reconcile(StoreId, StreamId, _CurrentVersion, ExpectedVersion,
                      Events, Attempts) ->
    do_append(StoreId, StreamId, ExpectedVersion, Events, Attempts - 1,
              consistent, undefined).

%% @private An append of no events writes nothing and answers the version it
%% read, which is what it answered before the version was verified in the
%% transaction. Short-circuiting here is what keeps the batch that reaches
%% write_batch_tx/8 non-empty - and keeps an empty append from crashing the
%% caller (and, through the gateway pool, the worker) on hd([]).
try_append(_StoreId, _StreamId, CurrentVersion, _CounterExisted,
           _ExpectedVersion, [], _Attempts) ->
    {ok, CurrentVersion};
try_append(StoreId, StreamId, CurrentVersion, CounterExisted, ExpectedVersion,
           Events, Attempts) ->
    case append_events_to_stream(StoreId, StreamId, CurrentVersion,
                                 CounterExisted, ExpectedVersion, Events) of
        {error, {version_changed, _}} when Attempts =< 1 ->
            %% The cap. Carry the version this attempt read, so a caller can
            %% see what the stream had reached: ExpectedVersion alone is -2
            %% for the writers that use ANY_VERSION.
            {error, {version_conflict, ExpectedVersion, CurrentVersion}};
        {error, {version_changed, _}} ->
            %% Someone took these versions between the read and the write.
            %% Back off, then re-read and try again: ANY_VERSION simply
            %% lands after them, an explicit expected version now fails its
            %% own check, which is the answer it asked for. The version this
            %% attempt read travels with the retry, so a re-read that answers
            %% it again is the stale-counter shape do_append/7 reconciles.
            retry_append(StoreId, StreamId, ExpectedVersion, Events, Attempts,
                         CurrentVersion);
        {error, {integrity_life_moved, _}} when Attempts =< 1 ->
            {error, {integrity_life_moved, StreamId}};
        {error, {integrity_life_moved, _}} ->
            %% The stream's life token changed under the batch (a first-append
            %% mint, or a migration), so the MACs it carries are stamped under
            %% a token the tree no longer holds. Nothing was written; re-read
            %% the context and stamp again.
            retry_append(StoreId, StreamId, ExpectedVersion, Events, Attempts,
                         CurrentVersion);
        Other ->
            Other
    end.

%% @private Re-read and append again, after a jittered pause.
retry_append(StoreId, StreamId, ExpectedVersion, Events, Attempts,
             CurrentVersion) ->
    backoff_before_retry(?APPEND_ATTEMPTS - Attempts + 1),
    do_append(StoreId, StreamId, ExpectedVersion, Events, Attempts - 1,
              consistent, CurrentVersion).

%% @private A capped, jittered pause on the lost-race path.
%%
%% Doubles from 1 ms and stops at ?APPEND_BACKOFF_CAP_MS. No second cap on
%% the exponent is needed: the shift overtakes the cap at the seventh
%% attempt and the numbers stay small anyway, the append being capped at
%% ?APPEND_ATTEMPTS.
backoff_before_retry(Attempt) ->
    Ceiling = min(?APPEND_BACKOFF_CAP_MS, 1 bsl (Attempt - 1)),
    timer:sleep(rand:uniform(Ceiling)).

%% @private Repair a version counter that a 5.11.x writer left behind.
%%
%% The counter is exact unless something other than this release wrote the
%% stream, and a rollback is how that happens. Two shapes:
%%
%% - 5.11.x appends past the counter. The version nodes move on - say to 15 -
%%   while the counter stays where this release left it, at 9. Every append is
%%   then built from a version an event already holds, and loses sixty races
%%   against an event the counter cannot see: the stream is stuck for every
%%   caller. The newest node is the truth, so the counter moves up to it.
%% - 5.11.x deletes the stream and re-creates it under the same id. It knows
%%   nothing about the counter, so a fresh tree (versions 0, 1) sits under a
%%   counter of 4, and appending from that would leave a hole at 2-4 and
%%   refuse an explicit version 2. Here the counter's own node is gone and the
%%   tree says less than the counter does, so the counter comes down.
%%
%% A counter that is ahead of the tree because a scavenge removed the nodes is
%% the one case where it is right - keeping it is what this release is for -
%% and it is told apart by its node still being there: a scavenge deletes a
%% prefix, so the newest version survives, while a re-created stream has a
%% tree that does not reach the counter at all.
%%
%% The walk over the version nodes is the expensive half, and an ordinary
%% wrong_expected_version - a caller that named the version another writer
%% took - must not pay for it. Two single-node reads say whether the counter
%% can be stale at all: the node at the counter missing (a tree that a
%% re-creation left short) or the node after it present (a writer that went
%% past the counter). With the counter exact, neither can be true.
-spec reconcile_counter(atom(), binary(), integer()) -> ok | stale | {error, term()}.
reconcile_counter(_StoreId, _StreamId, Counter) when Counter < 0 ->
    %% No counter and no version: there is no number for anything to disagree
    %% with.
    stale;
reconcile_counter(StoreId, StreamId, Counter) ->
    Options = read_options(consistent),
    case counter_looks_stale(StoreId, StreamId, Counter, Options) of
        false ->
            stale;
        true ->
            reconcile_from_tree(StoreId, StreamId, Counter, Options)
    end.

%% @private Whether the tree disagrees with the counter in either direction.
counter_looks_stale(StoreId, StreamId, Counter, Options) ->
    (not version_node_exists(StoreId, StreamId, Counter, Options))
        orelse version_node_exists(StoreId, StreamId, Counter + 1, Options).

%% @private The newest version node is the truth; move the counter to it.
reconcile_from_tree(StoreId, StreamId, Counter, Options) ->
    case version_nodes(StoreId, StreamId, Options) of
        {ok, []} ->
            %% Every event was scavenged: the counter is the only version
            %% left, and it is the right one.
            stale;
        {ok, Versions} ->
            reconcile_to_tree(StoreId, StreamId, Counter, Versions);
        {error, Reason} ->
            %% A tree this node could not read is not an empty one: answering
            %% `stale' would keep a counter nothing had checked.
            {error, {version_unverifiable, Counter, Reason}}
    end.

%% @private The newest live version is the truth: move the counter to it, in
%% either direction.
%%
%% Up is a 5.11.x append that ran past the counter. Down is a stream 5.11.x
%% deleted and re-created, or one it scavenged down from the top: the counter
%% outlived events that are no longer there. The tree is what the stream is;
%% the counter only remembers what this release wrote.
%%
%% A tree with no version node at all is not that shape - reconcile_from_tree
%% answers for it before this - because this release's own scavenge deletes
%% events without renumbering and leaves the counter as the only record of
%% how far the stream ever got.
reconcile_to_tree(StoreId, StreamId, Counter, Versions) ->
    Newest = lists:max(Versions),
    case Newest =:= Counter of
        true  -> stale;
        false -> repair_counter(StoreId, StreamId, Counter, Newest)
    end.

%% @private Move a stale counter to the version the tree says.
%%
%% A compare-and-set on the value this append read: a concurrent writer that
%% has already advanced the counter is left alone, and the retry then reads
%% what that writer put there.
-spec repair_counter(atom(), binary(), non_neg_integer(), non_neg_integer()) ->
    ok | {error, term()}.
repair_counter(StoreId, StreamId, Stale, New) ->
    CounterPath = reckon_db_stream_path:version_counter_path(StreamId),
    Fun = fun() -> repair_counter_tx(CounterPath, Stale, New) end,
    case khepri:transaction(StoreId, Fun) of
        {ok, ok}         -> ok;
        {ok, {error, E}} -> {error, E};
        {error, _} = Err -> Err
    end.

%% @private The transaction body: move the counter only while it still holds
%% the value this append read.
repair_counter_tx(CounterPath, Stale, New) ->
    case khepri_tx:get(CounterPath) of
        {ok, Stale} -> khepri_tx:put(CounterPath, New);
        _           -> ok
    end.

%% @doc Record the version a stream reached, raising a counter that is behind it.
%%
%% The scavenge calls this before it deletes anything. A stream written before
%% this release keeps no counter, so the tree is its only memory of how far it
%% got; delete every version node without leaving a number behind and the
%% next append starts at 0 and reuses versions the stream already used.
%%
%% A counter that is already there is compared, not trusted. A 5.11.x writer
%% kept no counter, so it could append past one, and the scavenge then finds a
%% counter BELOW the newest version it is about to keep. Leaving that number
%% alone leaves the stream reporting a version it has already gone past: the
%% next append compares against 9 while the tree holds 15, and the read that
%% asks from the counter's version misses the events above it. So the number
%% only ever moves up, never down, and the get and the put are one call inside
%% one transaction - a concurrent append that has already raised it further is
%% left alone, and the higher number wins.
-spec ensure_version_counter(atom(), binary(), integer()) -> ok | {error, term()}.
ensure_version_counter(_StoreId, _StreamId, Version) when Version < 0 ->
    %% Nothing was ever appended: there is no version to remember.
    ok;
ensure_version_counter(StoreId, StreamId, Version) ->
    Path = reckon_db_stream_path:version_counter_path(StreamId),
    Fun = fun() -> ensure_version_counter_tx(Path, Version) end,
    case khepri:transaction(StoreId, Fun) of
        {ok, ok}         -> ok;
        {ok, {error, E}} -> {error, E};
        {error, _} = Err -> Err
    end.

%% @private The transaction body: write the counter when it is absent, raise it
%% when it is below the version the scavenge is keeping, never lower it.
%%
%% The comparison and the write are one read-modify-write inside one Ra
%% command, so nothing can slip between them; a counter a concurrent writer has
%% already raised past this version makes the write a no-op.
ensure_version_counter_tx(Path, Version) ->
    case khepri_tx:get(Path) of
        {ok, Current} when is_integer(Current), Current >= Version ->
            ok;
        {ok, Current} when is_integer(Current) ->
            khepri_tx:put(Path, Version);
        {ok, NotAVersion} ->
            {error, {version_unverifiable, Version, {not_a_version, NotAVersion}}};
        {error, ?khepri_error(node_not_found, _)} ->
            khepri_tx:put(Path, Version);
        {error, Reason} ->
            {error, {version_unverifiable, Version, Reason}}
    end.

%% @private Whether one version node is present.
-spec version_node_exists(atom(), binary(), non_neg_integer(),
                          #{favor => consistency}) -> boolean().
version_node_exists(StoreId, StreamId, Version, Options) ->
    Path = reckon_db_stream_path:event_path(StreamId,
                                            pad_version(Version,
                                                        ?VERSION_PADDING)),
    case khepri:get(StoreId, Path, Options) of
        {ok, _}    -> true;
        {error, _} -> false
    end.

%% @private
-spec check_expected_version(integer(), integer()) -> ok | {error, term()}.
check_expected_version(?ANY_VERSION, _CurrentVersion) ->
    ok;
check_expected_version(?NO_STREAM, CurrentVersion) when CurrentVersion =:= ?NO_STREAM ->
    ok;
check_expected_version(?NO_STREAM, CurrentVersion) ->
    {error, {wrong_expected_version, ?NO_STREAM, CurrentVersion}};
check_expected_version(ExpectedVersion, CurrentVersion) when ExpectedVersion =:= CurrentVersion ->
    ok;
check_expected_version(ExpectedVersion, CurrentVersion) ->
    {error, {wrong_expected_version, ExpectedVersion, CurrentVersion}}.

%% @private
-spec append_events_to_stream(atom(), binary(), integer(), boolean(), integer(),
                              [new_event()]) ->
    {ok, non_neg_integer()} | {error, term()}.
append_events_to_stream(StoreId, StreamId, CurrentVersion, CounterExisted,
                        ExpectedVersion, Events) ->
    %% Resolve the tamper-resistance context for this batch. For
    %% stores with integrity disabled (the default and only mode
    %% pre-2.1) this is a constant `disabled` pass-through. For
    %% integrity-enabled stores we load the HMAC key, ensure the
    %% per-stream watermark exists, and resolve the initial chain
    %% tip — all once, before the per-event loop.
    %%
    %% Either step can refuse the batch: a watermark that is not sealed, or
    %% one that sits above the version this append would write, is reported
    %% as {error, _} and nothing is written. Failing the append loudly is the
    %% point - the alternative is stamping a chain onto a stream whose start
    %% the store cannot vouch for.
    case setup_integrity(StoreId, StreamId, CurrentVersion) of
        {error, _} = SetupError ->
            SetupError;
        IntegrityCtx ->
            append_stamped(StoreId, StreamId, CurrentVersion, CounterExisted,
                           ExpectedVersion, Events, IntegrityCtx)
    end.

%% @private The batch, once the integrity context is known.
-spec append_stamped(atom(), binary(), integer(), boolean(), integer(),
                     [new_event()], integrity_ctx()) ->
    {ok, non_neg_integer()} | {error, term()}.
append_stamped(StoreId, StreamId, CurrentVersion, CounterExisted,
               ExpectedVersion, Events, IntegrityCtx) ->
    case resolve_initial_tip(StoreId, StreamId, CurrentVersion + 1,
                             IntegrityCtx) of
        {error, _} = TipError ->
            TipError;
        InitialTip ->
            append_batch(StoreId, StreamId, CurrentVersion, CounterExisted,
                         ExpectedVersion, Events, IntegrityCtx, InitialTip)
    end.

%% @private Build and write the batch.
-spec append_batch(atom(), binary(), integer(), boolean(), integer(),
                   [new_event()], integrity_ctx(),
                   binary() | undefined) ->
    {ok, non_neg_integer()} | {error, term()}.
append_batch(StoreId, StreamId, CurrentVersion, CounterExisted,
             ExpectedVersion, Events, IntegrityCtx, InitialTip) ->
    Now = erlang:system_time(millisecond),
    EpochUs = erlang:system_time(microsecond),

    %% Build the event records AND their secondary-index entries OUTSIDE
    %% the transaction. Integrity stamping (HMAC + chain) reads the HMAC
    %% key from persistent_term and runs crypto — neither is available
    %% inside a Khepri (Ra) transaction fun, which must be deterministic
    %% and side-effect-free. This mirrors the DCB append: stamp outside,
    %% write inside.
    Declared = reckon_db_index_config:declared(StoreId),
    {Writes, IndexEntries, FinalVersion} = build_event_writes(
        StreamId, CurrentVersion, Events, Now, EpochUs, IntegrityCtx,
        InitialTip, Declared),

    %% Write the whole batch — every event record and every index entry —
    %% in ONE transaction. Either all land or none do, so the index is
    %% never left partially populated (a dropped index write would make
    %% queries silently incomplete, worse than slow). One Ra command per
    %% batch.
    %%
    %% The optimistic-version check runs INSIDE the transaction
    %% (append_guard/6): the version read above is a snapshot, and two
    %% concurrent appends that both read it would build the same versions and
    %% one would silently overwrite the other's events.
    %%
    %% A failed transaction (most importantly {error, noproc} while the
    %% store's Ra server is mid-(re)start) MUST surface as a retriable
    %% {error, _} return, NOT a hard `ok =` badmatch (the boot-time
    %% crash-loop lesson the old per-put loop encoded). khepri:transaction
    %% returns {error, _} for a not-ready store; reckon-gater retries.
    case write_batch(StreamId, StoreId, Writes, IndexEntries, FinalVersion,
                     CurrentVersion, CounterExisted, ExpectedVersion,
                     expected_life(IntegrityCtx)) of
        ok ->
            {ok, FinalVersion};
        {error, _} = Error ->
            Error
    end.

%% @private The life token this batch's MACs are stamped under, so the write
%% transaction can refuse a batch whose life is no longer the tree's. Crypto
%% runs outside the transaction, so the life is resolved outside it too; what
%% the guard buys is that it is checked at commit time as well, where a
%% migration or a second first-appender could have replaced it in between.
-spec expected_life(integrity_ctx()) -> binary() | undefined.
expected_life({enabled, _Key, _ChainStart, #{life := Life}}) when is_binary(Life) ->
    Life;
expected_life(_IntegrityCtx) ->
    undefined.

%% @private Build the {Path, Record} event writes and the flat list of
%% {Path, EventRef} index entries for a batch, applying integrity stamping
%% per event. Returns the writes, index entries, and the final version.
-spec build_event_writes(
    binary(), integer(), [new_event()], integer(), integer(),
    integrity_ctx(), binary() | undefined, [index_decl()]
) -> {[{khepri_path:native_path(), event()}],
      [{khepri_path:native_path(), reckon_db_index:event_ref()}],
      non_neg_integer()}.
build_event_writes(StreamId, CurrentVersion, Events, Now, EpochUs,
                   IntegrityCtx, InitialTip, Declared) ->
    {WritesRev, IndexRev, FinalVersion, _FinalTip} = lists:foldl(
        fun(Event, {WAcc, IAcc, AccVersion, AccTip}) ->
            NewVersion = AccVersion + 1,
            RecordedEvent0 = create_event_record(
                Event, StreamId, NewVersion, Now, EpochUs),
            {RecordedEvent, NextTip} = apply_integrity_if_enabled(
                RecordedEvent0, AccTip, IntegrityCtx),
            PaddedVersion = pad_version(NewVersion, ?VERSION_PADDING),
            Path = reckon_db_stream_path:event_path(StreamId, PaddedVersion),
            Entries = reckon_db_index:entries(RecordedEvent, Declared),
            {[{Path, RecordedEvent} | WAcc], Entries ++ IAcc, NewVersion, NextTip}
        end,
        {[], [], CurrentVersion, InitialTip},
        Events),
    {lists:reverse(WritesRev), IndexRev, FinalVersion}.

%% @private Write all event records + index entries + the stream's version
%% counter in one transaction.
-spec write_batch(
    binary(),
    atom(),
    [{khepri_path:native_path(), event()}],
    [{khepri_path:native_path(), reckon_db_index:event_ref()}],
    non_neg_integer(),
    integer(),
    boolean(),
    integer(),
    binary() | undefined
) -> ok | {error, term()}.
write_batch(StreamId, StoreId, Writes, IndexEntries, FinalVersion,
            CurrentVersion, CounterExisted, ExpectedVersion, ExpectedLife) ->
    CounterPath = reckon_db_stream_path:version_counter_path(StreamId),
    PredecessorPath = predecessor_path(StreamId, CurrentVersion),
    Fun = fun() ->
                  write_batch_tx(Writes, IndexEntries, CounterPath,
                                 PredecessorPath, FinalVersion, CurrentVersion,
                                 CounterExisted, ExpectedVersion,
                                 {StreamId, ExpectedLife})
          end,
    %% There is no bare `ok' clause: khepri:transaction/2 answers {ok, _} or
    %% {error, _}, and the dead clause that matched `ok' was what made
    %% dialyzer call the match impossible.
    case khepri:transaction(StoreId, Fun) of
        {ok, ok}            -> ok;
        %% Khepri 0.17.x catch-all wraps process_command errors as {ok, Err}
        %% when Ra is not yet ready (e.g. store startup race).
        {ok, {error, E}}    -> {error, E};
        {error, _} = Error  -> Error
    end.

%% @private The body of the write-batch transaction.
write_batch_tx(Writes, IndexEntries, CounterPath, PredecessorPath, FinalVersion,
               CurrentVersion, CounterExisted, ExpectedVersion,
               {StreamId, ExpectedLife}) ->
    ok = append_guard(CounterPath, PredecessorPath, CurrentVersion,
                      CounterExisted, hd(Writes), ExpectedVersion),
    ok = guard_life(StreamId, ExpectedLife),
    lists:foreach(fun put_tx_entry/1, Writes),
    lists:foreach(fun put_tx_entry/1, IndexEntries),
    %% The version the batch ended on, in the same Ra command as the
    %% events: a counter that could drift from the tree would reintroduce
    %% exactly the overwrite this replaces.
    ok = khepri_tx:put(CounterPath, FinalVersion),
    bump_global_event_count(length(Writes)),
    ok.

%% @private The life the batch stamped under is still the stream's life.
%%
%% Resolved outside the transaction (the MACs need it before the write), so it
%% has to be checked inside: a first-append mint or a migration that lands in
%% between replaces the token, and the events already stamped under the old one
%% would never verify again. Aborting is honest - nothing is written, and the
%% caller re-reads the context and retries.
guard_life(_StreamId, undefined) ->
    ok;
guard_life(StreamId, ExpectedLife) ->
    case khepri_tx:get(reckon_db_chain_watermark:life_path(StreamId)) of
        {ok, ExpectedLife} ->
            ok;
        _ ->
            khepri_tx:abort({integrity_life_moved, StreamId})
    end.

put_tx_entry({P, Record}) ->
    ok = khepri_tx:put(P, Record).

%% @private Refuse the batch if the stream moved since the version was read.
%%
%% Three checks, because there are three ways a version can be taken:
%%
%%   - the counter is not the one this batch was built from;
%%   - the version node the batch was built from is gone, which is what a
%%     stream with no counter has instead of a number to compare;
%%   - the first version node this batch would write already exists.
%%
%% The second catches a counter-less stream deleted between the read and the
%% write: the target node is free, so the third check is happy, and without
%% the second the batch re-creates the stream at N+1 under a caller that read
%% N - which is not an empty stream, it is a different one. It is also the
%% only evidence there is for a stream a store wrote before this release
%% had a counter at all.
%%
%% Aborting makes `khepri:transaction' answer `{error, {version_changed,
%% Actual}}' for the caller to retry from; nothing is written. It MUST abort
%% here rather than return an error tuple, because a transaction fun's return
%% value is an ordinary result, not an error channel: the tuple would come
%% back as a SUCCESSFUL transaction carrying `{error, _}', and the `ok =' on it
%% one level up turns that into a badmatch. `khepri_tx:abort/1' raises
%% instead, and `khepri_tx_adv:run/5' catches it and hands back the state from
%% before the transaction.
append_guard(CounterPath, PredecessorPath, CurrentVersion, CounterExisted,
             {FirstPath, _}, ExpectedVersion) ->
    case counter_matches(CounterPath, CurrentVersion, CounterExisted) of
        ok ->
            ok = guard_predecessor_present(PredecessorPath, CurrentVersion,
                                           CounterExisted),
            guard_first_path_free(FirstPath, CounterPath, ExpectedVersion);
        {error, Reason} ->
            khepri_tx:abort(Reason)
    end.

%% @private The version node the batch was built from is still there.
%%
%% Only asked when there is no counter to compare: with one, counter_matches/3
%% has already said whether the stream moved, and a counter it found absent is
%% the deleted-stream answer. A new stream has no predecessor, so there is
%% nothing to look at.
guard_predecessor_present(_PredecessorPath, _CurrentVersion, true) ->
    ok;
guard_predecessor_present(undefined, _CurrentVersion, false) ->
    ok;
guard_predecessor_present(PredecessorPath, CurrentVersion, false) ->
    case khepri_tx:get(PredecessorPath) of
        {ok, _} -> ok;
        {error, Reason} ->
            khepri_tx:abort({version_unverifiable, CurrentVersion, Reason})
    end.

%% @private The node the batch was built from, or undefined for a new stream.
%%
%% Resolved OUTSIDE the transaction: a transaction fun may not call
%% reckon_db_stream_path (horus walks the call graph and rejects the binary
%% library calls underneath it), which is why the path travels in with the
%% writes.
predecessor_path(_StreamId, ?NO_STREAM) ->
    undefined;
predecessor_path(StreamId, CurrentVersion) ->
    reckon_db_stream_path:event_path(StreamId,
                                     pad_version(CurrentVersion, ?VERSION_PADDING)).

%% @private
counter_matches(CounterPath, CurrentVersion, CounterExisted) ->
    counter_verdict(in_tx_counter_value(CounterPath), CurrentVersion, CounterExisted).

%% @private The counter as the transaction sees it: {ok, N}, absent, or the
%% error the read answered.
%%
%% Only `node_not_found' means "not there". Any other read error is a store
%% that cannot answer, and folding it into `absent' would read as "the stream
%% was deleted" - a `version_changed' the append retries sixty times before
%% answering `version_conflict', which is the wrong answer to a store that is
%% not serving.
in_tx_counter_value(CounterPath) ->
    case khepri_tx:get(CounterPath) of
        {ok, InTx} when is_integer(InTx) -> {ok, InTx};
        {error, ?khepri_error(node_not_found, _)} -> absent;
        {error, Reason} -> {unreadable, Reason};
        _ -> absent
    end.

%% @private
counter_verdict({ok, CurrentVersion}, CurrentVersion, true) ->
    ok;
counter_verdict({ok, InTx}, _CurrentVersion, _CounterExisted) ->
    {error, {version_changed, InTx}};
counter_verdict({unreadable, Reason}, CurrentVersion, _CounterExisted) ->
    {error, {version_unverifiable, CurrentVersion, Reason}};
counter_verdict(absent, _CurrentVersion, true) ->
    %% The counter is gone: the stream was deleted under us.
    {error, {version_changed, ?NO_STREAM}};
counter_verdict(absent, _CurrentVersion, false) ->
    %% Legacy stream, still without a counter: the node check below is the
    %% only guard there is.
    ok.

%% @private
%%
%% Only node_not_found means "free": any other read error is a store that
%% cannot answer, and reading that as an empty node would let the batch write
%% over whatever is there.
guard_first_path_free(FirstPath, CounterPath, ExpectedVersion) ->
    case khepri_tx:get(FirstPath) of
        {error, ?khepri_error(node_not_found, _)} ->
            ok;
        {ok, _} ->
            khepri_tx:abort({version_changed,
                             in_tx_counter(CounterPath, ExpectedVersion)});
        {error, Reason} ->
            khepri_tx:abort({version_unverifiable, ExpectedVersion, Reason})
    end.

%% @private
in_tx_counter(CounterPath, ExpectedVersion) ->
    case khepri_tx:get(CounterPath) of
        {ok, InTx} when is_integer(InTx) -> InTx;
        _ -> ExpectedVersion
    end.

%% @private Increment the store's monotonic global event counter by the
%% number of events in this batch, inside the append transaction. Same Ra
%% command as the writes, so it is exact and adds no extra round-trip
%% (mirrors the DCB sequence counter). Read back O(1) via
%% global_event_count/1.
bump_global_event_count(0) ->
    ok;
bump_global_event_count(N) ->
    Cur = case khepri_tx:get(?GLOBAL_EVENT_COUNT_PATH) of
              {ok, V} when is_integer(V) -> V;
              _                          -> 0
          end,
    ok = khepri_tx:put(?GLOBAL_EVENT_COUNT_PATH, Cur + N).

%% @private Tamper-resistance context for a single append batch.
%%
%% Either `disabled` (no integrity work) or
%% `{enabled, Key, ChainStart, MacCtx}` carrying the HMAC key, the
%% chain-start watermark for this stream.
-type integrity_ctx() ::
    disabled |
    {enabled, Key :: binary(), ChainStart :: non_neg_integer(),
     MacCtx :: #{store_id := atom(), life := binary()}}.

-spec setup_integrity(atom(), binary(), integer()) ->
    integrity_ctx()
        | {error, integrity_refusal()}.
setup_integrity(StoreId, StreamId, CurrentVersion) ->
    case reckon_db_integrity_key:is_enabled(StoreId) of
        false ->
            disabled;
        true ->
            setup_integrity_enabled(StoreId, StreamId, CurrentVersion + 1)
    end.

%% @private The watermark, or the reason this batch cannot be stamped.
%%
%% An unsealed watermark is an error and is left alone: only `seal_all/1'
%% seals, as an upgrade step the operator runs deliberately. An append that
%% sealed whatever bare integer it found would launder a forged chain start
%% into an authenticated one, and every event below it would read as legacy.
%%
%% A store with integrity enabled and no key cannot stamp anything either;
%% that is a configuration fault, answered rather than crashed on.
-spec setup_integrity_enabled(atom(), binary(), non_neg_integer()) ->
    {enabled, binary(), non_neg_integer(), map()} | {error, integrity_refusal()}.
setup_integrity_enabled(StoreId, StreamId, NextVersion) ->
    case reckon_db_chain_watermark:set_if_absent(StoreId, StreamId, NextVersion) of
        {ok, ChainStart} ->
            record_minted_life(StoreId, StreamId, NextVersion),
            enabled_ctx(StoreId, StreamId, ChainStart);
        {error, _} = Error ->
            Error
    end.

%% The ways a batch can be refused before anything is written.
-type integrity_refusal() ::
    {chain_start_unauthenticated, non_neg_integer()} |
    {chain_start_missing, binary()} |
    {life_missing, binary()} |
    {integrity_enabled_without_key, atom()}.

%% @private The node-local anchor's record of a stream's life, written when a
%% NEW stream is opened (the only append that mints).
%%
%% The life and generation are read back from the TREE, after the mint
%% transaction committed - never taken from the caller's own candidate, which
%% may be the loser of a create race. Recording a loser's read is harmless:
%% both creators read the winner's record.
record_minted_life(_StoreId, _StreamId, NextVersion) when NextVersion =/= 0 ->
    ok;
record_minted_life(StoreId, StreamId, 0) ->
    case reckon_db_chain_watermark:binding(StoreId, StreamId) of
        {ok, #{life := Life, gen := Gen}} when is_binary(Life) ->
            reckon_db_life_anchor:record_mint(StoreId, StreamId, Gen, Life);
        _ ->
            ok
    end.

%% @private The append context, or the reason this batch cannot be stamped.
%%
%% A sealed watermark with no life token is refused, never stamped scheme 1:
%% the stream is defined as scheme-2-from-its-chain-start, so a store-blind
%% MAC above that point is the exact shape the scheme-1 bound exists to
%% refuse. Minting writes both nodes together; a missing one means the tree
%% was damaged or edited, and the append says so rather than papering over it.
-spec enabled_ctx(atom(), binary(), non_neg_integer()) ->
    {enabled, Key :: binary(), ChainStart :: non_neg_integer(),
     MacCtx :: #{store_id := atom(), life := binary()}}
        | {error, {integrity_enabled_without_key, atom()}
                 | {life_missing, binary()}}.
enabled_ctx(StoreId, StreamId, ChainStart) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            append_ctx(StoreId, StreamId, Key, ChainStart);
        _ ->
            {error, {integrity_enabled_without_key, StoreId}}
    end.

%% @private
append_ctx(StoreId, StreamId, Key, ChainStart) ->
    case reckon_db_chain_watermark:life(StoreId, StreamId) of
        {ok, Life} when is_binary(Life) ->
            {enabled, Key, ChainStart, #{store_id => StoreId, life => Life}};
        _ ->
            {error, {life_missing, StreamId}}
    end.

%% @private Resolve the chain-tip value that the FIRST event in this
%% batch must reference as its `prev_event_hash`.
%%
%% Disabled context: tip is `undefined` (unused).
%% Enabled, first integrity event in stream (NextVersion =:= ChainStart):
%%   tip is the genesis 32-zero-byte value.
%% Enabled, later batch (NextVersion > ChainStart):
%%   tip is computed from the predecessor event on disk.
%%
%% A watermark ABOVE the version this batch would write is an error, not a
%% crash: the watermark is a tree node, and one written past the stream by
%% hand (or sealed there by an operator) must fail the append with a reason
%% the caller can report, not take the gateway worker down with it.
-spec resolve_initial_tip(
    atom(), binary(), non_neg_integer(), integrity_ctx()
) -> binary() | undefined | {error, term()}.
resolve_initial_tip(_StoreId, _StreamId, _NextVersion, disabled) ->
    undefined;
resolve_initial_tip(_StoreId, _StreamId, NextVersion,
                    {enabled, _Key, ChainStart, _MacCtx})
        when NextVersion < ChainStart ->
    {error, {chain_start_ahead_of_append,
             #{next_version => NextVersion, chain_start => ChainStart}}};
resolve_initial_tip(_StoreId, _StreamId, NextVersion,
                    {enabled, _Key, ChainStart, _MacCtx})
        when NextVersion =:= ChainStart ->
    reckon_gater_integrity:genesis_prev_hash();
resolve_initial_tip(StoreId, StreamId, NextVersion,
                    {enabled, _Key, ChainStart, _MacCtx})
        when NextVersion > ChainStart ->
    PrevVersion = NextVersion - 1,
    PaddedVersion = pad_version(PrevVersion, ?VERSION_PADDING),
    Path = reckon_db_stream_path:event_path(StreamId, PaddedVersion),
    case khepri:get(StoreId, Path) of
        {ok, #event{prev_event_hash = PrevPrevHash} = PrevEvent}
                when is_binary(PrevPrevHash) ->
            reckon_gater_integrity:compute_chain_hash(PrevEvent, PrevPrevHash);
        Other ->
            %% Invariant violation: stream's watermark says the
            %% predecessor should be integrity-bearing, but we cannot
            %% find a usable predecessor. Answered as an error, so the
            %% caller reports it instead of the append dying mid-way.
            {error, {integrity_setup_failed,
                     #{stream_id => StreamId,
                       looking_for_version => PrevVersion,
                       chain_start => ChainStart,
                       got => Other}}}
    end.

%% @private Compute and attach the integrity fields for one event.
%%
%% Disabled context: pass-through.
%% Enabled: set prev_event_hash to the running tip, compute MAC, then
%% compute the next tip (= chain hash of the just-built event) for the
%% next iteration of the fold.
-spec apply_integrity_if_enabled(
    event(),
    Tip :: binary() | undefined,
    integrity_ctx()
) -> {event(), NextTip :: binary() | undefined}.
apply_integrity_if_enabled(Event, _Tip, disabled) ->
    {Event, undefined};
apply_integrity_if_enabled(#event{} = Event, Tip,
                           {enabled, Key, _ChainStart, MacCtx})
        when is_binary(Tip) ->
    Event1 = Event#event{prev_event_hash = Tip},
    Mac = reckon_gater_integrity:compute_event_mac(Event1, Key, MacCtx),
    Event2 = Event1#event{mac = Mac},
    NextTip = reckon_gater_integrity:compute_chain_hash(Event2, Tip),
    {Event2, NextTip}.

%% @private
-spec create_event_record(new_event(), binary(), non_neg_integer(), integer(), integer()) -> event().
create_event_record(Event, StreamId, Version, Timestamp, EpochUs) ->
    EventId = maps:get(event_id, Event, generate_event_id()),
    EventType = maps:get(event_type, Event),
    Data = maps:get(data, Event),
    Metadata = maps:get(metadata, Event, #{}),
    Tags = maps:get(tags, Event, undefined),
    DataContentType = maps:get(data_content_type, Event, ?CONTENT_TYPE_JSON),
    MetadataContentType = maps:get(metadata_content_type, Event, ?CONTENT_TYPE_JSON),

    #event{
        event_id = EventId,
        event_type = EventType,
        stream_id = StreamId,
        version = Version,
        data = Data,
        metadata = Metadata,
        tags = Tags,
        timestamp = Timestamp,
        epoch_us = EpochUs,
        data_content_type = DataContentType,
        metadata_content_type = MetadataContentType
    }.

%% @private
-spec generate_event_id() -> binary().
generate_event_id() ->
    reckon_gater_uuid:to_string(reckon_gater_uuid:v7()).

%% @private
-spec pad_version(non_neg_integer(), pos_integer()) -> binary().
pad_version(Version, Length) ->
    VersionStr = integer_to_list(Version),
    Padding = Length - length(VersionStr),
    PaddedStr = lists:duplicate(Padding, $0) ++ VersionStr,
    list_to_binary(PaddedStr).

%% @private Backward-compatible: no verification (for internal callers
%% that have not yet adopted Opts).
-spec do_read(atom(), binary(), non_neg_integer(), pos_integer(), direction()) ->
    {ok, [event()]} | {error, term()}.
do_read(StoreId, StreamId, StartVersion, Count, Direction) ->
    do_read_with_verify(
        StoreId, StreamId, StartVersion, Count, Direction, #{verify => skip_all}).

%% @private Read with verification applied per the resolved options.
-spec do_read_with_verify(
    atom(), binary(), non_neg_integer(), pos_integer(), direction(), read_opts()
) -> {ok, [event()]} | {error, term()}.
do_read_with_verify(StoreId, StreamId, StartVersion, Count, Direction, Opts) ->
    read_if_stream_exists(exists(StoreId, StreamId),
                          StoreId, StreamId, StartVersion, Count, Direction, Opts).

read_if_stream_exists(false, _StoreId, StreamId, _StartVersion, _Count, _Direction, _Opts) ->
    {error, {stream_not_found, StreamId}};
read_if_stream_exists(true, StoreId, StreamId, StartVersion, Count, Direction, Opts) ->
    verify_read_result(read_events(StoreId, StreamId, StartVersion, Count, Direction),
                       StoreId, StreamId, StartVersion, Direction, Opts).

verify_read_result({ok, Events}, StoreId, StreamId, StartVersion, Direction, Opts) ->
    %% Wrap at the public API boundary so callers see {error, _}; internal
    %% helpers and the gater module return the bare integrity tuple.
    wrap_verification(
        maybe_verify_events(StoreId, StreamId, StartVersion, Direction, Events, Opts)).

wrap_verification({ok, _} = Ok) -> Ok;
wrap_verification({integrity_violation, _} = Violation) -> {error, Violation}.

%% @private Apply the configured verification mode to a fresh result.
%%
%% Backward reads verify the same chain as forward reads by walking
%% the result in forward order (smallest version first) and then
%% reversing the returned list to preserve the caller's requested
%% ordering. The chain semantics are direction-independent: every
%% event's `prev_event_hash` must equal the chain hash of its
%% predecessor, regardless of the order the caller chose to receive
%% them in.
-spec maybe_verify_events(
    atom(), binary(), non_neg_integer(), direction(), [event()], read_opts()
) -> {ok, [event()]} | {integrity_violation, map()}.
maybe_verify_events(StoreId, StreamId, StartVersion, Direction, Events, Opts) ->
    Mode = maps:get(verify, Opts, skip_legacy),
    case verify_required(StoreId, Direction, Mode) of
        false ->
            {ok, Events};
        true ->
            verify_in_direction(
                StoreId, StreamId, StartVersion, Direction, Events, Mode)
    end.

%% @private Verification runs on integrity-enabled stores regardless
%% of read direction. The verification surface is the same in both
%% directions — the only difference is the result-ordering of the
%% returned events.
-spec verify_required(atom(), direction(), verify_mode()) -> boolean().
verify_required(_StoreId, _Direction, skip_all) -> false;
verify_required(StoreId, _Direction, _Mode) ->
    reckon_db_integrity_key:is_enabled(StoreId).

%% @private Dispatch on direction: forward verifies in place; backward
%% reverses to forward order, verifies, and reverses the result back.
-spec verify_in_direction(
    atom(), binary(), non_neg_integer(), direction(), [event()], verify_mode()
) -> {ok, [event()]} | {integrity_violation, map()}.
verify_in_direction(StoreId, StreamId, StartVersion, forward, Events, Mode) ->
    verify_events_forward(StoreId, StreamId, StartVersion, Events, Mode);
verify_in_direction(StoreId, StreamId, StartVersion, backward, Events, Mode) ->
    %% Backward reads return events highest-version-first. To verify
    %% the chain we re-order them lowest-version-first, walk the
    %% forward verifier, then reverse the verified list before
    %% returning so the caller gets the ordering they asked for.
    %% The forward verifier's `StartVersion` is the LOWEST version in
    %% the batch, which for a backward read is the LAST event.
    ForwardEvents = lists:reverse(Events),
    ForwardStart = case ForwardEvents of
        [] -> StartVersion;
        [#event{version = V} | _] -> V
    end,
    case verify_events_forward(
            StoreId, StreamId, ForwardStart, ForwardEvents, Mode) of
        {ok, Verified} ->
            {ok, lists:reverse(Verified)};
        {integrity_violation, _} = Violation ->
            Violation
    end.

%% @private Walk events in forward order, verifying each against the
%% running chain tip. Short-circuits on the first integrity_violation.
%%
%% What is verified is decided by the events themselves, never by the
%% chain-start watermark. The watermark is a plain tree node, so a writer who
%% can delete one event can also delete it, or move it above the events they
%% want included: under skip_legacy every event below the watermark is handed
%% back unverified, and one khepri write would undo the whole chain. An event
%% that carries a `prev_event_hash' is integrity-bearing whatever the
%% watermark says, and it is verified.
-spec verify_events_forward(
    atom(), binary(), non_neg_integer(), [event()], verify_mode()
) -> {ok, [event()]} | {integrity_violation, map()}.
verify_events_forward(StoreId, StreamId, StartVersion, Events, Mode) ->
    {ok, Watermark} = reckon_db_chain_watermark:lookup(StoreId, StreamId),
    case mac_context(StoreId, StreamId) of
        {ok, Key, Ctx} ->
            verify_with_mac_context(StoreId, StreamId, StartVersion, Events,
                                    Mode, Watermark, Key, Ctx);
        error ->
            integrity_key_absent(StreamId, StartVersion)
    end.

%% @private
verify_with_mac_context(StoreId, StreamId, StartVersion, Events, Mode,
                        Watermark, Key, Ctx) ->
    case legacy_limit(StoreId, StreamId, Watermark, Events) of
        {error, Violation} ->
            {integrity_violation, Violation};
        {ok, Limit} ->
            life_anchor_then_region(StoreId, StreamId, StartVersion, Events,
                                    Mode, Key, Ctx, Limit)
    end.

%% @private The node-local life anchor first, then whatever the batch needs.
-spec life_anchor_then_region(atom(), binary(), non_neg_integer(), [event()],
                              verify_mode(), term(), term(),
                              non_neg_integer() | undefined) ->
        {ok, [event()]} | {integrity_violation, map()}.
life_anchor_then_region(StoreId, StreamId, StartVersion, Events, Mode, Key,
                        Ctx, Limit) ->
    case life_anchor_verdict(StoreId, StreamId, Ctx) of
        ok ->
            verify_legacy_region(StoreId, StreamId, StartVersion, Events, Mode,
                                 Key, Ctx, Limit);
        {integrity_violation, _} = Violation ->
            Violation
    end.

%% @private The generation this node recorded for the stream, against the one
%% its seal carries now.
%%
%% This is the whole rollback guard, and it is one ETS lookup: a life restored
%% from an older copy carries an older generation, and a generation is bound
%% into the seal's MAC, so it cannot be raised. Where the anchor knows nothing
%% about the stream, it adopts what the tree says (trust on first use, named in
%% #18) and the read goes on.
%% The generation is read with a 0 default: a stream with no seal has none, and
%% a map type that names only the keys this function requires is what the caller
%% can be held to.
-type life_anchor_refusal() ::
    #{layer := storage,
      stream_id := binary(),
      version := non_neg_integer(),
      kind := missing_integrity,
      context := #{detail := life_absent_in_tree
                             | life_anchor_unavailable
                             | life_gen_deleted
                             | life_gen_mismatch
                             | life_gen_rolled_back,
                   recorded_gen := non_neg_integer()}}.

-spec life_anchor_verdict(atom(), binary(),
                          #{store_id := atom(), life => binary(),
                            v2_from => non_neg_integer()}) ->
        ok | {integrity_violation, life_anchor_refusal()}.
life_anchor_verdict(StoreId, StreamId, Ctx) ->
    Gen = maps:get(gen, Ctx, 0),
    Life = maps:get(life, Ctx, undefined),
    case reckon_db_life_anchor:check(StoreId, StreamId, Gen, Life) of
        ok -> ok;
        {refuse, Violation} -> {integrity_violation, Violation}
    end.

%% @private The newest version the TREE holds, never the counter.
%%
%% The legacy-region walk below still needs a newest bound, and it takes it
%% from the version nodes rather than the counter: a counter put above a hole
%% (delete the event at 4, set the counter to 3) answered a version the walk
%% then started from, so the live events past the hole were never read. The
%% read path no longer calls this - the anchor's O(1) generation check replaced
%% it - but this walk is O(suffix) by construction and only runs for a
%% legacy or migrated region.
-spec newest_live_version(atom(), binary()) -> {ok, integer()} | {error, term()}.
newest_live_version(StoreId, StreamId) ->
    get_version_from_version_nodes(StoreId, StreamId, #{}).

%% @private The legacy region has to be explainable before anything in it is
%% handed back unverified.
verify_legacy_region(StoreId, StreamId, StartVersion, Events, Mode, Key, Ctx,
                     Limit) ->
    case honouring_legacy(Events, Limit) of
        false ->
            verify_from_anchor(StoreId, StreamId, StartVersion, Events, Mode,
                               Key, Ctx, Limit);
        true ->
            verify_explained_region(StoreId, StreamId, StartVersion, Events,
                                    Mode, Key, Ctx, Limit)
    end.

%% @private
verify_explained_region(StoreId, StreamId, StartVersion, Events, Mode, Key, Ctx,
                        Limit) ->
    case legacy_region_ok(StoreId, StreamId, Limit) of
        ok ->
            verify_explained_suffix(StoreId, StreamId, StartVersion, Events,
                                    Mode, Key, Ctx, Limit);
        {violation, Violation} ->
            {integrity_violation, Violation}
    end.

%% @private The store has explained the region below the seal, so it may serve
%% it - but only having verified the events this read did NOT ask for, from the
%% seal up to the newest one, exactly as a full read verifies them.
%%
%% Without that, a targeted read below the seal was blind to a newer event that
%% contradicts the seal, which a full read refuses: a restored older life's
%% seal, life token AND the event at the seal all agree with each other, and
%% only the new-life events above them disagree (Jupiter r18 JR7). Raf's
%% decision for round 19: a targeted read that touches a legacy or migrated
%% region carries the same guarantee as a full read, at O(suffix) cost, rather
%% than a weaker one.
verify_explained_suffix(StoreId, StreamId, StartVersion, Events, Mode, Key, Ctx,
                        Limit) ->
    {ok, Tail} = suffix_from(StoreId, StreamId, Limit),
    verify_from_anchor_then(StoreId, StreamId, StartVersion, Events, Mode, Key,
                            Ctx, Limit, Tail).

%% @private
verify_from_anchor_then(StoreId, StreamId, StartVersion, Events, Mode, Key, Ctx,
                        Limit, Tail) ->
    case verify_from_anchor(StoreId, StreamId, Limit, Tail, Mode, Key, Ctx,
                            Limit) of
        {ok, _SuffixVerified} ->
            verify_from_anchor(StoreId, StreamId, StartVersion, Events, Mode,
                               Key, Ctx, Limit);
        {integrity_violation, _} = Violation ->
            Violation
    end.

%% @private Every event from the seal to the newest one, in order.
%%
%% The newest version comes from the version nodes, never the counter: a
%% counter put above a hole answers a version the walk then starts from, and
%% the live events past the hole are never read, so the walk that exists to
%% cover them covers nothing (Terra r19 T19-1).
-spec suffix_from(atom(), binary(), non_neg_integer()) -> {ok, [event()]}.
suffix_from(StoreId, StreamId, Limit) ->
    case newest_live_version(StoreId, StreamId) of
        {ok, Newest} when is_integer(Newest), Newest >= Limit ->
            read_events(StoreId, StreamId, Limit, Newest - Limit + 1, forward);
        _ ->
            {ok, []}
    end.

%% @private Is this batch about to hand back an event it treats as legacy?
-spec honouring_legacy([event()], non_neg_integer() | undefined) -> boolean().
honouring_legacy(Events, Limit) ->
    lists:any(fun(Event) -> is_legacy(Event, Limit) end, Events).

%% @private Is there something the store wrote that explains the region below
%% `Limit'?
%%
%% There are exactly two explanations, and a REPLAYED watermark produces
%% neither. The event AT the limit carries the genesis predecessor, and its MAC
%% holds, when the limit is a chain start this stream really began at. A
%% verified scavenge boundary there says the prefix was REMOVED - and then
%% nothing may survive below it at all. Jupiter r15 JP3: an earlier life's
%% sealed record replayed over a new life lands on a MID-CHAIN event of that
%% life, whose predecessor is a chain hash, and with no boundary there either -
%% so no legacy region may be honoured below it.
%%
%% When there is no event at the limit (an honest chain-less stream is sealed
%% at newest + 1, so this is legal), the stream must not carry a chain at all:
%% a chain-bearing newest event under a limit nothing sits at is the same
%% replay over a shorter life.
legacy_region_ok(_StoreId, _StreamId, undefined) ->
    ok;
legacy_region_ok(StoreId, StreamId, Limit) ->
    case event_at_version(StoreId, StreamId, Limit) of
        {ok, Event} -> at_limit_verdict(StoreId, StreamId, Event, Limit);
        missing -> newest_verdict(StoreId, StreamId, Limit)
    end.

%% @private
at_limit_verdict(StoreId, StreamId, Event, Limit) ->
    case boundary_at_limit(StoreId, StreamId, Limit) of
        true ->
            %% A boundary records a prefix the scavenge REMOVED, so nothing
            %% honest survives below it - and this read is serving something
            %% that does: planted records, or a prefix put back (Terra r18
            %% T18-1). The two cannot both be true.
            {violation, survivors_below_boundary(StreamId, Limit)};
        false ->
            genesis_region_verdict(StoreId, StreamId, Event, Limit)
    end.

%% @private The other explanation, and it is about the EVENT, not a record
%% beside it: the event at the seal carries the genesis predecessor and its
%% MAC holds under the stream's current life. The field alone is tree data
%% (Jupiter r16 JR5). The MAC alone is not the whole answer either, because a
%% seal, a life token and the event at the seal can be restored as a set
%% (Jupiter r18 JR7) - which is why the caller also verifies the suffix.
genesis_region_verdict(StoreId, StreamId, Event, Limit) ->
    case is_genesis_event(Event) andalso
         event_verifies_at(StoreId, StreamId, Event,
                           reckon_gater_integrity:genesis_prev_hash()) of
        true -> ok;
        false -> {violation, legacy_region_unexplained(StreamId, Limit)}
    end.

%% @private Is a verified scavenge boundary recorded at exactly the limit?
boundary_at_limit(StoreId, StreamId, Limit) ->
    case reckon_db_scavenge_boundary:lookup(StoreId, StreamId) of
        {ok, #{version := Limit} = Boundary} ->
            reckon_db_scavenge_boundary:verify(StoreId, StreamId, Boundary);
        _ ->
            false
    end.

%% @private One MAC, with the context the event's own scheme needs.
event_verifies_at(StoreId, StreamId, Event, ExpectedPrev) ->
    case mac_context(StoreId, StreamId) of
        {ok, Key, Ctx} ->
            reckon_gater_integrity:verify_event(Event, ExpectedPrev, Key, Ctx)
                =:= ok;
        error ->
            false
    end.

%% @private
newest_verdict(StoreId, StreamId, Limit) ->
    case newest_event(StoreId, StreamId) of
        {ok, #event{prev_event_hash = PrevHash}} when is_binary(PrevHash) ->
            {violation, legacy_region_unexplained(StreamId, Limit)};
        _ ->
            ok
    end.

%% @private
-spec is_genesis_event(event()) -> boolean().
is_genesis_event(#event{prev_event_hash = PrevHash}) ->
    PrevHash =:= reckon_gater_integrity:genesis_prev_hash().

%% @private
-spec event_at_version(atom(), binary(), non_neg_integer()) -> {ok, event()} | missing.
event_at_version(StoreId, StreamId, Version) ->
    predecessor_event(StoreId, StreamId, Version).

%% @private The newest event the tree holds. The version comes from the version
%% nodes and never from the counter, for the same reason suffix_from/3 does:
%% this decides whether a region below a seal may be served at all, and a
%% counter put above a hole would answer "no chain-bearing newest event" while
%% five of them sit in the tree.
-spec newest_event(atom(), binary()) -> {ok, event()} | missing.
newest_event(StoreId, StreamId) ->
    case newest_live_version(StoreId, StreamId) of
        {ok, ?NO_STREAM} -> missing;
        {ok, Version} when is_integer(Version), Version >= 0 ->
            predecessor_event(StoreId, StreamId, Version);
        _ -> missing
    end.

%% @private
legacy_region_unexplained(StreamId, Limit) ->
    #{layer => storage,
      stream_id => StreamId,
      version => Limit,
      kind => missing_integrity,
      context => #{detail => legacy_region_unexplained, limit => Limit}}.

%% @private A scavenge boundary says the prefix was removed; events are there.
survivors_below_boundary(StreamId, Limit) ->
    #{layer => storage,
      stream_id => StreamId,
      version => Limit,
      kind => missing_integrity,
      context => #{detail => survivors_below_boundary, limit => Limit}}.

%% @private Integrity is enabled but the store has no key, so nothing this
%% read returns can be verified. Refusing is the only answer that does not
%% serve unverified bytes as ok.
integrity_key_absent(StreamId, StartVersion) ->
    {integrity_violation, #{
        layer => storage,
        stream_id => StreamId,
        version => StartVersion,
        kind => missing_integrity,
        context => #{detail => integrity_key_absent}
    }}.

%% @private What a scheme-2 MAC needs: the key, and - when the stream has a
%% sealed watermark - the store, the life token and the version scheme 2
%% begins at. A stream with no sealed watermark carries no scheme-2 events
%% the store would serve, so the context is just the store. With no key there
%% is no context at all: the caller refuses to verify rather than pass a
%% non-binary through to gater, which has no clause for it.
%%
%% The key travels beside the map, never inside it: gater's mac_context() is
%% a closed map over store_id, life and v2_from, and an extra key in the map
%% is not a subtype of it.
-spec mac_context(atom(), binary()) ->
        {ok, Key :: binary(),
         Ctx :: #{store_id := atom(), life => binary(),
                  v2_from => non_neg_integer()}}
        | error.
mac_context(StoreId, StreamId) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            {ok, Key, mac_context_fields(StoreId, StreamId)};
        _ ->
            error
    end.

%% @private
mac_context_fields(StoreId, StreamId) ->
    Base = #{store_id => StoreId},
    case reckon_db_chain_watermark:binding(StoreId, StreamId) of
        {ok, #{life := Life, v2_from := V2From, gen := Gen}} ->
            Base#{life => Life, v2_from => V2From, gen => Gen};
        _ ->
            %% No binding, or a tree that would not answer: with no scheme-2
            %% context a scheme-2 MAC cannot verify, so the read refuses.
            Base
    end.

%% @private
verify_from_anchor(StoreId, StreamId, StartVersion, Events, Mode, Key, Ctx,
                   Limit) ->
    case guard_page_start(StoreId, StreamId, Events) of
        {violation, Violation} ->
            {integrity_violation, Violation};
        ok ->
            verify_anchored_batch(StoreId, StreamId, StartVersion, Events, Mode,
                                  Key, Ctx, Limit)
    end.

%% @private
verify_anchored_batch(StoreId, StreamId, StartVersion, Events, Mode, Key, Ctx,
                      Limit) ->
    case batch_anchor(StoreId, StreamId, Events, StartVersion) of
        {error, Violation} ->
            {integrity_violation, Violation};
        {ok, Anchor} ->
            verify_events_loop(Events, Anchor, Limit, Key, Ctx, Mode, StreamId,
                               [])
    end.

%% @private A chain-less first event whose PREDECESSOR carries a chain.
%%
%% That order cannot happen honestly: the writer stamps every event above a
%% stream's chain start, so a chain-bearing event is never followed by a
%% chain-less one. It is what a replayed seal looks like - restore an earlier
%% life's token and record, then strip and rewrite only the newest event. A
%% full read still refused, at the surviving MAC'd event that no longer
%% verifies, but a read that STARTED at the stripped event never looked at
%% that neighbour, and served the forgery as ok (Jupiter r15, JP3 tail
%% variant). One predecessor read closes it; the rest of the page is covered
%% by the running tip in handle_legacy/8.
-spec guard_page_start(atom(), binary(), [event()]) -> ok | {violation, map()}.
guard_page_start(_StoreId, _StreamId, []) ->
    ok;
guard_page_start(_StoreId, _StreamId, [#event{prev_event_hash = Hash} | _])
        when is_binary(Hash) ->
    ok;
guard_page_start(_StoreId, _StreamId, [#event{version = 0} | _]) ->
    ok;
guard_page_start(StoreId, StreamId, [#event{version = Version} | _]) ->
    case predecessor_event(StoreId, StreamId, Version - 1) of
        {ok, #event{prev_event_hash = PrevHash}} when is_binary(PrevHash) ->
            {violation, chain_break(StreamId, Version)};
        _ ->
            ok
    end.

%% @private The version below which a hash-less event is legitimately
%% pre-integrity, or the reason the watermark cannot be believed.
%%
%% A hash-bearing event is verified whether or not the watermark covers it,
%% so deleting or raising the watermark no longer hides one. What the
%% watermark is still needed for is the opposite case: an event with no
%% integrity fields at all. That is either data written before integrity was
%% enabled, or an event whose fields were cleared, and the only thing that
%% tells them apart is the watermark. It is a tree node, so it has to be one
%% the store can prove it wrote: a watermark that is missing, or that is a
%% bare integer anybody could have put there, makes every hash-less event a
%% violation rather than a skip.
-spec legacy_limit(atom(), binary(), reckon_db_chain_watermark:watermark(),
                   [event()]) -> {ok, non_neg_integer() | undefined} | {error, map()}.
legacy_limit(_StoreId, _StreamId, _Watermark, []) ->
    {ok, undefined};
legacy_limit(StoreId, StreamId, {sealed, Version}, _Events) ->
    _ = StoreId,
    _ = StreamId,
    {ok, Version};
legacy_limit(_StoreId, _StreamId, {unsealed, Version}, Events) ->
    %% Never believed, whatever the batch carries. A bare integer (or a
    %% sealed record whose MAC does not verify) is a node anybody who can
    %% write the tree can put there, and `seal_all/1' is the documented
    %% upgrade step that replaces it. Trusting it whenever this particular
    %% batch happens not to carry a chain is the whole attack: clear the
    %% integrity fields off every event, write a large bare number, and the
    %% store hands the tampered stream back unverified.
    {error, unauthenticated_watermark(Events, Version)};
legacy_limit(_StoreId, _StreamId, undefined, Events) ->
    case any_chain_bearing(Events) of
        false -> {ok, undefined};
        true -> {error, unauthenticated_watermark(Events, undefined)}
    end.

%% @private
-spec any_chain_bearing([event()]) -> boolean().
any_chain_bearing(Events) ->
    lists:any(fun is_chain_bearing/1, Events).

%% @private A chain-less event that follows a chain: the one order an honest
%% writer cannot produce, whatever the watermark says.
-spec chain_break(binary(), non_neg_integer()) ->
        #{layer := storage,
          stream_id := binary(),
          version := non_neg_integer(),
          kind := missing_integrity,
          context := #{detail := chain_break}}.
chain_break(StreamId, Version) ->
    #{layer => storage,
      stream_id => StreamId,
      version => Version,
      kind => missing_integrity,
      context => #{detail => chain_break}}.

%% @private
unauthenticated_watermark(Events, Version) ->
    #{layer => storage,
      stream_id => (hd(Events))#event.stream_id,
      version => (hd(Events))#event.version,
      kind => missing_integrity,
      context => #{detail => chain_start_unauthenticated,
                   chain_start => Version}}.

%% @private The tip the first integrity-bearing event of the batch is checked
%% against, or the reason there is none.
%%
%% A batch that opens with legacy events (no hash at all) is not anchored
%% here: the loop anchors the chain at the event where it starts, which the
%% writer put at the genesis hash.
-spec batch_anchor(atom(), binary(), [event()], non_neg_integer())
        -> {ok, binary() | undefined} | {error, map()}.
batch_anchor(_StoreId, _StreamId, [], _StartVersion) ->
    {ok, undefined};
batch_anchor(_StoreId, _StreamId, [#event{prev_event_hash = NoHash} | _], _Start)
        when not is_binary(NoHash) ->
    {ok, undefined};
batch_anchor(StoreId, StreamId, [#event{} = First | _], _StartVersion) ->
    anchor_for(StoreId, StreamId, First).

%% @private
-type predecessor_absent() ::
    #{layer := storage,
      stream_id := binary(),
      version := pos_integer(),
      kind := missing_predecessor,
      context := #{detail := predecessor_absent,
                   missing_version := non_neg_integer()}}.

-spec anchor_for(atom(), binary(), event())
        -> {ok, binary()} | {error, predecessor_absent()}.
anchor_for(_StoreId, _StreamId, #event{version = 0}) ->
    {ok, reckon_gater_integrity:genesis_prev_hash()};
anchor_for(StoreId, StreamId, #event{version = Version}) ->
    case predecessor_event(StoreId, StreamId, Version - 1) of
        {ok, #event{prev_event_hash = PrevPrevHash} = PrevEvent}
                when is_binary(PrevPrevHash) ->
            {ok, reckon_gater_integrity:compute_chain_hash(PrevEvent,
                                                           PrevPrevHash)};
        {ok, _LegacyPredecessor} ->
            %% The event before this one predates integrity: this event is
            %% where the chain starts, and the writer sealed it with genesis.
            %% verify_event checks that it carries genesis and a valid MAC.
            {ok, reckon_gater_integrity:genesis_prev_hash()};
        missing ->
            scavenge_anchor(StoreId, StreamId, Version - 1)
    end.

%% @private
-spec predecessor_event(atom(), binary(), non_neg_integer())
        -> {ok, event()} | missing.
predecessor_event(StoreId, StreamId, Version) ->
    Path = reckon_db_stream_path:event_path(StreamId,
                                            pad_version(Version, ?VERSION_PADDING)),
    case khepri:get(StoreId, Path) of
        {ok, #event{} = Event} -> {ok, Event};
        _ -> missing
    end.

%% @private
verify_events_loop([], _Tip, _Limit, _Key, _Ctx, _Mode, _StreamId, Acc) ->
    {ok, lists:reverse(Acc)};
verify_events_loop([Event | Rest], Tip, Limit, Key, Ctx, Mode, StreamId, Acc) ->
    case is_legacy(Event, Limit) of
        true ->
            handle_legacy(Event, Rest, Tip, Limit, Key, Ctx, Mode, StreamId,
                          Acc);
        false ->
            handle_integrity(Event, Rest, Tip, Limit, Key, Ctx, Mode, StreamId,
                             Acc)
    end.

%% @private An event is legacy only if it carries no chain at all AND the
%% authenticated watermark says it predates integrity. Anything else is
%% verified, or refused.
-spec is_legacy(event(), non_neg_integer() | undefined) -> boolean().
is_legacy(Event, Limit) when is_integer(Limit) ->
    not is_chain_bearing(Event) andalso Event#event.version < Limit;
is_legacy(_Event, undefined) ->
    false.

%% @private An event carries a chain when it names the hash of its
%% predecessor. Nothing else decides that: not a watermark, not a version.
-spec is_chain_bearing(event()) -> boolean().
is_chain_bearing(#event{prev_event_hash = PrevHash}) -> is_binary(PrevHash).

handle_legacy(Event, _Rest, Tip, _Limit, _Key, _Ctx, _Mode, StreamId, _Acc)
        when is_binary(Tip) ->
    %% A chain-less event AFTER a chain-bearing one in the same page: the same
    %% contradiction guard_page_start/3 catches at the page boundary, and the
    %% shape a stripped tail takes inside a page that starts above it.
    {integrity_violation, chain_break(StreamId, Event#event.version)};
handle_legacy(Event, _Rest, _Tip, _Limit, _Key, _Ctx, strict, StreamId,
              _Acc) ->
    %% Strict mode refuses to return legacy events at all.
    {integrity_violation, #{
        layer => storage,
        stream_id => StreamId,
        version => Event#event.version,
        kind => missing_integrity,
        context => #{detail => legacy_event_under_strict_mode}
    }};
handle_legacy(Event, Rest, Tip, Limit, Key, Ctx, Mode, StreamId, Acc) ->
    %% skip_legacy: return the legacy event untouched. Emit telemetry
    %% so operators can monitor remediation progress.
    telemetry:execute(
        [reckon, db, read, legacy_event_returned],
        #{system_time => erlang:system_time(millisecond)},
        #{store_id_hint => StreamId, version => Event#event.version}
    ),
    verify_events_loop(Rest, Tip, Limit, Key, Ctx, Mode, StreamId,
                       [Event | Acc]).

handle_integrity(#event{prev_event_hash = undefined} = Event, _Rest, _Tip,
                 _Limit, _Key, _Ctx, _Mode, StreamId, _Acc) ->
    %% No chain, and the watermark did not make it legacy: its integrity
    %% fields were cleared somewhere above the point where the chain begins.
    {integrity_violation, #{
        layer => storage,
        stream_id => StreamId,
        version => Event#event.version,
        kind => missing_integrity,
        context => #{detail => integrity_fields_absent}
    }};
handle_integrity(#event{prev_event_hash = PrevHash} = Event, Rest, undefined,
                 Limit, Key, Ctx, Mode, StreamId, Acc) ->
    %% No running tip: this is where the chain begins in this batch. It may
    %% begin here because the events before it predate integrity, and the
    %% writer sealed the first one with the genesis hash. Any other carried
    %% hash is an anchor nothing can stand on.
    case PrevHash =:= reckon_gater_integrity:genesis_prev_hash() of
        true ->
            handle_integrity(Event, Rest, PrevHash, Limit, Key, Ctx, Mode,
                             StreamId, Acc);
        false ->
            {integrity_violation, #{
                layer => storage,
                stream_id => StreamId,
                version => Event#event.version,
                kind => missing_integrity,
                context => #{detail => no_chain_anchor}
            }}
    end;
handle_integrity(Event, Rest, Tip, Limit, Key, Ctx, Mode, StreamId, Acc) ->
    case reckon_gater_integrity:verify_event(Event, Tip, Key, Ctx) of
        ok ->
            NextTip = reckon_gater_integrity:compute_chain_hash(Event, Tip),
            verify_events_loop(Rest, NextTip, Limit, Key, Ctx, Mode, StreamId,
                               [Event | Acc]);
        {integrity_violation, _} = Violation ->
            Violation
    end.

%% @private The anchor a scavenge left for this batch, if this batch is
%% exactly the one it left it for.
%%
%% Both halves have to line up: the boundary names the oldest surviving
%% version, and the event at that version carries the chain hash of the last
%% removed one. The boundary's MAC says the store wrote it. A batch that
%% starts anywhere else, or carries any other hash, has a gap nobody recorded
%% and is reported as one.
-spec scavenge_anchor(atom(), binary(), non_neg_integer())
        -> {ok, binary()} | {error, predecessor_absent()}.
scavenge_anchor(StoreId, StreamId, PrevVersion) ->
    BoundaryVersion = PrevVersion + 1,
    Carried = carried_hash(StoreId, StreamId, BoundaryVersion),
    case matching_boundary(reckon_db_scavenge_boundary:lookup(StoreId, StreamId),
                           BoundaryVersion, Carried) of
        {true, Recorded, Boundary} ->
            verified_anchor(StoreId, StreamId, PrevVersion, Recorded, Boundary);
        false ->
            {error, missing_predecessor_violation(StreamId, BoundaryVersion,
                                                  PrevVersion)}
    end.

%% @private The recorded boundary, when this batch is the one it was recorded
%% for: same oldest surviving version, and the event there carries the hash
%% the boundary names. Handed back unwrapped, because the verifier takes the
%% record and not the lookup result.
-type scavenge_boundary_record() ::
    #{version := pos_integer(), prev_hash := binary(), mac := binary()}.

-spec matching_boundary({ok, scavenge_boundary_record()} | {ok, undefined},
                        non_neg_integer(), binary() | undefined)
        -> {true, binary(), scavenge_boundary_record()} | false.
matching_boundary({ok, #{version := Version, prev_hash := Recorded} = Boundary},
                  Version, Carried)
        when is_binary(Carried), Carried =:= Recorded ->
    {true, Recorded, Boundary};
matching_boundary(_Boundary, _Version, _Carried) ->
    false.

%% @private
carried_hash(StoreId, StreamId, Version) ->
    case predecessor_event(StoreId, StreamId, Version) of
        {ok, #event{prev_event_hash = Carried}} -> Carried;
        missing -> undefined
    end.

%% @private
missing_predecessor_violation(StreamId, Version, MissingVersion) ->
    #{layer => storage,
      stream_id => StreamId,
      version => Version,
      kind => missing_predecessor,
      context => #{detail => predecessor_absent,
                   missing_version => MissingVersion}}.

%% @private A boundary the store can prove it wrote.
-spec verified_anchor(atom(), binary(), non_neg_integer(), binary(),
                      reckon_db_scavenge_boundary:boundary())
        -> {ok, binary()} | {error, predecessor_absent()}.
verified_anchor(StoreId, StreamId, PrevVersion, Recorded, Boundary) ->
    case reckon_db_scavenge_boundary:verify(StoreId, StreamId, Boundary) of
        true ->
            {ok, Recorded};
        false ->
            {error, missing_predecessor_violation(StreamId, PrevVersion + 1,
                                                  PrevVersion)}
    end.

%% @private
-spec read_events(atom(), binary(), non_neg_integer(), pos_integer(), direction()) ->
    {ok, [event()]}.
read_events(StoreId, StreamId, StartVersion, Count, Direction) ->
    read_events(StoreId, StreamId, StartVersion, Count, Direction, #{}).

%% @private The same window, with the read options the caller names.
read_events(StoreId, StreamId, StartVersion, Count, Direction, Options) ->
    Versions = calculate_versions(StartVersion, Count, Direction),
    Events = lists:filtermap(
               fun(Version) ->
                       read_one_event(StoreId, StreamId, Version, Options)
               end, Versions),
    {ok, Events}.

%% @private Fetch a single event by version, converting a stored map to a
%% record if needed; drops versions with no event.
read_one_event(StoreId, StreamId, Version, Options) ->
    PaddedVersion = pad_version(Version, ?VERSION_PADDING),
    Path = reckon_db_stream_path:event_path(StreamId, PaddedVersion),
    event_from_khepri(khepri:get(StoreId, Path, Options)).

event_from_khepri({ok, Event}) when is_record(Event, event) -> {true, Event};
event_from_khepri({ok, EventMap}) when is_map(EventMap) -> {true, map_to_event(EventMap)};
event_from_khepri(_) -> false.

%% @private
-spec calculate_versions(non_neg_integer(), pos_integer(), direction()) -> [non_neg_integer()].
calculate_versions(StartVersion, Count, forward) ->
    lists:seq(StartVersion, StartVersion + Count - 1);
calculate_versions(StartVersion, Count, backward) ->
    EndVersion = max(0, StartVersion - Count + 1),
    lists:reverse(lists:seq(EndVersion, StartVersion)).

%% @private
-spec map_to_event(map()) -> event().
map_to_event(Map) ->
    #event{
        event_id = maps:get(event_id, Map, undefined),
        event_type = maps:get(event_type, Map, undefined),
        stream_id = maps:get(stream_id, Map, undefined),
        version = maps:get(version, Map, 0),
        data = maps:get(data, Map, #{}),
        metadata = maps:get(metadata, Map, #{}),
        tags = maps:get(tags, Map, undefined),
        timestamp = maps:get(timestamp, Map, 0),
        epoch_us = maps:get(epoch_us, Map, 0),
        data_content_type = maps:get(data_content_type, Map, ?CONTENT_TYPE_JSON),
        metadata_content_type = maps:get(metadata_content_type, Map, ?CONTENT_TYPE_JSON)
    }.

