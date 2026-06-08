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
    read_all_global/3,
    read_by_event_types/3,
    read_by_tags/4,
    get_version/2,
    exists/2,
    has_events/1,
    list_streams/1,
    delete/2
]).

%% Internal exports for workers
-export([
    do_append/4,
    do_read/5
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
%%   -2 (ANY_VERSION)  - No version check, always append
%%   N >= 0            - Stream version must equal N
%%
%% Returns {ok, NewVersion} on success or {error, Reason} on failure.
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
    TagFilter :: reckon_gater_types:tag_filter(),
    SeqCutoff :: reckon_gater_types:seq_cutoff(),
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

%% @doc Read all events from a stream
-spec read_all(atom(), binary(), pos_integer(), direction()) ->
    {ok, [event()]} | {error, term()}.
read_all(StoreId, StreamId, BatchSize, Direction) ->
    read(StoreId, StreamId, 0, BatchSize, Direction).

%% @doc Read all events across all streams in global epoch_us order.
%%
%% Returns events sorted by epoch_us, skipping `Offset' events and
%% returning up to `BatchSize' events. Used by catch-up subscriptions
%% to replay historical events to a subscriber.
%%
%% Parameters:
%%   StoreId   - The store identifier
%%   Offset    - Number of events to skip (0-based)
%%   BatchSize - Maximum number of events to return
%%
%% Returns events sorted by epoch_us (global ordering).
-spec read_all_global(atom(), non_neg_integer(), pos_integer()) ->
    {ok, [event()]} | {error, term()}.
read_all_global(StoreId, Offset, BatchSize) ->
    case query_event_results(StoreId, has_data_leaf()) of
        {ok, Results} ->
            Events = [convert_result_to_event(PathKey, Value)
                      || {PathKey, Value} <- Results],
            ValidEvents = [E || E <- Events, E =/= undefined],
            SortedEvents = sort_by_epoch(ValidEvents),
            Skipped = safe_nthtail(Offset, SortedEvents),
            {ok, lists:sublist(Skipped, BatchSize)};
        {error, _} = Error ->
            Error
    end.

%% @private Leaf condition matching any version/seq node that carries
%% event data: `#if_all{[*, has_data]}'. Shared by the global readers.
-spec has_data_leaf() -> khepri:condition().
has_data_leaf() ->
    #if_all{conditions = [
        ?KHEPRI_WILDCARD_STAR,
        #if_has_data{has_data = true}
    ]}.

%% @private Sort events into global epoch_us order.
-spec sort_by_epoch([event()]) -> [event()].
sort_by_epoch(Events) ->
    lists:sort(
        fun(#event{epoch_us = E1}, #event{epoch_us = E2}) -> E1 =< E2 end,
        Events).

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
%% Returns events sorted by epoch_us (global ordering).
-spec read_by_event_types(atom(), [binary()], pos_integer()) ->
    {ok, [event()]} | {error, term()}.
read_by_event_types(StoreId, EventTypes, BatchSize) when is_list(EventTypes) ->
    %% Push the event-type match into Khepri (DB-side filtering via
    %% #if_data_matches) so we don't load non-matching events. The leaf
    %% condition is applied at both depths by query_event_results (regular
    %% 4-level events + the 2-level DCB log).
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
            SortedEvents = sort_by_epoch(ValidEvents),
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
%% Events sorted by epoch_us (global ordering).
-spec read_by_tags(atom(), [binary()], any | all, pos_integer()) ->
    {ok, [event()]} | {error, term()}.
read_by_tags(StoreId, Tags, Match, BatchSize) when is_list(Tags), is_atom(Match) ->
    %% Query all events with data (both depths) and filter by tags in
    %% Erlang. Khepri's pattern matching doesn't easily support list
    %% membership, so we fetch events with data and filter client-side.
    %%
    %% For large stores, this is the cross-cutting scan the secondary-index
    %% RFC (DESIGN_SECONDARY_INDEX.md) is designed to replace.
    case query_event_results(StoreId, has_data_leaf()) of
        {ok, Results} ->
            AllEvents = [convert_result_to_event(PathKey, Value)
                         || {PathKey, Value} <- Results],
            ValidEvents = [E || E <- AllEvents, E =/= undefined],
            FilteredEvents = filter_events_by_tags(ValidEvents, Tags, Match),
            SortedEvents = sort_by_epoch(FilteredEvents),
            LimitedEvents = lists:sublist(SortedEvents, BatchSize),
            {ok, LimitedEvents};
        {error, _} = Error ->
            Error
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
-spec get_version(atom(), binary()) -> integer().
get_version(StoreId, StreamId) ->
    case khepri:count(StoreId, reckon_db_stream_path:versions_pattern(StreamId)) of
        {ok, 0} -> ?NO_STREAM;
        {ok, Count} -> Count - 1;
        {error, _} -> ?NO_STREAM
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

%% @private True for the DCB pseudo-stream's 2-level nodes
%% ([streams, _dcb, SeqKey]) — excluded from user-facing stream listings.
-spec is_dcb_path([atom() | binary()]) -> boolean().
is_dcb_path([streams, ?DCB_STREAM | _]) -> true;
is_dcb_path(_) -> false.

%% @doc Delete a stream and all its events
-spec delete(atom(), binary()) -> ok | {error, term()}.
delete(StoreId, StreamId) ->
    Path = reckon_db_stream_path:stream_path(StreamId),
    case khepri:delete(StoreId, Path) of
        ok -> ok;
        {error, _} = Error -> Error
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec do_append(atom(), binary(), integer(), [new_event()]) ->
    {ok, non_neg_integer()} | {error, term()}.
do_append(StoreId, StreamId, ExpectedVersion, Events) ->
    CurrentVersion = get_version(StoreId, StreamId),

    %% Check expected version
    case check_expected_version(ExpectedVersion, CurrentVersion) of
        ok ->
            append_events_to_stream(StoreId, StreamId, CurrentVersion, Events);
        {error, _} = Error ->
            Error
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
-spec append_events_to_stream(atom(), binary(), integer(), [new_event()]) ->
    {ok, non_neg_integer()} | {error, term()}.
append_events_to_stream(StoreId, StreamId, CurrentVersion, Events) ->
    Now = erlang:system_time(millisecond),
    EpochUs = erlang:system_time(microsecond),

    %% Resolve the tamper-resistance context for this batch. For
    %% stores with integrity disabled (the default and only mode
    %% pre-2.1) this is a constant `disabled` pass-through. For
    %% integrity-enabled stores we load the HMAC key, ensure the
    %% per-stream watermark exists, and resolve the initial chain
    %% tip — all once, before the per-event loop.
    IntegrityCtx = setup_integrity(StoreId, StreamId, CurrentVersion),
    InitialTip = resolve_initial_tip(StoreId, StreamId, CurrentVersion + 1, IntegrityCtx),

    %% A failed khepri:put — most importantly {error, noproc} while the
    %% store's Ra server is mid-(re)start — MUST surface as a retriable
    %% {error, _} return, NOT a hard `ok =` badmatch. The badmatch
    %% crashed the gateway worker on every transient store-not-ready;
    %% under a boot-time append (the simulator fires before Ra is
    %% write-ready) the worker crash-looped past its supervisor's
    %% restart intensity in under a second, and the cascade tore down
    %% the entire store subtree (reached_max_restart_intensity) — a
    %% sub-second readiness gap became a permanent, restart-looping
    %% outage. reckon-gater treats {error, _} as retriable and the
    %% append succeeds once Ra is ready.
    try
        {FinalVersion, _FinalTip} = lists:foldl(
            fun(Event, {AccVersion, AccTip}) ->
                NewVersion = AccVersion + 1,
                RecordedEvent0 = create_event_record(
                    Event, StreamId, NewVersion, Now, EpochUs),
                {RecordedEvent, NextTip} = apply_integrity_if_enabled(
                    RecordedEvent0, AccTip, IntegrityCtx),
                PaddedVersion = pad_version(NewVersion, ?VERSION_PADDING),
                Path = reckon_db_stream_path:event_path(StreamId, PaddedVersion),
                case khepri:put(StoreId, Path, RecordedEvent) of
                    ok ->
                        {NewVersion, NextTip};
                    {error, Reason} ->
                        throw({?MODULE, append_aborted, Reason})
                end
            end,
            {CurrentVersion, InitialTip},
            Events
        ),
        {ok, FinalVersion}
    catch
        throw:{?MODULE, append_aborted, Reason} ->
            {error, Reason}
    end.

%% @private Tamper-resistance context for a single append batch.
%%
%% Either `disabled` (no integrity work) or
%% `{enabled, Key, ChainStart}` carrying the HMAC key and the
%% chain-start watermark for this stream.
-type integrity_ctx() ::
    disabled |
    {enabled, Key :: binary(), ChainStart :: non_neg_integer()}.

-spec setup_integrity(atom(), binary(), integer()) -> integrity_ctx().
setup_integrity(StoreId, StreamId, CurrentVersion) ->
    case reckon_db_integrity_key:is_enabled(StoreId) of
        false ->
            disabled;
        true ->
            NextVersion = CurrentVersion + 1,
            {ok, ChainStart} = reckon_db_chain_watermark:set_if_absent(
                StoreId, StreamId, NextVersion),
            Key = reckon_db_integrity_key:get(StoreId),
            {enabled, Key, ChainStart}
    end.

%% @private Resolve the chain-tip value that the FIRST event in this
%% batch must reference as its `prev_event_hash`.
%%
%% Disabled context: tip is `undefined` (unused).
%% Enabled, first integrity event in stream (NextVersion =:= ChainStart):
%%   tip is the genesis 32-zero-byte value.
%% Enabled, later batch (NextVersion > ChainStart):
%%   tip is computed from the predecessor event on disk.
-spec resolve_initial_tip(
    atom(), binary(), non_neg_integer(), integrity_ctx()
) -> binary() | undefined.
resolve_initial_tip(_StoreId, _StreamId, _NextVersion, disabled) ->
    undefined;
resolve_initial_tip(_StoreId, _StreamId, NextVersion,
                    {enabled, _Key, ChainStart})
        when NextVersion =:= ChainStart ->
    reckon_gater_integrity:genesis_prev_hash();
resolve_initial_tip(StoreId, StreamId, NextVersion,
                    {enabled, _Key, ChainStart})
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
            %% find a usable predecessor. Surface fast — replay
            %% won't be able to verify anyway.
            erlang:error({integrity_setup_failed,
                          #{stream_id => StreamId,
                            looking_for_version => PrevVersion,
                            chain_start => ChainStart,
                            got => Other}})
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
apply_integrity_if_enabled(#event{} = Event, Tip, {enabled, Key, _ChainStart})
        when is_binary(Tip) ->
    Event1 = Event#event{prev_event_hash = Tip},
    Mac = reckon_gater_integrity:compute_event_mac(Event1, Key),
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
    case exists(StoreId, StreamId) of
        false ->
            {error, {stream_not_found, StreamId}};
        true ->
            case read_events(StoreId, StreamId, StartVersion, Count, Direction) of
                {ok, Events} ->
                    case maybe_verify_events(
                            StoreId, StreamId, StartVersion, Direction,
                            Events, Opts) of
                        {ok, _} = Ok ->
                            Ok;
                        {integrity_violation, _} = Violation ->
                            %% Wrap at the public API boundary so
                            %% callers see {error, _}. Internal
                            %% helpers and the gater module return
                            %% the bare tuple.
                            {error, Violation}
                    end;
                Other ->
                    Other
            end
    end.

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
) -> {ok, [event()]} | {error, term()}.
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
) -> {ok, [event()]} | {error, term()}.
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
-spec verify_events_forward(
    atom(), binary(), non_neg_integer(), [event()], verify_mode()
) -> {ok, [event()]} | {error, term()}.
verify_events_forward(StoreId, StreamId, StartVersion, Events, Mode) ->
    {ok, ChainStart} = reckon_db_chain_watermark:lookup(StoreId, StreamId),
    Key = reckon_db_integrity_key:get(StoreId),
    InitialTip = resolve_read_initial_tip(
        StoreId, StreamId, StartVersion, ChainStart),
    verify_events_loop(Events, InitialTip, ChainStart, Key, Mode, StreamId, []).

%% @private
verify_events_loop([], _Tip, _ChainStart, _Key, _Mode, _StreamId, Acc) ->
    {ok, lists:reverse(Acc)};
verify_events_loop([Event | Rest], Tip, ChainStart, Key, Mode, StreamId, Acc) ->
    case is_legacy_event(Event, ChainStart) of
        true ->
            handle_legacy(Event, Rest, Tip, ChainStart, Key, Mode, StreamId, Acc);
        false ->
            handle_integrity(Event, Rest, Tip, ChainStart, Key, Mode, StreamId, Acc)
    end.

%% @private An event is legacy if it predates the watermark (or the
%% watermark is absent, meaning the stream never had integrity events).
is_legacy_event(_Event, undefined) -> true;
is_legacy_event(#event{version = V}, ChainStart) when is_integer(ChainStart) ->
    V < ChainStart.

handle_legacy(Event, Rest, Tip, ChainStart, Key, strict, StreamId, _Acc) ->
    %% Strict mode refuses to return legacy events at all.
    _ = Tip, _ = ChainStart, _ = Key, _ = Rest,
    {integrity_violation, #{
        layer => storage,
        stream_id => StreamId,
        version => Event#event.version,
        kind => missing_integrity,
        context => #{detail => legacy_event_under_strict_mode}
    }};
handle_legacy(Event, Rest, Tip, ChainStart, Key, Mode, StreamId, Acc) ->
    %% skip_legacy: return the legacy event untouched. Emit telemetry
    %% so operators can monitor remediation progress.
    telemetry:execute(
        [reckon, db, read, legacy_event_returned],
        #{system_time => erlang:system_time(millisecond)},
        #{store_id_hint => StreamId, version => Event#event.version}
    ),
    verify_events_loop(Rest, Tip, ChainStart, Key, Mode, StreamId, [Event | Acc]).

handle_integrity(Event, Rest, undefined, ChainStart, Key, Mode, StreamId, Acc)
        when is_integer(ChainStart),
             is_record(Event, event),
             Event#event.version =:= ChainStart ->
    %% Mid-read transition from legacy region to integrity region: the
    %% first integrity-bearing event in a stream was written with
    %% prev_event_hash = genesis. Seed the running tip accordingly so
    %% the verifier has a concrete value to compare against.
    Genesis = reckon_gater_integrity:genesis_prev_hash(),
    handle_integrity(Event, Rest, Genesis, ChainStart, Key, Mode, StreamId, Acc);
handle_integrity(Event, Rest, Tip, ChainStart, Key, Mode, StreamId, Acc)
        when is_binary(Tip) ->
    case reckon_gater_integrity:verify_event(Event, Tip, Key) of
        ok ->
            NextTip = reckon_gater_integrity:compute_chain_hash(Event, Tip),
            verify_events_loop(
                Rest, NextTip, ChainStart, Key, Mode, StreamId,
                [Event | Acc]);
        {integrity_violation, _} = Violation ->
            Violation
    end.

%% @private Resolve the chain tip that the FIRST event in the returned
%% batch should reference as its `prev_event_hash`.
%%
%% - undefined watermark or StartVersion < watermark: legacy-only;
%%   the running tip is irrelevant (verification won't be called).
%% - StartVersion == watermark: tip = genesis (this is the first
%%   integrity-bearing event in the stream).
%% - StartVersion > watermark: tip = chain_hash of the event at
%%   (StartVersion - 1), which must itself be integrity-bearing.
-spec resolve_read_initial_tip(
    atom(), binary(), non_neg_integer(), non_neg_integer() | undefined
) -> binary() | undefined.
resolve_read_initial_tip(_StoreId, _StreamId, _StartVersion, undefined) ->
    undefined;
resolve_read_initial_tip(_StoreId, _StreamId, StartVersion, ChainStart)
        when StartVersion < ChainStart ->
    undefined;
resolve_read_initial_tip(_StoreId, _StreamId, StartVersion, ChainStart)
        when StartVersion =:= ChainStart ->
    reckon_gater_integrity:genesis_prev_hash();
resolve_read_initial_tip(StoreId, StreamId, StartVersion, _ChainStart)
        when StartVersion > 0 ->
    PrevVersion = StartVersion - 1,
    PaddedVersion = pad_version(PrevVersion, ?VERSION_PADDING),
    Path = reckon_db_stream_path:event_path(StreamId, PaddedVersion),
    case khepri:get(StoreId, Path) of
        {ok, #event{prev_event_hash = PrevPrevHash} = PrevEvent}
                when is_binary(PrevPrevHash) ->
            reckon_gater_integrity:compute_chain_hash(PrevEvent, PrevPrevHash);
        _ ->
            %% No usable predecessor; legacy regime in practice.
            undefined
    end.

%% @private
-spec read_events(atom(), binary(), non_neg_integer(), pos_integer(), direction()) ->
    {ok, [event()]}.
read_events(StoreId, StreamId, StartVersion, Count, Direction) ->
    Versions = calculate_versions(StartVersion, Count, Direction),
    Events = lists:filtermap(
        fun(Version) ->
            PaddedVersion = pad_version(Version, ?VERSION_PADDING),
            Path = reckon_db_stream_path:event_path(StreamId, PaddedVersion),
            case khepri:get(StoreId, Path) of
                {ok, Event} when is_record(Event, event) ->
                    {true, Event};
                {ok, EventMap} when is_map(EventMap) ->
                    %% Convert map to record if needed
                    {true, map_to_event(EventMap)};
                _ ->
                    false
            end
        end,
        Versions
    ),
    {ok, Events}.

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

%% @private Safe version of lists:nthtail that returns [] when Offset >= length.
-spec safe_nthtail(non_neg_integer(), list()) -> list().
safe_nthtail(0, List) -> List;
safe_nthtail(_, []) -> [];
safe_nthtail(N, [_ | Rest]) -> safe_nthtail(N - 1, Rest).
