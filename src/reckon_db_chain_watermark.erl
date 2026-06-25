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

-export([
    lookup/2,
    set_if_absent/3,
    delete/2
]).

%%====================================================================
%% Public API
%%====================================================================

%% @doc Read the chain-start version for a stream.
%%
%% Returns the version at which integrity began for the stream, or
%% 'undefined' if no watermark has been recorded (i.e., the stream
%% has never had an integrity-bearing append).
-spec lookup(StoreId :: atom(), StreamId :: binary()) ->
    {ok, non_neg_integer() | undefined}.
lookup(StoreId, StreamId) ->
    Path = ?INTEGRITY_CHAIN_START_PATH ++ [StreamId],
    case khepri:get(StoreId, Path) of
        {ok, Version} when is_integer(Version), Version >= 0 ->
            {ok, Version};
        _ ->
            {ok, undefined}
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
        {ok, Existing} when is_integer(Existing) ->
            {ok, Existing};
        {ok, undefined} ->
            Path = ?INTEGRITY_CHAIN_START_PATH ++ [StreamId],
            ok = khepri:put(StoreId, Path, Version),
            %% Re-read to handle the rare race where two writers both
            %% saw 'undefined' and both wrote. Whoever's write
            %% Khepri/Ra serialised last wins; we return that value.
            {ok, Recorded} = lookup(StoreId, StreamId),
            recorded_or_version(Recorded, Version)
    end.

%% @private Prefer the re-read value; fall back to our own write (defensive;
%% lookup should not be undefined immediately after a put).
recorded_or_version(undefined, Version) -> {ok, Version};
recorded_or_version(Recorded, _Version) -> {ok, Recorded}.

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
