%% @doc Behaviour for pluggable event-log backends.
%%
%% This is the HOT PATH contract. An implementation owns the append-only
%% event log — how events are written to disk, how they are indexed,
%% how they are read back. Everything else (subscriptions, snapshots,
%% cluster metadata, schema registry) is the responsibility of other
%% behaviours or of the existing Khepri-based control plane.
%%
%% == Design Philosophy ==
%%
%% Today reckon-db uses Khepri (on top of Ra) for both the log AND the
%% control plane. Measured cost: ~51 KB disk write per 256-byte event
%% and ~22 appends/sec sustained on cx33. Both are ORDERS OF MAGNITUDE
%% worse than purpose-built event-store log engines. Root cause:
%% Khepri is a tree-structured replicated KV, not a log. Events get
%% modeled as `/streams/<id>/<padded-version>` tree nodes, each paying
%% the full Raft + tree + WAL + segment overhead.
%%
%% This behaviour separates the log from the control plane so a Rust
%% log engine (wrapping RocksDB, LMDB, or a custom append-only format)
%% can be dropped in without touching the facade (`reckon_db_streams`),
%% the gater, evoq, or consumers.
%%
%% == Implementations ==
%%
%%   - `reckon_db_khepri_log_backend`  — current Khepri-backed engine
%%     (baseline; extracts existing code unchanged).
%%
%%   - `reckon_db_rocksdb_log_backend` — planned, RocksDB via Rustler NIF.
%%     Expected: 10-100× throughput, ~1-3 KB/op disk amplification.
%%
%%   - `reckon_db_log_nif` — planned custom Rust engine. Long-term
%%     moat. Append-only segments, mmap, fsync batching, zstd.
%%
%% == Contract Notes ==
%%
%%   - All callbacks take a backend `state()` as first argument. State
%%     is opaque — it's whatever the implementation returned from
%%     `init/1'. The facade passes it through unchanged.
%%
%%   - `append_events/4' is the ONLY write operation. All writes go
%%     through it. Version-check semantics match
%%     `reckon_db_streams:append/4': NO_STREAM, ANY_VERSION,
%%     STREAM_EXISTS, or a concrete integer version.
%%
%%   - Reads come in three flavours: by-stream (`read_stream/5'),
%%     global (`read_all/3'), and metadata (`stream_version/2',
%%     `stream_exists/2', `has_events/1', `list_streams/1').
%%
%%   - `append_batch/2' is optional. If implemented, runners will
%%     use it for multi-stream transactions. Otherwise they fall back
%%     to N serial `append_events/4' calls.
%%
%%   - Errors are returned as `{error, Reason}' tuples. Backends MUST
%%     NOT crash on expected errors (wrong version, stream not found).
%%     They MAY crash on unrecoverable failures (disk full, corruption)
%%     — the supervisor will restart.
%%
%% @author rgfaber
-module(reckon_db_log_backend).

%% Intentionally NOT including reckon_gater/include/reckon_gater_types.hrl.
%% Behaviour modules must be standalone — implementations include the
%% header themselves to get macros (?NO_STREAM, ?ANY_VERSION, ?STREAM_EXISTS)
%% and the #event record. This contract only cares about the underlying
%% integer/term types those resolve to.

%% ------------------------------------------------------------------
%% Types
%% ------------------------------------------------------------------

-type store_id()   :: atom().
-type stream_id()  :: binary().
-type version()    :: non_neg_integer().
-type offset()     :: non_neg_integer().
-type direction()  :: forward | backward.

%% Expected version (optimistic concurrency):
%%   ?NO_STREAM      (-1) — stream must not exist (first write)
%%   ?ANY_VERSION    (-2) — no version check
%%   ?STREAM_EXISTS  (-4) — stream must exist (any current version)
%%   non_neg_integer()    — stream version must equal this
-type expected_version() :: integer().

%% Backend-internal state returned by init/1. Opaque to callers.
-type state() :: term().

%% The shape the facade hands in for writes. See
%% `reckon_db_streams:new_event/0' and
%% `reckon_gater/include/reckon_gater_types.hrl' for the canonical
%% definitions.
-type new_event() :: #{
    event_type := binary(),
    data       := map() | binary(),
    metadata   => map(),
    tags       => [binary()],
    event_id   => binary()
}.

%% The shape backends return on reads. Implementations SHOULD populate
%% the #event record defined in `reckon_gater_types.hrl'.
-type event() :: term().  %% #event{} — avoiding circular include

-export_type([
    store_id/0,
    stream_id/0,
    version/0,
    offset/0,
    direction/0,
    expected_version/0,
    state/0,
    new_event/0,
    event/0
]).

%% ------------------------------------------------------------------
%% Lifecycle callbacks
%% ------------------------------------------------------------------

%% @doc Initialize a backend instance for a store.
%%
%% `Opts' is backend-specific. Minimum expected keys:
%%   - `store_id' :: store_id()
%%   - `data_dir' :: file:filename_all()
%%
%% Additional keys are backend-specific (e.g. RocksDB: `write_buffer_size',
%% `column_families'; Khepri: `machine_opts').
-callback init(Opts :: map()) ->
    {ok, state()} | {error, Reason :: term()}.

%% @doc Flush and close. Called on orderly shutdown.
-callback close(state()) -> ok.

%% @doc Health check. Returns `ok' if the backend is serving requests,
%% `{error, Reason}' if degraded. The supervisor uses this to decide
%% restart vs escalation.
-callback health(state()) -> ok | {error, term()}.

%% ------------------------------------------------------------------
%% Write path
%% ------------------------------------------------------------------

%% @doc Append events to a single stream with optimistic concurrency.
%%
%% Returns `{ok, NewVersion}' where `NewVersion' is the version of the
%% LAST event appended (0-based). On success the version monotonically
%% increases by `length(Events)'.
%%
%% Error semantics:
%%   - `{error, {wrong_expected_version, Actual}}' — optimistic concurrency
%%     violation. `Actual' is the stream's current version (or -1 if the
%%     stream does not exist).
%%   - `{error, Other}' — backend-specific failure. Crash is allowed for
%%     unrecoverable errors.
-callback append_events(
    state(),
    stream_id(),
    expected_version(),
    [new_event()]
) ->
    {ok, version()}
    | {error, {wrong_expected_version, version()}}
    | {error, Reason :: term()}.

%% ------------------------------------------------------------------
%% Read path
%% ------------------------------------------------------------------

%% @doc Read a range of events from one stream.
%%
%%   - `StartVersion' — 0-based. For `backward', start from this version
%%     and read down.
%%   - `Count' — maximum number of events. Backends MAY return fewer.
%%   - `Direction' — `forward' (version ascending) or `backward' (descending).
-callback read_stream(
    state(),
    stream_id(),
    StartVersion :: non_neg_integer(),
    Count        :: pos_integer(),
    direction()
) ->
    {ok, [event()]} | {error, term()}.

%% @doc Read across all streams in global-offset order.
%%
%% Every event has an implicit global offset (the `$all' stream in
%% EventStoreDB terms). `Offset' is 0-based; events are returned in
%% insertion order (not wall-clock — insertion).
-callback read_all(
    state(),
    offset(),
    Count :: pos_integer()
) ->
    {ok, [event()]} | {error, term()}.

%% ------------------------------------------------------------------
%% Metadata callbacks
%% ------------------------------------------------------------------

-callback stream_version(state(), stream_id()) ->
    {ok, version()} | {error, stream_not_found} | {error, term()}.

-callback stream_exists(state(), stream_id()) -> boolean().

-callback has_events(state()) -> boolean().

-callback list_streams(state()) ->
    {ok, [stream_id()]} | {error, term()}.

-callback delete_stream(state(), stream_id()) ->
    ok | {error, stream_not_found} | {error, term()}.

%% ------------------------------------------------------------------
%% Optional callbacks
%% ------------------------------------------------------------------

%% @doc Atomic multi-stream append. Implementations that support cross-
%% stream transactions return per-stream results. Implementations that
%% don't should OMIT this callback — the facade will fall back to N
%% serial `append_events/4' calls.
-callback append_batch(
    state(),
    [{stream_id(), expected_version(), [new_event()]}]
) ->
    {ok, [{stream_id(), {ok, version()} | {error, term()}}]}
    | {error, term()}.

%% @doc Truncate a stream at/below a version. Used by scavenge and
%% retention policies. Implementations without truncation support
%% should OMIT this callback.
-callback truncate_stream(state(), stream_id(), version()) ->
    ok | {error, term()}.

%% @doc Compact / scavenge the log. Backend-specific — RocksDB compacts
%% LSM levels; a custom append-only engine rewrites segments.
-callback compact(state()) -> ok | {error, term()}.

%% @doc Total on-disk size in bytes, for observability. Best-effort.
-callback disk_bytes(state()) -> {ok, non_neg_integer()} | {error, term()}.

-optional_callbacks([
    append_batch/2,
    truncate_stream/3,
    compact/1,
    disk_bytes/1,
    health/1
]).
