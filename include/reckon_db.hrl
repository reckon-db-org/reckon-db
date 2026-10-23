%% @doc Core records and macros for reckon-db
%% @author Reckon-DB

-ifndef(RECKON_DB_HRL).
-define(RECKON_DB_HRL, true).

%% Include shared types from reckon-gater
%% These define: #event{}, #subscription{}, #snapshot{}, #append_result{},
%% subscription_type(), read_direction(), append_error(), read_error(),
%% and constants: NO_STREAM, ANY_VERSION, STREAM_EXISTS, CONTENT_TYPE_*
-include_lib("reckon_gater/include/reckon_gater_types.hrl").

%%====================================================================
%% Version
%%====================================================================

-define(RECKON_DB_VERSION, "5.11.13").

%%====================================================================
%% Khepri Paths
%%====================================================================

-define(STREAMS_PATH, [streams]).
-define(SNAPSHOTS_PATH, [snapshots]).
-define(SUBSCRIPTIONS_PATH, [subscriptions]).
-define(METADATA_PATH, [metadata]).

%% Monotonic global event counter — a single node bumped by every append
%% batch inside its Khepri transaction (mirrors ?DCB_SEQ_COUNTER_PATH), so
%% the total event count is an O(1) read instead of a full store scan.
-define(GLOBAL_EVENT_COUNT_PATH, [metadata, stats, global_event_count]).

%% DCB (Dynamic Consistency Boundary) paths — Phase 3, 2.4.0+.
%% DCB events live under one pseudo-stream `_dcb` AT THE SAME PATH
%% PREFIX as regular streams, so all existing read paths
%% (`read_all_global`, `read_by_event_types`, tag subscriptions, ...)
%% see DCB events automatically. The append path is different (the
%% conditional `append_if_no_tag_matches` primitive), but the storage
%% shape is identical to a normal stream.
%%
%% Each DCB event is ALSO indexed by every tag it carries at
%% `?BY_TAG_PATH ++ [Tag, SeqKey]`, so the conditional-append primitive
%% can scan a bounded subtree (per-tag) inside a Khepri transaction
%% instead of the full event log.
%%
%% See: plans/PLAN_DCB_IMPLEMENTATION.md
-define(DCB_STREAM, <<"_dcb">>).
-define(DCB_STREAM_PATH, ?STREAMS_PATH ++ [?DCB_STREAM]).
-define(BY_TAG_PATH, [by_tag]).
-define(BY_EVENT_TYPE_PATH, [by_event_type]).
%% CCC payload indexes (5.3.0+). Separate from the secondary index [idx, ...]
%% which is OrderKey-keyed for general event lookups. These are DCB-scoped,
%% SeqKey-ordered, and serve the conditional-append consistency check.
%%   [by_payload, Key, Value, SeqKey] -> #{}   (single-field)
%%   [by_payload_hash, Hash, SeqKey]  -> #{}   (composite SHA-256 hash)
-define(BY_PAYLOAD_PATH, [by_payload]).
-define(BY_PAYLOAD_HASH_PATH, [by_payload_hash]).
%% Monotonic global counter for DCB seqs. One node, atomic via Khepri
%% transactions. Holds the LAST-ASSIGNED seq (or absent if no DCB
%% events yet).
-define(DCB_SEQ_COUNTER_PATH, [metadata, dcb, last_seq]).

%% Integrity chain tip for the DCB pseudo-stream. Binary (32 bytes,
%% SHA-256) — the chain-hash of the last DCB event written under an
%% integrity-enabled store. The next integrity-bearing DCB event uses
%% this as its prev_event_hash. Updated atomically with the seq
%% counter inside the conditional-append transaction. Absent path =
%% no integrity-bearing DCB events yet; use genesis_prev_hash.
-define(DCB_CHAIN_TIP_PATH, [metadata, dcb, chain_tip]).
%% Fixed-width zero-padded DECIMAL keys so lexicographic order == numeric
%% order for subtree iteration. 20 digits covers up to 10^20 events.
%% Matches the existing pad_version convention in reckon_db_snapshots_store.
-define(DCB_SEQ_KEY_WIDTH, 20).

%% Generic write-maintained secondary index (opt-in per store).
%% Mirrors the DCB by_tag shape (fixed-width ordered leaf keys, subtree
%% iteration) but for ALL events, not just _dcb. Three index kinds share
%% one mechanism under a single `idx' root:
%%
%%   [idx, tag,        Tag,       OrderKey] -> EventRef
%%   [idx, event_type, EventType, OrderKey] -> EventRef
%%   [idx, meta, Key,  Value,     OrderKey] -> EventRef
%%
%% OrderKey = pad(epoch_us) | stream_id | pad(version) — globally ordered,
%% unique. EventRef = #{stream_id := binary(), version := non_neg_integer()}.
%%
%% The `idx' root is DELIBERATELY separate from DCB's ?BY_TAG_PATH ([by_tag]):
%% DCB's by_tag is seq-keyed, unconditional, and serves the conditional-append
%% primitive for _dcb events only; this generalized index is OrderKey-keyed and
%% opt-in. Keeping distinct roots avoids mixing the two leaf schemes in one
%% subtree. See plans/DESIGN_SECONDARY_INDEX.md (§13.5).
-define(INDEX_PATH, [idx]).
%% Fixed-width zero-padded epoch_us component of an index OrderKey, so
%% lexicographic subtree order == event (time) order. 20 digits matches the
%% DCB seq-key width and covers epoch_us well beyond any realistic horizon.
-define(INDEX_ORDER_KEY_WIDTH, 20).

%% Per-stream tamper-resistance watermark (2.1.0+).
%% Path: [metadata, integrity, chain_start, StreamId] -> non_neg_integer()
%% Records the version at which integrity-bearing writes began for that
%% stream. Events with version < watermark are pre-integrity legacy;
%% events at or above the watermark must carry prev_event_hash + mac.
-define(INTEGRITY_CHAIN_START_PATH, [metadata, integrity, chain_start]).

%%====================================================================
%% Default Values
%%====================================================================

-ifndef(DEFAULT_TIMEOUT).
-define(DEFAULT_TIMEOUT, 5000).
-endif.
-define(DEFAULT_BATCH_SIZE, 100).
-define(DEFAULT_POOL_SIZE, 10).
%% How many times an append re-reads the stream version and tries again after
%% losing the race to another append. Each lost attempt means another writer
%% committed in between, so the cap bounds livelock rather than ordinary
%% contention. With ?APPEND_BACKOFF_CAP_MS of growth below, a caller that
%% loses all of them is answered {error, {version_conflict, ExpectedVersion,
%% CurrentVersion}} after the 59 pauses - a mean of 1.39 s and a maximum of
%% 2.71 s of sleep on their own, plus the 60 transactions it queued behind.
%% Measured end to end, one stream: 20 writers stay clear of the cap (p99
%% 451 ms, max 909 ms, no conflicts); at 50 writers a share do reach it (5 of
%% 2000 appends, 0.25%), the slowest taking 2.5 s; at 100 writers, 17% (513
%% of 3000) and a slowest of 8.8 s. Through reckon-gater a version_conflict
%% is retried, and each retry is a fresh 60-attempt append: a contended caller
%% therefore usually lands on a later attempt, seconds to tens of seconds
%% later, so the 81-89 s gater figure (measured on an instantly refused
%% error) understates this path. The appenders are the gateway workers or an
%% embedded caller in the same VM, not the writer pool: `?DEFAULT_POOL_SIZE'
%% above is writer/reader_pool_size and is off the append path, while
%% gateway_pool_size defaults to 1 and macula-realm runs 2 - both far inside
%% the clear range.
-define(APPEND_ATTEMPTS, 60).

%% The pause before attempt N+1 is a random delay of at most
%% min(?APPEND_BACKOFF_CAP_MS, 2^(N-1)) milliseconds. Both matter: the
%% doubling takes a writer out of lockstep with the others on a hot stream,
%% and the cap keeps the wait bounded when many writers contend at once. A
%% fixed 0-2 ms jitter is shorter than one append transaction, so writers
%% kept colliding: at 10 and 20 writers on one stream that jitter still
%% reached the cap and lost appends.
-define(APPEND_BACKOFF_CAP_MS, 50).

-define(VERSION_PADDING, 12).  %% Supports up to 999,999,999,999 events per stream

%%====================================================================
%% Store Configuration Record
%%====================================================================

-define(DEFAULT_GATEWAY_POOL_SIZE, 1).

-record(store_config, {
    %% Store identifier
    store_id :: atom(),

    %% Data directory for Khepri/Ra
    data_dir :: string(),

    %% Mode: single | cluster (cluster = default for resilience/backup)
    mode = cluster :: single | cluster,

    %% Default timeout for operations
    timeout = ?DEFAULT_TIMEOUT :: pos_integer(),

    %% Writer pool size
    writer_pool_size = ?DEFAULT_POOL_SIZE :: pos_integer(),

    %% Reader pool size
    reader_pool_size = ?DEFAULT_POOL_SIZE :: pos_integer(),

    %% Gateway worker pool size (for load balancing)
    gateway_pool_size = ?DEFAULT_GATEWAY_POOL_SIZE :: pos_integer(),

    %% Additional options
    options = #{} :: map(),

    %% Tamper-resistance configuration (introduced in 2.1.0).
    %% - `disabled`            : no integrity fields on writes; reads
    %%                           treat all events as legacy. Default.
    %% - #{enabled := true,
    %%    key_source := Src}   : enable HMAC + chain on writes;
    %%                           Src = {env_var, binary()} |
    %%                                 {sealed_file, file:filename()}.
    integrity = disabled :: integrity_config(),

    %% Declared secondary indexes (opt-in, none by default). Each declared
    %% index is maintained transactionally with every append. Declared at
    %% store creation; built from genesis (no backfill — recreate to add).
    %% See plans/DESIGN_SECONDARY_INDEX.md.
    indexes = [] :: [index_decl()]
}).

-type index_decl() ::
    tags |                              %% index every tag in #event.tags
    event_type |                        %% index #event.event_type
    {meta, Key :: binary()} |           %% index maps:get(Key, metadata) when present
    {payload, Key :: binary()} |        %% DCB payload index: top-level JSON field (5.3.0+)
    {payload_hash, Keys :: [binary()]}. %% DCB composite payload hash (5.3.0+)

-type integrity_key_source() ::
    {env_var, EnvName :: binary()} |
    {sealed_file, Path :: file:filename()}.

-type integrity_config() ::
    disabled |
    #{enabled := true, key_source := integrity_key_source()}.

-type store_config() :: #store_config{}.

%% The integrity violation the store itself returns: storage or snapshot
%% layer, always about a concrete version, always with context. Narrower
%% than reckon_gater's integrity_violation(), which also covers the replay
%% and gateway layers.
-type store_integrity_violation() :: {integrity_violation, #{
    layer := storage | snapshot,
    stream_id := binary(),
    version := non_neg_integer(),
    kind := integrity_failure_kind(),
    context := map()
}}.

%%====================================================================
%% Cluster Node Record
%%====================================================================

-record(cluster_node, {
    %% Erlang node name
    node :: node(),

    %% Whether this node is the leader
    is_leader = false :: boolean(),

    %% Node status: up | down | suspected
    status = up :: up | down | suspected,

    %% Last heartbeat timestamp
    last_heartbeat :: integer() | undefined,

    %% Store memberships
    stores :: [atom()]
}).

-type cluster_node() :: #cluster_node{}.

%%====================================================================
%% PG Scope
%%====================================================================

%% Process group scope for event distribution
-define(RECKON_DB_PG_SCOPE, reckon_db_pg).

%%====================================================================
%% Telemetry Event Prefixes
%%====================================================================

-define(TELEMETRY_PREFIX, [reckon_db]).

-endif. %% RECKON_DB_HRL
