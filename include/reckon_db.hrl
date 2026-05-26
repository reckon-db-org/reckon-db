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

-define(RECKON_DB_VERSION, "2.2.0").

%%====================================================================
%% Khepri Paths
%%====================================================================

-define(STREAMS_PATH, [streams]).
-define(SNAPSHOTS_PATH, [snapshots]).
-define(SUBSCRIPTIONS_PATH, [subscriptions]).
-define(METADATA_PATH, [metadata]).

%% DCB (Dynamic Consistency Boundary) paths — Phase 3, 2.4.0+.
%% DCB events live under one pseudo-stream `_dcb`. Each event is also
%% indexed by every tag it carries, so the conditional-append primitive
%% can scan a bounded subtree (per-tag) inside a Khepri transaction
%% instead of the full event log.
%%
%% See: plans/PLAN_DCB_IMPLEMENTATION.md
-define(DCB_STREAM, <<"_dcb">>).
-define(DCB_STREAM_PATH, [events, ?DCB_STREAM]).
-define(BY_TAG_PATH, [by_tag]).
%% Fixed-width zero-padded DECIMAL keys so lexicographic order == numeric
%% order for subtree iteration. 20 digits covers up to 10^20 events.
%% Matches the existing pad_version convention in reckon_db_snapshots_store.
-define(DCB_SEQ_KEY_WIDTH, 20).

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
    integrity = disabled :: integrity_config()
}).

-type integrity_key_source() ::
    {env_var, EnvName :: binary()} |
    {sealed_file, Path :: file:filename()}.

-type integrity_config() ::
    disabled |
    #{enabled := true, key_source := integrity_key_source()}.

-type store_config() :: #store_config{}.

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
