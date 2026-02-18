%% @doc Core records and macros for reckon-db
%% @author Reckon-DB

-ifndef(RECKON_DB_HRL).
-define(RECKON_DB_HRL, true).

%% Include shared types from reckon-gater
%% These define: #event{}, #subscription{}, #snapshot{}, #append_result{},
%% subscription_type(), read_direction(), append_error(), read_error(),
%% and constants: NO_STREAM, ANY_VERSION, STREAM_EXISTS, CONTENT_TYPE_*
-include_lib("reckon_gater/include/esdb_gater_types.hrl").

%%====================================================================
%% Version
%%====================================================================

-define(RECKON_DB_VERSION, "0.1.0").

%%====================================================================
%% Khepri Paths
%%====================================================================

-define(STREAMS_PATH, [streams]).
-define(SNAPSHOTS_PATH, [snapshots]).
-define(SUBSCRIPTIONS_PATH, [subscriptions]).
-define(METADATA_PATH, [metadata]).

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

    %% Mode: single | cluster
    mode = single :: single | cluster,

    %% Default timeout for operations
    timeout = ?DEFAULT_TIMEOUT :: pos_integer(),

    %% Writer pool size
    writer_pool_size = ?DEFAULT_POOL_SIZE :: pos_integer(),

    %% Reader pool size
    reader_pool_size = ?DEFAULT_POOL_SIZE :: pos_integer(),

    %% Gateway worker pool size (for load balancing)
    gateway_pool_size = ?DEFAULT_GATEWAY_POOL_SIZE :: pos_integer(),

    %% Additional options
    options = #{} :: map()
}).

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
