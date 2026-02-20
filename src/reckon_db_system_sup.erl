%% @doc Per-store system supervisor for reckon-db
%%
%% This supervisor manages all subsystems for a single store instance.
%% Uses rest_for_one strategy to ensure proper startup order:
%%
%% 1. CoreSystem (one_for_all) - persistence, notification, store management
%% 2. ClusterSystem (cluster mode only) - discovery, coordination, monitoring
%% 3. GatewaySystem - external interface workers
%%
%% @author rgfaber

-module(reckon_db_system_sup).
-behaviour(supervisor).

-include("reckon_db.hrl").

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the system supervisor for a store
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:system_sup_name(StoreId),
    supervisor:start_link({local, Name}, ?MODULE, Config).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
-spec init(store_config()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(#store_config{store_id = StoreId, mode = Mode} = Config) ->
    SupFlags = #{
        strategy => rest_for_one,
        intensity => 5,
        period => 30
    },

    %% Core children (always present)
    CoreChildren = [
        core_sup_spec(Config)
    ],

    %% Cluster children (cluster mode only)
    ClusterChildren = case Mode of
        cluster ->
            [cluster_sup_spec(Config)];
        single ->
            []
    end,

    %% Node monitor (all modes) — detects Ra leader and activates
    %% leader responsibilities. Must start after CoreSystem (store up)
    %% and after ClusterSystem (coordinator available in cluster mode).
    MonitorChildren = [
        node_monitor_spec(Config)
    ],

    %% Gateway children (always present, starts last)
    GatewayChildren = [
        gateway_sup_spec(Config)
    ],

    Children = CoreChildren ++ ClusterChildren ++ MonitorChildren ++ GatewayChildren,

    logger:info("Starting reckon-db system for store ~p in ~p mode", [StoreId, Mode]),

    {ok, {SupFlags, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec core_sup_spec(store_config()) -> supervisor:child_spec().
core_sup_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:core_sup_name(StoreId),
        start => {reckon_db_core_sup, start_link, [Config]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [reckon_db_core_sup]
    }.

%% @private
-spec cluster_sup_spec(store_config()) -> supervisor:child_spec().
cluster_sup_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:cluster_sup_name(StoreId),
        start => {reckon_db_cluster_sup, start_link, [Config]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [reckon_db_cluster_sup]
    }.

%% @private Node monitor — runs in all modes for leader detection.
%% In single mode: detects Ra leader, activates LeaderWorker, stops polling.
%% In cluster mode: continuous leader/membership monitoring.
-spec node_monitor_spec(store_config()) -> supervisor:child_spec().
node_monitor_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:node_monitor_name(StoreId),
        start => {reckon_db_node_monitor, start_link, [Config]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [reckon_db_node_monitor]
    }.

%% @private
-spec gateway_sup_spec(store_config()) -> supervisor:child_spec().
gateway_sup_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:gateway_sup_name(StoreId),
        start => {reckon_db_gateway_sup, start_link, [Config]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [reckon_db_gateway_sup]
    }.
