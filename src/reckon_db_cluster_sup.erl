%% @doc Cluster supervisor for reckon-db
%%
%% Manages cluster-related components (cluster mode only):
%% - Discovery (UDP multicast / K8s DNS)
%% - Store coordinator (cluster join coordination)
%%
%% Note: Node monitor is started by system_sup for all modes.
%%
%% @author rgfaber

-module(reckon_db_cluster_sup).
-behaviour(supervisor).

-include("reckon_db.hrl").

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the cluster supervisor
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:cluster_sup_name(StoreId),
    supervisor:start_link({local, Name}, ?MODULE, Config).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
-spec init(store_config()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(#store_config{store_id = StoreId} = Config) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 30
    },

    Children = [
        discovery_spec(Config),
        coordinator_spec(Config)
    ],

    logger:debug("Starting cluster supervisor for store ~p", [StoreId]),

    {ok, {SupFlags, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec discovery_spec(store_config()) -> supervisor:child_spec().
discovery_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:discovery_name(StoreId),
        start => {reckon_db_discovery, start_link, [Config]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [reckon_db_discovery]
    }.

%% @private
-spec coordinator_spec(store_config()) -> supervisor:child_spec().
coordinator_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:coordinator_name(StoreId),
        start => {reckon_db_store_coordinator, start_link, [Config]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [reckon_db_store_coordinator]
    }.

