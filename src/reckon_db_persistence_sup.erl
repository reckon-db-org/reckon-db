%% @doc Persistence supervisor for reckon-db
%%
%% Manages persistence-related components:
%% - Khepri store worker
%% - Streams supervisor (writers/readers)
%% - Persistence worker (batched flush, currently disabled)
%%
%% Note: Snapshots and Subscriptions stores are facade modules that
%% work directly with Khepri without needing gen_servers.
%%
%% @author rgfaber

-module(reckon_db_persistence_sup).
-behaviour(supervisor).

-include("reckon_db.hrl").

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the persistence supervisor
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:persistence_sup_name(StoreId),
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
        %% Khepri store must start first
        store_spec(Config),
        %% Streams supervisor (writers/readers)
        streams_sup_spec(Config),
        %% Persistence worker (batched flush, disabled for now)
        persistence_worker_spec(Config)
    ],

    logger:debug("Starting persistence supervisor for store ~p", [StoreId]),

    {ok, {SupFlags, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec store_spec(store_config()) -> supervisor:child_spec().
store_spec(Config) ->
    #{
        id => reckon_db_store,
        start => {reckon_db_store, start_link, [Config]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [reckon_db_store]
    }.

%% @private
-spec streams_sup_spec(store_config()) -> supervisor:child_spec().
streams_sup_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:streams_sup_name(StoreId),
        start => {reckon_db_streams_sup, start_link, [Config]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [reckon_db_streams_sup]
    }.

%% @private
-spec persistence_worker_spec(store_config()) -> supervisor:child_spec().
persistence_worker_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:persistence_worker_name(StoreId),
        start => {reckon_db_persistence_worker, start_link, [Config]},
        restart => permanent,
        %% Allow time for final persistence on shutdown
        shutdown => 10000,
        type => worker,
        modules => [reckon_db_persistence_worker]
    }.
