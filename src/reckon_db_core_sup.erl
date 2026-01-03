%% @doc Core supervisor for reckon-db
%%
%% Manages tightly-coupled core components using one_for_all strategy:
%% - PersistenceSystem (Khepri store, streams, snapshots, subscriptions)
%% - NotificationSystem (leader, emitters)
%% - StoreMgr (store lifecycle coordination)
%%
%% If any child fails, all children are restarted to ensure consistency.
%%
%% @author rgfaber

-module(reckon_db_core_sup).
-behaviour(supervisor).

-include("reckon_db.hrl").

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the core supervisor
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:core_sup_name(StoreId),
    supervisor:start_link({local, Name}, ?MODULE, Config).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
-spec init(store_config()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(#store_config{store_id = StoreId} = Config) ->
    %% CRITICAL: one_for_all ensures all core components restart together
    %% This maintains consistency between persistence, notification, and store management
    SupFlags = #{
        strategy => one_for_all,
        intensity => 5,
        period => 30
    },

    Children = [
        persistence_sup_spec(Config),
        notification_sup_spec(Config),
        store_mgr_spec(Config)
    ],

    logger:debug("Starting core supervisor for store ~p", [StoreId]),

    {ok, {SupFlags, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec persistence_sup_spec(store_config()) -> supervisor:child_spec().
persistence_sup_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:persistence_sup_name(StoreId),
        start => {reckon_db_persistence_sup, start_link, [Config]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [reckon_db_persistence_sup]
    }.

%% @private
-spec notification_sup_spec(store_config()) -> supervisor:child_spec().
notification_sup_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:notification_sup_name(StoreId),
        start => {reckon_db_notification_sup, start_link, [Config]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [reckon_db_notification_sup]
    }.

%% @private
-spec store_mgr_spec(store_config()) -> supervisor:child_spec().
store_mgr_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:store_mgr_name(StoreId),
        start => {reckon_db_store_mgr, start_link, [Config]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [reckon_db_store_mgr]
    }.
