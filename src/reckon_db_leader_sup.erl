%% @doc Leader supervisor for reckon-db
%%
%% Manages leader-related components:
%% - Leader tracker (subscription tracking)
%% - Leader worker (leader responsibilities)
%%
%% @author rgfaber

-module(reckon_db_leader_sup).
-behaviour(supervisor).

-include("reckon_db.hrl").

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the leader supervisor
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:leader_sup_name(StoreId),
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
        leader_tracker_spec(Config),
        leader_worker_spec(Config)
    ],

    logger:debug("Starting leader supervisor for store ~p", [StoreId]),

    {ok, {SupFlags, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec leader_tracker_spec(store_config()) -> supervisor:child_spec().
leader_tracker_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:leader_tracker_name(StoreId),
        start => {reckon_db_leader_tracker, start_link, [Config]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [reckon_db_leader_tracker]
    }.

%% @private
-spec leader_worker_spec(store_config()) -> supervisor:child_spec().
leader_worker_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:leader_name(StoreId),
        start => {reckon_db_leader, start_link, [Config]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [reckon_db_leader]
    }.
