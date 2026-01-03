%% @doc Notification supervisor for reckon-db
%%
%% Manages notification-related components:
%% - LeaderSystem (leader responsibilities, tracking)
%% - EmitterSystem (event distribution workers)
%%
%% @author rgfaber

-module(reckon_db_notification_sup).
-behaviour(supervisor).

-include("reckon_db.hrl").

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the notification supervisor
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:notification_sup_name(StoreId),
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
        leader_sup_spec(Config),
        emitter_sup_spec(Config)
    ],

    logger:debug("Starting notification supervisor for store ~p", [StoreId]),

    {ok, {SupFlags, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec leader_sup_spec(store_config()) -> supervisor:child_spec().
leader_sup_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:leader_sup_name(StoreId),
        start => {reckon_db_leader_sup, start_link, [Config]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [reckon_db_leader_sup]
    }.

%% @private
-spec emitter_sup_spec(store_config()) -> supervisor:child_spec().
emitter_sup_spec(#store_config{store_id = StoreId} = Config) ->
    #{
        id => reckon_db_naming:emitter_sup_name(StoreId),
        start => {reckon_db_emitter_sup, start_link, [Config]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [reckon_db_emitter_sup]
    }.
