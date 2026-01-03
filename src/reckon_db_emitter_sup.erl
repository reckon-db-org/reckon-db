%% @doc Emitter supervisor for reckon-db
%%
%% Manages emitter pools for subscriptions. Emitter pools are created
%% dynamically when subscriptions are registered.
%%
%% @author rgfaber

-module(reckon_db_emitter_sup).
-behaviour(supervisor).

-include("reckon_db.hrl").

%% API
-export([start_link/1]).
-export([start_emitter_pool/2, stop_emitter_pool/2]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the emitter supervisor
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:emitter_sup_name(StoreId),
    supervisor:start_link({local, Name}, ?MODULE, Config).

%% @doc Start an emitter pool for a subscription
-spec start_emitter_pool(atom(), subscription()) -> {ok, pid()} | {error, term()}.
start_emitter_pool(StoreId, Subscription) ->
    SupName = reckon_db_naming:emitter_sup_name(StoreId),
    ChildSpec = emitter_pool_spec(StoreId, Subscription),
    supervisor:start_child(SupName, ChildSpec).

%% @doc Stop an emitter pool
-spec stop_emitter_pool(atom(), binary()) -> ok | {error, term()}.
stop_emitter_pool(StoreId, SubscriptionId) ->
    SupName = reckon_db_naming:emitter_sup_name(StoreId),
    ChildId = reckon_db_naming:emitter_pool_name(StoreId, SubscriptionId),
    case supervisor:terminate_child(SupName, ChildId) of
        ok ->
            supervisor:delete_child(SupName, ChildId);
        Error ->
            Error
    end.

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
-spec init(store_config()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(#store_config{store_id = StoreId} = _Config) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },

    %% Emitter pools are started dynamically
    Children = [],

    logger:debug("Starting emitter supervisor for store ~p", [StoreId]),

    {ok, {SupFlags, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec emitter_pool_spec(atom(), subscription()) -> supervisor:child_spec().
emitter_pool_spec(StoreId, #subscription{id = SubId} = Subscription) ->
    #{
        id => reckon_db_naming:emitter_pool_name(StoreId, SubId),
        start => {reckon_db_emitter_pool, start_link, [StoreId, Subscription]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [reckon_db_emitter_pool]
    }.
