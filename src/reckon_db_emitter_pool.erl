%% @doc Emitter pool supervisor for reckon-db
%%
%% Supervises a pool of emitter workers for a single subscription.
%% Each subscription can have multiple emitter workers for load distribution.
%%
%% @author rgfaber

-module(reckon_db_emitter_pool).
-behaviour(supervisor).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([
    start_link/2,
    start_emitter/2,
    stop_emitter/2,
    update_emitter/2,
    stop/2,
    name/2
]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Generate the name for an emitter pool
-spec name(atom(), binary()) -> atom().
name(StoreId, SubscriptionKey) ->
    reckon_db_naming:emitter_pool_name(StoreId, SubscriptionKey).

%% @doc Start the emitter pool supervisor
-spec start_link(atom(), subscription()) -> {ok, pid()} | {error, term()}.
start_link(StoreId, #subscription{id = SubId} = Subscription) ->
    Name = name(StoreId, SubId),
    supervisor:start_link({local, Name}, ?MODULE, {StoreId, Subscription}).

%% @doc Start an emitter pool for a subscription
-spec start_emitter(atom(), subscription()) -> {ok, pid()} | {error, term()}.
start_emitter(StoreId, #subscription{id = SubId} = Subscription) ->
    Name = name(StoreId, SubId),
    case whereis(Name) of
        undefined ->
            %% Start via emitter supervisor
            reckon_db_emitter_sup:start_emitter_pool(StoreId, Subscription);
        Pid ->
            {error, {already_started, Pid}}
    end.

%% @doc Stop an emitter pool for a subscription
-spec stop_emitter(atom(), subscription()) -> ok | {error, term()}.
stop_emitter(StoreId, #subscription{id = SubId}) ->
    Name = name(StoreId, SubId),
    case whereis(Name) of
        undefined ->
            ok;
        _Pid ->
            reckon_db_emitter_sup:stop_emitter_pool(StoreId, SubId)
    end.

%% @doc Update an emitter pool configuration
-spec update_emitter(atom(), subscription()) -> ok | {error, term()}.
update_emitter(StoreId, #subscription{} = Subscription) ->
    %% For now, restart the pool to apply changes
    %% Future: implement hot reconfiguration
    case stop_emitter(StoreId, Subscription) of
        ok ->
            case start_emitter(StoreId, Subscription) of
                {ok, _Pid} -> ok;
                Error -> Error
            end;
        Error ->
            Error
    end.

%% @doc Stop an emitter pool by key (deprecated, use stop_emitter/2)
-spec stop(atom(), binary()) -> ok | {error, term()}.
stop(StoreId, SubscriptionKey) ->
    Name = name(StoreId, SubscriptionKey),
    case whereis(Name) of
        undefined -> ok;
        _Pid ->
            reckon_db_emitter_sup:stop_emitter_pool(StoreId, SubscriptionKey)
    end.

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init({StoreId, #subscription{id = SubId, subscriber_pid = Subscriber,
                              pool_size = PoolSize} = _Subscription}) ->
    %% Get or create emitter names
    EmitterNames = case reckon_db_emitter_group:retrieve_emitters(StoreId, SubId) of
        [] ->
            %% Not yet persisted, create them
            reckon_db_emitter_group:persist_emitters(StoreId, SubId, PoolSize);
        Names ->
            Names
    end,

    %% Create child specs for each emitter
    Children = [
        reckon_db_emitter:child_spec(StoreId, SubId, Subscriber, EmitterName)
        || EmitterName <- EmitterNames
    ],

    %% Emit telemetry
    telemetry:execute(
        ?EMITTER_POOL_CREATED,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, subscription_id => SubId, pool_size => PoolSize}
    ),

    logger:info("Starting emitter pool: store=~p, subscription=~s, pool_size=~p",
                [StoreId, SubId, PoolSize]),

    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },

    {ok, {SupFlags, Children}}.
