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
    stop_for_dead/3,
    serialized/3,
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
    case stop_emitter(StoreId, Subscription) of
        ok ->
            restart_emitter(StoreId, Subscription);
        Error ->
            Error
    end.

restart_emitter(StoreId, Subscription) ->
    case start_emitter(StoreId, Subscription) of
        {ok, _Pid} -> ok;
        Error -> Error
    end.

%% @doc Stop the pool of a subscription whose subscriber `Dead' died,
%% unless the subscription names another subscriber by now.
%%
%% The emitter that found `Dead' asks for this asynchronously, so a
%% re-subscribe by the restarted subscriber can come first. It wrote the
%% new pid and saw this pool running, so it started none: stopping the pool
%% then left the new subscriber with no emitters (reckon-db#24). The check
%% and the stop run under serialized/3, the same section as the
%% re-subscribe's write and its pool check, so one of the two always sees
%% what the other did.
-spec stop_for_dead(atom(), binary(), pid()) -> ok | {error, term()}.
stop_for_dead(StoreId, SubscriptionKey, Dead) ->
    serialized(StoreId, SubscriptionKey,
               fun() -> stop_unless_replaced(named_subscriber(StoreId, SubscriptionKey), Dead,
                                             StoreId, SubscriptionKey) end).

%% @doc Run `Fun' in the section a subscription's pool is stopped or
%% re-pointed in, on this node. The pools stopped here are this node's,
%% and a subscriber that re-subscribes does so on its own node.
-spec serialized(atom(), binary(), fun(() -> Result)) -> Result.
serialized(StoreId, SubscriptionKey, Fun) ->
    global:trans({{?MODULE, StoreId, SubscriptionKey}, self()}, Fun, [node()]).

named_subscriber(StoreId, SubscriptionKey) ->
    subscriber_of(reckon_db_subscriptions_store:get(StoreId, SubscriptionKey)).

subscriber_of(#subscription{subscriber_pid = Pid}) -> Pid;
subscriber_of(_NotFound) -> undefined.

stop_unless_replaced(Other, Dead, _StoreId, _SubscriptionKey) when is_pid(Other), Other =/= Dead ->
    ok;
stop_unless_replaced(_Same, _Dead, StoreId, SubscriptionKey) ->
    stop(StoreId, SubscriptionKey).

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
