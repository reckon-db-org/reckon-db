%% @doc Leader worker for reckon-db
%%
%% Handles leader responsibilities when this node is the Raft leader.
%%
%% Responsibilities:
%% - Save default subscriptions (like $all stream)
%% - Start emitter pools for active subscriptions
%% - Coordinate leader-specific tasks
%%
%% @author rgfaber

-module(reckon_db_leader).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([start_link/1]).
-export([activate/1]).
-export([is_active/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    store_id :: atom(),
    config :: store_config(),
    active :: boolean()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:leader_name(StoreId),
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

%% @doc Activate leader responsibilities
%% Called when this node becomes the cluster leader.
-spec activate(atom()) -> ok | {error, term()}.
activate(StoreId) ->
    Name = reckon_db_naming:leader_name(StoreId),
    case whereis(Name) of
        undefined ->
            {error, not_started};
        _Pid ->
            %% First save default subscriptions synchronously
            case gen_server:call(Name, {save_default_subscriptions, StoreId}, 10000) of
                {ok, _} ->
                    %% Then activate leadership asynchronously
                    gen_server:cast(Name, {activate, StoreId}),
                    logger:info("Leader activated (store: ~p, node: ~p)", [StoreId, node()]),
                    ok;
                {error, Reason} ->
                    logger:warning("Failed to activate leader: ~p (store: ~p)", [Reason, StoreId]),
                    {error, Reason}
            end
    end.

%% @doc Check if leader is currently active
-spec is_active(atom()) -> boolean().
is_active(StoreId) ->
    Name = reckon_db_naming:leader_name(StoreId),
    case whereis(Name) of
        undefined -> false;
        _Pid -> gen_server:call(Name, is_active)
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(#store_config{store_id = StoreId} = Config) ->
    process_flag(trap_exit, true),
    logger:info("Leader worker started (store: ~p)", [StoreId]),
    State = #state{
        store_id = StoreId,
        config = Config,
        active = false
    },
    {ok, State}.

handle_call({save_default_subscriptions, StoreId}, _From, State) ->
    Result = save_default_subscriptions(StoreId),
    {reply, {ok, Result}, State};

handle_call(is_active, _From, #state{active = Active} = State) ->
    {reply, Active, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({activate, StoreId}, State) ->
    logger:info("Activating leadership responsibilities (store: ~p, node: ~p)",
               [StoreId, node()]),

    %% Get all subscriptions
    Subscriptions = get_subscriptions(StoreId),
    SubscriptionCount = length(Subscriptions),

    case SubscriptionCount of
        0 ->
            logger:info("No active subscriptions to manage (store: ~p)", [StoreId]);
        N ->
            logger:info("Managing ~p active subscriptions (store: ~p)", [N, StoreId]),
            %% Start emitters for each subscription
            start_emitters_for_subscriptions(StoreId, Subscriptions)
    end,

    telemetry:execute(
        ?CLUSTER_LEADER_ELECTED,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, leader => node(),
          subscription_count => SubscriptionCount}
    ),

    logger:info("Leadership activation complete (store: ~p)", [StoreId]),
    {noreply, State#state{active = true}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #state{store_id = StoreId}) ->
    logger:info("Leader worker terminating (store: ~p, reason: ~p)", [StoreId, Reason]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Save default subscriptions
-spec save_default_subscriptions(atom()) -> ok | {error, term()}.
save_default_subscriptions(StoreId) ->
    %% Create default $all subscription if it doesn't exist
    case reckon_db_subscriptions:exists(StoreId, <<"all-events">>) of
        false ->
            logger:info("Creating default $all subscription (store: ~p)", [StoreId]),
            reckon_db_subscriptions:subscribe(StoreId, by_stream, <<"$all">>,
                                            <<"all-events">>, #{});
        true ->
            logger:debug("Default $all subscription already exists (store: ~p)", [StoreId]),
            ok
    end.

%% @private Get all subscriptions
-spec get_subscriptions(atom()) -> [subscription()].
get_subscriptions(StoreId) ->
    case reckon_db_subscriptions:list(StoreId) of
        {ok, Subscriptions} -> Subscriptions;
        {error, _} -> []
    end.

%% @private Start emitters for all subscriptions
-spec start_emitters_for_subscriptions(atom(), [subscription()]) -> ok.
start_emitters_for_subscriptions(StoreId, Subscriptions) ->
    lists:foreach(
        fun(Subscription) ->
            start_emitter_for_subscription(StoreId, Subscription)
        end,
        Subscriptions
    ).

%% @private Start emitter for a single subscription
-spec start_emitter_for_subscription(atom(), subscription()) -> ok.
start_emitter_for_subscription(StoreId, #subscription{subscription_name = Name} = Subscription) ->
    case reckon_db_emitter_pool:start_emitter(StoreId, Subscription) of
        {ok, _Pid} ->
            logger:debug("Started emitter pool for subscription: ~s (store: ~p)",
                        [Name, StoreId]);
        {error, {already_started, _Pid}} ->
            logger:debug("Emitter pool already running for subscription: ~s (store: ~p)",
                        [Name, StoreId]);
        {error, Reason} ->
            logger:warning("Failed to start emitter pool for ~s: ~p (store: ~p)",
                          [Name, Reason, StoreId])
    end,
    ok.
