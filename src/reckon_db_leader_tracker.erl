%% @doc Leader tracker for reckon-db
%%
%% Tracks subscriptions and coordinates with pg groups.
%%
%% Responsibilities:
%% - Observe subscription changes via tracker_group
%% - Start emitter pools when subscriptions are created (on leader)
%% - Stop emitter pools when subscriptions are deleted (on leader)
%% - Update emitter pools when subscriptions are modified (on leader)
%%
%% Since Khepri triggers execute on the leader node, this module
%% coordinates emitter lifecycle with subscription changes.
%%
%% @author rgfaber

-module(reckon_db_leader_tracker).
-behaviour(gen_server).

-include("reckon_db.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    store_id :: atom(),
    config :: store_config()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:leader_tracker_name(StoreId),
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(#store_config{store_id = StoreId} = Config) ->
    process_flag(trap_exit, true),
    logger:info("Leader tracker started (store: ~p)", [StoreId]),

    %% Setup subscription tracking via tracker_group
    ok = reckon_db_subscriptions:setup_tracking(StoreId, self()),

    State = #state{
        store_id = StoreId,
        config = Config
    },
    {ok, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Handle subscription created event
handle_info({feature_created, subscriptions, Data}, #state{store_id = StoreId} = State) ->
    logger:debug("Subscription created: ~p (store: ~p)", [Data, StoreId]),
    handle_subscription_created(StoreId, Data),
    {noreply, State};

%% Handle subscription updated event
handle_info({feature_updated, subscriptions, Data}, #state{store_id = StoreId} = State) ->
    logger:debug("Subscription updated: ~p (store: ~p)", [Data, StoreId]),
    handle_subscription_updated(StoreId, Data),
    {noreply, State};

%% Handle subscription deleted event
handle_info({feature_deleted, subscriptions, Data}, #state{store_id = StoreId} = State) ->
    logger:debug("Subscription deleted: ~p (store: ~p)", [Data, StoreId]),
    handle_subscription_deleted(StoreId, Data),
    {noreply, State};

%% Handle EXIT from linked processes
handle_info({'EXIT', Pid, Reason}, #state{store_id = StoreId} = State) ->
    logger:warning("Linked process ~p exited: ~p (store: ~p)", [Pid, Reason, StoreId]),
    %% Leave tracker group on exit
    reckon_db_tracker_group:leave(StoreId, subscriptions, self()),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #state{store_id = StoreId}) ->
    logger:info("Leader tracker terminating: ~p (store: ~p)", [Reason, StoreId]),
    %% Leave tracker group
    reckon_db_tracker_group:leave(StoreId, subscriptions, self()),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Handle subscription created
-spec handle_subscription_created(atom(), map() | subscription()) -> ok.
handle_subscription_created(StoreId, Data) ->
    case reckon_db_store_coordinator:is_leader(StoreId) of
        true ->
            Subscription = format_subscription_data(Data),
            start_emitter_pool(StoreId, Subscription);
        false ->
            ok
    end.

%% @private Handle subscription updated
-spec handle_subscription_updated(atom(), map() | subscription()) -> ok.
handle_subscription_updated(StoreId, Data) ->
    case reckon_db_store_coordinator:is_leader(StoreId) of
        true ->
            Subscription = format_subscription_data(Data),
            update_emitter_pool(StoreId, Subscription);
        false ->
            ok
    end.

%% @private Handle subscription deleted
-spec handle_subscription_deleted(atom(), map() | subscription()) -> ok.
handle_subscription_deleted(StoreId, Data) ->
    case reckon_db_store_coordinator:is_leader(StoreId) of
        true ->
            Subscription = format_subscription_data(Data),
            stop_emitter_pool(StoreId, Subscription);
        false ->
            ok
    end.

%% @private Start emitter pool for subscription
-spec start_emitter_pool(atom(), subscription()) -> ok.
start_emitter_pool(StoreId, #subscription{subscription_name = Name} = Subscription) ->
    case reckon_db_emitter_pool:start_emitter(StoreId, Subscription) of
        {ok, _Pid} ->
            logger:info("Started emitter pool for subscription: ~s (store: ~p)",
                       [Name, StoreId]);
        {error, {already_started, _Pid}} ->
            logger:debug("Emitter pool already running for: ~s (store: ~p)",
                        [Name, StoreId]);
        {error, Reason} ->
            logger:warning("Failed to start emitter pool for ~s: ~p (store: ~p)",
                          [Name, Reason, StoreId])
    end,
    ok.

%% @private Update emitter pool for subscription
-spec update_emitter_pool(atom(), subscription()) -> ok.
update_emitter_pool(StoreId, #subscription{subscription_name = Name} = Subscription) ->
    case reckon_db_emitter_pool:update_emitter(StoreId, Subscription) of
        ok ->
            logger:debug("Updated emitter pool for: ~s (store: ~p)", [Name, StoreId]);
        {error, Reason} ->
            logger:warning("Failed to update emitter pool for ~s: ~p (store: ~p)",
                          [Name, Reason, StoreId])
    end,
    ok.

%% @private Stop emitter pool for subscription
-spec stop_emitter_pool(atom(), subscription()) -> ok.
stop_emitter_pool(StoreId, #subscription{subscription_name = Name} = Subscription) ->
    case reckon_db_emitter_pool:stop_emitter(StoreId, Subscription) of
        ok ->
            logger:info("Stopped emitter pool for: ~s (store: ~p)", [Name, StoreId]);
        {error, Reason} ->
            logger:warning("Failed to stop emitter pool for ~s: ~p (store: ~p)",
                          [Name, Reason, StoreId])
    end,
    ok.

%% @private Format subscription data into a subscription record
-spec format_subscription_data(map() | subscription()) -> subscription().
format_subscription_data(#subscription{} = Sub) ->
    Sub;
format_subscription_data(Data) when is_map(Data) ->
    #subscription{
        id = maps:get(id, Data, undefined),
        type = maps:get(type, Data, by_stream),
        selector = maps:get(selector, Data, undefined),
        subscription_name = maps:get(subscription_name, Data,
                                     maps:get(name, Data, <<"unknown">>)),
        subscriber_pid = maps:get(subscriber_pid, Data,
                                  maps:get(subscriber, Data, undefined)),
        created_at = maps:get(created_at, Data, 0),
        pool_size = maps:get(pool_size, Data, 1)
    };
format_subscription_data(_) ->
    #subscription{
        type = by_stream,
        subscription_name = <<"unknown">>,
        selector = <<"unknown">>,
        pool_size = 1
    }.
