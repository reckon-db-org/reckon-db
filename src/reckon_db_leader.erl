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

-define(DCB_REINDEX_FIRST_RETRY_MS, 5000).
-define(DCB_REINDEX_MAX_RETRY_MS, 300000).

-record(state, {
    store_id :: atom(),
    config :: store_config(),
    active :: boolean(),
    %% The DCB re-index in flight: its process, monitor, and the backoff for
    %% a retry. At most one at a time, run in its own process so this loop
    %% keeps answering is_active/1 (see reindex_dcb/2).
    reindex = undefined :: {pid(), reference(), pos_integer()} | undefined
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
            do_activate(Name, StoreId)
    end.

do_activate(Name, StoreId) ->
    %% Non-blocking: cast activation to the leader worker.
    %% The worker handles save_default_subscriptions + activate sequentially
    %% in its own process, avoiding timeout crashes in the node monitor.
    gen_server:cast(Name, {do_activate, StoreId}),
    ok.

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

%% Legacy — keep for backward compat if anything still calls it
handle_call({activate_sync, StoreId}, _From, State) ->
    save_default_subscriptions(StoreId),
    {reply, ok, State};

handle_call(is_active, _From, #state{active = Active} = State) ->
    {reply, Active, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({do_activate, StoreId}, State) ->
    logger:info("Leader activation starting (store: ~p)", [StoreId]),
    save_default_subscriptions(StoreId),
    activate_leadership(StoreId, State);

handle_cast({activate, StoreId}, State) ->
    activate_leadership(StoreId, State);

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({reindex_dcb, Backoff}, #state{active = true} = State) ->
    {noreply, reindex_dcb(State, Backoff)};
handle_info({dcb_reindexed, Pid, Result}, #state{reindex = {Pid, Ref, Backoff}} = State) ->
    erlang:demonitor(Ref, [flush]),
    {noreply, reindexed(Result, Backoff, State#state{reindex = undefined})};
handle_info({'DOWN', Ref, process, _Pid, Reason}, #state{reindex = {_, Ref, Backoff}} = State) ->
    {noreply, reindexed({error, {crashed, Reason}}, Backoff, State#state{reindex = undefined})};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #state{store_id = StoreId}) ->
    logger:info("Leader worker terminating (store: ~p, reason: ~p)", [StoreId, Reason]),
    ok.

%%====================================================================
%% Internal — activation
%%====================================================================

%% @private Activate leadership: start emitters and manage subscriptions.
activate_leadership(StoreId, State) ->
    logger:info("Activating leadership responsibilities (store: ~p, node: ~p)",
               [StoreId, node()]),

    Subscriptions = get_subscriptions(StoreId),
    SubscriptionCount = length(Subscriptions),

    case SubscriptionCount of
        0 ->
            logger:info("No active subscriptions to manage (store: ~p)", [StoreId]);
        N ->
            logger:info("Managing ~p active subscriptions (store: ~p)", [N, StoreId]),
            start_emitters_for_subscriptions(StoreId, Subscriptions)
    end,

    telemetry:execute(
        ?CLUSTER_LEADER_ELECTED,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, leader => node(),
          subscription_count => SubscriptionCount}
    ),

    logger:info("Leadership activation complete (store: ~p)", [StoreId]),
    {noreply, reindex_dcb(State#state{active = true}, ?DCB_REINDEX_FIRST_RETRY_MS)}.

%% @private Give DCB events written before 5.11.11 their secondary-index
%% entries, once (reckon_db_dcb_reindex). Runs here because activation
%% happens on the Ra leader only, after every start and leadership change;
%% with its marker in place it is one read.
%%
%% It runs in a monitored process of its own: on a large DCB log it takes
%% long enough that running it in this loop blocked is_active/1, the node
%% monitor's call timed out every tick, and its crash restarted the gateway
%% pool (rest_for_one). One run at a time: an activation while one is in
%% flight starts nothing. A failure (logged loudly by the re-index) or a
%% not_leader answer during an election is retried with backoff; activation
%% itself never waits for it, since subscriptions and emitters do not depend
%% on it.
reindex_dcb(#state{reindex = {_InFlight, _, _}} = State, _NewBackoff) ->
    State;
reindex_dcb(#state{store_id = StoreId} = State, Backoff) ->
    Self = self(),
    {Pid, Ref} = spawn_monitor(fun() ->
                                   Self ! {dcb_reindexed, self(), reckon_db_dcb_reindex:run(StoreId)}
                               end),
    State#state{reindex = {Pid, Ref, Backoff}}.

reindexed({ok, #{}}, _Backoff, State) ->
    State;
reindexed(_NotDone, Backoff, State) ->
    erlang:send_after(Backoff, self(),
                      {reindex_dcb, min(Backoff * 2, ?DCB_REINDEX_MAX_RETRY_MS)}),
    State.

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
%%
%% Catches exceptions from the emitter pool startup chain. A stale
%% subscription persisted in Khepri may reference dead subscriber PIDs
%% or an emitter supervisor that hasn't started yet. These must not
%% crash the leader worker — the subscription is skipped with a warning.
-spec start_emitter_for_subscription(atom(), subscription()) -> ok.
start_emitter_for_subscription(StoreId, #subscription{subscription_name = Name} = Subscription) ->
    try reckon_db_emitter_pool:start_emitter(StoreId, Subscription) of
        {ok, _Pid} ->
            logger:debug("Started emitter pool for subscription: ~s (store: ~p)",
                        [Name, StoreId]);
        {error, {already_started, _Pid}} ->
            logger:debug("Emitter pool already running for subscription: ~s (store: ~p)",
                        [Name, StoreId]);
        {error, Reason} ->
            logger:warning("Failed to start emitter pool for ~s: ~p (store: ~p)",
                          [Name, Reason, StoreId])
    catch
        Class:Reason:_Stacktrace ->
            logger:warning("Exception starting emitter pool for ~s: ~p:~p (store: ~p)",
                          [Name, Class, Reason, StoreId]),
            ok
    end,
    ok.
