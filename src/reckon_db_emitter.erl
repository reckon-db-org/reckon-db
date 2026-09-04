%% @doc Emitter worker for reckon-db
%%
%% A gen_server that handles event broadcasting for subscriptions.
%% Each subscription has a pool of emitter workers that receive events
%% from Khepri triggers and forward them to subscribers.
%%
%% Message types handled:
%% - {broadcast, Topic, Event}: Broadcast event to all subscribers on topic
%% - {forward_to_local, Topic, Event}: Forward event locally (optimization)
%% - {events, [Event]}: Direct event delivery to subscriber pid
%%
%% @author rgfaber

-module(reckon_db_emitter).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([
    start_link/4,
    child_spec/4,
    update_subscriber/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    store_id :: atom(),
    subscription_key :: binary(),
    subscriber :: pid() | undefined,
    topic :: binary(),
    active = true :: boolean()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start an emitter worker
-spec start_link(atom(), binary(), pid() | undefined, atom()) -> {ok, pid()} | {error, term()}.
start_link(StoreId, SubscriptionKey, Subscriber, EmitterName) ->
    gen_server:start_link({local, EmitterName}, ?MODULE,
                          {StoreId, SubscriptionKey, Subscriber}, []).

%% @doc Create a child spec for the emitter worker
-spec child_spec(atom(), binary(), pid() | undefined, atom()) -> supervisor:child_spec().
child_spec(StoreId, SubscriptionKey, Subscriber, EmitterName) ->
    #{
        id => EmitterName,
        start => {?MODULE, start_link, [StoreId, SubscriptionKey, Subscriber, EmitterName]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

%% @doc Update the subscriber pid
-spec update_subscriber(pid() | atom(), pid()) -> ok.
update_subscriber(Emitter, NewSubscriber) ->
    gen_server:cast(Emitter, {update_subscriber, NewSubscriber}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init({StoreId, SubscriptionKey, Subscriber}) ->
    process_flag(trap_exit, true),

    Topic = reckon_db_emitter_group:topic(StoreId, SubscriptionKey),

    %% Join the emitter group
    ok = reckon_db_emitter_group:join(StoreId, SubscriptionKey, self()),

    logger:info("Emitter worker started: store=~p, subscription=~s, topic=~s",
                [StoreId, SubscriptionKey, Topic]),

    State = #state{
        store_id = StoreId,
        subscription_key = SubscriptionKey,
        subscriber = Subscriber,
        topic = Topic,
        active = true
    },

    {ok, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({update_subscriber, NewSubscriber}, State) ->
    {noreply, State#state{subscriber = NewSubscriber}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Handle broadcast messages (from remote emitters)
handle_info({broadcast, Topic, Event}, State) ->
    handle_event_delivery(Topic, Event, State),
    {noreply, State};

%% Handle forward_to_local messages (from local emitters)
handle_info({forward_to_local, Topic, Event}, State) ->
    handle_event_delivery(Topic, Event, State),
    {noreply, State};

%% Handle direct events message
handle_info({events, Events}, #state{subscriber = Subscriber} = State) when is_list(Events) ->
    maybe_forward_events(Subscriber, Events),
    {noreply, State};

%% Handle EXIT from linked processes
handle_info({'EXIT', Pid, Reason}, #state{subscriber = Subscriber} = State) ->
    case Subscriber of
        Pid ->
            logger:info("Subscriber ~p exited with reason: ~p", [Pid, Reason]),
            {noreply, State#state{subscriber = undefined}};
        _ ->
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #state{store_id = StoreId, subscription_key = SubscriptionKey}) ->
    %% Leave the emitter group
    ok = reckon_db_emitter_group:leave(StoreId, SubscriptionKey, self()),

    logger:info("Emitter worker terminating: store=~p, subscription=~s, reason=~p",
                [StoreId, SubscriptionKey, Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Handle event delivery to subscriber
-spec handle_event_delivery(binary(), event(), #state{}) -> ok.
handle_event_delivery(_Topic, _Event, #state{active = false}) ->
    ok;
handle_event_delivery(Topic, Event, #state{
    store_id = StoreId,
    subscription_key = SubscriptionKey,
    subscriber = Subscriber
}) ->
    telemetry:execute(
        ?SUBSCRIPTION_EVENT_DELIVERED,
        #{count => 1},
        #{store_id => StoreId, subscription_key => SubscriptionKey, topic => Topic}
    ),
    deliver_event(Subscriber, Event, StoreId, SubscriptionKey, Topic).

maybe_forward_events(undefined, _Events) ->
    ok;
maybe_forward_events(Pid, Events) when is_pid(Pid), node(Pid) =:= node() ->
    case erlang:is_process_alive(Pid) of
        true -> Pid ! {events, Events};
        false -> ok
    end;
maybe_forward_events(Pid, Events) when is_pid(Pid) ->
    %% Remote pid — `!' works across Erlang distribution. The previous
    %% silent-drop here caused subscriptions to lose events whenever
    %% the broadcast/catch-up source ran on a node other than the
    %% subscriber's node.
    Pid ! {events, Events},
    ok.

deliver_event(undefined, Event, StoreId, _SubscriptionKey, Topic) ->
    broadcast_to_topic(StoreId, Topic, Event);
deliver_event(Pid, Event, StoreId, SubscriptionKey, _Topic) when is_pid(Pid) ->
    send_to_subscriber(Pid, Event, StoreId, SubscriptionKey).

%% @private Broadcast event to topic subscribers via pg
-spec broadcast_to_topic(atom(), binary(), event()) -> ok.
broadcast_to_topic(StoreId, Topic, Event) ->
    %% Get subscribers for the topic
    Group = {StoreId, Topic, subscribers},
    Members = pg:get_members(?RECKON_DB_PG_SCOPE, Group),

    lists:foreach(
        fun(Pid) ->
            Pid ! {events, [Event]}
        end,
        Members
    ),
    ok.

%% @private Send event directly to subscriber. Liveness probing only
%% runs for local pids — `erlang:is_process_alive/1' is undefined for
%% remote pids. For remote subscribers we rely on Erlang distribution
%% delivery semantics (the message is dropped silently by the runtime
%% if the remote pid is dead — same effect as the local liveness
%% short-circuit, just less observable).
%%
%% The previous implementation silently dropped EVERY remote-pid
%% delivery, which made `by_stream' subscriptions miss events whenever
%% the trigger fired on a node other than the subscriber's node
%% (i.e. virtually always, for subscribers attached to a non-leader
%% gateway).
-spec send_to_subscriber(pid(), event(), atom(), binary()) -> ok.
send_to_subscriber(Pid, Event, StoreId, SubscriptionKey) when node(Pid) =:= node() ->
    case erlang:is_process_alive(Pid) of
        true ->
            Pid ! {events, [Event]},
            ok;
        false ->
            logger:warning("Subscriber ~p is dead for subscription ~s in store ~p, "
                           "stopping emitter pool",
                          [Pid, SubscriptionKey, StoreId]),
            stop_pool_async(StoreId, SubscriptionKey)
    end;
send_to_subscriber(Pid, Event, _StoreId, _SubscriptionKey) when is_pid(Pid) ->
    Pid ! {events, [Event]},
    ok.

%% @private Stop the emitter pool asynchronously so event delivery isn't
%% blocked during the (rare) dead-subscriber cleanup.
stop_pool_async(StoreId, SubscriptionKey) ->
    spawn(fun() -> reckon_db_emitter_pool:stop(StoreId, SubscriptionKey) end),
    ok.
