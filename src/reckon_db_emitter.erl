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
    %% Forward events to subscriber if present
    case Subscriber of
        undefined -> ok;
        Pid when is_pid(Pid) ->
            case erlang:is_process_alive(Pid) of
                true -> Pid ! {events, Events};
                false -> ok
            end
    end,
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
handle_event_delivery(Topic, Event, #state{
    store_id = StoreId,
    subscription_key = SubscriptionKey,
    subscriber = Subscriber,
    active = Active
}) ->
    case Active of
        false ->
            ok;
        true ->
            %% Emit telemetry
            telemetry:execute(
                ?SUBSCRIPTION_EVENT_DELIVERED,
                #{count => 1},
                #{store_id => StoreId, subscription_key => SubscriptionKey, topic => Topic}
            ),

            %% Deliver the event
            case Subscriber of
                undefined ->
                    %% Broadcast to pg group for topic
                    broadcast_to_topic(StoreId, Topic, Event);
                Pid when is_pid(Pid) ->
                    %% Send directly to subscriber
                    send_to_subscriber(Pid, Event, StoreId, SubscriptionKey)
            end
    end.

%% @private Broadcast event to topic subscribers via pg
-spec broadcast_to_topic(atom(), binary(), event()) -> ok.
broadcast_to_topic(StoreId, Topic, Event) ->
    %% Get subscribers for the topic
    Group = {StoreId, Topic, subscribers},
    Members = pg:get_members(?RECKON_DB_PG_SCOPE, Group),

    lists:foreach(
        fun(Pid) ->
            catch Pid ! {events, [Event]}
        end,
        Members
    ),
    ok.

%% @private Send event directly to subscriber, stop pool if subscriber is dead
-spec send_to_subscriber(pid(), event(), atom(), binary()) -> ok.
send_to_subscriber(Pid, Event, StoreId, SubscriptionKey) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Pid ! {events, [Event]},
            ok;
        false ->
            logger:warning("Subscriber ~p is dead for subscription ~s in store ~p, "
                           "stopping emitter pool",
                          [Pid, SubscriptionKey, StoreId]),
            %% Stop the emitter pool asynchronously to avoid blocking the
            %% emitter worker during event delivery
            spawn(fun() -> reckon_db_emitter_pool:stop(StoreId, SubscriptionKey) end),
            ok
    end.
