%% @doc Backpressure management for reckon-db subscriptions
%%
%% Provides queue-based backpressure handling to prevent memory
%% explosion when subscribers are slower than event producers.
%%
%% Features:
%% - Configurable queue size limits
%% - Multiple overflow strategies (drop_oldest, drop_newest, block, error)
%% - Pull mode for explicit demand
%% - Warning thresholds with telemetry
%%
%% Usage:
%% ```
%% {ok, Queue} = reckon_db_backpressure:new(#{
%%     max_queue => 1000,
%%     strategy => drop_oldest,
%%     warning_threshold => 800
%% }),
%% {ok, Queue2} = reckon_db_backpressure:enqueue(Queue, Event),
%% {ok, Events, Queue3} = reckon_db_backpressure:dequeue(Queue2, 10).
%% '''
%%
%% @author rgfaber

-module(reckon_db_backpressure).

-include("reckon_db.hrl").

%% API
-export([
    new/1,
    enqueue/2,
    enqueue_many/2,
    dequeue/2,
    dequeue_all/1,
    size/1,
    is_empty/1,
    is_full/1,
    info/1,
    set_demand/2,
    add_demand/2,
    get_demand/1
]).

%%====================================================================
%% Types
%%====================================================================

-type strategy() :: drop_oldest | drop_newest | block | error.
-type mode() :: push | pull.

-record(bp_queue, {
    queue :: queue:queue(event()),
    max_size :: pos_integer(),
    strategy :: strategy(),
    mode :: mode(),
    warning_threshold :: pos_integer(),
    demand :: non_neg_integer(),  %% Outstanding demand (for pull mode)
    dropped :: non_neg_integer(), %% Count of dropped events
    store_id :: atom(),
    subscription_key :: binary()
}).

-type bp_queue() :: #bp_queue{}.
-type bp_opts() :: #{
    max_queue => pos_integer(),
    strategy => strategy(),
    mode => mode(),
    warning_threshold => pos_integer(),
    store_id => atom(),
    subscription_key => binary()
}.

-export_type([bp_queue/0, bp_opts/0, strategy/0, mode/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new backpressure queue.
-spec new(bp_opts()) -> {ok, bp_queue()}.
new(Opts) ->
    MaxQueue = maps:get(max_queue, Opts, 1000),
    Strategy = maps:get(strategy, Opts, drop_oldest),
    Mode = maps:get(mode, Opts, push),
    WarningThreshold = maps:get(warning_threshold, Opts, trunc(MaxQueue * 0.8)),
    StoreId = maps:get(store_id, Opts, undefined),
    SubscriptionKey = maps:get(subscription_key, Opts, undefined),

    Queue = #bp_queue{
        queue = queue:new(),
        max_size = MaxQueue,
        strategy = Strategy,
        mode = Mode,
        warning_threshold = WarningThreshold,
        demand = case Mode of
            push -> infinity;
            pull -> 0
        end,
        dropped = 0,
        store_id = StoreId,
        subscription_key = SubscriptionKey
    },
    {ok, Queue}.

%% @doc Enqueue an event, applying backpressure strategy if full.
-spec enqueue(bp_queue(), event()) ->
    {ok, bp_queue()} | {error, queue_full | blocked}.
enqueue(Queue, Event) ->
    CurrentSize = queue:len(Queue#bp_queue.queue),
    MaxSize = Queue#bp_queue.max_size,

    %% Check warning threshold
    WarningThreshold = Queue#bp_queue.warning_threshold,
    case CurrentSize >= WarningThreshold of
        true ->
            emit_warning_telemetry(Queue, CurrentSize);
        false ->
            ok
    end,

    %% Apply backpressure strategy if at capacity
    case CurrentSize >= MaxSize of
        true ->
            handle_overflow(Queue, Event);
        false ->
            NewQueue = queue:in(Event, Queue#bp_queue.queue),
            {ok, Queue#bp_queue{queue = NewQueue}}
    end.

%% @doc Enqueue multiple events.
-spec enqueue_many(bp_queue(), [event()]) ->
    {ok, bp_queue()} | {error, term()}.
enqueue_many(Queue, []) ->
    {ok, Queue};
enqueue_many(Queue, [Event | Rest]) ->
    case enqueue(Queue, Event) of
        {ok, NewQueue} ->
            enqueue_many(NewQueue, Rest);
        {error, _} = Error ->
            Error
    end.

%% @doc Dequeue up to N events.
-spec dequeue(bp_queue(), pos_integer()) -> {ok, [event()], bp_queue()}.
dequeue(Queue, Count) ->
    {Events, NewQ} = dequeue_n(Queue#bp_queue.queue, Count, []),
    NewDemand = case Queue#bp_queue.demand of
        infinity -> infinity;
        D -> max(0, D - length(Events))
    end,
    {ok, Events, Queue#bp_queue{queue = NewQ, demand = NewDemand}}.

%% @doc Dequeue all events.
-spec dequeue_all(bp_queue()) -> {ok, [event()], bp_queue()}.
dequeue_all(Queue) ->
    Events = queue:to_list(Queue#bp_queue.queue),
    NewDemand = case Queue#bp_queue.demand of
        infinity -> infinity;
        _ -> 0
    end,
    {ok, Events, Queue#bp_queue{queue = queue:new(), demand = NewDemand}}.

%% @doc Get current queue size.
-spec size(bp_queue()) -> non_neg_integer().
size(#bp_queue{queue = Q}) ->
    queue:len(Q).

%% @doc Check if queue is empty.
-spec is_empty(bp_queue()) -> boolean().
is_empty(#bp_queue{queue = Q}) ->
    queue:is_empty(Q).

%% @doc Check if queue is at capacity.
-spec is_full(bp_queue()) -> boolean().
is_full(#bp_queue{queue = Q, max_size = MaxSize}) ->
    queue:len(Q) >= MaxSize.

%% @doc Get queue statistics.
-spec info(bp_queue()) -> map().
info(#bp_queue{} = Queue) ->
    #{
        size => queue:len(Queue#bp_queue.queue),
        max_size => Queue#bp_queue.max_size,
        strategy => Queue#bp_queue.strategy,
        mode => Queue#bp_queue.mode,
        warning_threshold => Queue#bp_queue.warning_threshold,
        demand => Queue#bp_queue.demand,
        dropped => Queue#bp_queue.dropped,
        is_full => is_full(Queue),
        utilization => queue:len(Queue#bp_queue.queue) / Queue#bp_queue.max_size
    }.

%% @doc Set demand (for pull mode).
-spec set_demand(bp_queue(), non_neg_integer()) -> bp_queue().
set_demand(Queue, Demand) ->
    Queue#bp_queue{demand = Demand}.

%% @doc Add to demand (for pull mode).
-spec add_demand(bp_queue(), pos_integer()) -> bp_queue().
add_demand(#bp_queue{demand = infinity} = Queue, _Count) ->
    Queue;
add_demand(#bp_queue{demand = Current} = Queue, Count) ->
    Queue#bp_queue{demand = Current + Count}.

%% @doc Get current demand.
-spec get_demand(bp_queue()) -> non_neg_integer() | infinity.
get_demand(#bp_queue{demand = Demand}) ->
    Demand.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Handle queue overflow based on strategy
-spec handle_overflow(bp_queue(), event()) ->
    {ok, bp_queue()} | {error, queue_full | blocked}.
handle_overflow(#bp_queue{strategy = drop_oldest} = Queue, Event) ->
    %% Drop the oldest event and add the new one
    OldQueue = Queue#bp_queue.queue,
    {_, TrimmedQueue} = queue:out(OldQueue),
    NewQueue = queue:in(Event, TrimmedQueue),
    emit_dropped_telemetry(Queue, 1),
    {ok, Queue#bp_queue{
        queue = NewQueue,
        dropped = Queue#bp_queue.dropped + 1
    }};

handle_overflow(#bp_queue{strategy = drop_newest} = Queue, _Event) ->
    %% Drop the new event (don't add it)
    emit_dropped_telemetry(Queue, 1),
    {ok, Queue#bp_queue{dropped = Queue#bp_queue.dropped + 1}};

handle_overflow(#bp_queue{strategy = block}, _Event) ->
    %% Signal that we're blocked - caller should wait
    {error, blocked};

handle_overflow(#bp_queue{strategy = error}, _Event) ->
    %% Return error - caller should handle
    {error, queue_full}.

%% @private Dequeue N events from queue
-spec dequeue_n(queue:queue(event()), non_neg_integer(), [event()]) ->
    {[event()], queue:queue(event())}.
dequeue_n(Queue, 0, Acc) ->
    {lists:reverse(Acc), Queue};
dequeue_n(Queue, N, Acc) ->
    case queue:out(Queue) of
        {{value, Event}, NewQueue} ->
            dequeue_n(NewQueue, N - 1, [Event | Acc]);
        {empty, Queue} ->
            {lists:reverse(Acc), Queue}
    end.

%% @private Emit telemetry when warning threshold is reached
-spec emit_warning_telemetry(bp_queue(), non_neg_integer()) -> ok.
emit_warning_telemetry(#bp_queue{store_id = StoreId,
                                  subscription_key = SubscriptionKey,
                                  max_size = MaxSize}, CurrentSize) ->
    telemetry:execute(
        [reckon_db, subscription, backpressure, warning],
        #{queue_size => CurrentSize, max_size => MaxSize},
        #{store_id => StoreId, subscription_key => SubscriptionKey}
    ),
    ok.

%% @private Emit telemetry when events are dropped
-spec emit_dropped_telemetry(bp_queue(), pos_integer()) -> ok.
emit_dropped_telemetry(#bp_queue{store_id = StoreId,
                                  subscription_key = SubscriptionKey,
                                  strategy = Strategy}, Count) ->
    telemetry:execute(
        [reckon_db, subscription, backpressure, dropped],
        #{count => Count},
        #{store_id => StoreId, subscription_key => SubscriptionKey, strategy => Strategy}
    ),
    ok.
