%% @doc Subscriptions API facade for reckon-db
%%
%% Provides the public API for subscription operations:
%% - subscribe: Create a new subscription
%% - unsubscribe: Remove a subscription
%% - get: Get a subscription by key
%% - list: List all subscriptions
%% - exists: Check if a subscription exists
%%
%% Subscription types:
%% - stream: Subscribe to all events in a specific stream
%% - event_type: Subscribe to events of a specific type
%% - event_pattern: Subscribe to events matching a pattern
%% - event_payload: Subscribe to events with specific payload patterns
%%
%% @author rgfaber

-module(reckon_db_subscriptions).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([
    subscribe/4,
    subscribe/5,
    unsubscribe/2,
    unsubscribe/3,
    ack/4,
    get/2,
    list/1,
    exists/2,
    setup_tracking/2
]).

%%====================================================================
%% Types
%%====================================================================

-type subscribe_opts() :: #{
    subscription_name => binary(),
    pool_size => pos_integer(),
    start_from => non_neg_integer(),
    subscriber => pid()
}.

-export_type([subscribe_opts/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a subscription with default options
%%
%% Parameters:
%%   StoreId          - The store identifier
%%   Type             - Subscription type (stream, event_type, event_pattern, event_payload)
%%   Selector         - The selector for matching events
%%   SubscriptionName - Human-readable name for the subscription
%%
%% Returns {ok, SubscriptionKey} on success or {error, Reason} on failure.
-spec subscribe(atom(), subscription_type(), binary() | map(), binary()) ->
    {ok, binary()} | {error, term()}.
subscribe(StoreId, Type, Selector, SubscriptionName) ->
    subscribe(StoreId, Type, Selector, SubscriptionName, #{}).

%% @doc Create a subscription with options
%%
%% Options:
%% - pool_size: Number of emitter workers (default: 1)
%% - start_from: Starting position for replay (default: 0)
%% - subscriber: PID to receive events directly (default: undefined)
-spec subscribe(atom(), subscription_type(), binary() | map(), binary(), subscribe_opts()) ->
    {ok, binary()} | {error, term()}.
subscribe(StoreId, Type, Selector, SubscriptionName, Opts) ->
    StartTime = erlang:monotonic_time(),

    %% Create subscription record
    Subscription = #subscription{
        type = Type,
        selector = Selector,
        subscription_name = SubscriptionName,
        subscriber_pid = maps:get(subscriber, Opts, undefined),
        created_at = erlang:system_time(millisecond),
        pool_size = maps:get(pool_size, Opts, 1),
        checkpoint = maps:get(start_from, Opts, undefined),
        options = Opts
    },

    %% Emit start telemetry
    telemetry:execute(
        ?SUBSCRIPTION_CREATED,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, type => Type, selector => Selector,
          subscription_name => SubscriptionName}
    ),

    %% Check if subscription already exists
    Key = reckon_db_subscriptions_store:key(Subscription),
    %% Set id on the subscription so emitter pools use the correct key
    SubscriptionWithId = Subscription#subscription{id = Key},
    case reckon_db_subscriptions_store:exists(StoreId, Key) of
        true ->
            reregister_subscriber(StoreId, Key, SubscriptionName, Opts);
        false ->
            store_and_setup(StoreId, Key, Type, Selector, SubscriptionWithId, StartTime)
    end.

%% @private
store_and_setup(StoreId, Key, Type, Selector, Subscription, StartTime) ->
    case reckon_db_subscriptions_store:put(StoreId, Subscription) of
        ok ->
            finalize_subscription(StoreId, Key, Type, Selector, Subscription, StartTime);
        {error, _} = Error ->
            Error
    end.

%% @private
finalize_subscription(StoreId, Key, Type, Selector, Subscription, StartTime) ->
    case create_filter(Type, Selector) of
        {error, FilterReason} ->
            _ = reckon_db_subscriptions_store:delete(StoreId, Key),
            {error, {invalid_filter, FilterReason}};
        Filter ->
            setup_event_notification(StoreId, Key, Filter, Subscription),
            reckon_db_tracker_group:notify_created(StoreId, subscriptions, Subscription),
            %% Replay historical events to the subscriber (catch-up subscription).
            %% The trigger handles live events; this fills the gap for events
            %% that existed before the subscription was created.
            maybe_start_catchup(StoreId, Subscription),
            Duration = erlang:monotonic_time() - StartTime,
            telemetry:execute(
                ?SUBSCRIPTION_CREATED,
                #{duration => Duration},
                #{store_id => StoreId, subscription_key => Key}
            ),
            {ok, Key}
    end.

%% @private Re-register subscriber PID on an existing subscription.
%% After a restart, subscriptions persist in Khepri but carry a dead PID
%% from the previous BEAM instance. This updates the PID AND re-registers
%% the Khepri trigger so that new events fire the notification mechanism.
%%
%% Khepri triggers store Erlang funs (stored procedures) that may become
%% stale after a BEAM restart. Re-registering ensures the trigger's proc
%% function is fresh and the emitter group names are persisted.
-spec reregister_subscriber(atom(), binary(), binary(), subscribe_opts()) ->
    {ok, binary()}.
reregister_subscriber(StoreId, Key, SubscriptionName, Opts) ->
    NewPid = maps:get(subscriber, Opts, undefined),
    ok = update_subscriber_pid(StoreId, Key, SubscriptionName, NewPid),
    %% Re-register the Khepri trigger and emitter names.
    %% The trigger's stored proc (an Erlang fun) may be stale after restart.
    ok = reregister_trigger(StoreId, Key),
    %% Replay historical events from checkpoint (catch-up after restart).
    case reckon_db_subscriptions_store:get(StoreId, Key) of
        #subscription{} = Sub ->
            maybe_start_catchup(StoreId, Sub#subscription{subscriber_pid = NewPid});
        _ ->
            ok
    end,
    {ok, Key}.

%% @private Re-register the Khepri trigger for an existing subscription.
%% Reads the subscription from the store to obtain type and selector,
%% then re-creates the filter and re-registers the trigger.
-spec reregister_trigger(atom(), binary()) -> ok.
reregister_trigger(StoreId, Key) ->
    case reckon_db_subscriptions_store:get(StoreId, Key) of
        #subscription{type = Type, selector = Selector, pool_size = PoolSize} ->
            case create_filter(Type, Selector) of
                {error, _} ->
                    ok;
                Filter ->
                    _ = reckon_db_emitter_group:persist_emitters(StoreId, Key, PoolSize),
                    ok = register_trigger(StoreId, Key, Filter)
            end;
        _ ->
            ok
    end.

-spec update_subscriber_pid(atom(), binary(), binary(), pid() | undefined) -> ok.
update_subscriber_pid(_StoreId, _Key, _Name, undefined) ->
    ok;
update_subscriber_pid(StoreId, Key, Name, Pid) when is_pid(Pid) ->
    do_update_pid(reckon_db_subscriptions_store:get(StoreId, Key), StoreId, Key, Name, Pid).

do_update_pid(undefined, _StoreId, _Key, _Name, _Pid) ->
    ok;
do_update_pid(Existing, StoreId, _Key, Name, Pid) when is_record(Existing, subscription) ->
    Updated = Existing#subscription{subscriber_pid = Pid},
    _ = reckon_db_subscriptions_store:put(StoreId, Updated),
    logger:info("Subscription ~s re-registered with new PID (store: ~p)", [Name, StoreId]),
    ok.

%% @doc Remove a subscription by key
-spec unsubscribe(atom(), binary()) -> ok | {error, term()}.
unsubscribe(StoreId, Key) when is_binary(Key) ->
    case reckon_db_subscriptions_store:get(StoreId, Key) of
        undefined ->
            {error, not_found};
        Subscription ->
            do_unsubscribe(StoreId, Key, Subscription)
    end.

%% @doc Remove a subscription by type, selector, and name
-spec unsubscribe(atom(), subscription_type(), binary()) -> ok | {error, term()}.
unsubscribe(StoreId, Type, SubscriptionName) ->
    %% Find the subscription
    case find_subscription(StoreId, Type, SubscriptionName) of
        {ok, Key, Subscription} ->
            do_unsubscribe(StoreId, Key, Subscription);
        {error, _} = Error ->
            Error
    end.

%% @doc Get a subscription by key
-spec get(atom(), binary()) -> {ok, subscription()} | {error, not_found}.
get(StoreId, Key) ->
    case reckon_db_subscriptions_store:get(StoreId, Key) of
        undefined -> {error, not_found};
        Subscription -> {ok, Subscription}
    end.

%% @doc List all subscriptions in the store
-spec list(atom()) -> {ok, [subscription()]} | {error, term()}.
list(StoreId) ->
    reckon_db_subscriptions_store:list(StoreId).

%% @doc Check if a subscription exists
-spec exists(atom(), binary()) -> boolean().
exists(StoreId, Key) ->
    reckon_db_subscriptions_store:exists(StoreId, Key).

%% @doc Setup subscription tracking for a process
%%
%% Joins the tracker group for subscriptions, allowing the process
%% to receive notifications about subscription lifecycle events
%% (created, updated, deleted).
%%
%% The process will receive messages in the format:
%% - {feature_created, subscriptions, Data}
%% - {feature_updated, subscriptions, Data}
%% - {feature_deleted, subscriptions, Data}
-spec setup_tracking(atom(), pid()) -> ok.
setup_tracking(StoreId, Pid) ->
    reckon_db_tracker_group:join(StoreId, subscriptions, Pid).

%% @doc Acknowledge event delivery for a subscription
%%
%% Updates the checkpoint for the subscription to track progress.
%% This is typically called after successfully processing an event.
%% The checkpoint allows subscriptions to resume from where they left off
%% after a restart.
%%
%% Parameters:
%%   StoreId          - The store identifier
%%   SubscriptionName - Name of the subscription
%%   StreamId         - ID of the stream the event came from (may be undefined for cross-stream)
%%   EventNumber      - Version/position of the acknowledged event
%%
%% Returns ok on success, or {error, Reason} if the subscription is not found.
-spec ack(atom(), binary(), binary() | undefined, non_neg_integer()) -> ok | {error, term()}.
ack(StoreId, SubscriptionName, _StreamId, EventNumber) ->
    case reckon_db_subscriptions_store:find_by_name(StoreId, SubscriptionName) of
        {ok, Key, _Subscription} ->
            do_ack_checkpoint(StoreId, Key, SubscriptionName, EventNumber);
        {error, not_found} ->
            {error, {subscription_not_found, SubscriptionName}}
    end.

do_ack_checkpoint(StoreId, Key, SubscriptionName, EventNumber) ->
    case reckon_db_subscriptions_store:update_checkpoint(StoreId, Key, EventNumber) of
        ok ->
            telemetry:execute(
                ?SUBSCRIPTION_CHECKPOINT,
                #{position => EventNumber},
                #{store_id => StoreId, subscription_name => SubscriptionName}
            ),
            ok;
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Perform the actual unsubscribe operation
-spec do_unsubscribe(atom(), binary(), subscription()) -> ok | {error, term()}.
do_unsubscribe(StoreId, Key, Subscription) ->
    %% Emit telemetry
    telemetry:execute(
        ?SUBSCRIPTION_DELETED,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, subscription_key => Key}
    ),

    %% Cleanup emitter pool
    cleanup_emitter_pool(StoreId, Key),

    %% Unregister trigger
    unregister_trigger(StoreId, Key),

    %% Delete from store
    case reckon_db_subscriptions_store:delete(StoreId, Key) of
        ok ->
            %% Notify trackers
            reckon_db_tracker_group:notify_deleted(StoreId, subscriptions, Subscription),
            ok;
        {error, _} = Error ->
            Error
    end.

%% @private Find a subscription by type and name
-spec find_subscription(atom(), subscription_type(), binary()) ->
    {ok, binary(), subscription()} | {error, not_found}.
find_subscription(StoreId, Type, SubscriptionName) ->
    case reckon_db_subscriptions_store:list(StoreId) of
        {ok, Subscriptions} ->
            match_subscription(Type, SubscriptionName, Subscriptions);
        {error, _} ->
            {error, not_found}
    end.

match_subscription(Type, SubscriptionName, Subscriptions) ->
    Matching = lists:filter(
        fun(#subscription{type = T, subscription_name = N}) ->
            T =:= Type andalso N =:= SubscriptionName
        end,
        Subscriptions
    ),
    case Matching of
        [Subscription | _] ->
            Key = reckon_db_subscriptions_store:key(Subscription),
            {ok, Key, Subscription};
        [] ->
            {error, not_found}
    end.

%% @private Create an event filter based on subscription type
%%
%% Supports both evoq-style types (stream, event_type, etc.) and
%% gater-style types (by_stream, by_event_type, etc.) for compatibility
%% with the reckon_evoq_adapter translation layer.
-spec create_filter(subscription_type(), binary() | map()) -> term().
create_filter(stream, StreamId) ->
    reckon_db_filters:by_stream(StreamId);
create_filter(by_stream, StreamId) ->
    reckon_db_filters:by_stream(StreamId);
create_filter(event_type, EventType) ->
    reckon_db_filters:by_event_type(EventType);
create_filter(by_event_type, EventType) ->
    reckon_db_filters:by_event_type(EventType);
create_filter(event_pattern, Pattern) ->
    reckon_db_filters:by_event_pattern(Pattern);
create_filter(by_event_pattern, Pattern) ->
    reckon_db_filters:by_event_pattern(Pattern);
create_filter(event_payload, PayloadPattern) ->
    reckon_db_filters:by_event_payload(PayloadPattern);
create_filter(by_event_payload, PayloadPattern) ->
    reckon_db_filters:by_event_payload(PayloadPattern);
create_filter(by_tags, Tags) ->
    reckon_db_filters:by_tags(Tags).

%% @private Setup event notification mechanism
-spec setup_event_notification(atom(), binary(), term(), subscription()) -> ok.
setup_event_notification(StoreId, SubscriptionKey, Filter, #subscription{pool_size = PoolSize} = Subscription) ->
    _Emitters = reckon_db_emitter_group:persist_emitters(StoreId, SubscriptionKey, PoolSize),
    case register_trigger(StoreId, SubscriptionKey, Filter) of
        ok -> ok;
        {error, Reason} ->
            logger:warning("Trigger registration deferred for ~s (store: ~p): ~p",
                          [SubscriptionKey, StoreId, Reason])
    end,
    maybe_start_emitter_pool(StoreId, SubscriptionKey, Subscription),
    ok.

%% @private Attempt to start the emitter pool eagerly.
%%
%% If the emitter supervisor is already running (leader activated), the pool
%% starts immediately so late subscriptions can deliver events right away.
%% If the supervisor is not yet running, start_emitter returns an error and
%% the pool will be started later by leader activation or the health monitor.
%%
%% We avoid calling reckon_db_leader:is_active/1 here because this function
%% may be invoked from within the leader worker itself (save_default_subscriptions),
%% which would deadlock on the gen_server:call.
-spec maybe_start_emitter_pool(atom(), binary(), subscription()) -> ok.
maybe_start_emitter_pool(StoreId, SubscriptionKey, Subscription) ->
    SupName = reckon_db_naming:emitter_sup_name(StoreId),
    maybe_start_emitter_pool(StoreId, SubscriptionKey, Subscription, whereis(SupName)).

maybe_start_emitter_pool(_StoreId, _SubscriptionKey, _Subscription, undefined) ->
    ok;
maybe_start_emitter_pool(StoreId, SubscriptionKey, Subscription, _SupPid) ->
    case reckon_db_emitter_pool:start_emitter(StoreId, Subscription) of
        {ok, _Pid} ->
            logger:info("Started emitter pool for subscription ~s (store: ~p)",
                       [SubscriptionKey, StoreId]);
        {error, {already_started, _}} ->
            ok;
        {error, _} ->
            ok
    end.

%% @private Register a Khepri trigger for event notification.
%% May fail if the Khepri store isn't ready yet (noproc during leader
%% activation race). Returns {error, _} instead of crashing — trigger
%% registration will be retried on next leader activation.
-spec register_trigger(atom(), binary(), term()) -> ok | {error, term()}.
register_trigger(StoreId, SubscriptionKey, Filter) ->
    Topic = reckon_db_emitter_group:topic(StoreId, SubscriptionKey),
    ProcPath = [procs, on_new_event, Topic],
    ProcFun = make_trigger_proc(StoreId, SubscriptionKey),
    case store_trigger_proc(StoreId, SubscriptionKey, ProcPath, ProcFun) of
        ok -> activate_trigger(StoreId, SubscriptionKey, Filter, ProcPath);
        {error, _} = Error -> Error
    end.

store_trigger_proc(StoreId, SubscriptionKey, ProcPath, ProcFun) ->
    case khepri:put(StoreId, ProcPath, ProcFun) of
        ok -> ok;
        {error, Reason} ->
            logger:warning("Trigger proc store failed for ~s (~p): ~p",
                          [SubscriptionKey, StoreId, Reason]),
            {error, Reason}
    end.

activate_trigger(StoreId, SubscriptionKey, Filter, ProcPath) ->
    TriggerId = binary_to_atom(SubscriptionKey, utf8),
    PropOpts = #{
        expect_specific_node => false,
        props_to_return => [payload, payload_version, child_list_version,
                           child_list_length, child_names],
        include_root_props => true
    },
    case khepri:register_trigger(StoreId, TriggerId, Filter, ProcPath, PropOpts) of
        ok -> ok;
        {error, Reason} ->
            logger:warning("Trigger activation failed for ~s (~p): ~p",
                          [SubscriptionKey, StoreId, Reason]),
            {error, Reason}
    end.

make_trigger_proc(StoreId, SubscriptionKey) ->
    fun(Props) ->
        case maps:get(path, Props, undefined) of
            undefined -> ok;
            Path -> broadcast_event_at_path(StoreId, SubscriptionKey, Path)
        end
    end.

broadcast_event_at_path(StoreId, SubscriptionKey, Path) ->
    case get_event_from_path(StoreId, Path) of
        {ok, Event} ->
            reckon_db_emitter_group:broadcast(StoreId, SubscriptionKey, Event),
            ok;
        {error, Reason} ->
            logger:warning("Broadcasting failed for path ~p: ~p", [Path, Reason]),
            ok
    end.

%% @private Unregister a Khepri trigger
-spec unregister_trigger(atom(), binary()) -> ok.
unregister_trigger(StoreId, SubscriptionKey) ->
    %% Cleanup the proc function and trigger data
    %% Note: Khepri triggers are automatically cleaned when their proc is deleted
    Topic = reckon_db_emitter_group:topic(StoreId, SubscriptionKey),
    ProcPath = [procs, on_new_event, Topic],
    _ = khepri:delete(StoreId, ProcPath),

    %% Also try to delete the trigger registration if it exists
    %% The trigger is stored at a specific path
    TriggerId = binary_to_atom(SubscriptionKey, utf8),
    TriggerPath = [triggers, TriggerId],
    _ = khepri:delete(StoreId, TriggerPath),
    ok.

%% @private Get an event from a Khepri path
-spec get_event_from_path(atom(), [atom() | binary()]) -> {ok, event()} | {error, term()}.
get_event_from_path(StoreId, Path) ->
    case khepri:get(StoreId, Path) of
        {ok, Event} when is_record(Event, event) ->
            {ok, Event};
        {ok, EventMap} when is_map(EventMap) ->
            {ok, map_to_event(EventMap)};
        {ok, _} ->
            {error, not_an_event};
        {error, _} = Error ->
            Error
    end.

%% @private Convert map to event record
-spec map_to_event(map()) -> event().
map_to_event(Map) ->
    #event{
        event_id = maps:get(event_id, Map, undefined),
        event_type = maps:get(event_type, Map, undefined),
        stream_id = maps:get(stream_id, Map, undefined),
        version = maps:get(version, Map, 0),
        data = maps:get(data, Map, #{}),
        metadata = maps:get(metadata, Map, #{}),
        timestamp = maps:get(timestamp, Map, 0),
        epoch_us = maps:get(epoch_us, Map, 0),
        data_content_type = maps:get(data_content_type, Map, ?CONTENT_TYPE_JSON),
        metadata_content_type = maps:get(metadata_content_type, Map, ?CONTENT_TYPE_JSON)
    }.

%%====================================================================
%% Catch-up subscription (historical replay)
%%====================================================================

%% @private Replay historical events to a subscriber asynchronously.
%%
%% After a subscription is created or re-registered, this reads all
%% existing events from the store and delivers them to the subscriber.
%% Combined with the Khepri trigger (which handles live events), this
%% implements a catch-up subscription: the subscriber receives ALL
%% events from the requested position, not just new ones.
%%
%% Events during the overlap (written while catch-up runs) may be
%% delivered twice. The subscriber must handle idempotency.
-spec maybe_start_catchup(atom(), subscription()) -> ok.
maybe_start_catchup(_StoreId, #subscription{subscriber_pid = undefined}) ->
    ok;
maybe_start_catchup(StoreId, #subscription{subscriber_pid = Pid, checkpoint = Checkpoint}) ->
    StartFrom = case Checkpoint of
        undefined -> 0;
        N when is_integer(N) -> N
    end,
    spawn_link(fun() -> do_catchup(StoreId, Pid, StartFrom) end),
    ok.

%% @private Read historical events in batches and deliver to subscriber.
-spec do_catchup(atom(), pid(), non_neg_integer()) -> ok.
do_catchup(StoreId, SubscriberPid, Offset) ->
    BatchSize = 500,
    case reckon_db_streams:read_all_global(StoreId, Offset, BatchSize) of
        {ok, []} ->
            logger:info("[catchup] Store ~p: replay complete (~b events delivered)",
                        [StoreId, Offset]),
            ok;
        {ok, Events} ->
            case is_local_process_alive(SubscriberPid) of
                true ->
                    SubscriberPid ! {events, Events},
                    case length(Events) < BatchSize of
                        true ->
                            Total = Offset + length(Events),
                            logger:info("[catchup] Store ~p: replay complete (~b events delivered)",
                                        [StoreId, Total]),
                            ok;
                        false ->
                            do_catchup(StoreId, SubscriberPid, Offset + BatchSize)
                    end;
                false ->
                    logger:warning("[catchup] Store ~p: subscriber died during replay", [StoreId]),
                    ok
            end;
        {error, Reason} ->
            logger:warning("[catchup] Store ~p: read failed at offset ~b: ~p",
                           [StoreId, Offset, Reason]),
            ok
    end.

%% @private Cleanup emitter pool for a subscription
-spec cleanup_emitter_pool(atom(), binary()) -> ok.
cleanup_emitter_pool(StoreId, SubscriptionKey) ->
    %% Leave the emitter group for all members
    Members = reckon_db_emitter_group:members(StoreId, SubscriptionKey),
    lists:foreach(
        fun(Pid) ->
            catch reckon_db_emitter_group:leave(StoreId, SubscriptionKey, Pid)
        end,
        Members
    ),

    %% Remove persisted emitter names
    Key = reckon_db_emitter_group:group_key(StoreId, SubscriptionKey),
    catch persistent_term:erase(Key),
    ok.

%% @private Check if a local PID is alive. Remote PIDs are assumed alive.
is_local_process_alive(Pid) when is_pid(Pid), node(Pid) =:= node() ->
    erlang:is_process_alive(Pid);
is_local_process_alive(_) ->
    true.
