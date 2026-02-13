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
            {error, {already_exists, SubscriptionName}};
        false ->
            %% Store the subscription
            case reckon_db_subscriptions_store:put(StoreId, SubscriptionWithId) of
                ok ->
                    %% Setup the event notification mechanism
                    case create_filter(Type, Selector) of
                        {error, FilterReason} ->
                            %% Rollback: remove the stored subscription
                            _ = reckon_db_subscriptions_store:delete(StoreId, Key),
                            {error, {invalid_filter, FilterReason}};
                        Filter ->
                            setup_event_notification(StoreId, Key, Filter, SubscriptionWithId),

                            %% Notify trackers
                            reckon_db_tracker_group:notify_created(StoreId, subscriptions, SubscriptionWithId),

                            Duration = erlang:monotonic_time() - StartTime,
                            telemetry:execute(
                                ?SUBSCRIPTION_CREATED,
                                #{duration => Duration},
                                #{store_id => StoreId, subscription_key => Key}
                            ),

                            {ok, Key}
                    end;
                {error, _} = Error ->
                    Error
            end
    end.

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
            case reckon_db_subscriptions_store:update_checkpoint(StoreId, Key, EventNumber) of
                ok ->
                    %% Emit telemetry for checkpoint update
                    telemetry:execute(
                        ?SUBSCRIPTION_CHECKPOINT,
                        #{position => EventNumber},
                        #{store_id => StoreId, subscription_name => SubscriptionName}
                    ),
                    ok;
                {error, _} = Error ->
                    Error
            end;
        {error, not_found} ->
            {error, {subscription_not_found, SubscriptionName}}
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
            case lists:filter(
                fun(#subscription{type = T, subscription_name = N}) ->
                    T =:= Type andalso N =:= SubscriptionName
                end,
                Subscriptions
            ) of
                [Subscription | _] ->
                    Key = reckon_db_subscriptions_store:key(Subscription),
                    {ok, Key, Subscription};
                [] ->
                    {error, not_found}
            end;
        {error, _} ->
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
setup_event_notification(StoreId, SubscriptionKey, Filter, #subscription{pool_size = PoolSize}) ->
    %% Persist emitter names for the subscription
    _Emitters = reckon_db_emitter_group:persist_emitters(StoreId, SubscriptionKey, PoolSize),

    %% Register the Khepri trigger
    ok = register_trigger(StoreId, SubscriptionKey, Filter),

    %% Start the emitter pool (if we have an emitter supervisor running)
    %% This will be done by the notification supervisor when it starts
    ok.

%% @private Register a Khepri trigger for event notification
-spec register_trigger(atom(), binary(), term()) -> ok | {error, term()}.
register_trigger(StoreId, SubscriptionKey, Filter) ->
    %% Store the proc function that will be called on trigger
    Topic = reckon_db_emitter_group:topic(StoreId, SubscriptionKey),
    ProcPath = [procs, on_new_event, Topic],

    %% Create the proc function
    ProcFun = fun(Props) ->
        case maps:get(path, Props, undefined) of
            undefined -> ok;
            Path ->
                case get_event_from_path(StoreId, Path) of
                    {ok, Event} ->
                        reckon_db_emitter_group:broadcast(StoreId, SubscriptionKey, Event),
                        ok;
                    {error, Reason} ->
                        logger:warning("Broadcasting failed for path ~p: ~p", [Path, Reason]),
                        ok
                end
        end
    end,

    %% Store the proc
    ok = khepri:put(StoreId, ProcPath, ProcFun),

    %% Register the trigger
    TriggerId = binary_to_atom(SubscriptionKey, utf8),
    PropOpts = #{
        expect_specific_node => false,
        props_to_return => [payload, payload_version, child_list_version,
                           child_list_length, child_names],
        include_root_props => true
    },

    case khepri:register_trigger(StoreId, TriggerId, Filter, ProcPath, PropOpts) of
        ok -> ok;
        {error, _} = Error -> Error
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
