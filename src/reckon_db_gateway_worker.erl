%% @doc Gateway worker for reckon-db
%%
%% This worker process acts as the gateway endpoint for a store.
%% It registers with reckon-gater and handles incoming requests
%% routed through the gateway API.
%%
%% Multiple gateway workers can run per store for load balancing.
%% Each worker registers independently with the gater's Ra-based
%% worker registry.
%%
%% The message format matches the ExESDB.GatewayWorker from the
%% original Elixir implementation.
%%
%% @author rgfaber

-module(reckon_db_gateway_worker).
-behaviour(gen_server).

-include("reckon_db.hrl").

%% API
-export([
    start_link/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    store_id :: atom(),
    config :: store_config()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start a gateway worker for a store
%% Workers are not locally registered to allow multiple per store.
%% They register with reckon-gater for discovery and load balancing.
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init(#store_config{store_id = StoreId} = Config) ->
    process_flag(trap_exit, true),

    %% Register with the gateway
    ok = reckon_gater_api:register_worker(StoreId, self()),
    logger:info("Gateway worker for store ~p registered with gater", [StoreId]),

    {ok, #state{store_id = StoreId, config = Config}}.

%%====================================================================
%% Stream Operations (handle_call)
%%====================================================================

%% Stream forward
handle_call({stream_forward, _StoreId, StreamId, StartVersion, Count}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:read(StoreId, StreamId, StartVersion, Count, forward),
    {reply, Result, State};

%% Stream backward
handle_call({stream_backward, _StoreId, StreamId, StartVersion, Count}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:read(StoreId, StreamId, StartVersion, Count, backward),
    {reply, Result, State};

%% Get events
handle_call({get_events, _StoreId, StreamId, StartVersion, Count, Direction}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:read(StoreId, StreamId, StartVersion, Count, Direction),
    {reply, Result, State};

%% Read all events globally (cross-stream, sorted by epoch_us)
handle_call({read_all_global, _StoreId, Offset, BatchSize}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:read_all_global(StoreId, Offset, BatchSize),
    {reply, Result, State};

%% Has events
handle_call({has_events, _StoreId}, _From, #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:has_events(StoreId),
    {reply, Result, State};

%% Get streams
handle_call({get_streams, _StoreId}, _From, #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:list_streams(StoreId),
    {reply, Result, State};

handle_call({global_event_count, _StoreId}, _From, #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:global_event_count(StoreId),
    {reply, Result, State};

%% Delete stream
handle_call({delete_stream, _StoreId, StreamId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:delete(StoreId, StreamId),
    {reply, Result, State};

%% Read events by type (native Khepri filtering)
handle_call({read_by_event_types, _StoreId, EventTypes, BatchSize}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:read_by_event_types(StoreId, EventTypes, BatchSize),
    {reply, Result, State};

%% Read events by tags (native Khepri filtering with ANY/ALL matching)
handle_call({read_by_tags, _StoreId, Tags, Match, BatchSize}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:read_by_tags(StoreId, Tags, Match, BatchSize),
    {reply, Result, State};

%% Read events by a metadata key=value pair (the indexed primitive apps
%% build causation/correlation read models on)
handle_call({read_by_metadata, _StoreId, Key, Value}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:read_by_metadata(StoreId, Key, Value),
    {reply, Result, State};

%% Get subscription by name (for checkpoint retrieval)
handle_call({get_subscription, _StoreId, SubscriptionName}, _From,
            #state{store_id = StoreId} = State) ->
    Result = find_subscription_by_name(StoreId, SubscriptionName),
    {reply, Result, State};

%% Get subscriptions
handle_call({get_subscriptions, _StoreId}, _From, #state{store_id = StoreId} = State) ->
    Result = reckon_db_subscriptions:list(StoreId),
    {reply, Result, State};

%% Get version
handle_call({get_version, _StoreId, StreamId}, _From, #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:get_version(StoreId, StreamId),
    {reply, Result, State};

%% Append events (auto-versioned)
handle_call({append_events, _StoreId, StreamId, Events}, _From,
            #state{store_id = StoreId} = State) ->
    %% Get current version for auto-versioning
    %% get_version returns integer directly: -1 for no stream, or version number
    CurrentVersion = reckon_db_streams:get_version(StoreId, StreamId),
    Result = reckon_db_streams:append(StoreId, StreamId, CurrentVersion, Events),
    {reply, Result, State};

%% Append events (with expected version)
%% Delegate version checking to reckon_db_streams:append/4 which handles
%% all version constants: ANY_VERSION (-2), NO_STREAM (-1), STREAM_EXISTS (-4).
handle_call({append_events, _StoreId, StreamId, ExpectedVersion, Events}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:append(StoreId, StreamId, ExpectedVersion, Events),
    {reply, Result, State};

%% DCB log read (reckon-db 5.2.2+)
handle_call({dcb_read_log, _StoreId, FromSeq, Limit}, _From,
            #state{store_id = StoreId} = State) ->
    Result = case reckon_db_dcb:read_log(StoreId, FromSeq, Limit) of
        {ok, Events, TotalCount} ->
            {ok, #{events => Events, total_count => TotalCount}};
        Err -> Err
    end,
    {reply, Result, State};

%% DCB tags index (reckon-db 5.2.2+)
handle_call({dcb_all_tags, _StoreId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_dcb:all_tags(StoreId),
    {reply, Result, State};

%% DCB event-types index (reckon-db 5.2.2+)
handle_call({dcb_all_event_types, _StoreId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_dcb:all_event_types(StoreId),
    {reply, Result, State};

%% DCB conditional-append (reckon-gater 2.3.0+, reckon-db 3.1.0+).
%% TagFilter is `reckon_gater_types:tag_filter()`; SeqCutoff is `integer()`
%% (-1 means "saw nothing"). See plans/PLAN_DCB_IMPLEMENTATION.md.
handle_call({append_if_no_tag_matches, _StoreId, TagFilter, SeqCutoff, Events},
            _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:append_if_no_tag_matches(
               StoreId, TagFilter, SeqCutoff, Events),
    {reply, Result, State};

%% DCB payload-indexed read (reckon-db 5.3.0+, reckon-gater 3.5.1+).
%% Requires {payload, Key} declared in the store's index_config.
handle_call({dcb_read_by_payload, _StoreId, Key, Value, Limit}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_dcb:read_by_payload(StoreId, Key, Value, Limit),
    {reply, Result, State};

%% DCB composite payload hash read (reckon-db 5.3.0+, reckon-gater 3.5.1+).
%% Requires {payload_hash, Keys} declared in the store's index_config.
handle_call({dcb_read_by_payload_hash, _StoreId, Keys, Values, Limit}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_dcb:read_by_payload_hash(StoreId, Keys, Values, Limit),
    {reply, Result, State};

%%====================================================================
%% Snapshot Operations (handle_call)
%%====================================================================

%% Read snapshot. Version =:= 0 means "latest" — there is no version-0
%% snapshot in the store (snapshot versions reflect an event index,
%% which is the first written event's version, never 0 for an aggregate
%% that has emitted events). Use load/2 in that case so callers can
%% ask for the most recent snapshot without a List+At round-trip.
handle_call({read_snapshot, _StoreId, _SourceUuid, StreamUuid, 0}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_snapshots:load(StoreId, StreamUuid),
    {reply, Result, State};
handle_call({read_snapshot, _StoreId, _SourceUuid, StreamUuid, Version}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_snapshots:load_at(StoreId, StreamUuid, Version),
    {reply, Result, State};

%% List snapshots
handle_call({list_snapshots, _StoreId, _SourceUuid, StreamUuid}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_snapshots:list(StoreId, StreamUuid),
    {reply, Result, State};

%%====================================================================
%% Diagnostics Operations (handle_call)
%%====================================================================

%% Verify cluster consistency
handle_call({verify_cluster_consistency, _StoreId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_cluster:verify_consistency(StoreId),
    {reply, Result, State};

%% Quick health check
handle_call({quick_health_check, _StoreId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_cluster:health_check(StoreId),
    {reply, Result, State};

%% Verify membership consensus
handle_call({verify_membership_consensus, _StoreId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_cluster:verify_membership(StoreId),
    {reply, Result, State};

%% Check Raft log consistency
handle_call({check_raft_log_consistency, _StoreId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_cluster:check_log_consistency(StoreId),
    {reply, Result, State};

%% List stores
handle_call({list_stores}, _From, State) ->
    Result = reckon_db_store_registry:list_stores(),
    {reply, Result, State};

%%====================================================================
%% Temporal Query Operations (handle_call)
%%====================================================================

%% Read events up to timestamp
handle_call({read_until, _StoreId, StreamId, Timestamp}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_temporal:read_until(StoreId, StreamId, Timestamp),
    {reply, Result, State};

%% Read events up to timestamp with options
handle_call({read_until, _StoreId, StreamId, Timestamp, Opts}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_temporal:read_until(StoreId, StreamId, Timestamp, Opts),
    {reply, Result, State};

%% Read events in time range
handle_call({read_range, _StoreId, StreamId, FromTimestamp, ToTimestamp}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_temporal:read_range(StoreId, StreamId, FromTimestamp, ToTimestamp),
    {reply, Result, State};

%% Read events in time range with options
handle_call({read_range, _StoreId, StreamId, FromTimestamp, ToTimestamp, Opts}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_temporal:read_range(StoreId, StreamId, FromTimestamp, ToTimestamp, Opts),
    {reply, Result, State};

%% Get version at timestamp
handle_call({version_at, _StoreId, StreamId, Timestamp}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_temporal:version_at(StoreId, StreamId, Timestamp),
    {reply, Result, State};

%%====================================================================
%% Scavenge Operations (handle_call)
%%====================================================================

%% Scavenge stream
handle_call({scavenge, _StoreId, StreamId, Opts}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_scavenge:scavenge(StoreId, StreamId, Opts),
    {reply, Result, State};

%% Scavenge streams matching pattern
handle_call({scavenge_matching, _StoreId, Pattern, Opts}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_scavenge:scavenge_matching(StoreId, Pattern, Opts),
    {reply, Result, State};

%% Dry-run scavenge
handle_call({scavenge_dry_run, _StoreId, StreamId, Opts}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_scavenge:dry_run(StoreId, StreamId, Opts),
    {reply, Result, State};

%%====================================================================
%% Schema Operations (handle_call)
%%====================================================================

%% Get schema
handle_call({get_schema, _StoreId, EventType}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_schema:get(StoreId, EventType),
    {reply, Result, State};

%% List schemas
handle_call({list_schemas, _StoreId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_schema:list(StoreId),
    {reply, Result, State};

%% Get schema version
handle_call({get_schema_version, _StoreId, EventType}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_schema:get_version(StoreId, EventType),
    {reply, Result, State};

%% Upcast events
handle_call({upcast_events, _StoreId, Events}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_schema:upcast(StoreId, Events),
    {reply, {ok, Result}, State};

%%====================================================================
%% Memory Pressure Operations (handle_call)
%%====================================================================

%% Get memory pressure level
handle_call({get_memory_level, _StoreId}, _From, State) ->
    Result = try
        reckon_db_memory:level()
    catch
        exit:{noproc, _} -> normal
    end,
    {reply, {ok, Result}, State};

%% Get memory stats
handle_call({get_memory_stats, _StoreId}, _From, State) ->
    Result = try
        reckon_db_memory:get_stats()
    catch
        exit:{noproc, _} -> #{level => unknown, memory_used => 0, memory_total => 0}
    end,
    {reply, {ok, Result}, State};

%%====================================================================
%% Link Operations (handle_call)
%%====================================================================

%% Get link
handle_call({get_link, _StoreId, LinkName}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_links:get(StoreId, LinkName),
    {reply, Result, State};

%% List links
handle_call({list_links, _StoreId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_links:list(StoreId),
    {reply, Result, State};

%% Get link info
handle_call({link_info, _StoreId, LinkName}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_links:info(StoreId, LinkName),
    {reply, Result, State};

%%====================================================================
%% Store Inspector Operations
%%====================================================================

handle_call({store_stats, _StoreId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_store_inspector:store_stats(StoreId),
    {reply, Result, State};

handle_call({list_all_snapshots, _StoreId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_store_inspector:list_all_snapshots(StoreId),
    {reply, Result, State};

handle_call({list_store_subscriptions, _StoreId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_store_inspector:list_subscriptions(StoreId),
    {reply, Result, State};

handle_call({subscription_lag, _StoreId, SubscriptionName}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_store_inspector:subscription_lag(StoreId, SubscriptionName),
    {reply, Result, State};

handle_call({event_type_summary, _StoreId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_store_inspector:event_type_summary(StoreId),
    {reply, Result, State};

handle_call({stream_info, _StoreId, StreamId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_store_inspector:stream_info(StoreId, StreamId),
    {reply, Result, State};

%% Save subscription. Synchronous as of reckon-db 2.3.5 / reckon-gater
%% 2.1.2 — used to be a fire-and-forget handle_cast that swallowed
%% {invalid_filter, _} into a logger warning, leaving gRPC clients
%% with a phantom success. Now the worker returns the real result
%% of reckon_db_subscriptions:subscribe/5; the gateway translates
%% to gRPC InvalidArgument when needed.
%%
%% {already_exists, _} from the store layer is still mapped to
%% {ok, Key} — re-registering with the same name is idempotent
%% (the subscribe path re-binds the pid and re-arms the trigger;
%% see reckon_db_subscriptions:reregister_subscriber/4).
handle_call({save_subscription, _StoreId, Type, Selector, SubscriptionName, StartFrom, Subscriber}, _From,
            #state{store_id = StoreId} = State) ->
    Result = case reckon_db_subscriptions:subscribe(StoreId, Type, Selector, SubscriptionName, #{
                start_from => StartFrom,
                subscriber => Subscriber
            }) of
        {ok, _Key} = Ok -> Ok;
        {error, {already_exists, Key}} -> {ok, Key};
        {error, Reason} = Err ->
            logger:warning("[gateway_worker] subscription ~s failed: ~p",
                           [SubscriptionName, Reason]),
            Err
    end,
    {reply, Result, State};

%% Remove subscription. Synchronous as of reckon-db 2.3.6 / reckon-gater
%% 2.1.3. Idempotent: a not_found result is treated as ok because
%% removal is the desired terminal state regardless of starting state.
%% Other store-layer errors (e.g. khepri write failure) propagate.
handle_call({remove_subscription, _StoreId, Type, _Selector, SubscriptionName}, _From,
            #state{store_id = StoreId} = State) ->
    Result = case reckon_db_subscriptions:unsubscribe(StoreId, Type, SubscriptionName) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, _} = Err -> Err
    end,
    {reply, Result, State};

%% Ack event. Synchronous as of reckon-db 2.3.6 / reckon-gater 2.1.3.
%% {error, {subscription_not_found, _}} from the store surfaces to the
%% gRPC client as InvalidArgument (whitelisted in reckon_gater_retry).
handle_call({ack_event, _StoreId, SubscriptionName, _SubscriberPid, Event}, _From,
            #state{store_id = StoreId} = State) ->
    StreamId = maps:get(event_stream_id, Event,
                        maps:get(stream_id, Event, undefined)),
    EventNumber = maps:get(event_number, Event,
                           maps:get(version, Event, 0)),
    Result = reckon_db_subscriptions:ack(StoreId, SubscriptionName, StreamId, EventNumber),
    {reply, Result, State};

%% Payload index introspection (reckon-db 5.5.0+, reckon-gater 3.7.0+).
%% Reads directly from the store_config stored in worker state — no store I/O.
handle_call({get_payload_indexes, _StoreId}, _From,
            #state{config = #store_config{indexes = Indexes}} = State) ->
    Keys = [K || {payload, K} <- Indexes],
    {reply, {ok, Keys}, State};

handle_call({get_payload_hash_indexes, _StoreId}, _From,
            #state{config = #store_config{indexes = Indexes}} = State) ->
    KeySets = [KS || {payload_hash, KS} <- Indexes],
    {reply, {ok, KeySets}, State};

%% Unknown request
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%%====================================================================
%% Snapshot Operations (handle_cast)
%%====================================================================

%% Record snapshot
handle_cast({record_snapshot, _StoreId, _SourceUuid, StreamUuid, Version, SnapshotRecord}, State) ->
    #state{store_id = StoreId} = State,
    %% The gateway wraps the payload as #{data, metadata, version}. Unwrap it so
    %% data and metadata land in their own snapshot fields (save/5) instead of
    %% the whole envelope being stored as `data` with metadata dropped. The
    %% fallback keeps older callers that pass a bare data term working.
    Data = maps:get(data, SnapshotRecord, SnapshotRecord),
    Metadata = maps:get(metadata, SnapshotRecord, #{}),
    reckon_db_snapshots:save(StoreId, StreamUuid, Version, Data, Metadata),
    {noreply, State};

%% Delete snapshot
handle_cast({delete_snapshot, _StoreId, _SourceUuid, StreamUuid, Version}, State) ->
    #state{store_id = StoreId} = State,
    reckon_db_snapshots:delete_at(StoreId, StreamUuid, Version),
    {noreply, State};

%%====================================================================
%% Schema Operations (handle_cast)
%%====================================================================

%% Register schema
handle_cast({register_schema, _StoreId, EventType, Schema}, State) ->
    #state{store_id = StoreId} = State,
    reckon_db_schema:register(StoreId, EventType, Schema),
    {noreply, State};

%% Unregister schema
handle_cast({unregister_schema, _StoreId, EventType}, State) ->
    #state{store_id = StoreId} = State,
    reckon_db_schema:unregister(StoreId, EventType),
    {noreply, State};

%%====================================================================
%% Link Operations (handle_cast)
%%====================================================================

%% Create link
handle_cast({create_link, _StoreId, LinkSpec}, State) ->
    #state{store_id = StoreId} = State,
    reckon_db_links:create(StoreId, LinkSpec),
    {noreply, State};

%% Delete link
handle_cast({delete_link, _StoreId, LinkName}, State) ->
    #state{store_id = StoreId} = State,
    reckon_db_links:delete(StoreId, LinkName),
    {noreply, State};

%% Start link
handle_cast({start_link, _StoreId, LinkName}, State) ->
    #state{store_id = StoreId} = State,
    reckon_db_links:start(StoreId, LinkName),
    {noreply, State};

%% Stop link
handle_cast({stop_link, _StoreId, LinkName}, State) ->
    #state{store_id = StoreId} = State,
    reckon_db_links:stop(StoreId, LinkName),
    {noreply, State};

%% Unknown cast
handle_cast(_Msg, State) ->
    {noreply, State}.

%%====================================================================
%% Info handlers
%%====================================================================

handle_info(_Info, State) ->
    {noreply, State}.

%%====================================================================
%% Termination
%%====================================================================

terminate(_Reason, #state{store_id = StoreId}) ->
    %% Unregister from the gateway
    reckon_gater_api:unregister_worker(StoreId, self()),
    logger:info("Gateway worker for store ~p unregistered from gater", [StoreId]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Find subscription by name across all types
-spec find_subscription_by_name(atom(), binary()) -> {ok, map()} | {error, not_found}.
find_subscription_by_name(StoreId, SubscriptionName) ->
    case reckon_db_subscriptions:list(StoreId) of
        {ok, Subscriptions} ->
            match_subscription_by_name(Subscriptions, SubscriptionName);
        {error, _} = Error ->
            Error
    end.

match_subscription_by_name(Subscriptions, SubscriptionName) ->
    Matches = lists:filter(
                fun(S) -> subscription_name_is(S, SubscriptionName) end, Subscriptions),
    first_named_subscription(Matches).

subscription_name_is(S, Name) when is_record(S, subscription) ->
    S#subscription.subscription_name =:= Name;
subscription_name_is(S, Name) when is_map(S) ->
    maps:get(subscription_name, S, <<>>) =:= Name.

first_named_subscription([Sub | _]) -> {ok, subscription_to_map(Sub)};
first_named_subscription([]) -> {error, not_found}.

%% @private Convert subscription record to map
-spec subscription_to_map(subscription() | map()) -> map().
subscription_to_map(#subscription{} = S) ->
    #{
        type => S#subscription.type,
        selector => S#subscription.selector,
        subscription_name => S#subscription.subscription_name,
        subscriber_pid => S#subscription.subscriber_pid,
        created_at => S#subscription.created_at,
        pool_size => S#subscription.pool_size,
        checkpoint => S#subscription.checkpoint,
        options => S#subscription.options
    };
subscription_to_map(Map) when is_map(Map) ->
    Map.
