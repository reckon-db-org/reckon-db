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
    ok = esdb_gater_api:register_worker(StoreId, self()),
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

%% Get streams
handle_call({get_streams, _StoreId}, _From, #state{store_id = StoreId} = State) ->
    Result = reckon_db_streams:list_streams(StoreId),
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
handle_call({append_events, _StoreId, StreamId, ExpectedVersion, Events}, _From,
            #state{store_id = StoreId} = State) ->
    %% get_version returns integer directly: -1 for no stream, or version number
    CurrentVersion = reckon_db_streams:get_version(StoreId, StreamId),
    Result = case version_matches(CurrentVersion, ExpectedVersion) of
        true ->
            reckon_db_streams:append(StoreId, StreamId, CurrentVersion, Events);
        false ->
            {error, {wrong_expected_version, CurrentVersion}}
    end,
    {reply, Result, State};

%%====================================================================
%% Snapshot Operations (handle_call)
%%====================================================================

%% Read snapshot
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
%% Causation Operations (handle_call)
%%====================================================================

%% Get events caused by an event
handle_call({get_effects, _StoreId, EventId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_causation:get_effects(StoreId, EventId),
    {reply, Result, State};

%% Get the event that caused another
handle_call({get_cause, _StoreId, EventId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_causation:get_cause(StoreId, EventId),
    {reply, Result, State};

%% Get causation chain
handle_call({get_causation_chain, _StoreId, EventId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_causation:get_chain(StoreId, EventId),
    {reply, Result, State};

%% Get correlated events
handle_call({get_correlated, _StoreId, CorrelationId}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_causation:get_correlated(StoreId, CorrelationId),
    {reply, Result, State};

%% Build causation graph
handle_call({build_causation_graph, _StoreId, Id}, _From,
            #state{store_id = StoreId} = State) ->
    Result = reckon_db_causation:build_graph(StoreId, Id),
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

%% Unknown request
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%%====================================================================
%% Subscription Operations (handle_cast)
%%====================================================================

%% Remove subscription
%% Note: unsubscribe uses (StoreId, Type, SubscriptionName), Selector is not needed
handle_cast({remove_subscription, _StoreId, Type, _Selector, SubscriptionName}, State) ->
    #state{store_id = StoreId} = State,
    reckon_db_subscriptions:unsubscribe(StoreId, Type, SubscriptionName),
    {noreply, State};

%% Save subscription
handle_cast({save_subscription, _StoreId, Type, Selector, SubscriptionName, StartFrom, Subscriber}, State) ->
    #state{store_id = StoreId} = State,
    case reckon_db_subscriptions:subscribe(StoreId, Type, Selector, SubscriptionName, #{
        start_from => StartFrom,
        subscriber => Subscriber
    }) of
        {ok, _Key} -> ok;
        {error, {already_exists, _}} -> ok;
        {error, Reason} ->
            logger:warning("[gateway_worker] subscription ~s failed: ~p", [SubscriptionName, Reason])
    end,
    {noreply, State};

%% Ack event
handle_cast({ack_event, _StoreId, SubscriptionName, _SubscriberPid, Event}, State) ->
    #state{store_id = StoreId} = State,
    %% Update subscription position
    StreamId = maps:get(event_stream_id, Event, maps:get(stream_id, Event, undefined)),
    EventNumber = maps:get(event_number, Event, maps:get(version, Event, 0)),
    reckon_db_subscriptions:ack(StoreId, SubscriptionName, StreamId, EventNumber),
    {noreply, State};

%%====================================================================
%% Snapshot Operations (handle_cast)
%%====================================================================

%% Record snapshot
handle_cast({record_snapshot, _StoreId, _SourceUuid, StreamUuid, Version, SnapshotRecord}, State) ->
    #state{store_id = StoreId} = State,
    reckon_db_snapshots:save(StoreId, StreamUuid, Version, SnapshotRecord),
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
    esdb_gater_api:unregister_worker(StoreId, self()),
    logger:info("Gateway worker for store ~p unregistered from gater", [StoreId]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Check if current version matches expected version
%%
%% Version semantics:
%% - `-1` = stream doesn't exist (empty)
%% - `0` = first event exists (version after first append)
%% - For event sourcing, ExpectedVersion=0 with CurrentVersion=-1 is valid
%%   (both mean "no events yet")
-spec version_matches(integer(), integer() | any | stream_exists) -> boolean().
version_matches(_Current, any) -> true;
version_matches(Current, stream_exists) when Current >= 0 -> true;
version_matches(-1, stream_exists) -> false;
%% Special case: ExpectedVersion=0 matches empty stream (-1)
%% This is common in event sourcing when aggregate initializes with version 0
version_matches(-1, 0) -> true;
version_matches(Current, Expected) when is_integer(Expected) -> Current =:= Expected.

%% @private Find subscription by name across all types
-spec find_subscription_by_name(atom(), binary()) -> {ok, map()} | {error, not_found}.
find_subscription_by_name(StoreId, SubscriptionName) ->
    case reckon_db_subscriptions:list(StoreId) of
        {ok, Subscriptions} ->
            case lists:filter(
                fun(S) when is_record(S, subscription) ->
                    S#subscription.subscription_name =:= SubscriptionName;
                   (S) when is_map(S) ->
                    maps:get(subscription_name, S, <<>>) =:= SubscriptionName
                end,
                Subscriptions
            ) of
                [Sub | _] -> {ok, subscription_to_map(Sub)};
                [] -> {error, not_found}
            end;
        {error, _} = Error ->
            Error
    end.

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
