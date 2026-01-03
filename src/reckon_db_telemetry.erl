%% @doc Telemetry handler for reckon-db
%%
%% Provides logging handler for telemetry events and utilities for
%% attaching/detaching handlers. Additional handlers (e.g., OpenTelemetry)
%% can be added for datacenter deployments.
%%
%% == Usage ==
%%
%% Attach the default logger handler:
%%   ok = reckon_db_telemetry:attach_default_handler().
%%
%% Attach a custom handler:
%%   ok = reckon_db_telemetry:attach(my_handler, fun my_module:handle/4, #{}).
%%
%% Emit an event:
%%   reckon_db_telemetry:emit(?STREAM_WRITE_STOP, #{duration => 1000}, #{store_id => my_store}).
%%
%% @author rgfaber

-module(reckon_db_telemetry).

-include("reckon_db_telemetry.hrl").

%% API
-export([
    attach_default_handler/0,
    detach_default_handler/0,
    attach/3,
    detach/1,
    emit/3,
    span/3,
    handle_event/4
]).

-define(HANDLER_ID, reckon_db_telemetry_handler).

%%====================================================================
%% API
%%====================================================================

%% @doc Attach the default logger handler for all reckon_db events
-spec attach_default_handler() -> ok | {error, already_exists}.
attach_default_handler() ->
    Events = all_events(),
    case telemetry:attach_many(?HANDLER_ID, Events, fun ?MODULE:handle_event/4, #{}) of
        ok -> ok;
        {error, already_exists} -> {error, already_exists}
    end.

%% @doc Detach the default logger handler
-spec detach_default_handler() -> ok | {error, not_found}.
detach_default_handler() ->
    telemetry:detach(?HANDLER_ID).

%% @doc Attach a custom handler for all reckon_db events
-spec attach(term(), fun((telemetry:event_name(), telemetry:event_measurements(),
                          telemetry:event_metadata(), term()) -> ok), term()) ->
    ok | {error, already_exists}.
attach(HandlerId, HandlerFun, Config) ->
    Events = all_events(),
    telemetry:attach_many(HandlerId, Events, HandlerFun, Config).

%% @doc Detach a handler by ID
-spec detach(term()) -> ok | {error, not_found}.
detach(HandlerId) ->
    telemetry:detach(HandlerId).

%% @doc Emit a telemetry event
-spec emit(telemetry:event_name(), telemetry:event_measurements(), telemetry:event_metadata()) -> ok.
emit(Event, Measurements, Metadata) ->
    telemetry:execute(Event, Measurements, Metadata).

%% @doc Execute a function with start/stop telemetry
%% Emits start event before, stop event after (with duration)
-spec span(telemetry:event_prefix(), telemetry:event_metadata(), fun(() -> Result)) ->
    Result when Result :: term().
span(EventPrefix, Metadata, Fun) ->
    telemetry:span(EventPrefix, Metadata, fun() ->
        Result = Fun(),
        {Result, #{}}
    end).

%% @doc Handle telemetry events (logger handler)
-spec handle_event(
    telemetry:event_name(),
    telemetry:event_measurements(),
    telemetry:event_metadata(),
    term()
) -> ok.

%% Stream events
handle_event(?STREAM_WRITE_START, #{system_time := _Time}, Meta, _Config) ->
    #{store_id := StoreId, stream_id := StreamId, event_count := Count} = Meta,
    logger:debug("Stream write starting: store=~p stream=~s events=~p",
                [StoreId, StreamId, Count]),
    ok;

handle_event(?STREAM_WRITE_STOP, Measurements, Meta, _Config) ->
    Duration = maps:get(duration, Measurements, 0),
    Count = maps:get(event_count, Measurements, 0),
    #{store_id := StoreId, stream_id := StreamId} = Meta,
    logger:debug("Stream write completed: store=~p stream=~s events=~p duration=~pus",
                [StoreId, StreamId, Count, Duration]),
    ok;

handle_event(?STREAM_WRITE_ERROR, Measurements, Meta, _Config) ->
    Duration = maps:get(duration, Measurements, 0),
    #{store_id := StoreId, stream_id := StreamId, reason := Reason} = Meta,
    logger:warning("Stream write failed: store=~p stream=~s reason=~p duration=~pus",
                  [StoreId, StreamId, Reason, Duration]),
    ok;

handle_event(?STREAM_READ_START, #{system_time := _Time}, Meta, _Config) ->
    #{store_id := StoreId, stream_id := StreamId} = Meta,
    logger:debug("Stream read starting: store=~p stream=~s", [StoreId, StreamId]),
    ok;

handle_event(?STREAM_READ_STOP, Measurements, Meta, _Config) ->
    Duration = maps:get(duration, Measurements, 0),
    Count = maps:get(event_count, Measurements, 0),
    #{store_id := StoreId, stream_id := StreamId} = Meta,
    logger:debug("Stream read completed: store=~p stream=~s events=~p duration=~pus",
                [StoreId, StreamId, Count, Duration]),
    ok;

%% Subscription events
handle_event(?SUBSCRIPTION_CREATED, _Measurements, Meta, _Config) ->
    #{store_id := StoreId, subscription_id := SubId, type := Type} = Meta,
    logger:info("Subscription created: store=~p id=~s type=~p",
               [StoreId, SubId, Type]),
    ok;

handle_event(?SUBSCRIPTION_DELETED, _Measurements, Meta, _Config) ->
    #{store_id := StoreId, subscription_id := SubId} = Meta,
    logger:info("Subscription deleted: store=~p id=~s", [StoreId, SubId]),
    ok;

handle_event(?SUBSCRIPTION_EVENT_DELIVERED, Measurements, Meta, _Config) ->
    Duration = maps:get(duration, Measurements, 0),
    #{store_id := StoreId, subscription_id := SubId} = Meta,
    EventId = maps:get(event_id, Meta, undefined),
    logger:debug("Event delivered: store=~p subscription=~s event=~p duration=~pus",
                [StoreId, SubId, EventId, Duration]),
    ok;

%% Snapshot events
handle_event(?SNAPSHOT_CREATED, Measurements, Meta, _Config) ->
    SizeBytes = maps:get(size_bytes, Measurements, 0),
    Duration = maps:get(duration, Measurements, 0),
    #{store_id := StoreId, stream_id := StreamId, version := Version} = Meta,
    logger:info("Snapshot created: store=~p stream=~s version=~p size=~p bytes duration=~pus",
               [StoreId, StreamId, Version, SizeBytes, Duration]),
    ok;

handle_event(?SNAPSHOT_READ, Measurements, Meta, _Config) ->
    Duration = maps:get(duration, Measurements, 0),
    SizeBytes = maps:get(size_bytes, Measurements, 0),
    #{store_id := StoreId, stream_id := StreamId, version := Version} = Meta,
    logger:debug("Snapshot read: store=~p stream=~s version=~p size=~p bytes duration=~pus",
                [StoreId, StreamId, Version, SizeBytes, Duration]),
    ok;

%% Cluster events
handle_event(?CLUSTER_NODE_UP, _Measurements, Meta, _Config) ->
    #{store_id := StoreId, node := Node, member_count := Count} = Meta,
    logger:info("Cluster node up: store=~p node=~p members=~p",
               [StoreId, Node, Count]),
    ok;

handle_event(?CLUSTER_NODE_DOWN, _Measurements, Meta, _Config) ->
    #{store_id := StoreId, node := Node} = Meta,
    Reason = maps:get(reason, Meta, unknown),
    MemberCount = maps:get(member_count, Meta, unknown),
    logger:warning("Cluster node down: store=~p node=~p reason=~p members=~p",
                  [StoreId, Node, Reason, MemberCount]),
    ok;

handle_event(?CLUSTER_LEADER_ELECTED, _Measurements, Meta, _Config) ->
    #{store_id := StoreId} = Meta,
    %% Handle both 'leader' and 'leader_node' keys for compatibility
    Leader = maps:get(leader, Meta, maps:get(leader_node, Meta, unknown)),
    PreviousLeader = maps:get(previous_leader, Meta, undefined),
    case PreviousLeader of
        undefined ->
            logger:info("Cluster leader elected: store=~p leader=~p", [StoreId, Leader]);
        _ ->
            logger:info("Cluster leader changed: store=~p leader=~p previous=~p",
                       [StoreId, Leader, PreviousLeader])
    end,
    ok;

%% Store events
handle_event(?STORE_STARTED, _Measurements, Meta, _Config) ->
    #{store_id := StoreId} = Meta,
    Mode = maps:get(mode, Meta, unknown),
    DataDir = maps:get(data_dir, Meta, unknown),
    logger:info("Store started: store=~p mode=~p data_dir=~s",
               [StoreId, Mode, DataDir]),
    ok;

handle_event(?STORE_STOPPED, Measurements, Meta, _Config) ->
    #{store_id := StoreId} = Meta,
    Reason = maps:get(reason, Meta, normal),
    UptimeMs = maps:get(uptime_ms, Measurements, 0),
    logger:info("Store stopped: store=~p reason=~p uptime=~pms",
               [StoreId, Reason, UptimeMs]),
    ok;

%% Emitter events
handle_event(?EMITTER_BROADCAST, Measurements, Meta, _Config) ->
    Duration = maps:get(duration, Measurements, 0),
    #{store_id := StoreId, subscription_id := SubId} = Meta,
    RecipientCount = maps:get(recipient_count, Meta, 0),
    logger:debug("Emitter broadcast: store=~p subscription=~s recipients=~p duration=~pus",
                [StoreId, SubId, RecipientCount, Duration]),
    ok;

handle_event(?EMITTER_POOL_CREATED, _Measurements, Meta, _Config) ->
    #{store_id := StoreId, subscription_id := SubId, pool_size := PoolSize} = Meta,
    logger:info("Emitter pool created: store=~p subscription=~s pool_size=~p",
               [StoreId, SubId, PoolSize]),
    ok;

%% Catch-all handler
handle_event(Event, Measurements, Meta, _Config) ->
    logger:debug("Telemetry event: ~p measurements=~p meta=~p",
                [Event, Measurements, Meta]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Get all reckon_db telemetry events
-spec all_events() -> [telemetry:event_name()].
all_events() ->
    [
        %% Stream events
        ?STREAM_WRITE_START,
        ?STREAM_WRITE_STOP,
        ?STREAM_WRITE_ERROR,
        ?STREAM_READ_START,
        ?STREAM_READ_STOP,
        %% Subscription events
        ?SUBSCRIPTION_CREATED,
        ?SUBSCRIPTION_DELETED,
        ?SUBSCRIPTION_EVENT_DELIVERED,
        %% Snapshot events
        ?SNAPSHOT_CREATED,
        ?SNAPSHOT_READ,
        %% Cluster events
        ?CLUSTER_NODE_UP,
        ?CLUSTER_NODE_DOWN,
        ?CLUSTER_LEADER_ELECTED,
        %% Store events
        ?STORE_STARTED,
        ?STORE_STOPPED,
        %% Emitter events
        ?EMITTER_BROADCAST,
        ?EMITTER_POOL_CREATED
    ].
