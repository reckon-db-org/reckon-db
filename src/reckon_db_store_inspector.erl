%% @doc Store-level introspection for debugging and monitoring.
%%
%% Provides aggregate queries across streams, snapshots, and subscriptions
%% within a single store. Designed for the Observer UI and operational tooling.
%%
%% All functions read directly from the Khepri tree -- no data modification.
%%
%% See the Store Inspector guide for detailed usage examples.
%%
%% @author rgfaber
-module(reckon_db_store_inspector).

-include("reckon_db.hrl").

-export([
    store_stats/1,
    list_all_snapshots/1,
    list_subscriptions/1,
    subscription_lag/2,
    event_type_summary/1,
    stream_info/2
]).

%% @doc Aggregate statistics for a store.
-spec store_stats(atom()) -> {ok, map()} | {error, term()}.
store_stats(StoreId) ->
    StreamIds = list_streams_safe(StoreId),
    StreamCount = length(StreamIds),
    TotalEvents = sum_events(StoreId, StreamIds),
    SnapshotCount = count_all_snapshots(StoreId, StreamIds),
    SubCount = length(list_subs_safe(StoreId)),
    {ok, #{
        store_id => StoreId,
        stream_count => StreamCount,
        total_events => TotalEvents,
        snapshot_count => SnapshotCount,
        subscription_count => SubCount,
        has_events => TotalEvents > 0
    }}.

%% @doc List all snapshots across all streams in a store.
-spec list_all_snapshots(atom()) -> {ok, [map()]} | {error, term()}.
list_all_snapshots(StoreId) ->
    StreamIds = list_streams_safe(StoreId),
    Snapshots = lists:flatmap(fun(StreamId) -> snapshots_for_stream(StoreId, StreamId) end, StreamIds),
    Sorted = lists:sort(fun(A, B) -> maps:get(timestamp, A, 0) >= maps:get(timestamp, B, 0) end, Snapshots),
    {ok, Sorted}.

%% @doc List all subscriptions for a store with their current state.
-spec list_subscriptions(atom()) -> {ok, [map()]} | {error, term()}.
list_subscriptions(StoreId) ->
    Subs = list_subs_safe(StoreId),
    SubMaps = lists:filtermap(fun safe_subscription_summary/1, Subs),
    {ok, SubMaps}.

%% @doc Calculate lag for a specific subscription.
-spec subscription_lag(atom(), binary()) -> {ok, map()} | {error, term()}.
subscription_lag(StoreId, SubscriptionName) ->
    %% find_by_name returns {ok, Key, Sub} — the previous {ok, Sub}
    %% match crashed the worker with case_clause on every successful
    %% lookup, surfacing as gRPC Internal.
    case reckon_db_subscriptions_store:find_by_name(StoreId, SubscriptionName) of
        {ok, _Key, Sub} ->
            Checkpoint = extract_checkpoint(Sub),
            TotalEvents = sum_events(StoreId, list_streams_safe(StoreId)),
            Lag = max(0, TotalEvents - Checkpoint - 1),
            {ok, #{
                subscription_name => SubscriptionName,
                checkpoint => Checkpoint,
                latest_position => TotalEvents,
                lag_events => Lag
            }};
        {error, _} = Err ->
            Err
    end.

%% @doc Summary of event types in the store.
-spec event_type_summary(atom()) -> {ok, [map()]} | {error, term()}.
event_type_summary(StoreId) ->
    StreamIds = list_streams_safe(StoreId),
    TypeCounts = lists:foldl(fun(StreamId, Acc) -> count_types_in_stream(StoreId, StreamId, Acc) end, #{}, StreamIds),
    TypeList = maps_to_sorted_list(TypeCounts),
    {ok, TypeList}.

%% @doc Detailed info for a single stream.
-spec stream_info(atom(), binary()) -> {ok, map()} | {error, term()}.
stream_info(StoreId, StreamId) ->
    case reckon_db_streams:get_version(StoreId, StreamId) of
        {ok, Version} ->
            {ok, #{
                stream_id => StreamId,
                version => Version,
                event_count => Version + 1,
                first_event_at => get_event_timestamp(StoreId, StreamId, 0),
                last_event_at => get_event_timestamp(StoreId, StreamId, Version),
                snapshots => snapshot_coverage(StoreId, StreamId)
            }};
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Internal: Safe data access
%%====================================================================

list_streams_safe(StoreId) ->
    case reckon_db_streams:list_streams(StoreId) of
        {ok, Streams} -> Streams;
        _ -> []
    end.

list_subs_safe(StoreId) ->
    case reckon_db_subscriptions_store:list(StoreId) of
        {ok, Subs} -> Subs;
        Subs when is_list(Subs) -> Subs;
        _ -> []
    end.

sum_events(StoreId, StreamIds) ->
    lists:foldl(fun(StreamId, Acc) -> Acc + stream_event_count(StoreId, StreamId) end, 0, StreamIds).

stream_event_count(StoreId, StreamId) ->
    case reckon_db_streams:get_version(StoreId, StreamId) of
        {ok, Version} -> Version + 1;
        _ -> 0
    end.

%%====================================================================
%% Internal: Snapshots
%%====================================================================

count_all_snapshots(StoreId, StreamIds) ->
    lists:foldl(fun(StreamId, Acc) -> Acc + length(snapshots_for_stream(StoreId, StreamId)) end, 0, StreamIds).

snapshots_for_stream(StoreId, StreamId) ->
    case reckon_db_snapshots_store:list(StoreId, StreamId) of
        {ok, SnapList} -> [snapshot_summary(StreamId, S) || S <- SnapList];
        _ -> []
    end.

snapshot_summary(StreamId, #snapshot{version = V, metadata = M, timestamp = T}) ->
    #{stream_id => StreamId, version => V, timestamp => T, metadata => M};
snapshot_summary(StreamId, #{version := V} = Map) ->
    #{stream_id => StreamId, version => V, timestamp => maps:get(timestamp, Map, undefined), metadata => maps:get(metadata, Map, #{})};
snapshot_summary(StreamId, _) ->
    #{stream_id => StreamId, version => unknown, timestamp => undefined, metadata => #{}}.

snapshot_coverage(StoreId, StreamId) ->
    case reckon_db_snapshots_store:list(StoreId, StreamId) of
        {ok, []} -> #{count => 0, latest_version => undefined};
        {ok, Snaps} -> #{count => length(Snaps), latest_version => latest_snap_version(lists:last(Snaps))};
        _ -> #{count => 0, latest_version => undefined}
    end.

latest_snap_version(#snapshot{version = V}) -> V;
latest_snap_version(#{version := V}) -> V;
latest_snap_version(_) -> undefined.

%%====================================================================
%% Internal: Subscriptions
%%====================================================================

safe_subscription_summary(Sub) ->
    case subscription_summary(Sub) of
        #{} = Map -> {true, Map};
        _ -> false
    end.

subscription_summary(#subscription{} = Sub) ->
    #{
        subscription_name => Sub#subscription.subscription_name,
        type => Sub#subscription.type,
        selector => Sub#subscription.selector,
        checkpoint => Sub#subscription.checkpoint,
        pool_size => Sub#subscription.pool_size,
        created_at => Sub#subscription.created_at,
        subscriber_pid => format_pid(Sub#subscription.subscriber_pid)
    };
subscription_summary(#{subscription_name := _} = Map) ->
    Map#{subscriber_pid => format_pid(maps:get(subscriber_pid, Map, undefined))};
subscription_summary(_) ->
    #{}.

extract_checkpoint(#subscription{checkpoint = CP}) when is_integer(CP) -> CP;
extract_checkpoint(_) -> -1.

format_pid(Pid) when is_pid(Pid) -> list_to_binary(pid_to_list(Pid));
format_pid(Bin) when is_binary(Bin) -> Bin;
format_pid(undefined) -> <<"undefined">>;
format_pid(Other) -> list_to_binary(io_lib:format("~p", [Other])).

%%====================================================================
%% Internal: Event types
%%====================================================================

count_types_in_stream(StoreId, StreamId, Acc) ->
    case reckon_db_streams:read_all(StoreId, StreamId, forward, 10000) of
        {ok, Events} -> lists:foldl(fun count_event_type/2, Acc, Events);
        _ -> Acc
    end.

count_event_type(#event{event_type = T}, Acc) ->
    maps:update_with(T, fun(C) -> C + 1 end, 1, Acc);
count_event_type(#{event_type := T}, Acc) ->
    maps:update_with(T, fun(C) -> C + 1 end, 1, Acc);
count_event_type(_, Acc) ->
    maps:update_with(<<"unknown">>, fun(C) -> C + 1 end, 1, Acc).

maps_to_sorted_list(TypeCounts) ->
    List = [#{event_type => T, count => C} || {T, C} <- maps:to_list(TypeCounts)],
    lists:sort(fun(A, B) -> maps:get(count, A) >= maps:get(count, B) end, List).

%%====================================================================
%% Internal: Timestamps
%%====================================================================

get_event_timestamp(StoreId, StreamId, Version) ->
    case reckon_db_streams:read(StoreId, StreamId, Version, 1, forward) of
        {ok, [Event | _]} -> extract_timestamp(Event);
        _ -> undefined
    end.

extract_timestamp(#event{timestamp = T}) -> T;
extract_timestamp(#{timestamp := T}) -> T;
extract_timestamp(_) -> undefined.
