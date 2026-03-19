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
%%
%% Returns stream count, total events, snapshot count, subscription count,
%% and whether the store has any events.
-spec store_stats(atom()) -> {ok, map()} | {error, term()}.
store_stats(StoreId) ->
    try
        Streams = list_streams_safe(StoreId),
        StreamCount = length(Streams),
        TotalEvents = lists:foldl(fun({_Id, Version}, Acc) ->
            Acc + Version + 1
        end, 0, Streams),
        SnapshotCount = count_all_snapshots(StoreId, Streams),
        Subs = list_subs_safe(StoreId),
        SubCount = length(Subs),
        HasEvents = TotalEvents > 0,
        {ok, #{
            store_id => StoreId,
            stream_count => StreamCount,
            total_events => TotalEvents,
            snapshot_count => SnapshotCount,
            subscription_count => SubCount,
            has_events => HasEvents
        }}
    catch
        _:Reason -> {error, Reason}
    end.

%% @doc List all snapshots across all streams in a store.
%%
%% Returns a flat list of snapshot summaries (without data payloads)
%% sorted by timestamp descending (newest first).
-spec list_all_snapshots(atom()) -> {ok, [map()]} | {error, term()}.
list_all_snapshots(StoreId) ->
    try
        Streams = list_streams_safe(StoreId),
        Snapshots = lists:flatmap(fun({StreamId, _Version}) ->
            try
                case reckon_db_snapshots_store:list(StoreId, StreamId) of
                    {ok, SnapList} ->
                        [snapshot_summary(StreamId, S) || S <- SnapList];
                    _ ->
                        []
                end
            catch _:_ -> []
            end
        end, Streams),
        Sorted = lists:sort(fun(A, B) ->
            maps:get(timestamp, A, 0) >= maps:get(timestamp, B, 0)
        end, Snapshots),
        {ok, Sorted}
    catch
        _:Reason -> {error, Reason}
    end.

%% @doc List all subscriptions for a store with their current state.
-spec list_subscriptions(atom()) -> {ok, [map()]} | {error, term()}.
list_subscriptions(StoreId) ->
    try
        Subs = list_subs_safe(StoreId),
        SubMaps = lists:filtermap(fun(S) ->
            try
                {true, subscription_summary(S)}
            catch _:_ -> false
            end
        end, Subs),
        {ok, SubMaps}
    catch
        _:Reason -> {error, Reason}
    end.

%% @doc Calculate lag for a specific subscription.
%%
%% Returns how many events behind the subscription is from the latest
%% event in the store's $all stream.
-spec subscription_lag(atom(), binary()) -> {ok, map()} | {error, term()}.
subscription_lag(StoreId, SubscriptionName) ->
    try
        case reckon_db_subscriptions_store:find_by_name(StoreId, SubscriptionName) of
            {ok, Sub} ->
                Checkpoint = case Sub of
                    #subscription{checkpoint = CP} when is_integer(CP) -> CP;
                    _ -> -1
                end,
                %% Get total event count as proxy for latest position
                Streams = list_streams_safe(StoreId),
                TotalEvents = lists:foldl(fun({_Id, Version}, Acc) ->
                    Acc + Version + 1
                end, 0, Streams),
                Lag = max(0, TotalEvents - Checkpoint - 1),
                {ok, #{
                    subscription_name => SubscriptionName,
                    checkpoint => Checkpoint,
                    latest_position => TotalEvents,
                    lag_events => Lag
                }};
            {error, _} = Err ->
                Err
        end
    catch
        _:Reason -> {error, Reason}
    end.

%% @doc Summary of event types in the store.
%%
%% Walks all streams and collects unique event types with counts.
%% This can be expensive for large stores — use with care.
-spec event_type_summary(atom()) -> {ok, [map()]} | {error, term()}.
event_type_summary(StoreId) ->
    try
        Streams = list_streams_safe(StoreId),
        TypeCounts = lists:foldl(fun({StreamId, _Version}, Acc) ->
            case reckon_db_streams:read_all(StoreId, StreamId, forward, 10000) of
                {ok, Events} ->
                    lists:foldl(fun(Event, InnerAcc) ->
                        Type = case Event of
                            #event{event_type = T} -> T;
                            #{event_type := T} -> T;
                            _ -> <<"unknown">>
                        end,
                        maps:update_with(Type, fun(C) -> C + 1 end, 1, InnerAcc)
                    end, Acc, Events);
                _ ->
                    Acc
            end
        end, #{}, Streams),
        TypeList = lists:sort(fun(A, B) ->
            maps:get(count, A) >= maps:get(count, B)
        end, [#{event_type => T, count => C} || {T, C} <- maps:to_list(TypeCounts)]),
        {ok, TypeList}
    catch
        _:Reason -> {error, Reason}
    end.

%% @doc Detailed info for a single stream.
%%
%% Returns version, event count, snapshot info, and the first/last event timestamps.
-spec stream_info(atom(), binary()) -> {ok, map()} | {error, term()}.
stream_info(StoreId, StreamId) ->
    try
        case reckon_db_streams:get_version(StoreId, StreamId) of
            {ok, Version} ->
                EventCount = Version + 1,
                %% Get first and last event timestamps
                FirstTs = get_first_event_timestamp(StoreId, StreamId),
                LastTs = get_last_event_timestamp(StoreId, StreamId, Version),
                %% Check for snapshots
                SnapshotInfo = case reckon_db_snapshots_store:list(StoreId, StreamId) of
                    {ok, Snaps} ->
                        #{
                            count => length(Snaps),
                            latest_version => case Snaps of
                                [] -> undefined;
                                _ ->
                                    Latest = lists:last(Snaps),
                                    case Latest of
                                        #snapshot{version = V} -> V;
                                        #{version := V} -> V;
                                        _ -> undefined
                                    end
                            end
                        };
                    _ ->
                        #{count => 0, latest_version => undefined}
                end,
                {ok, #{
                    stream_id => StreamId,
                    version => Version,
                    event_count => EventCount,
                    first_event_at => FirstTs,
                    last_event_at => LastTs,
                    snapshots => SnapshotInfo
                }};
            {error, _} = Err ->
                Err
        end
    catch
        _:Reason -> {error, Reason}
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

list_streams_safe(StoreId) ->
    case reckon_db_streams:list_streams(StoreId) of
        {ok, Streams} ->
            %% Streams is [{StreamId, Version}]
            Streams;
        _ ->
            []
    end.

list_subs_safe(StoreId) ->
    case reckon_db_subscriptions_store:list(StoreId) of
        {ok, Subs} -> Subs;
        Subs when is_list(Subs) -> Subs;
        _ -> []
    end.

count_all_snapshots(StoreId, Streams) ->
    lists:foldl(fun({StreamId, _Version}, Acc) ->
        case reckon_db_snapshots_store:list(StoreId, StreamId) of
            {ok, Snaps} -> Acc + length(Snaps);
            _ -> Acc
        end
    end, 0, Streams).

snapshot_summary(StreamId, Snapshot) ->
    case Snapshot of
        #snapshot{version = V, metadata = M, timestamp = T} ->
            #{
                stream_id => StreamId,
                version => V,
                timestamp => T,
                metadata => M
            };
        #{version := V} = Map ->
            #{
                stream_id => StreamId,
                version => V,
                timestamp => maps:get(timestamp, Map, undefined),
                metadata => maps:get(metadata, Map, #{})
            };
        _ ->
            #{stream_id => StreamId, version => unknown, timestamp => undefined, metadata => #{}}
    end.

subscription_summary(Sub) ->
    case Sub of
        #subscription{} ->
            #{
                subscription_name => Sub#subscription.subscription_name,
                type => Sub#subscription.type,
                selector => Sub#subscription.selector,
                checkpoint => Sub#subscription.checkpoint,
                pool_size => Sub#subscription.pool_size,
                created_at => Sub#subscription.created_at,
                subscriber_pid => format_pid(Sub#subscription.subscriber_pid)
            };
        #{subscription_name := _} = Map ->
            Map#{subscriber_pid => format_pid(maps:get(subscriber_pid, Map, undefined))};
        _ ->
            #{}
    end.

format_pid(Pid) when is_pid(Pid) -> list_to_binary(pid_to_list(Pid));
format_pid(Bin) when is_binary(Bin) -> Bin;
format_pid(undefined) -> <<"undefined">>;
format_pid(Other) -> list_to_binary(io_lib:format("~p", [Other])).

get_first_event_timestamp(StoreId, StreamId) ->
    case reckon_db_streams:read(StoreId, StreamId, 0, 1, forward) of
        {ok, [Event | _]} -> extract_timestamp(Event);
        _ -> undefined
    end.

get_last_event_timestamp(StoreId, StreamId, Version) ->
    case reckon_db_streams:read(StoreId, StreamId, Version, 1, forward) of
        {ok, [Event | _]} -> extract_timestamp(Event);
        _ -> undefined
    end.

extract_timestamp(#event{timestamp = T}) -> T;
extract_timestamp(#{timestamp := T}) -> T;
extract_timestamp(_) -> undefined.
