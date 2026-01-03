%% @doc Temporal queries for reckon-db.
%%
%% Provides point-in-time and time-range queries for event streams.
%% These queries filter events by their epoch_us timestamp field.
%%
%% Use cases:
%% - Reconstruct aggregate state at a historical point in time
%% - Audit queries ("what was the state on date X?")
%% - Time-range analytics
%%
%% @author rgfaber

-module(reckon_db_temporal).

-include("reckon_db.hrl").

%% API
-export([
    read_until/3,
    read_until/4,
    read_range/4,
    read_range/5,
    version_at/3
]).

%%====================================================================
%% Types
%%====================================================================

-type timestamp() :: integer().  %% Microseconds since epoch (epoch_us format)
-type opts() :: #{
    direction => forward | backward,
    limit => pos_integer()
}.

-export_type([timestamp/0, opts/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Read all events from a stream up to (and including) a timestamp.
%%
%% Returns events where epoch_us is less than or equal to Timestamp, sorted by version (ascending).
%% This is useful for reconstructing aggregate state at a point in time.
%%
%% Example:
%% ```
%% %% Get all events up to January 1, 2025 00:00:00 UTC
%% Timestamp = 1735689600000000, %% microseconds
%% {ok, Events} = reckon_db_temporal:read_until(my_store, <<"orders-123">>, Timestamp).
%% '''
-spec read_until(atom(), binary(), timestamp()) ->
    {ok, [event()]} | {error, term()}.
read_until(StoreId, StreamId, Timestamp) ->
    read_until(StoreId, StreamId, Timestamp, #{}).

%% @doc Read events up to a timestamp with options.
%%
%% Options:
%% - direction: forward (default) or backward
%% - limit: Maximum number of events to return
-spec read_until(atom(), binary(), timestamp(), opts()) ->
    {ok, [event()]} | {error, term()}.
read_until(StoreId, StreamId, Timestamp, Opts) ->
    StartTime = erlang:monotonic_time(),

    Result = do_read_until(StoreId, StreamId, Timestamp, Opts),

    Duration = erlang:monotonic_time() - StartTime,
    emit_telemetry(read_until, StoreId, StreamId, Timestamp, Result, Duration),

    Result.

%% @doc Read events within a time range [FromTimestamp, ToTimestamp].
%%
%% Returns events where FromTimestamp is less than or equal to epoch_us, and epoch_us is less than or equal to ToTimestamp.
%%
%% Example:
%% ```
%% %% Get all events from the first week of 2025
%% From = 1735689600000000,  %% Jan 1, 2025
%% To   = 1736294400000000,  %% Jan 8, 2025
%% {ok, Events} = reckon_db_temporal:read_range(my_store, <<"orders-123">>, From, To).
%% '''
-spec read_range(atom(), binary(), timestamp(), timestamp()) ->
    {ok, [event()]} | {error, term()}.
read_range(StoreId, StreamId, FromTimestamp, ToTimestamp) ->
    read_range(StoreId, StreamId, FromTimestamp, ToTimestamp, #{}).

%% @doc Read events within a time range with options.
-spec read_range(atom(), binary(), timestamp(), timestamp(), opts()) ->
    {ok, [event()]} | {error, term()}.
read_range(StoreId, StreamId, FromTimestamp, ToTimestamp, Opts) ->
    StartTime = erlang:monotonic_time(),

    Result = do_read_range(StoreId, StreamId, FromTimestamp, ToTimestamp, Opts),

    Duration = erlang:monotonic_time() - StartTime,
    emit_telemetry(read_range, StoreId, StreamId, {FromTimestamp, ToTimestamp}, Result, Duration),

    Result.

%% @doc Get the stream version at a specific timestamp.
%%
%% Returns the version of the last event with epoch_us less than or equal to Timestamp.
%% This is useful for determining what version to replay up to.
%%
%% Returns:
%% - {ok, Version} if events exist before the timestamp
%% - {ok, -1} if no events exist before the timestamp
%% - {error, Reason} on failure
-spec version_at(atom(), binary(), timestamp()) ->
    {ok, integer()} | {error, term()}.
version_at(StoreId, StreamId, Timestamp) ->
    case do_read_until(StoreId, StreamId, Timestamp, #{direction => backward, limit => 1}) of
        {ok, []} ->
            {ok, ?NO_STREAM};
        {ok, [Event | _]} ->
            {ok, Event#event.version};
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec do_read_until(atom(), binary(), timestamp(), opts()) ->
    {ok, [event()]} | {error, term()}.
do_read_until(StoreId, StreamId, Timestamp, Opts) ->
    case reckon_db_streams:exists(StoreId, StreamId) of
        false ->
            {error, {stream_not_found, StreamId}};
        true ->
            %% Read all events and filter by timestamp
            %% For optimization, we could add a Khepri index on epoch_us
            {ok, AllEvents} = read_all_events(StoreId, StreamId),
            FilteredEvents = filter_events_until(AllEvents, Timestamp),
            apply_opts(FilteredEvents, Opts)
    end.

%% @private
-spec do_read_range(atom(), binary(), timestamp(), timestamp(), opts()) ->
    {ok, [event()]} | {error, term()}.
do_read_range(StoreId, StreamId, FromTimestamp, ToTimestamp, Opts) ->
    case reckon_db_streams:exists(StoreId, StreamId) of
        false ->
            {error, {stream_not_found, StreamId}};
        true ->
            {ok, AllEvents} = read_all_events(StoreId, StreamId),
            FilteredEvents = filter_events_range(AllEvents, FromTimestamp, ToTimestamp),
            apply_opts(FilteredEvents, Opts)
    end.

%% @private Read all events from a stream
-spec read_all_events(atom(), binary()) -> {ok, [event()]}.
read_all_events(StoreId, StreamId) ->
    Version = reckon_db_streams:get_version(StoreId, StreamId),
    case Version of
        ?NO_STREAM ->
            {ok, []};
        _ ->
            reckon_db_streams:read(StoreId, StreamId, 0, Version + 1, forward)
    end.

%% @private Filter events up to timestamp
-spec filter_events_until([event()], timestamp()) -> [event()].
filter_events_until(Events, Timestamp) ->
    [E || E <- Events, E#event.epoch_us =< Timestamp].

%% @private Filter events within time range
-spec filter_events_range([event()], timestamp(), timestamp()) -> [event()].
filter_events_range(Events, FromTimestamp, ToTimestamp) ->
    [E || E <- Events,
          E#event.epoch_us >= FromTimestamp,
          E#event.epoch_us =< ToTimestamp].

%% @private Apply options (direction, limit) to events
-spec apply_opts([event()], opts()) -> {ok, [event()]}.
apply_opts(Events, Opts) ->
    Direction = maps:get(direction, Opts, forward),
    Limit = maps:get(limit, Opts, infinity),

    Ordered = case Direction of
        forward -> Events;
        backward -> lists:reverse(Events)
    end,

    Limited = case Limit of
        infinity -> Ordered;
        N when is_integer(N), N > 0 -> lists:sublist(Ordered, N)
    end,

    {ok, Limited}.

%% @private Emit telemetry for temporal queries
-spec emit_telemetry(atom(), atom(), binary(), term(), term(), integer()) -> ok.
emit_telemetry(Operation, StoreId, StreamId, TimestampSpec, Result, Duration) ->
    EventCount = case Result of
        {ok, Events} -> length(Events);
        _ -> 0
    end,

    telemetry:execute(
        [reckon_db, temporal, Operation],
        #{duration => Duration, event_count => EventCount},
        #{store_id => StoreId, stream_id => StreamId, timestamp => TimestampSpec}
    ),
    ok.
