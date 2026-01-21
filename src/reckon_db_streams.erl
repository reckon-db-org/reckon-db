%% @doc Streams API facade for reckon-db
%%
%% Provides the public API for stream operations:
%% - append: Write events to a stream with optimistic concurrency
%% - read: Read events from a stream
%% - get_version: Get current stream version
%% - exists: Check if stream exists
%% - list_streams: List all streams in the store
%%
%% @author rgfaber

-module(reckon_db_streams).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").
-include_lib("khepri/include/khepri.hrl").

%% API
-export([
    append/4,
    append/5,
    read/5,
    read_all/4,
    read_by_event_types/3,
    read_by_tags/4,
    get_version/2,
    exists/2,
    list_streams/1,
    delete/2
]).

%% Internal exports for workers
-export([
    do_append/4,
    do_read/5
]).

%%====================================================================
%% Types
%%====================================================================

-type new_event() :: #{
    event_type := binary(),
    data := map() | binary(),
    metadata => map(),
    tags => [binary()],
    event_id => binary()
}.

-type direction() :: forward | backward.

-export_type([new_event/0, direction/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Append events to a stream with expected version check
%%
%% Expected version semantics:
%%   -1 (NO_STREAM)    - Stream must not exist (first write)
%%   -2 (ANY_VERSION)  - No version check, always append
%%   N >= 0            - Stream version must equal N
%%
%% Returns {ok, NewVersion} on success or {error, Reason} on failure.
-spec append(atom(), binary(), integer(), [new_event()]) ->
    {ok, non_neg_integer()} | {error, term()}.
append(StoreId, StreamId, ExpectedVersion, Events) ->
    append(StoreId, StreamId, ExpectedVersion, Events, #{}).

-spec append(atom(), binary(), integer(), [new_event()], map()) ->
    {ok, non_neg_integer()} | {error, term()}.
append(StoreId, StreamId, ExpectedVersion, Events, _Opts) ->
    StartTime = erlang:monotonic_time(),

    %% Emit start telemetry
    telemetry:execute(
        ?STREAM_WRITE_START,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, stream_id => StreamId,
          event_count => length(Events), expected_version => ExpectedVersion}
    ),

    Result = do_append(StoreId, StreamId, ExpectedVersion, Events),

    Duration = erlang:monotonic_time() - StartTime,

    %% Emit stop/error telemetry
    case Result of
        {ok, NewVersion} ->
            telemetry:execute(
                ?STREAM_WRITE_STOP,
                #{duration => Duration, event_count => length(Events)},
                #{store_id => StoreId, stream_id => StreamId, new_version => NewVersion}
            ),
            Result;
        {error, Reason} ->
            telemetry:execute(
                ?STREAM_WRITE_ERROR,
                #{duration => Duration},
                #{store_id => StoreId, stream_id => StreamId, reason => Reason}
            ),
            Result
    end.

%% @doc Read events from a stream
%%
%% Parameters:
%%   StoreId      - The store identifier
%%   StreamId     - The stream identifier
%%   StartVersion - Starting version (0-based)
%%   Count        - Maximum number of events to read
%%   Direction    - forward or backward
%%
%% Returns {ok, [Event]} or {error, Reason}
-spec read(atom(), binary(), non_neg_integer(), pos_integer(), direction()) ->
    {ok, [event()]} | {error, term()}.
read(StoreId, StreamId, StartVersion, Count, Direction) ->
    StartTime = erlang:monotonic_time(),

    %% Emit start telemetry
    telemetry:execute(
        ?STREAM_READ_START,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, stream_id => StreamId,
          start_version => StartVersion, count => Count, direction => Direction}
    ),

    Result = do_read(StoreId, StreamId, StartVersion, Count, Direction),

    Duration = erlang:monotonic_time() - StartTime,

    %% Emit stop telemetry
    case Result of
        {ok, Events} ->
            telemetry:execute(
                ?STREAM_READ_STOP,
                #{duration => Duration, event_count => length(Events)},
                #{store_id => StoreId, stream_id => StreamId}
            ),
            Result;
        Error ->
            Error
    end.

%% @doc Read all events from a stream
-spec read_all(atom(), binary(), pos_integer(), direction()) ->
    {ok, [event()]} | {error, term()}.
read_all(StoreId, StreamId, BatchSize, Direction) ->
    read(StoreId, StreamId, 0, BatchSize, Direction).

%% @doc Read all events of specific types from all streams using Khepri native filtering.
%%
%% This function uses Khepri's built-in #if_data_matches condition to filter
%% events by type at the database level, avoiding loading all events into memory.
%%
%% Parameters:
%%   StoreId    - The store identifier
%%   EventTypes - List of event type binaries to match
%%   BatchSize  - Maximum number of events to return (for pagination)
%%
%% Returns events sorted by epoch_us (global ordering).
-spec read_by_event_types(atom(), [binary()], pos_integer()) ->
    {ok, [event()]} | {error, term()}.
read_by_event_types(StoreId, EventTypes, BatchSize) when is_list(EventTypes) ->
    %% Build Khepri path pattern with native data matching
    %% Path structure: [streams, StreamId, PaddedVersion]
    %% We use #if_any to match any of the specified event types
    TypeConditions = [
        #if_data_matches{pattern = #event{event_type = ET, _ = '_'}}
        || ET <- EventTypes
    ],

    %% Combine conditions: match any event type AND has data
    DataCondition = case TypeConditions of
        [] ->
            #if_has_data{has_data = true};
        [SingleCondition] ->
            #if_all{conditions = [
                #if_has_data{has_data = true},
                SingleCondition
            ]};
        _ ->
            #if_all{conditions = [
                #if_has_data{has_data = true},
                #if_any{conditions = TypeConditions}
            ]}
    end,

    %% Build path pattern matching all events across all streams
    Path = [streams,
            ?KHEPRI_WILDCARD_STAR,  %% Any stream ID
            #if_all{conditions = [
                ?KHEPRI_WILDCARD_STAR,  %% Any version
                DataCondition
            ]}],

    case khepri:get_many(StoreId, Path) of
        {ok, Results} when is_map(Results) ->
            %% Convert results to events and sort by epoch_us
            Events = [convert_result_to_event(PathKey, Value)
                      || {PathKey, Value} <- maps:to_list(Results)],
            ValidEvents = [E || E <- Events, E =/= undefined],

            %% Sort by epoch_us for global ordering
            SortedEvents = lists:sort(
                fun(#event{epoch_us = E1}, #event{epoch_us = E2}) -> E1 =< E2 end,
                ValidEvents
            ),

            %% Apply batch size limit
            LimitedEvents = lists:sublist(SortedEvents, BatchSize),
            {ok, LimitedEvents};
        {ok, _} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

%% @doc Read all events matching tags from all streams.
%%
%% Tags provide a mechanism for cross-stream querying without affecting
%% stream-based concurrency control. This is useful for the process-centric
%% model where you want to find all events related to specific participants.
%%
%% == Match Modes ==
%%
%% `any' (default): Returns events containing ANY of the specified tags (union).
%%   Example: `read_by_tags(Store, [<<"student:456">>, <<"student:789">>], any, 100)'
%%   Returns events for either student.
%%
%% `all': Returns events containing ALL of the specified tags (intersection).
%%   Example: `read_by_tags(Store, [<<"student:456">>, <<"course:CS101">>], all, 100)'
%%   Returns only events tagged with both student 456 AND course CS101.
%%
%% == Parameters ==
%%
%%   StoreId   - The store identifier
%%   Tags      - List of tag binaries to match
%%   Match     - `any' | `all' (matching strategy)
%%   BatchSize - Maximum number of events to return
%%
%% == Returns ==
%%
%% Events sorted by epoch_us (global ordering).
-spec read_by_tags(atom(), [binary()], any | all, pos_integer()) ->
    {ok, [event()]} | {error, term()}.
read_by_tags(StoreId, Tags, Match, BatchSize) when is_list(Tags), is_atom(Match) ->
    %% Query all events from all streams and filter by tags in Erlang.
    %% Khepri's pattern matching doesn't easily support list membership checks,
    %% so we fetch events with data and filter client-side.
    %%
    %% For large stores, consider maintaining a separate tag index.
    Path = [streams,
            ?KHEPRI_WILDCARD_STAR,  %% Any stream ID
            #if_all{conditions = [
                ?KHEPRI_WILDCARD_STAR,  %% Any version
                #if_has_data{has_data = true}
            ]}],

    case khepri:get_many(StoreId, Path) of
        {ok, Results} when is_map(Results) ->
            %% Convert results to events
            AllEvents = [convert_result_to_event(PathKey, Value)
                         || {PathKey, Value} <- maps:to_list(Results)],
            ValidEvents = [E || E <- AllEvents, E =/= undefined],

            %% Filter by tag match mode (only events with tags list)
            FilteredEvents = filter_events_by_tags(ValidEvents, Tags, Match),

            %% Sort by epoch_us for global ordering
            SortedEvents = lists:sort(
                fun(#event{epoch_us = E1}, #event{epoch_us = E2}) -> E1 =< E2 end,
                FilteredEvents
            ),

            %% Apply batch size limit
            LimitedEvents = lists:sublist(SortedEvents, BatchSize),
            {ok, LimitedEvents};
        {ok, _} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

%% @private Filter events by tags according to match mode
%% Empty search tags always returns empty (no criteria = no results)
-spec filter_events_by_tags([event()], [binary()], any | all) -> [event()].
filter_events_by_tags(_Events, [], _Match) ->
    [];
filter_events_by_tags(Events, Tags, any) ->
    %% Return events that have ANY of the tags (union)
    TagSet = sets:from_list(Tags),
    lists:filter(
        fun(#event{tags = EventTags}) when is_list(EventTags), EventTags =/= [] ->
            EventTagSet = sets:from_list(EventTags),
            sets:size(sets:intersection(TagSet, EventTagSet)) > 0;
           (_) ->
            false
        end,
        Events
    );
filter_events_by_tags(Events, Tags, all) ->
    %% Return events that have ALL of the tags (intersection)
    TagSet = sets:from_list(Tags),
    lists:filter(
        fun(#event{tags = EventTags}) when is_list(EventTags), EventTags =/= [] ->
            EventTagSet = sets:from_list(EventTags),
            sets:is_subset(TagSet, EventTagSet);
           (_) ->
            false
        end,
        Events
    ).

%% @private Convert a Khepri result to an event, adding stream_id from path
-spec convert_result_to_event([atom() | binary()], term()) -> event() | undefined.
convert_result_to_event([streams, StreamId | _], Event) when is_record(Event, event) ->
    Event#event{stream_id = StreamId};
convert_result_to_event([streams, StreamId | _], EventMap) when is_map(EventMap) ->
    Event = map_to_event(EventMap),
    Event#event{stream_id = StreamId};
convert_result_to_event(_, _) ->
    undefined.

%% @doc Get current version of a stream
%%
%% Returns:
%%   -1    - if stream doesn't exist or is empty
%%   N >= 0 - representing the version of the latest event
-spec get_version(atom(), binary()) -> integer().
get_version(StoreId, StreamId) ->
    Path = ?STREAMS_PATH ++ [StreamId],
    case khepri:count(StoreId, Path ++ [?KHEPRI_WILDCARD_STAR]) of
        {ok, 0} -> ?NO_STREAM;
        {ok, Count} -> Count - 1;
        {error, _} -> ?NO_STREAM
    end.

%% @doc Check if a stream exists
-spec exists(atom(), binary()) -> boolean().
exists(StoreId, StreamId) ->
    Path = ?STREAMS_PATH ++ [StreamId],
    case khepri:exists(StoreId, Path) of
        true -> true;
        false -> false;
        {error, _} -> false
    end.

%% @doc List all streams in the store
-spec list_streams(atom()) -> {ok, [binary()]} | {error, term()}.
list_streams(StoreId) ->
    %% Use get_many with wildcard to find all stream IDs
    Path = ?STREAMS_PATH ++ [?KHEPRI_WILDCARD_STAR],
    case khepri:get_many(StoreId, Path) of
        {ok, Results} when is_map(Results) ->
            %% Extract stream IDs from paths: {[streams, StreamId, ...], _}
            StreamIds = lists:usort([
                extract_stream_id(P) || P <- maps:keys(Results)
            ]),
            {ok, StreamIds};
        {ok, _} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

%% @private Extract stream ID from path [streams, StreamId, ...]
-spec extract_stream_id([atom() | binary()]) -> binary().
extract_stream_id([streams, StreamId | _]) ->
    StreamId;
extract_stream_id(_) ->
    <<>>.

%% @doc Delete a stream and all its events
-spec delete(atom(), binary()) -> ok | {error, term()}.
delete(StoreId, StreamId) ->
    Path = ?STREAMS_PATH ++ [StreamId],
    case khepri:delete(StoreId, Path) of
        ok -> ok;
        {error, _} = Error -> Error
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec do_append(atom(), binary(), integer(), [new_event()]) ->
    {ok, non_neg_integer()} | {error, term()}.
do_append(StoreId, StreamId, ExpectedVersion, Events) ->
    CurrentVersion = get_version(StoreId, StreamId),

    %% Check expected version
    case check_expected_version(ExpectedVersion, CurrentVersion) of
        ok ->
            append_events_to_stream(StoreId, StreamId, CurrentVersion, Events);
        {error, _} = Error ->
            Error
    end.

%% @private
-spec check_expected_version(integer(), integer()) -> ok | {error, term()}.
check_expected_version(?ANY_VERSION, _CurrentVersion) ->
    ok;
check_expected_version(?NO_STREAM, CurrentVersion) when CurrentVersion =:= ?NO_STREAM ->
    ok;
check_expected_version(?NO_STREAM, CurrentVersion) ->
    {error, {wrong_expected_version, ?NO_STREAM, CurrentVersion}};
check_expected_version(ExpectedVersion, CurrentVersion) when ExpectedVersion =:= CurrentVersion ->
    ok;
check_expected_version(ExpectedVersion, CurrentVersion) ->
    {error, {wrong_expected_version, ExpectedVersion, CurrentVersion}}.

%% @private
-spec append_events_to_stream(atom(), binary(), integer(), [new_event()]) ->
    {ok, non_neg_integer()}.
append_events_to_stream(StoreId, StreamId, CurrentVersion, Events) ->
    Now = erlang:system_time(millisecond),
    EpochUs = erlang:system_time(microsecond),

    FinalVersion = lists:foldl(
        fun(Event, AccVersion) ->
            NewVersion = AccVersion + 1,
            RecordedEvent = create_event_record(Event, StreamId, NewVersion, Now, EpochUs),
            PaddedVersion = pad_version(NewVersion, ?VERSION_PADDING),
            Path = ?STREAMS_PATH ++ [StreamId, PaddedVersion],
            ok = khepri:put(StoreId, Path, RecordedEvent),
            NewVersion
        end,
        CurrentVersion,
        Events
    ),
    {ok, FinalVersion}.

%% @private
-spec create_event_record(new_event(), binary(), non_neg_integer(), integer(), integer()) -> event().
create_event_record(Event, StreamId, Version, Timestamp, EpochUs) ->
    EventId = maps:get(event_id, Event, generate_event_id()),
    EventType = maps:get(event_type, Event),
    Data = maps:get(data, Event),
    Metadata = maps:get(metadata, Event, #{}),
    Tags = maps:get(tags, Event, undefined),
    DataContentType = maps:get(data_content_type, Event, ?CONTENT_TYPE_JSON),
    MetadataContentType = maps:get(metadata_content_type, Event, ?CONTENT_TYPE_JSON),

    #event{
        event_id = EventId,
        event_type = EventType,
        stream_id = StreamId,
        version = Version,
        data = Data,
        metadata = Metadata,
        tags = Tags,
        timestamp = Timestamp,
        epoch_us = EpochUs,
        data_content_type = DataContentType,
        metadata_content_type = MetadataContentType
    }.

%% @private
-spec generate_event_id() -> binary().
generate_event_id() ->
    %% Generate a UUID-like identifier
    Bytes = crypto:strong_rand_bytes(16),
    list_to_binary(uuid_to_string(Bytes)).

%% @private
uuid_to_string(<<A:32, B:16, C:16, D:16, E:48>>) ->
    io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [A, B, C, D, E]).

%% @private
-spec pad_version(non_neg_integer(), pos_integer()) -> binary().
pad_version(Version, Length) ->
    VersionStr = integer_to_list(Version),
    Padding = Length - length(VersionStr),
    PaddedStr = lists:duplicate(Padding, $0) ++ VersionStr,
    list_to_binary(PaddedStr).

%% @private
-spec do_read(atom(), binary(), non_neg_integer(), pos_integer(), direction()) ->
    {ok, [event()]} | {error, term()}.
do_read(StoreId, StreamId, StartVersion, Count, Direction) ->
    case exists(StoreId, StreamId) of
        false ->
            {error, {stream_not_found, StreamId}};
        true ->
            read_events(StoreId, StreamId, StartVersion, Count, Direction)
    end.

%% @private
-spec read_events(atom(), binary(), non_neg_integer(), pos_integer(), direction()) ->
    {ok, [event()]}.
read_events(StoreId, StreamId, StartVersion, Count, Direction) ->
    Versions = calculate_versions(StartVersion, Count, Direction),
    Events = lists:filtermap(
        fun(Version) ->
            PaddedVersion = pad_version(Version, ?VERSION_PADDING),
            Path = ?STREAMS_PATH ++ [StreamId, PaddedVersion],
            case khepri:get(StoreId, Path) of
                {ok, Event} when is_record(Event, event) ->
                    {true, Event};
                {ok, EventMap} when is_map(EventMap) ->
                    %% Convert map to record if needed
                    {true, map_to_event(EventMap)};
                _ ->
                    false
            end
        end,
        Versions
    ),
    {ok, Events}.

%% @private
-spec calculate_versions(non_neg_integer(), pos_integer(), direction()) -> [non_neg_integer()].
calculate_versions(StartVersion, Count, forward) ->
    lists:seq(StartVersion, StartVersion + Count - 1);
calculate_versions(StartVersion, Count, backward) ->
    EndVersion = max(0, StartVersion - Count + 1),
    lists:reverse(lists:seq(EndVersion, StartVersion)).

%% @private
-spec map_to_event(map()) -> event().
map_to_event(Map) ->
    #event{
        event_id = maps:get(event_id, Map, undefined),
        event_type = maps:get(event_type, Map, undefined),
        stream_id = maps:get(stream_id, Map, undefined),
        version = maps:get(version, Map, 0),
        data = maps:get(data, Map, #{}),
        metadata = maps:get(metadata, Map, #{}),
        tags = maps:get(tags, Map, undefined),
        timestamp = maps:get(timestamp, Map, 0),
        epoch_us = maps:get(epoch_us, Map, 0),
        data_content_type = maps:get(data_content_type, Map, ?CONTENT_TYPE_JSON),
        metadata_content_type = maps:get(metadata_content_type, Map, ?CONTENT_TYPE_JSON)
    }.
