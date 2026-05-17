%% @doc Khepri event filters for reckon-db
%%
%% Provides filter builders for Khepri event subscriptions.
%% These filters are used to watch for new events matching specific criteria.
%%
%% @author rgfaber

-module(reckon_db_filters).

-include_lib("khepri/include/khepri.hrl").
-include("reckon_db.hrl").

-export([
    by_stream/1,
    by_event_type/1,
    by_event_pattern/1,
    by_event_payload/1,
    by_tags/1,
    matches/3
]).

%% @doc Create a filter for all events in a specific stream.
%%
%% Special case: the binary &lt;&lt;"$all"&gt;&gt; matches events in all streams.
%% Any other binary is used verbatim as the stream-id path segment;
%% streams are stored at [streams, StreamId, PaddedVersion].
-spec by_stream(binary()) -> khepri_evf:tree_event_filter().
by_stream(<<"$all">>) ->
    khepri_evf:tree(
        [streams,
         #if_path_matches{regex = any},
         #if_all{conditions = [
             #if_path_matches{regex = any},
             #if_has_data{has_data = true}
         ]}],
        #{on_actions => [create]}
    );
by_stream(Stream) when is_binary(Stream) ->
    khepri_evf:tree(
        [streams,
         Stream,
         #if_all{conditions = [
             #if_path_matches{regex = any},
             #if_has_data{has_data = true}
         ]}],
        #{on_actions => [create]}
    ).

%% @doc Create a filter for events of a specific type
%%
%% Uses #event{} record pattern matching since events are stored as records.
-spec by_event_type(binary()) -> khepri_evf:tree_event_filter().
by_event_type(EventType) when is_binary(EventType) ->
    khepri_evf:tree(
        [streams,
         #if_path_matches{regex = any},
         #if_all{conditions = [
             #if_path_matches{regex = any},
             #if_has_data{has_data = true},
             #if_data_matches{pattern = #event{event_type = EventType, _ = '_'}}
         ]}],
        #{on_actions => [create]}
    ).

%% @doc Create a filter matching events with a specific pattern in their metadata
%%
%% The pattern is a map that must be a subset of the event record.
-spec by_event_pattern(map()) -> khepri_evf:tree_event_filter().
by_event_pattern(EventPattern) when is_map(EventPattern) ->
    khepri_evf:tree(
        [streams,
         #if_path_matches{regex = any},
         #if_all{conditions = [
             #if_path_matches{regex = any},
             #if_has_data{has_data = true},
             #if_data_matches{pattern = EventPattern}
         ]}],
        #{on_actions => [create]}
    ).

%% @doc Create a filter matching events with a specific pattern in their payload
%%
%% The pattern is checked against the data field of the event.
-spec by_event_payload(map()) -> khepri_evf:tree_event_filter().
by_event_payload(PayloadPattern) when is_map(PayloadPattern) ->
    khepri_evf:tree(
        [streams,
         #if_path_matches{regex = any},
         #if_all{conditions = [
             #if_path_matches{regex = any},
             #if_has_data{has_data = true},
             #if_data_matches{pattern = #{data => PayloadPattern}}
         ]}],
        #{on_actions => [create]}
    ).

%% @doc Create a filter matching events with specific tags
%%
%% Note: Khepri's pattern matching doesn't natively support list membership,
%% so this creates a broad filter that matches all events with tags.
%% The actual tag filtering must be done by the subscription consumer.
%% For efficient tag-based queries, use {@link reckon_db_streams:read_by_tags/4}.
-spec by_tags([binary()]) -> khepri_evf:tree_event_filter().
by_tags(Tags) when is_list(Tags) ->
    %% Create a filter that matches events with any tags field
    %% The consumer must filter for specific tag membership
    khepri_evf:tree(
        [streams,
         #if_path_matches{regex = any},
         #if_all{conditions = [
             #if_path_matches{regex = any},
             #if_has_data{has_data = true}
         ]}],
        #{on_actions => [create]}
    ).

%% @doc In-memory predicate matching an `#event{}' record against a
%% subscription's (Type, Selector) pair.
%%
%% The Khepri event filters above are evaluated by Khepri itself on
%% live triggers, but the catch-up replay path bypasses Khepri's
%% trigger engine — it reads the global stream and delivers events
%% directly to the subscriber. Without this predicate, every
%% subscription with `start_from = 0' would receive the entire log
%% regardless of its declared selector. Use this to filter the
%% catch-up batch before sending.
%%
%% Tag membership uses set inclusion: the event must carry ALL of
%% the requested tags (consistent with `read_by_tags' default match
%% semantics).
-spec matches(subscription_type(), binary() | map() | [binary()], event()) ->
    boolean().
matches(by_stream, <<"$all">>, _Event) ->
    true;
matches(stream, <<"$all">>, _Event) ->
    true;
matches(by_stream, StreamId, #event{stream_id = StreamId}) when is_binary(StreamId) ->
    true;
matches(stream, StreamId, #event{stream_id = StreamId}) when is_binary(StreamId) ->
    true;
matches(by_stream, _Other, _Event) ->
    false;
matches(stream, _Other, _Event) ->
    false;
matches(by_event_type, EventType, #event{event_type = EventType}) when is_binary(EventType) ->
    true;
matches(event_type, EventType, #event{event_type = EventType}) when is_binary(EventType) ->
    true;
matches(by_event_type, _Other, _Event) ->
    false;
matches(event_type, _Other, _Event) ->
    false;
matches(by_tags, RequestedTags, #event{tags = EventTags})
        when is_list(RequestedTags), is_list(EventTags) ->
    lists:all(fun(T) -> lists:member(T, EventTags) end, RequestedTags);
matches(tags, RequestedTags, Event) ->
    matches(by_tags, RequestedTags, Event);
matches(by_tags, _Other, _Event) ->
    false;
%% by_event_pattern / by_event_payload — for now, deliver all (catch-up
%% replays the full log and the live trigger handles real filtering).
%% Tightening these is a separate task that needs map-pattern eval here.
matches(by_event_pattern, _Pattern, _Event) ->
    true;
matches(event_pattern, _Pattern, _Event) ->
    true;
matches(by_event_payload, _Pattern, _Event) ->
    true;
matches(event_payload, _Pattern, _Event) ->
    true;
matches(_Type, _Selector, _Event) ->
    false.
