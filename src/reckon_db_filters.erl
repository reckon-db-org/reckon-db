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
    event_type_pattern/1,
    matches/3
]).

%% @doc Build a Khepri tree-event filter for subscriptions of
%% type `stream' (a.k.a. `by_stream').
%%
%% == Selector grammar ==
%%
%% <ul>
%% <li>`&lt;&lt;"$all"&gt;&gt;' — the only recognised wildcard sentinel.
%%     Matches every event in every stream in the store.</li>
%% <li>Any other non-empty binary — a literal stream id; the
%%     filter matches events on that exact stream only.</li>
%% </ul>
%%
%% == Stream id conventions ==
%%
%% The filter doesn't enforce stream-id shape (it just treats the
%% selector as a path component), but reckon-db has two formats:
%%
%% <ul>
%% <li>**User streams** — `&lt;prefix&gt;-&lt;hex&gt;', where prefix is
%%     `[A-Za-z]+' and hex is `[A-Fa-f0-9]+' (typically a UUIDv7
%%     with dashes stripped). Example:
%%     `account-018f6a7b8c9d4abc8901234567890abc'.</li>
%% <li>**System streams** — `$&lt;namespace&gt;:&lt;name&gt;', where the
%%     `$' prefix marks "reckon-db-managed, not user data" and
%%     `&lt;name&gt;' is intentionally human-readable. Example:
%%     `$link:high-value-orders' from {@link reckon_db_links}.
%%     Same role as EventStoreDB's `$ce-' / `$et-' / `$by_*'
%%     prefixes.</li>
%% </ul>
%%
%% Both formats subscribe identically — the filter doesn't care.
%% The `$' namespace is only special at the sentinel level
%% (`$all').
%%
%% Returns `{error, empty_selector}' for an empty binary, and
%% `reckon_db_stream_path:validate/1''s error for an id the grammar
%% rejects; every other input builds a filter.
%%
%% The malformed case used to raise from the path builder, and it raised
%% after the subscription record had already been written: the caller's
%% cleanup branch never ran, so a poisoned subscription stayed in the
%% store and raised again on the next restore or trigger re-register
%% (reckon-db#4). Returning the error lets that branch delete it.
-spec by_stream(term()) ->
    khepri_evf:tree_event_filter()
    | {error, empty_selector | reckon_db_stream_path:validation_error()}.
by_stream(<<"$all">>) ->
    %% Model C layout: [streams, Type, Id, Version]. The $all selector
    %% matches every regular event leaf across all aggregate types.
    khepri_evf:tree(
        [streams,
         #if_path_matches{regex = any},
         #if_path_matches{regex = any},
         #if_all{conditions = [
             #if_path_matches{regex = any},
             #if_has_data{has_data = true}
         ]}],
        #{on_actions => [create]}
    );
by_stream(<<>>) ->
    {error, empty_selector};
by_stream(Stream) ->
    case reckon_db_stream_path:validate(Stream) of
        ok ->
            %% A literal stream id is split into its structural {Type, Id}
            %% path segments so the trigger watches exactly
            %% [streams, Type, Id, *].
            [streams, Type, Id] = reckon_db_stream_path:stream_path(Stream),
            khepri_evf:tree(
                [streams,
                 Type,
                 Id,
                 #if_all{conditions = [
                     #if_path_matches{regex = any},
                     #if_has_data{has_data = true}
                 ]}],
                #{on_actions => [create]}
            );
        {error, _} = Invalid ->
            Invalid
    end.

%% @doc Create a filter for events of a specific type
%%
%% Uses #event{} record pattern matching since events are stored as records.
%% Returns `{error, empty_selector}' for an empty binary and
%% `{error, {not_a_binary, Selector}}' for anything else that is not one.
-spec by_event_type(term()) ->
    khepri_evf:tree_event_filter() | {error, empty_selector | {not_a_binary, term()}}.
by_event_type(<<>>) ->
    {error, empty_selector};
by_event_type(EventType) when is_binary(EventType) ->
    khepri_evf:tree(
        [streams,
         #if_path_matches{regex = any},
         #if_path_matches{regex = any},
         #if_all{conditions = [
             #if_path_matches{regex = any},
             #if_has_data{has_data = true},
             #if_data_matches{pattern = event_type_pattern(EventType)}
         ]}],
        #{on_actions => [create]}
    );
by_event_type(Other) ->
    {error, {not_a_binary, Other}}.

%% @doc A Khepri data-match pattern for a stored `#event{}' of this type:
%% the record's tag, the type, and `'_'' in every other field.
%%
%% It is a match pattern, not an event, so it is built as a tuple:
%% `#event{event_type = T, _ = '_'}' says the same thing in record syntax
%% and violates every other field's declared type.
-spec event_type_pattern(binary()) -> tuple().
event_type_pattern(EventType) ->
    Wild = erlang:make_tuple(record_info(size, event), '_', [{1, event}]),
    setelement(#event.event_type, Wild, EventType).

%% @doc Create a filter for an `event_pattern' subscription.
%%
%% The selector is a map whose keys are `#event{}' field names
%% (`event_type', `stream_id', `version', `data', `metadata', `tags', ...).
%% An event matches when every field given matches: a map-valued field
%% (`data', `metadata') matches when every key given has that value, any
%% other field when it is equal. See {@link matches/3}.
%%
%% Khepri's data-match condition cannot express that over the stored
%% `#event{}' record (a map pattern never matches a tuple), so the trigger
%% fires for every new event and the subscription's trigger proc applies
%% {@link matches/3} before delivering, the same predicate catch-up uses.
%%
%% Refused: a selector that is not a map, an empty map, and a map with a
%% key that is not an `#event{}' field.
-spec by_event_pattern(term()) ->
    khepri_evf:tree_event_filter()
    | {error, empty_selector | {not_a_map, term()} | {unknown_event_fields, [term()]}}.
by_event_pattern(Pattern) when Pattern =:= #{} ->
    {error, empty_selector};
by_event_pattern(Pattern) when is_map(Pattern) ->
    case [K || K <- maps:keys(Pattern), not lists:member(K, record_info(fields, event))] of
        [] -> every_new_event();
        Unknown -> {error, {unknown_event_fields, Unknown}}
    end;
by_event_pattern(Other) ->
    {error, {not_a_map, Other}}.

%% @doc Create a filter for an `event_payload' subscription.
%%
%% The selector is a map over the event's data: an event matches when every
%% key given has that value in its data. Like {@link by_event_pattern/1},
%% the trigger fires for every new event and {@link matches/3} decides.
%% Refused: a selector that is not a map, and an empty map.
-spec by_event_payload(term()) ->
    khepri_evf:tree_event_filter() | {error, empty_selector | {not_a_map, term()}}.
by_event_payload(Pattern) when Pattern =:= #{} ->
    {error, empty_selector};
by_event_payload(Pattern) when is_map(Pattern) ->
    every_new_event();
by_event_payload(Other) ->
    {error, {not_a_map, Other}}.

%% @doc Create a filter for a `by_tags' subscription.
%%
%% An event matches when it carries every tag given. Khepri's pattern
%% matching has no list membership, so the trigger fires for every new event
%% and {@link matches/3} decides, live as on catch-up. For queries rather
%% than subscriptions, use {@link reckon_db_streams:read_by_tags/4}.
%% Refused: anything but a non-empty list of binaries.
-spec by_tags(term()) ->
    khepri_evf:tree_event_filter() | {error, empty_selector | {not_a_tag_list, term()}}.
by_tags([]) ->
    {error, empty_selector};
by_tags(Tags) when is_list(Tags) ->
    case lists:all(fun is_binary/1, Tags) of
        true -> every_new_event();
        false -> {error, {not_a_tag_list, Tags}}
    end;
by_tags(Other) ->
    {error, {not_a_tag_list, Other}}.

%% Every event created in every stream; the selector is applied by
%% matches/3 on delivery.
every_new_event() ->
    khepri_evf:tree(
        [streams,
         #if_path_matches{regex = any},
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
%% the requested tags (the `all' match of `read_by_tags/4'). `event_pattern' and `event_payload' are described at
%% {@link by_event_pattern/1} and {@link by_event_payload/1}.
%%
%% The live path applies this too: the trigger proc of every
%% subscription checks it before broadcasting, so the two paths agree
%% by construction (reckon-db#8, #11).
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
matches(by_event_pattern, Pattern, Event) when is_map(Pattern) ->
    maps:fold(fun(Field, Want, Acc) -> Acc andalso field_matches(Want, event_field(Field, Event)) end,
              true, Pattern);
matches(event_pattern, Pattern, Event) ->
    matches(by_event_pattern, Pattern, Event);
matches(by_event_payload, Pattern, #event{data = Data}) when is_map(Pattern), is_map(Data) ->
    has_every_key(Pattern, Data);
matches(event_payload, Pattern, Event) ->
    matches(by_event_payload, Pattern, Event);
matches(_Type, _Selector, _Event) ->
    false.

%% A map-valued field matches when every key given has that value; any
%% other field when it is equal.
field_matches(Want, {ok, Have}) when is_map(Want), is_map(Have) ->
    has_every_key(Want, Have);
field_matches(Want, {ok, Have}) ->
    Want =:= Have;
field_matches(_Want, error) ->
    false.

has_every_key(Want, Have) ->
    maps:fold(fun(K, V, Acc) -> Acc andalso maps:find(K, Have) =:= {ok, V} end, true, Want).

event_field(Field, Event) ->
    Values = tl(tuple_to_list(Event)),
    maps:find(Field, maps:from_list(lists:zip(record_info(fields, event), Values))).
