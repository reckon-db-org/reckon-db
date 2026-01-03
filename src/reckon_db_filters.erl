%% @doc Khepri event filters for reckon-db
%%
%% Provides filter builders for Khepri event subscriptions.
%% These filters are used to watch for new events matching specific criteria.
%%
%% @author rgfaber

-module(reckon_db_filters).

-include_lib("khepri/include/khepri.hrl").

-export([
    by_stream/1,
    by_event_type/1,
    by_event_pattern/1,
    by_event_payload/1
]).

%% @doc Create a filter for all events in a specific stream
%%
%% Special case: the binary &lt;&lt;"$all"&gt;&gt; matches events in all streams.
-spec by_stream(binary()) -> khepri_evf:tree() | {error, invalid_stream}.
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
    List = binary_to_list(Stream),
    case string:chr(List, $$) of
        0 ->
            {error, invalid_stream};
        DollarPos ->
            StreamUuid = string:substr(List, DollarPos + 1),
            khepri_evf:tree(
                [streams,
                 list_to_binary(StreamUuid),
                 #if_all{conditions = [
                     #if_path_matches{regex = any},
                     #if_has_data{has_data = true}
                 ]}],
                #{on_actions => [create]}
            )
    end.

%% @doc Create a filter for events of a specific type
-spec by_event_type(binary()) -> khepri_evf:tree().
by_event_type(EventType) when is_binary(EventType) ->
    by_event_pattern(#{event_type => EventType}).

%% @doc Create a filter matching events with a specific pattern in their metadata
%%
%% The pattern is a map that must be a subset of the event record.
-spec by_event_pattern(map()) -> khepri_evf:tree().
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
-spec by_event_payload(map()) -> khepri_evf:tree().
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
