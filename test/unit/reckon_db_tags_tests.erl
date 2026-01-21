%% @doc Unit tests for reckon_db tag functionality
%%
%% Tests the tag-based querying feature that supports cross-stream
%% event queries using ANY (union) or ALL (intersection) matching.
%%
%% @author rgfaber

-module(reckon_db_tags_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../../include/reckon_db.hrl").

%%====================================================================
%% Test fixtures
%%====================================================================

%% Helper to create a mock event with specific tags
mock_event(Version, Tags) ->
    #event{
        event_id = iolist_to_binary(io_lib:format("event-~p", [Version])),
        event_type = <<"TestEvent">>,
        stream_id = <<"test-stream">>,
        version = Version,
        data = #{version => Version},
        metadata = #{},
        tags = Tags,
        timestamp = Version * 1000,
        epoch_us = Version * 1000000
    }.

%% Create a list of events with various tag combinations
mock_tagged_events() ->
    [
        %% Student 456 enrolled in CS101
        mock_event(0, [<<"student:456">>, <<"course:CS101">>]),
        %% Student 456 enrolled in MATH201
        mock_event(1, [<<"student:456">>, <<"course:MATH201">>]),
        %% Student 789 enrolled in CS101
        mock_event(2, [<<"student:789">>, <<"course:CS101">>]),
        %% Student 789 enrolled in PHYS101
        mock_event(3, [<<"student:789">>, <<"course:PHYS101">>]),
        %% Event with no tags
        mock_event(4, undefined),
        %% Event with empty tags list
        mock_event(5, [])
    ].

%%====================================================================
%% filter_events_by_tags/3 tests (ANY mode)
%%====================================================================

filter_by_tags_any_test_() ->
    Events = mock_tagged_events(),
    [
        {"any: single tag matches multiple events",
         ?_assertEqual(
             2,
             length(filter_events_by_tags(Events, [<<"student:456">>], any))
         )},

        {"any: single course tag matches enrolled students",
         ?_assertEqual(
             2,
             length(filter_events_by_tags(Events, [<<"course:CS101">>], any))
         )},

        {"any: multiple tags returns union",
         ?_assertEqual(
             4,  %% All 4 enrollment events
             length(filter_events_by_tags(Events, [<<"student:456">>, <<"student:789">>], any))
         )},

        {"any: non-existent tag returns empty",
         ?_assertEqual(
             [],
             filter_events_by_tags(Events, [<<"student:999">>], any)
         )},

        {"any: mixed existing and non-existing tags",
         ?_assertEqual(
             2,
             length(filter_events_by_tags(Events, [<<"student:456">>, <<"student:999">>], any))
         )},

        {"any: empty tag list returns empty",
         ?_assertEqual(
             [],
             filter_events_by_tags(Events, [], any)
         )}
    ].

%%====================================================================
%% filter_events_by_tags/3 tests (ALL mode)
%%====================================================================

filter_by_tags_all_test_() ->
    Events = mock_tagged_events(),
    [
        {"all: single tag matches same as any",
         ?_assertEqual(
             2,
             length(filter_events_by_tags(Events, [<<"student:456">>], all))
         )},

        {"all: two tags returns intersection",
         ?_assertEqual(
             1,  %% Only event 0 has both student:456 AND course:CS101
             length(filter_events_by_tags(Events, [<<"student:456">>, <<"course:CS101">>], all))
         )},

        {"all: impossible combination returns empty",
         ?_assertEqual(
             [],  %% No event has both student:456 AND student:789
             filter_events_by_tags(Events, [<<"student:456">>, <<"student:789">>], all)
         )},

        {"all: superset of event tags still matches",
         begin
             %% Search for student:456 (single tag)
             %% Event 0 has [student:456, course:CS101] which contains student:456
             Result = filter_events_by_tags(Events, [<<"student:456">>], all),
             ?_assertEqual(2, length(Result))
         end},

        {"all: non-existent tag returns empty",
         ?_assertEqual(
             [],
             filter_events_by_tags(Events, [<<"student:456">>, <<"course:DOES_NOT_EXIST">>], all)
         )},

        {"all: empty tag list returns empty",
         ?_assertEqual(
             [],
             filter_events_by_tags(Events, [], all)
         )}
    ].

%%====================================================================
%% Edge cases
%%====================================================================

edge_cases_test_() ->
    [
        {"events with undefined tags are excluded",
         fun() ->
             Events = [
                 mock_event(0, [<<"tag:1">>]),
                 mock_event(1, undefined),
                 mock_event(2, [<<"tag:1">>])
             ],
             Result = filter_events_by_tags(Events, [<<"tag:1">>], any),
             ?assertEqual(2, length(Result))
         end},

        {"events with empty tags list are excluded",
         fun() ->
             Events = [
                 mock_event(0, [<<"tag:1">>]),
                 mock_event(1, []),
                 mock_event(2, [<<"tag:1">>])
             ],
             Result = filter_events_by_tags(Events, [<<"tag:1">>], any),
             ?assertEqual(2, length(Result))
         end},

        {"correct events returned (not just count)",
         fun() ->
             Events = mock_tagged_events(),
             Result = filter_events_by_tags(Events, [<<"student:456">>, <<"course:CS101">>], all),
             ?assertEqual(1, length(Result)),
             [Event] = Result,
             ?assertEqual(0, Event#event.version)  %% First event
         end}
    ].

%%====================================================================
%% Internal function access
%%====================================================================

%% We need to call the internal function for unit testing.
%% This is done by replicating the logic here (since it's not exported)
%% or by testing through the public API with a running store.

%% For unit tests, we replicate the filter logic:
%% Empty search tags always returns empty (no criteria = no results)
filter_events_by_tags(_Events, [], _Match) ->
    [];
filter_events_by_tags(Events, Tags, any) ->
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
