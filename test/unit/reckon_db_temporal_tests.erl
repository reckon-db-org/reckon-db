%% @doc Unit tests for reckon_db_temporal module
%% @author Macula.io

-module(reckon_db_temporal_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../../include/reckon_db.hrl").

%%====================================================================
%% Test fixtures
%%====================================================================

%% Helper to create a mock event with specific epoch_us
mock_event(Version, EpochUs) ->
    #event{
        event_id = iolist_to_binary(io_lib:format("event-~p", [Version])),
        event_type = <<"TestEvent">>,
        stream_id = <<"test-stream">>,
        version = Version,
        data = #{version => Version},
        metadata = #{},
        timestamp = EpochUs div 1000,
        epoch_us = EpochUs
    }.

%% Create a list of events spanning different timestamps
mock_events() ->
    [
        mock_event(0, 1000000),   %% 1 second
        mock_event(1, 2000000),   %% 2 seconds
        mock_event(2, 3000000),   %% 3 seconds
        mock_event(3, 5000000),   %% 5 seconds
        mock_event(4, 10000000)   %% 10 seconds
    ].

%%====================================================================
%% Internal function tests
%%====================================================================

filter_events_until_test_() ->
    Events = mock_events(),
    [
        {"filter all events up to 3 seconds",
         ?_assertEqual(
             [mock_event(0, 1000000), mock_event(1, 2000000), mock_event(2, 3000000)],
             filter_events_until(Events, 3000000)
         )},

        {"filter all events up to 1 second",
         ?_assertEqual(
             [mock_event(0, 1000000)],
             filter_events_until(Events, 1000000)
         )},

        {"filter with timestamp before first event",
         ?_assertEqual(
             [],
             filter_events_until(Events, 500000)
         )},

        {"filter with timestamp after last event",
         ?_assertEqual(
             Events,
             filter_events_until(Events, 15000000)
         )},

        {"filter empty list",
         ?_assertEqual(
             [],
             filter_events_until([], 5000000)
         )}
    ].

filter_events_range_test_() ->
    Events = mock_events(),
    [
        {"filter events between 2 and 5 seconds",
         ?_assertEqual(
             [mock_event(1, 2000000), mock_event(2, 3000000), mock_event(3, 5000000)],
             filter_events_range(Events, 2000000, 5000000)
         )},

        {"filter with exact boundary match",
         ?_assertEqual(
             [mock_event(0, 1000000), mock_event(1, 2000000)],
             filter_events_range(Events, 1000000, 2000000)
         )},

        {"filter with no matching events",
         ?_assertEqual(
             [],
             filter_events_range(Events, 6000000, 9000000)
         )},

        {"filter single event",
         ?_assertEqual(
             [mock_event(2, 3000000)],
             filter_events_range(Events, 3000000, 3000000)
         )},

        {"filter empty list",
         ?_assertEqual(
             [],
             filter_events_range([], 1000000, 5000000)
         )}
    ].

apply_opts_test_() ->
    Events = mock_events(),
    [
        {"default options (forward, no limit)",
         ?_assertEqual(
             {ok, Events},
             apply_opts(Events, #{})
         )},

        {"forward direction explicit",
         ?_assertEqual(
             {ok, Events},
             apply_opts(Events, #{direction => forward})
         )},

        {"backward direction",
         ?_assertEqual(
             {ok, lists:reverse(Events)},
             apply_opts(Events, #{direction => backward})
         )},

        {"limit to 2 events",
         ?_assertEqual(
             {ok, [mock_event(0, 1000000), mock_event(1, 2000000)]},
             apply_opts(Events, #{limit => 2})
         )},

        {"limit with backward direction",
         ?_assertEqual(
             {ok, [mock_event(4, 10000000), mock_event(3, 5000000)]},
             apply_opts(Events, #{direction => backward, limit => 2})
         )},

        {"limit larger than event count",
         ?_assertEqual(
             {ok, Events},
             apply_opts(Events, #{limit => 100})
         )},

        {"empty list with options",
         ?_assertEqual(
             {ok, []},
             apply_opts([], #{direction => backward, limit => 10})
         )}
    ].

%%====================================================================
%% Helper functions (extracted from module for testing)
%%====================================================================

%% These are copies of internal functions for isolated testing
%% In production, these are in reckon_db_temporal module

filter_events_until(Events, Timestamp) ->
    [E || E <- Events, E#event.epoch_us =< Timestamp].

filter_events_range(Events, FromTimestamp, ToTimestamp) ->
    [E || E <- Events,
          E#event.epoch_us >= FromTimestamp,
          E#event.epoch_us =< ToTimestamp].

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

%%====================================================================
%% Timestamp conversion helpers tests
%%====================================================================

timestamp_edge_cases_test_() ->
    [
        {"zero timestamp filters nothing",
         ?_assertEqual(
             [],
             filter_events_until(mock_events(), 0)
         )},

        {"max integer timestamp includes all",
         ?_assertEqual(
             mock_events(),
             filter_events_until(mock_events(), 9999999999999999)
         )},

        {"range with from > to returns empty",
         ?_assertEqual(
             [],
             filter_events_range(mock_events(), 5000000, 2000000)
         )}
    ].
