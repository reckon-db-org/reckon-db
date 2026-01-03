%% @doc EUnit tests for reckon_db_aggregator module
%% @author Macula.io

-module(reckon_db_aggregator_tests).

-include_lib("eunit/include/eunit.hrl").
-include("reckon_db.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

aggregator_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
        {"foldl with empty list returns empty map",
         fun foldl_empty_list_test/0},
        {"foldl applies events in order",
         fun foldl_applies_events_test/0},
        {"foldl with initial state",
         fun foldl_with_initial_state_test/0},
        {"sum tag accumulates values",
         fun sum_tag_test/0},
        {"overwrite tag replaces value",
         fun overwrite_tag_test/0},
        {"plain values overwrite",
         fun plain_values_test/0},
        {"mixed tags in single event",
         fun mixed_tags_test/0},
        {"finalize unwraps tagged values",
         fun finalize_test/0},
        {"aggregate with event records",
         fun aggregate_with_records_test/0},
        {"aggregate with snapshot",
         fun aggregate_with_snapshot_test/0},
        {"aggregate without finalize",
         fun aggregate_without_finalize_test/0}
     ]}.

setup() ->
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

foldl_empty_list_test() ->
    Result = reckon_db_aggregator:foldl([]),
    ?assertEqual(#{}, Result).

foldl_applies_events_test() ->
    Events = [
        #{name => <<"Alice">>},
        #{age => 25},
        #{city => <<"London">>}
    ],
    Result = reckon_db_aggregator:foldl(Events),
    Expected = #{name => <<"Alice">>, age => 25, city => <<"London">>},
    ?assertEqual(Expected, Result).

foldl_with_initial_state_test() ->
    InitialState = #{country => <<"UK">>},
    Events = [
        #{name => <<"Bob">>}
    ],
    Result = reckon_db_aggregator:foldl(Events, InitialState),
    Expected = #{country => <<"UK">>, name => <<"Bob">>},
    ?assertEqual(Expected, Result).

sum_tag_test() ->
    Events = [
        #{count => {sum, 1}},
        #{count => {sum, 5}},
        #{count => {sum, -2}}
    ],
    Result = reckon_db_aggregator:foldl(Events),
    Finalized = reckon_db_aggregator:finalize(Result),
    ?assertEqual(#{count => 4}, Finalized).

overwrite_tag_test() ->
    Events = [
        #{status => {overwrite, <<"pending">>}},
        #{status => {overwrite, <<"active">>}},
        #{status => {overwrite, <<"completed">>}}
    ],
    Result = reckon_db_aggregator:foldl(Events),
    Finalized = reckon_db_aggregator:finalize(Result),
    ?assertEqual(#{status => <<"completed">>}, Finalized).

plain_values_test() ->
    Events = [
        #{name => <<"Alice">>},
        #{name => <<"Bob">>},
        #{name => <<"Charlie">>}
    ],
    Result = reckon_db_aggregator:foldl(Events),
    ?assertEqual(#{name => <<"Charlie">>}, Result).

mixed_tags_test() ->
    Events = [
        #{
            name => <<"John">>,
            age => {sum, 1},
            location => {overwrite, <<"NYC">>}
        },
        #{
            age => {sum, 1},
            location => {overwrite, <<"LA">>}
        },
        #{
            age => {sum, 1}
        }
    ],
    Result = reckon_db_aggregator:foldl(Events),
    Finalized = reckon_db_aggregator:finalize(Result),
    Expected = #{
        name => <<"John">>,
        age => 3,
        location => <<"LA">>
    },
    ?assertEqual(Expected, Finalized).

finalize_test() ->
    TaggedMap = #{
        count => {sum, 10},
        status => {overwrite, <<"done">>},
        plain => <<"value">>
    },
    Result = reckon_db_aggregator:finalize(TaggedMap),
    Expected = #{
        count => 10,
        status => <<"done">>,
        plain => <<"value">>
    },
    ?assertEqual(Expected, Result).

aggregate_with_records_test() ->
    Events = [
        #event{
            event_id = <<"1">>,
            event_type = <<"user_created">>,
            stream_id = <<"user-123">>,
            version = 0,
            data = #{name => <<"Alice">>, balance => {sum, 0}},
            metadata = #{},
            timestamp = 1000,
            epoch_us = 1000000
        },
        #event{
            event_id = <<"2">>,
            event_type = <<"balance_credited">>,
            stream_id = <<"user-123">>,
            version = 1,
            data = #{balance => {sum, 100}},
            metadata = #{},
            timestamp = 2000,
            epoch_us = 2000000
        },
        #event{
            event_id = <<"3">>,
            event_type = <<"balance_debited">>,
            stream_id = <<"user-123">>,
            version = 2,
            data = #{balance => {sum, -30}},
            metadata = #{},
            timestamp = 3000,
            epoch_us = 3000000
        }
    ],
    Result = reckon_db_aggregator:aggregate(Events, undefined, #{}),
    Expected = #{name => <<"Alice">>, balance => 70},
    ?assertEqual(Expected, Result).

aggregate_with_snapshot_test() ->
    Snapshot = #snapshot{
        stream_id = <<"user-123">>,
        version = 5,
        data = #{name => <<"Alice">>, balance => 500},
        metadata = #{},
        timestamp = 5000
    },
    Events = [
        #{data => #{balance => {sum, 100}}},
        #{data => #{balance => {sum, -50}}}
    ],
    Result = reckon_db_aggregator:aggregate(Events, Snapshot, #{}),
    Expected = #{name => <<"Alice">>, balance => 550},
    ?assertEqual(Expected, Result).

aggregate_without_finalize_test() ->
    Events = [
        #{count => {sum, 5}}
    ],
    Result = reckon_db_aggregator:aggregate(Events, undefined, #{finalize => false}),
    %% Should still have the tagged value
    ?assertEqual(#{count => {sum, 5}}, Result).
