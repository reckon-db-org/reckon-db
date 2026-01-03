%% @doc Unit tests for backpressure module
%% @author R. Lefever

-module(reckon_db_backpressure_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../../include/reckon_db.hrl").

%%====================================================================
%% Queue Creation Tests
%%====================================================================

new_default_opts_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{}),
    Info = reckon_db_backpressure:info(Queue),
    ?assertEqual(0, maps:get(size, Info)),
    ?assertEqual(1000, maps:get(max_size, Info)),
    ?assertEqual(drop_oldest, maps:get(strategy, Info)),
    ?assertEqual(push, maps:get(mode, Info)).

new_custom_opts_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{
        max_queue => 500,
        strategy => drop_newest,
        mode => pull,
        warning_threshold => 400
    }),
    Info = reckon_db_backpressure:info(Queue),
    ?assertEqual(500, maps:get(max_size, Info)),
    ?assertEqual(drop_newest, maps:get(strategy, Info)),
    ?assertEqual(pull, maps:get(mode, Info)),
    ?assertEqual(400, maps:get(warning_threshold, Info)).

%%====================================================================
%% Enqueue/Dequeue Tests
%%====================================================================

enqueue_single_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{}),
    Event = mock_event(1),
    {ok, Queue2} = reckon_db_backpressure:enqueue(Queue, Event),
    ?assertEqual(1, reckon_db_backpressure:size(Queue2)).

enqueue_many_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{}),
    Events = [mock_event(N) || N <- lists:seq(1, 5)],
    {ok, Queue2} = reckon_db_backpressure:enqueue_many(Queue, Events),
    ?assertEqual(5, reckon_db_backpressure:size(Queue2)).

dequeue_partial_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{}),
    Events = [mock_event(N) || N <- lists:seq(1, 10)],
    {ok, Queue2} = reckon_db_backpressure:enqueue_many(Queue, Events),

    {ok, Dequeued, Queue3} = reckon_db_backpressure:dequeue(Queue2, 3),
    ?assertEqual(3, length(Dequeued)),
    ?assertEqual(7, reckon_db_backpressure:size(Queue3)).

dequeue_more_than_available_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{}),
    Events = [mock_event(N) || N <- lists:seq(1, 3)],
    {ok, Queue2} = reckon_db_backpressure:enqueue_many(Queue, Events),

    {ok, Dequeued, Queue3} = reckon_db_backpressure:dequeue(Queue2, 10),
    ?assertEqual(3, length(Dequeued)),
    ?assertEqual(0, reckon_db_backpressure:size(Queue3)).

dequeue_all_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{}),
    Events = [mock_event(N) || N <- lists:seq(1, 5)],
    {ok, Queue2} = reckon_db_backpressure:enqueue_many(Queue, Events),

    {ok, Dequeued, Queue3} = reckon_db_backpressure:dequeue_all(Queue2),
    ?assertEqual(5, length(Dequeued)),
    ?assertEqual(0, reckon_db_backpressure:size(Queue3)).

dequeue_fifo_order_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{}),
    Events = [mock_event(N) || N <- lists:seq(1, 3)],
    {ok, Queue2} = reckon_db_backpressure:enqueue_many(Queue, Events),

    {ok, Dequeued, _Queue3} = reckon_db_backpressure:dequeue_all(Queue2),
    Versions = [E#event.version || E <- Dequeued],
    ?assertEqual([1, 2, 3], Versions).

%%====================================================================
%% Overflow Strategy Tests
%%====================================================================

drop_oldest_strategy_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{max_queue => 3, strategy => drop_oldest}),
    Events = [mock_event(N) || N <- lists:seq(1, 3)],
    {ok, Queue2} = reckon_db_backpressure:enqueue_many(Queue, Events),

    %% Queue is full, add one more
    NewEvent = mock_event(4),
    {ok, Queue3} = reckon_db_backpressure:enqueue(Queue2, NewEvent),

    %% Size should still be 3, oldest should be dropped
    ?assertEqual(3, reckon_db_backpressure:size(Queue3)),

    {ok, Dequeued, _} = reckon_db_backpressure:dequeue_all(Queue3),
    Versions = [E#event.version || E <- Dequeued],
    ?assertEqual([2, 3, 4], Versions).  %% Version 1 was dropped

drop_newest_strategy_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{max_queue => 3, strategy => drop_newest}),
    Events = [mock_event(N) || N <- lists:seq(1, 3)],
    {ok, Queue2} = reckon_db_backpressure:enqueue_many(Queue, Events),

    %% Queue is full, add one more
    NewEvent = mock_event(4),
    {ok, Queue3} = reckon_db_backpressure:enqueue(Queue2, NewEvent),

    %% Size should still be 3, newest should be dropped
    ?assertEqual(3, reckon_db_backpressure:size(Queue3)),

    {ok, Dequeued, _} = reckon_db_backpressure:dequeue_all(Queue3),
    Versions = [E#event.version || E <- Dequeued],
    ?assertEqual([1, 2, 3], Versions).  %% Version 4 was dropped

block_strategy_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{max_queue => 3, strategy => block}),
    Events = [mock_event(N) || N <- lists:seq(1, 3)],
    {ok, Queue2} = reckon_db_backpressure:enqueue_many(Queue, Events),

    %% Queue is full, add one more
    NewEvent = mock_event(4),
    Result = reckon_db_backpressure:enqueue(Queue2, NewEvent),

    ?assertEqual({error, blocked}, Result).

error_strategy_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{max_queue => 3, strategy => error}),
    Events = [mock_event(N) || N <- lists:seq(1, 3)],
    {ok, Queue2} = reckon_db_backpressure:enqueue_many(Queue, Events),

    %% Queue is full, add one more
    NewEvent = mock_event(4),
    Result = reckon_db_backpressure:enqueue(Queue2, NewEvent),

    ?assertEqual({error, queue_full}, Result).

%%====================================================================
%% State Tests
%%====================================================================

is_empty_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{}),
    ?assert(reckon_db_backpressure:is_empty(Queue)),

    {ok, Queue2} = reckon_db_backpressure:enqueue(Queue, mock_event(1)),
    ?assertNot(reckon_db_backpressure:is_empty(Queue2)).

is_full_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{max_queue => 2}),
    ?assertNot(reckon_db_backpressure:is_full(Queue)),

    {ok, Queue2} = reckon_db_backpressure:enqueue(Queue, mock_event(1)),
    ?assertNot(reckon_db_backpressure:is_full(Queue2)),

    {ok, Queue3} = reckon_db_backpressure:enqueue(Queue2, mock_event(2)),
    ?assert(reckon_db_backpressure:is_full(Queue3)).

info_utilization_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{max_queue => 4}),
    Events = [mock_event(N) || N <- lists:seq(1, 2)],
    {ok, Queue2} = reckon_db_backpressure:enqueue_many(Queue, Events),

    Info = reckon_db_backpressure:info(Queue2),
    ?assertEqual(0.5, maps:get(utilization, Info)).

dropped_count_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{max_queue => 2, strategy => drop_oldest}),
    Events = [mock_event(N) || N <- lists:seq(1, 5)],
    {ok, Queue2} = reckon_db_backpressure:enqueue_many(Queue, Events),

    Info = reckon_db_backpressure:info(Queue2),
    ?assertEqual(3, maps:get(dropped, Info)).  %% 5 events, capacity 2 = 3 dropped

%%====================================================================
%% Demand Tests (Pull Mode)
%%====================================================================

pull_mode_initial_demand_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{mode => pull}),
    ?assertEqual(0, reckon_db_backpressure:get_demand(Queue)).

push_mode_demand_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{mode => push}),
    ?assertEqual(infinity, reckon_db_backpressure:get_demand(Queue)).

set_demand_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{mode => pull}),
    Queue2 = reckon_db_backpressure:set_demand(Queue, 10),
    ?assertEqual(10, reckon_db_backpressure:get_demand(Queue2)).

add_demand_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{mode => pull}),
    Queue2 = reckon_db_backpressure:set_demand(Queue, 5),
    Queue3 = reckon_db_backpressure:add_demand(Queue2, 3),
    ?assertEqual(8, reckon_db_backpressure:get_demand(Queue3)).

add_demand_infinity_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{mode => push}),
    Queue2 = reckon_db_backpressure:add_demand(Queue, 10),
    %% Should still be infinity
    ?assertEqual(infinity, reckon_db_backpressure:get_demand(Queue2)).

dequeue_reduces_demand_test() ->
    {ok, Queue} = reckon_db_backpressure:new(#{mode => pull}),
    Events = [mock_event(N) || N <- lists:seq(1, 5)],
    {ok, Queue2} = reckon_db_backpressure:enqueue_many(Queue, Events),
    Queue3 = reckon_db_backpressure:set_demand(Queue2, 10),

    {ok, _, Queue4} = reckon_db_backpressure:dequeue(Queue3, 3),
    ?assertEqual(7, reckon_db_backpressure:get_demand(Queue4)).

%%====================================================================
%% Helper Functions
%%====================================================================

mock_event(Version) ->
    #event{
        event_id = iolist_to_binary(io_lib:format("evt-~p", [Version])),
        event_type = <<"TestEvent">>,
        stream_id = <<"test-stream">>,
        version = Version,
        data = #{version => Version},
        metadata = #{},
        timestamp = Version * 1000,
        epoch_us = Version * 1000000
    }.
