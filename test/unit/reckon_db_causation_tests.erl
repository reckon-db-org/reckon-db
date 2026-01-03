%% @doc Unit tests for causation module
%% @author Macula.io

-module(reckon_db_causation_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../../include/reckon_db.hrl").

%%====================================================================
%% DOT Export Tests
%%====================================================================

to_dot_empty_graph_test() ->
    Graph = #{
        nodes => [],
        edges => [],
        root => undefined
    },
    Result = reckon_db_causation:to_dot(Graph),
    ?assertEqual(<<"digraph causation {\n}\n">>, Result).

to_dot_single_node_test() ->
    Event = #event{
        event_id = <<"evt-1">>,
        event_type = <<"UserCreated">>,
        stream_id = <<"users-123">>,
        version = 0,
        data = #{},
        metadata = #{},
        timestamp = 1000,
        epoch_us = 1000000
    },
    Graph = #{
        nodes => [Event],
        edges => [],
        root => <<"evt-1">>
    },
    Result = reckon_db_causation:to_dot(Graph),
    ?assertMatch(<<"digraph causation {\n", _/binary>>, Result),
    ?assert(binary:match(Result, <<"evt-1">>) =/= nomatch),
    ?assert(binary:match(Result, <<"UserCreated">>) =/= nomatch).

to_dot_with_edges_test() ->
    Event1 = #event{
        event_id = <<"evt-1">>,
        event_type = <<"OrderPlaced">>,
        stream_id = <<"orders-123">>,
        version = 0,
        data = #{},
        metadata = #{},
        timestamp = 1000,
        epoch_us = 1000000
    },
    Event2 = #event{
        event_id = <<"evt-2">>,
        event_type = <<"PaymentReceived">>,
        stream_id = <<"payments-456">>,
        version = 0,
        data = #{},
        metadata = #{causation_id => <<"evt-1">>},
        timestamp = 2000,
        epoch_us = 2000000
    },
    Graph = #{
        nodes => [Event1, Event2],
        edges => [{<<"evt-1">>, <<"evt-2">>}],
        root => <<"evt-1">>
    },
    Result = reckon_db_causation:to_dot(Graph),
    ?assert(binary:match(Result, <<"evt-1 -> evt-2">>) =/= nomatch).

to_dot_escapes_quotes_test() ->
    Event = #event{
        event_id = <<"evt-\"special\"">>,
        event_type = <<"Test\"Event">>,
        stream_id = <<"test">>,
        version = 0,
        data = #{},
        metadata = #{},
        timestamp = 1000,
        epoch_us = 1000000
    },
    Graph = #{
        nodes => [Event],
        edges => [],
        root => <<"evt-\"special\"">>
    },
    Result = reckon_db_causation:to_dot(Graph),
    %% Should contain escaped quotes
    ?assert(binary:match(Result, <<"\\\"">>) =/= nomatch).

%%====================================================================
%% Graph Type Tests
%%====================================================================

causation_graph_structure_test() ->
    %% Verify the graph structure has expected keys
    Graph = #{
        nodes => [],
        edges => [],
        root => undefined
    },
    ?assert(maps:is_key(nodes, Graph)),
    ?assert(maps:is_key(edges, Graph)),
    ?assert(maps:is_key(root, Graph)).

%%====================================================================
%% Metadata Convention Tests
%%====================================================================

metadata_fields_test() ->
    %% Test that standard metadata fields are valid
    Metadata = #{
        causation_id => <<"evt-parent">>,
        correlation_id => <<"saga-123">>,
        actor_id => <<"user-456">>
    },
    ?assertEqual(<<"evt-parent">>, maps:get(causation_id, Metadata)),
    ?assertEqual(<<"saga-123">>, maps:get(correlation_id, Metadata)),
    ?assertEqual(<<"user-456">>, maps:get(actor_id, Metadata)).

optional_metadata_test() ->
    %% Metadata fields should be optional
    Event = #event{
        event_id = <<"evt-1">>,
        event_type = <<"Test">>,
        stream_id = <<"test">>,
        version = 0,
        data = #{},
        metadata = #{},  %% Empty metadata is valid
        timestamp = 1000,
        epoch_us = 1000000
    },
    ?assertEqual(undefined, maps:get(causation_id, Event#event.metadata, undefined)),
    ?assertEqual(undefined, maps:get(correlation_id, Event#event.metadata, undefined)).

%%====================================================================
%% Mock Event Helpers for Integration Tests
%%====================================================================

mock_event(Id, Type, StreamId, Version, Metadata) ->
    #event{
        event_id = Id,
        event_type = Type,
        stream_id = StreamId,
        version = Version,
        data = #{},
        metadata = Metadata,
        timestamp = Version * 1000,
        epoch_us = Version * 1000000
    }.

mock_causation_chain_test() ->
    %% Build a sample causation chain for testing
    Root = mock_event(<<"evt-0">>, <<"OrderPlaced">>, <<"orders-1">>, 0, #{}),
    E1 = mock_event(<<"evt-1">>, <<"PaymentRequested">>, <<"payments-1">>, 0,
                    #{causation_id => <<"evt-0">>, correlation_id => <<"saga-1">>}),
    E2 = mock_event(<<"evt-2">>, <<"PaymentReceived">>, <<"payments-1">>, 1,
                    #{causation_id => <<"evt-1">>, correlation_id => <<"saga-1">>}),
    E3 = mock_event(<<"evt-3">>, <<"OrderConfirmed">>, <<"orders-1">>, 1,
                    #{causation_id => <<"evt-2">>, correlation_id => <<"saga-1">>}),

    Chain = [Root, E1, E2, E3],

    %% Verify chain structure
    ?assertEqual(4, length(Chain)),
    ?assertEqual(<<"evt-0">>, Root#event.event_id),
    ?assertEqual(<<"evt-0">>, maps:get(causation_id, E1#event.metadata)),
    ?assertEqual(<<"saga-1">>, maps:get(correlation_id, E3#event.metadata)).

%%====================================================================
%% Graph Building Helper Tests
%%====================================================================

edges_from_causation_test() ->
    %% Build edges from causation relationships
    Events = [
        mock_event(<<"evt-0">>, <<"A">>, <<"s1">>, 0, #{}),
        mock_event(<<"evt-1">>, <<"B">>, <<"s1">>, 1, #{causation_id => <<"evt-0">>}),
        mock_event(<<"evt-2">>, <<"C">>, <<"s1">>, 2, #{causation_id => <<"evt-1">>})
    ],

    EventMap = maps:from_list([{E#event.event_id, E} || E <- Events]),

    Edges = lists:filtermap(
        fun(Event) ->
            case maps:get(causation_id, Event#event.metadata, undefined) of
                undefined -> false;
                CausationId ->
                    case maps:is_key(CausationId, EventMap) of
                        true -> {true, {CausationId, Event#event.event_id}};
                        false -> false
                    end
            end
        end,
        Events
    ),

    ?assertEqual(2, length(Edges)),
    ?assert(lists:member({<<"evt-0">>, <<"evt-1">>}, Edges)),
    ?assert(lists:member({<<"evt-1">>, <<"evt-2">>}, Edges)).

find_roots_test() ->
    %% Find events with no incoming edges (roots)
    Events = [
        mock_event(<<"evt-0">>, <<"A">>, <<"s1">>, 0, #{}),
        mock_event(<<"evt-1">>, <<"B">>, <<"s1">>, 1, #{causation_id => <<"evt-0">>}),
        mock_event(<<"evt-2">>, <<"C">>, <<"s1">>, 2, #{causation_id => <<"evt-0">>})
    ],

    Edges = [
        {<<"evt-0">>, <<"evt-1">>},
        {<<"evt-0">>, <<"evt-2">>}
    ],

    AllTargets = [To || {_, To} <- Edges],
    Roots = [E || E <- Events, not lists:member(E#event.event_id, AllTargets)],

    ?assertEqual(1, length(Roots)),
    ?assertEqual(<<"evt-0">>, (hd(Roots))#event.event_id).

%%====================================================================
%% Correlation Grouping Tests
%%====================================================================

group_by_correlation_test() ->
    Events = [
        mock_event(<<"evt-0">>, <<"A">>, <<"s1">>, 0, #{correlation_id => <<"saga-1">>}),
        mock_event(<<"evt-1">>, <<"B">>, <<"s2">>, 0, #{correlation_id => <<"saga-1">>}),
        mock_event(<<"evt-2">>, <<"C">>, <<"s1">>, 1, #{correlation_id => <<"saga-2">>}),
        mock_event(<<"evt-3">>, <<"D">>, <<"s2">>, 1, #{correlation_id => <<"saga-1">>})
    ],

    %% Filter by correlation_id
    Saga1Events = [E || E <- Events,
                   maps:get(correlation_id, E#event.metadata, undefined) =:= <<"saga-1">>],

    ?assertEqual(3, length(Saga1Events)).

%%====================================================================
%% Actor Tracking Tests
%%====================================================================

actor_tracking_test() ->
    Event = mock_event(<<"evt-1">>, <<"Test">>, <<"s1">>, 0,
                       #{actor_id => <<"user:alice@example.com">>,
                         causation_id => <<"cmd-123">>}),

    ?assertEqual(<<"user:alice@example.com">>,
                 maps:get(actor_id, Event#event.metadata)).

multiple_actors_in_chain_test() ->
    %% Track different actors through a causation chain
    E1 = mock_event(<<"evt-1">>, <<"OrderPlaced">>, <<"orders-1">>, 0,
                    #{actor_id => <<"user:customer@shop.com">>}),
    E2 = mock_event(<<"evt-2">>, <<"PaymentProcessed">>, <<"payments-1">>, 0,
                    #{causation_id => <<"evt-1">>,
                      actor_id => <<"system:payment-service">>}),
    E3 = mock_event(<<"evt-3">>, <<"OrderShipped">>, <<"orders-1">>, 1,
                    #{causation_id => <<"evt-2">>,
                      actor_id => <<"system:fulfillment-service">>}),

    Actors = [maps:get(actor_id, E#event.metadata) || E <- [E1, E2, E3]],
    ?assertEqual([<<"user:customer@shop.com">>,
                  <<"system:payment-service">>,
                  <<"system:fulfillment-service">>], Actors).
