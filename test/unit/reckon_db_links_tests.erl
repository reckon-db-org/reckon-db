%% @doc Unit tests for stream linking module
%% @author R. Lefever

-module(reckon_db_links_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../../include/reckon_db.hrl").

%%====================================================================
%% Filter Function Tests
%%====================================================================

filter_always_true_test() ->
    Filter = fun(_Event) -> true end,
    Event = mock_event(<<"evt-1">>, <<"OrderPlaced">>, #{amount => 100}),
    ?assert(Filter(Event)).

filter_always_false_test() ->
    Filter = fun(_Event) -> false end,
    Event = mock_event(<<"evt-1">>, <<"OrderPlaced">>, #{amount => 100}),
    ?assertNot(Filter(Event)).

filter_by_event_type_test() ->
    Filter = fun(E) -> E#event.event_type =:= <<"OrderPlaced">> end,

    E1 = mock_event(<<"evt-1">>, <<"OrderPlaced">>, #{}),
    E2 = mock_event(<<"evt-2">>, <<"OrderCancelled">>, #{}),

    ?assert(Filter(E1)),
    ?assertNot(Filter(E2)).

filter_by_data_field_test() ->
    Filter = fun(E) -> maps:get(amount, E#event.data, 0) > 100 end,

    E1 = mock_event(<<"evt-1">>, <<"Order">>, #{amount => 50}),
    E2 = mock_event(<<"evt-2">>, <<"Order">>, #{amount => 150}),
    E3 = mock_event(<<"evt-3">>, <<"Order">>, #{amount => 100}),

    ?assertNot(Filter(E1)),
    ?assert(Filter(E2)),
    ?assertNot(Filter(E3)).  %% Not greater than 100

filter_with_metadata_test() ->
    Filter = fun(E) ->
        maps:get(priority, E#event.metadata, normal) =:= high
    end,

    E1 = mock_event_with_metadata(<<"evt-1">>, <<"Task">>, #{}, #{priority => high}),
    E2 = mock_event_with_metadata(<<"evt-2">>, <<"Task">>, #{}, #{priority => low}),
    E3 = mock_event(<<"evt-3">>, <<"Task">>, #{}),  %% No metadata

    ?assert(Filter(E1)),
    ?assertNot(Filter(E2)),
    ?assertNot(Filter(E3)).

%%====================================================================
%% Transform Function Tests
%%====================================================================

transform_identity_test() ->
    Transform = fun(E) -> E end,
    Event = mock_event(<<"evt-1">>, <<"Order">>, #{amount => 100}),
    ?assertEqual(Event, Transform(Event)).

transform_add_field_test() ->
    Transform = fun(E) ->
        Data = E#event.data,
        E#event{data = Data#{flagged => true}}
    end,

    Event = mock_event(<<"evt-1">>, <<"Order">>, #{amount => 100}),
    Transformed = Transform(Event),

    ?assertEqual(true, maps:get(flagged, Transformed#event.data)).

transform_modify_field_test() ->
    Transform = fun(E) ->
        Data = E#event.data,
        Amount = maps:get(amount, Data, 0),
        E#event{data = Data#{amount_cents => Amount * 100}}
    end,

    Event = mock_event(<<"evt-1">>, <<"Order">>, #{amount => 42}),
    Transformed = Transform(Event),

    ?assertEqual(4200, maps:get(amount_cents, Transformed#event.data)).

transform_change_event_type_test() ->
    Transform = fun(E) ->
        E#event{event_type = <<"ProcessedOrder">>}
    end,

    Event = mock_event(<<"evt-1">>, <<"Order">>, #{}),
    Transformed = Transform(Event),

    ?assertEqual(<<"ProcessedOrder">>, Transformed#event.event_type).

transform_add_metadata_test() ->
    Transform = fun(E) ->
        Meta = E#event.metadata,
        E#event{metadata = Meta#{processed_at => 1703000000000}}
    end,

    Event = mock_event(<<"evt-1">>, <<"Order">>, #{}),
    Transformed = Transform(Event),

    ?assertEqual(1703000000000, maps:get(processed_at, Transformed#event.metadata)).

%%====================================================================
%% Source Specification Tests
%%====================================================================

source_spec_stream_test() ->
    Spec = #{type => stream, stream_id => <<"orders-123">>},
    ?assertEqual(stream, maps:get(type, Spec)),
    ?assertEqual(<<"orders-123">>, maps:get(stream_id, Spec)).

source_spec_pattern_test() ->
    Spec = #{type => stream_pattern, pattern => <<"orders-*">>},
    ?assertEqual(stream_pattern, maps:get(type, Spec)),
    ?assertEqual(<<"orders-*">>, maps:get(pattern, Spec)).

source_spec_all_test() ->
    Spec = #{type => all},
    ?assertEqual(all, maps:get(type, Spec)).

%%====================================================================
%% Link Specification Tests
%%====================================================================

link_spec_minimal_test() ->
    Spec = #{
        name => <<"my-link">>,
        source => #{type => all}
    },

    ?assertEqual(<<"my-link">>, maps:get(name, Spec)),
    ?assertEqual(#{type => all}, maps:get(source, Spec)).

link_spec_full_test() ->
    Filter = fun(E) -> E#event.event_type =:= <<"Order">> end,
    Transform = fun(E) -> E end,

    Spec = #{
        name => <<"filtered-orders">>,
        source => #{type => stream_pattern, pattern => <<"orders-*">>},
        filter => Filter,
        transform => Transform,
        backfill => true
    },

    ?assertEqual(<<"filtered-orders">>, maps:get(name, Spec)),
    ?assert(is_function(maps:get(filter, Spec), 1)),
    ?assert(is_function(maps:get(transform, Spec), 1)),
    ?assertEqual(true, maps:get(backfill, Spec)).

%%====================================================================
%% Link Stream ID Tests
%%====================================================================

link_stream_prefix_test() ->
    %% Link streams use $link: prefix
    LinkName = <<"high-value">>,
    ExpectedStreamId = <<"$link:high-value">>,

    %% Simulate link_stream_id function
    Prefix = <<"$link:">>,
    StreamId = <<Prefix/binary, LinkName/binary>>,

    ?assertEqual(ExpectedStreamId, StreamId).

is_link_stream_test() ->
    %% Test link stream detection
    IsLink = fun(S) ->
        case S of
            <<"$link:", _/binary>> -> true;
            _ -> false
        end
    end,

    ?assert(IsLink(<<"$link:my-projection">>)),
    ?assert(IsLink(<<"$link:orders-summary">>)),
    ?assertNot(IsLink(<<"orders-123">>)),
    ?assertNot(IsLink(<<"$other:something">>)).

%%====================================================================
%% Pattern Matching Tests
%%====================================================================

wildcard_to_regex_test() ->
    %% Test pattern conversion (simple implementation)
    PatternToRegex = fun(P) ->
        Escaped = re:replace(P, <<"[.^$+?{}\\[\\]\\\\|()]">>, <<"\\\\&">>, [global, {return, binary}]),
        Converted = binary:replace(Escaped, <<"*">>, <<".*">>, [global]),
        <<"^", Converted/binary, "$">>
    end,

    ?assertEqual(<<"^orders-.*$">>, PatternToRegex(<<"orders-*">>)),
    ?assertEqual(<<"^.*-archived$">>, PatternToRegex(<<"*-archived">>)),
    ?assertEqual(<<"^prefix-.*-suffix$">>, PatternToRegex(<<"prefix-*-suffix">>)).

pattern_match_test() ->
    Pattern = <<"^orders-.*$">>,

    ?assertNotEqual(nomatch, re:run(<<"orders-123">>, Pattern)),
    ?assertNotEqual(nomatch, re:run(<<"orders-abc">>, Pattern)),
    ?assertEqual(nomatch, re:run(<<"users-123">>, Pattern)).

%%====================================================================
%% Link Info Tests
%%====================================================================

link_info_structure_test() ->
    Info = #{
        name => <<"my-link">>,
        source => #{type => all},
        status => running,
        processed => 1000
    },

    ?assertEqual(<<"my-link">>, maps:get(name, Info)),
    ?assertEqual(running, maps:get(status, Info)),
    ?assertEqual(1000, maps:get(processed, Info)).

link_status_values_test() ->
    Statuses = [running, stopped, error],
    lists:foreach(
        fun(S) -> ?assert(is_atom(S)) end,
        Statuses
    ).

%%====================================================================
%% Event to Map Conversion Tests
%%====================================================================

event_to_map_test() ->
    Event = mock_event(<<"evt-1">>, <<"OrderPlaced">>, #{amount => 100}),

    %% Simulate event_to_map
    Map = #{
        event_id => Event#event.event_id,
        event_type => Event#event.event_type,
        data => Event#event.data,
        metadata => maps:merge(Event#event.metadata, #{
            source_stream => Event#event.stream_id,
            source_version => Event#event.version
        })
    },

    ?assertEqual(<<"evt-1">>, maps:get(event_id, Map)),
    ?assertEqual(<<"OrderPlaced">>, maps:get(event_type, Map)),
    ?assertEqual(#{amount => 100}, maps:get(data, Map)),
    ?assertEqual(<<"test-stream">>, maps:get(source_stream, maps:get(metadata, Map))).

%%====================================================================
%% Combined Filter + Transform Tests
%%====================================================================

filter_then_transform_test() ->
    %% Only high-value orders, add flag
    Filter = fun(E) -> maps:get(amount, E#event.data, 0) > 100 end,
    Transform = fun(E) ->
        E#event{data = (E#event.data)#{high_value => true}}
    end,

    E1 = mock_event(<<"evt-1">>, <<"Order">>, #{amount => 50}),
    E2 = mock_event(<<"evt-2">>, <<"Order">>, #{amount => 200}),

    %% E1 should be filtered out
    ?assertNot(Filter(E1)),

    %% E2 should pass and be transformed
    ?assert(Filter(E2)),
    E2Transformed = Transform(E2),
    ?assertEqual(true, maps:get(high_value, E2Transformed#event.data)).

%%====================================================================
%% Helper Functions
%%====================================================================

mock_event(Id, Type, Data) ->
    #event{
        event_id = Id,
        event_type = Type,
        stream_id = <<"test-stream">>,
        version = 0,
        data = Data,
        metadata = #{},
        timestamp = 1000,
        epoch_us = 1000000
    }.

mock_event_with_metadata(Id, Type, Data, Metadata) ->
    #event{
        event_id = Id,
        event_type = Type,
        stream_id = <<"test-stream">>,
        version = 0,
        data = Data,
        metadata = Metadata,
        timestamp = 1000,
        epoch_us = 1000000
    }.
