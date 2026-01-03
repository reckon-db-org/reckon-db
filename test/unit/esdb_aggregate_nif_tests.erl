%% @doc Unit tests for esdb_aggregate_nif module.
%%
%% Tests both the pure Erlang implementations (always tested) and the NIF
%% implementations (tested only when available in Enterprise builds).
%%
%% @author Macula.io

-module(esdb_aggregate_nif_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Data
%%====================================================================

simple_events() ->
    [
        #{amount => 100, status => active},
        #{amount => 200, status => inactive},
        #{amount => 150, status => active}
    ].

tagged_events() ->
    [
        #{balance => {sum, 100}, last_update => {overwrite, <<"2025-01-01">>}},
        #{balance => {sum, 50}, last_update => {overwrite, <<"2025-01-02">>}},
        #{balance => {sum, -30}, last_update => {overwrite, <<"2025-01-03">>}}
    ].

data_wrapped_events() ->
    [
        #{data => #{count => {sum, 1}, name => <<"first">>}},
        #{data => #{count => {sum, 1}, name => <<"second">>}},
        #{data => #{count => {sum, 1}, name => <<"third">>}}
    ].

%%====================================================================
%% Pure Erlang Implementation Tests (Always Run)
%%====================================================================

erlang_aggregate_simple_test() ->
    Events = simple_events(),
    Result = esdb_aggregate_nif:erlang_aggregate_events(Events, #{}, true),
    ?assertEqual(150, maps:get(amount, Result)),
    ?assertEqual(active, maps:get(status, Result)).

erlang_aggregate_tagged_sum_test() ->
    Events = tagged_events(),
    Result = esdb_aggregate_nif:erlang_aggregate_events(Events, #{}, true),
    %% 100 + 50 - 30 = 120
    ?assertEqual(120, maps:get(balance, Result)),
    ?assertEqual(<<"2025-01-03">>, maps:get(last_update, Result)).

erlang_aggregate_with_initial_state_test() ->
    Events = [#{balance => {sum, 50}}],
    InitialState = #{balance => {sum, 100}},
    Result = esdb_aggregate_nif:erlang_aggregate_events(Events, InitialState, true),
    ?assertEqual(150, maps:get(balance, Result)).

erlang_aggregate_no_finalize_test() ->
    Events = [#{balance => {sum, 100}}],
    Result = esdb_aggregate_nif:erlang_aggregate_events(Events, #{}, false),
    ?assertEqual({sum, 100}, maps:get(balance, Result)).

erlang_aggregate_data_wrapped_test() ->
    Events = data_wrapped_events(),
    Result = esdb_aggregate_nif:erlang_aggregate_events(Events, #{}, true),
    ?assertEqual(3, maps:get(count, Result)),
    ?assertEqual(<<"third">>, maps:get(name, Result)).

erlang_sum_field_test() ->
    Events = simple_events(),
    Sum = esdb_aggregate_nif:erlang_sum_field(Events, amount),
    ?assertEqual(450, Sum).

erlang_sum_field_tagged_test() ->
    Events = tagged_events(),
    Sum = esdb_aggregate_nif:erlang_sum_field(Events, balance),
    ?assertEqual(120, Sum).

erlang_count_where_test() ->
    Events = simple_events(),
    Count = esdb_aggregate_nif:erlang_count_where(Events, status, active),
    ?assertEqual(2, Count).

erlang_merge_tagged_batch_test() ->
    Pairs = [{a, {sum, 10}}, {b, {sum, 20}}, {c, value}],
    State = #{a => {sum, 5}},
    Result = esdb_aggregate_nif:erlang_merge_tagged_batch(Pairs, State),
    ?assertEqual({sum, 15}, maps:get(a, Result)),
    ?assertEqual({sum, 20}, maps:get(b, Result)),
    ?assertEqual(value, maps:get(c, Result)).

erlang_finalize_test() ->
    Tagged = #{
        sum_val => {sum, 100},
        over_val => {overwrite, <<"hello">>},
        plain => 42
    },
    Result = esdb_aggregate_nif:erlang_finalize(Tagged),
    ?assertEqual(100, maps:get(sum_val, Result)),
    ?assertEqual(<<"hello">>, maps:get(over_val, Result)),
    ?assertEqual(42, maps:get(plain, Result)).

erlang_aggregation_stats_test() ->
    Events = simple_events(),
    Stats = esdb_aggregate_nif:erlang_aggregation_stats(Events),
    ?assertEqual(3, maps:get(total_events, Stats)),
    ?assertEqual(3, maps:get(events_with_data, Stats)),
    ?assertEqual(2, maps:get(unique_fields, Stats)).

erlang_empty_events_test() ->
    Result = esdb_aggregate_nif:erlang_aggregate_events([], #{}, true),
    ?assertEqual(#{}, Result).

erlang_mixed_types_test() ->
    Events = [
        #{count => {sum, 5}, name => <<"a">>},
        #{count => {sum, 3}, value => 100},
        #{count => {sum, 2}, name => <<"b">>}
    ],
    Result = esdb_aggregate_nif:erlang_aggregate_events(Events, #{}, true),
    ?assertEqual(10, maps:get(count, Result)),
    ?assertEqual(<<"b">>, maps:get(name, Result)),
    ?assertEqual(100, maps:get(value, Result)).

%%====================================================================
%% Public API Tests (Uses Dispatch)
%%====================================================================

api_aggregate_events_test() ->
    Events = tagged_events(),
    Result = esdb_aggregate_nif:aggregate_events(Events, #{}, true),
    ?assertEqual(120, maps:get(balance, Result)).

api_sum_field_test() ->
    Events = simple_events(),
    Sum = esdb_aggregate_nif:sum_field(Events, amount),
    %% NIF returns float, Erlang returns integer - compare numerically
    ?assertEqual(450, trunc(Sum)).

api_count_where_test() ->
    Events = simple_events(),
    Count = esdb_aggregate_nif:count_where(Events, status, active),
    ?assertEqual(2, Count).

api_merge_tagged_batch_test() ->
    Pairs = [{x, {sum, 100}}],
    State = #{x => {sum, 50}},
    Result = esdb_aggregate_nif:merge_tagged_batch(Pairs, State),
    ?assert(is_map(Result)).

api_finalize_test() ->
    Tagged = #{val => {sum, 42}},
    Result = esdb_aggregate_nif:finalize(Tagged),
    ?assertEqual(42, maps:get(val, Result)).

api_aggregation_stats_test() ->
    Events = [#{a => 1}, #{b => 2}],
    Stats = esdb_aggregate_nif:aggregation_stats(Events),
    ?assertEqual(2, maps:get(total_events, Stats)).

%%====================================================================
%% Introspection Tests
%%====================================================================

implementation_returns_atom_test() ->
    Impl = esdb_aggregate_nif:implementation(),
    ?assert(Impl =:= nif orelse Impl =:= erlang).

is_nif_loaded_returns_boolean_test() ->
    Loaded = esdb_aggregate_nif:is_nif_loaded(),
    ?assert(is_boolean(Loaded)).

%%====================================================================
%% NIF Implementation Tests (Only When Available)
%%====================================================================

nif_tests_when_available_test_() ->
    case esdb_aggregate_nif:is_nif_loaded() of
        true ->
            [
                {"NIF aggregate_events works", fun nif_aggregate_events_works/0},
                {"NIF sum_field works", fun nif_sum_field_works/0},
                {"NIF count_where works", fun nif_count_where_works/0},
                {"NIF merge_tagged_batch works", fun nif_merge_tagged_batch_works/0},
                {"NIF finalize works", fun nif_finalize_works/0},
                {"NIF aggregation_stats works", fun nif_aggregation_stats_works/0}
            ];
        false ->
            []
    end.

nif_aggregate_events_works() ->
    Events = tagged_events(),
    Result = esdb_aggregate_nif:nif_aggregate_events(Events, #{}, true),
    ?assertEqual(120, maps:get(balance, Result)).

nif_sum_field_works() ->
    Events = simple_events(),
    Sum = esdb_aggregate_nif:nif_sum_field(Events, amount),
    ?assert(Sum >= 449 andalso Sum =< 451). %% Allow small float differences

nif_count_where_works() ->
    Events = simple_events(),
    Count = esdb_aggregate_nif:nif_count_where(Events, status, active),
    ?assertEqual(2, Count).

nif_merge_tagged_batch_works() ->
    Pairs = [{a, {sum, 10}}],
    State = #{a => {sum, 5}},
    Result = esdb_aggregate_nif:nif_merge_tagged_batch(Pairs, State),
    ?assert(is_map(Result)).

nif_finalize_works() ->
    Tagged = #{val => {sum, 42}},
    Result = esdb_aggregate_nif:nif_finalize(Tagged),
    ?assertEqual(42, maps:get(val, Result)).

nif_aggregation_stats_works() ->
    Events = simple_events(),
    Stats = esdb_aggregate_nif:nif_aggregation_stats(Events),
    ?assertEqual(3, maps:get(total_events, Stats)).

%%====================================================================
%% Equivalence Tests (Enterprise builds only)
%%====================================================================

equivalence_tests_when_available_test_() ->
    case esdb_aggregate_nif:is_nif_loaded() of
        true ->
            [
                {"Equivalence: aggregate_events", fun equivalence_aggregate_events/0},
                {"Equivalence: finalize", fun equivalence_finalize/0}
            ];
        false ->
            []
    end.

equivalence_aggregate_events() ->
    Events = tagged_events(),
    ErlResult = esdb_aggregate_nif:erlang_aggregate_events(Events, #{}, true),
    NifResult = esdb_aggregate_nif:nif_aggregate_events(Events, #{}, true),
    ?assertEqual(maps:get(balance, ErlResult), maps:get(balance, NifResult)).

equivalence_finalize() ->
    Tagged = #{a => {sum, 100}, b => {overwrite, <<"test">>}, c => plain},
    ErlResult = esdb_aggregate_nif:erlang_finalize(Tagged),
    NifResult = esdb_aggregate_nif:nif_finalize(Tagged),
    ?assertEqual(maps:get(a, ErlResult), maps:get(a, NifResult)),
    ?assertEqual(maps:get(b, ErlResult), maps:get(b, NifResult)),
    ?assertEqual(maps:get(c, ErlResult), maps:get(c, NifResult)).

%%====================================================================
%% Benchmark Tests (Only When NIF Available)
%%====================================================================

benchmark_test_() ->
    case esdb_aggregate_nif:is_nif_loaded() of
        true ->
            [{"Benchmark aggregation", fun benchmark_aggregation/0}];
        false ->
            []
    end.

benchmark_aggregation() ->
    %% Create a larger dataset for benchmarking
    Events = [#{amount => {sum, I rem 100}, status => active} || I <- lists:seq(1, 1000)],
    Iterations = 100,

    %% Benchmark Erlang path
    {TimeErl, _} = timer:tc(fun() ->
        [esdb_aggregate_nif:erlang_aggregate_events(Events, #{}, true)
         || _ <- lists:seq(1, Iterations)]
    end),

    %% Benchmark NIF path
    {TimeNif, _} = timer:tc(fun() ->
        [esdb_aggregate_nif:nif_aggregate_events(Events, #{}, true)
         || _ <- lists:seq(1, Iterations)]
    end),

    %% Log results
    logger:info("Aggregate benchmark (~p events, ~p iterations): Erlang=~pus, NIF=~pus, Speedup=~.2fx",
               [length(Events), Iterations, TimeErl, TimeNif, TimeErl / max(TimeNif, 1)]),

    %% NIF should be competitive
    ?assert(TimeNif =< TimeErl * 3).
