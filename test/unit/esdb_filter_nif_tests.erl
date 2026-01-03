%% @doc Unit tests for esdb_filter_nif module.
%%
%% Tests both the pure Erlang implementations (always tested) and the NIF
%% implementations (tested only when available in Enterprise builds).
%%
%% @author R. Lefever

-module(esdb_filter_nif_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Data
%%====================================================================

stream_ids() ->
    [
        <<"orders-123">>,
        <<"orders-456">>,
        <<"orders-789">>,
        <<"users-100">>,
        <<"users-200">>,
        <<"products-abc">>,
        <<"inventory-xyz">>
    ].

%%====================================================================
%% Pure Erlang Implementation Tests (Always Run)
%%====================================================================

erlang_wildcard_to_regex_simple_test() ->
    Pattern = esdb_filter_nif:erlang_wildcard_to_regex(<<"orders-*">>),
    ?assertEqual(<<"^orders-.*$">>, Pattern).

erlang_wildcard_to_regex_question_test() ->
    Pattern = esdb_filter_nif:erlang_wildcard_to_regex(<<"user-?">>),
    ?assertEqual(<<"^user-.$">>, Pattern).

erlang_wildcard_to_regex_complex_test() ->
    Pattern = esdb_filter_nif:erlang_wildcard_to_regex(<<"order*.log">>),
    %% The dot should be escaped
    ?assert(binary:match(Pattern, <<"\\.">>) =/= nomatch).

erlang_wildcard_match_star_test() ->
    ?assert(esdb_filter_nif:erlang_wildcard_match(<<"orders-123">>, <<"orders-*">>)),
    ?assert(esdb_filter_nif:erlang_wildcard_match(<<"orders-456">>, <<"orders-*">>)),
    ?assertNot(esdb_filter_nif:erlang_wildcard_match(<<"users-123">>, <<"orders-*">>)).

erlang_wildcard_match_question_test() ->
    ?assert(esdb_filter_nif:erlang_wildcard_match(<<"a1">>, <<"a?">>)),
    ?assert(esdb_filter_nif:erlang_wildcard_match(<<"ab">>, <<"a?">>)),
    ?assertNot(esdb_filter_nif:erlang_wildcard_match(<<"abc">>, <<"a?">>)).

erlang_wildcard_match_combined_test() ->
    ?assert(esdb_filter_nif:erlang_wildcard_match(<<"user-abc-123">>, <<"user-*-???">>)),
    ?assertNot(esdb_filter_nif:erlang_wildcard_match(<<"user-abc-12">>, <<"user-*-???">>)).

erlang_regex_match_test() ->
    ?assert(esdb_filter_nif:erlang_regex_match(<<"hello">>, <<"^hel.*">>)),
    ?assertNot(esdb_filter_nif:erlang_regex_match(<<"world">>, <<"^hel.*">>)).

erlang_has_prefix_test() ->
    ?assert(esdb_filter_nif:erlang_has_prefix(<<"orders-123">>, <<"orders">>)),
    ?assert(esdb_filter_nif:erlang_has_prefix(<<"orders-123">>, <<"orders-">>)),
    ?assertNot(esdb_filter_nif:erlang_has_prefix(<<"orders-123">>, <<"users">>)),
    ?assertNot(esdb_filter_nif:erlang_has_prefix(<<"abc">>, <<"abcd">>)).

erlang_has_suffix_test() ->
    ?assert(esdb_filter_nif:erlang_has_suffix(<<"orders-123">>, <<"123">>)),
    ?assert(esdb_filter_nif:erlang_has_suffix(<<"orders-123">>, <<"-123">>)),
    ?assertNot(esdb_filter_nif:erlang_has_suffix(<<"orders-123">>, <<"456">>)),
    ?assertNot(esdb_filter_nif:erlang_has_suffix(<<"abc">>, <<"abcd">>)).

erlang_filter_by_wildcard_test() ->
    Streams = stream_ids(),
    Matching = esdb_filter_nif:erlang_filter_by_wildcard(Streams, <<"orders-*">>),
    ?assertEqual(3, length(Matching)),
    ?assert(lists:member(<<"orders-123">>, Matching)),
    ?assert(lists:member(<<"orders-456">>, Matching)),
    ?assert(lists:member(<<"orders-789">>, Matching)).

erlang_filter_by_prefix_test() ->
    Streams = stream_ids(),
    Matching = esdb_filter_nif:erlang_filter_by_prefix(Streams, <<"users-">>),
    ?assertEqual(2, length(Matching)),
    ?assert(lists:member(<<"users-100">>, Matching)),
    ?assert(lists:member(<<"users-200">>, Matching)).

erlang_filter_by_suffix_test() ->
    Streams = stream_ids(),
    Matching = esdb_filter_nif:erlang_filter_by_suffix(Streams, <<"xyz">>),
    ?assertEqual(1, length(Matching)),
    ?assertEqual([<<"inventory-xyz">>], Matching).

erlang_match_indices_test() ->
    Streams = stream_ids(),
    Indices = esdb_filter_nif:erlang_match_indices(Streams, <<"users-*">>),
    ?assertEqual([3, 4], Indices).

erlang_count_matches_test() ->
    Streams = stream_ids(),
    Count = esdb_filter_nif:erlang_count_matches(Streams, <<"orders-*">>),
    ?assertEqual(3, Count).  %% orders-123, orders-456, orders-789

erlang_is_valid_regex_test() ->
    ?assert(esdb_filter_nif:erlang_is_valid_regex(<<"^hello.*$">>)),
    ?assert(esdb_filter_nif:erlang_is_valid_regex(<<"[a-z]+">>)),
    ?assertNot(esdb_filter_nif:erlang_is_valid_regex(<<"[unclosed">>)).

%%====================================================================
%% Public API Tests (Uses Dispatch)
%%====================================================================

api_wildcard_to_regex_test() ->
    Pattern = esdb_filter_nif:wildcard_to_regex(<<"test-*">>),
    ?assert(is_binary(Pattern)),
    ?assert(binary:match(Pattern, <<".*">>) =/= nomatch).

api_wildcard_match_test() ->
    ?assert(esdb_filter_nif:wildcard_match(<<"stream-123">>, <<"stream-*">>)),
    ?assertNot(esdb_filter_nif:wildcard_match(<<"other-123">>, <<"stream-*">>)).

api_regex_match_test() ->
    ?assert(esdb_filter_nif:regex_match(<<"hello123">>, <<"^hello\\d+$">>)),
    ?assertNot(esdb_filter_nif:regex_match(<<"hello">>, <<"^hello\\d+$">>)).

api_has_prefix_test() ->
    ?assert(esdb_filter_nif:has_prefix(<<"prefix-value">>, <<"prefix">>)),
    ?assertNot(esdb_filter_nif:has_prefix(<<"value">>, <<"prefix">>)).

api_has_suffix_test() ->
    ?assert(esdb_filter_nif:has_suffix(<<"value-suffix">>, <<"suffix">>)),
    ?assertNot(esdb_filter_nif:has_suffix(<<"value">>, <<"suffix">>)).

api_filter_by_wildcard_test() ->
    Items = [<<"a-1">>, <<"a-2">>, <<"b-1">>],
    Matching = esdb_filter_nif:filter_by_wildcard(Items, <<"a-*">>),
    ?assertEqual(2, length(Matching)).

api_filter_by_prefix_test() ->
    Items = [<<"prefix-a">>, <<"prefix-b">>, <<"other">>],
    Matching = esdb_filter_nif:filter_by_prefix(Items, <<"prefix-">>),
    ?assertEqual(2, length(Matching)).

api_filter_by_suffix_test() ->
    Items = [<<"a-end">>, <<"b-end">>, <<"other">>],
    Matching = esdb_filter_nif:filter_by_suffix(Items, <<"-end">>),
    ?assertEqual(2, length(Matching)).

api_match_indices_test() ->
    Items = [<<"a">>, <<"b">>, <<"a">>, <<"c">>],
    Indices = esdb_filter_nif:match_indices(Items, <<"a">>),
    ?assertEqual([0, 2], Indices).

api_count_matches_test() ->
    Items = [<<"a">>, <<"b">>, <<"a">>, <<"c">>],
    Count = esdb_filter_nif:count_matches(Items, <<"a">>),
    ?assertEqual(2, Count).

api_is_valid_regex_test() ->
    ?assert(esdb_filter_nif:is_valid_regex(<<"^test$">>)),
    ?assertNot(esdb_filter_nif:is_valid_regex(<<"(">>)).

%%====================================================================
%% Introspection Tests
%%====================================================================

implementation_returns_atom_test() ->
    Impl = esdb_filter_nif:implementation(),
    ?assert(Impl =:= nif orelse Impl =:= erlang).

is_nif_loaded_returns_boolean_test() ->
    Loaded = esdb_filter_nif:is_nif_loaded(),
    ?assert(is_boolean(Loaded)).

%%====================================================================
%% NIF Implementation Tests (Only When Available)
%%====================================================================

nif_tests_when_available_test_() ->
    case esdb_filter_nif:is_nif_loaded() of
        true ->
            [
                {"NIF wildcard_to_regex works", fun nif_wildcard_to_regex_works/0},
                {"NIF wildcard_match works", fun nif_wildcard_match_works/0},
                {"NIF regex_match works", fun nif_regex_match_works/0},
                {"NIF filter_by_wildcard works", fun nif_filter_by_wildcard_works/0},
                {"NIF has_prefix works", fun nif_has_prefix_works/0},
                {"NIF has_suffix works", fun nif_has_suffix_works/0},
                {"NIF filter_by_prefix works", fun nif_filter_by_prefix_works/0},
                {"NIF match_indices works", fun nif_match_indices_works/0},
                {"NIF count_matches works", fun nif_count_matches_works/0}
            ];
        false ->
            []
    end.

nif_wildcard_to_regex_works() ->
    Pattern = esdb_filter_nif:nif_wildcard_to_regex(<<"test-*">>),
    ?assert(is_binary(Pattern)),  %% NIF returns binary
    ?assert(binary:match(Pattern, <<"^test-">>) =/= nomatch).

nif_wildcard_match_works() ->
    ?assert(esdb_filter_nif:nif_wildcard_match(<<"hello-world">>, <<"hello-*">>)),
    ?assertNot(esdb_filter_nif:nif_wildcard_match(<<"goodbye">>, <<"hello-*">>)).

nif_regex_match_works() ->
    ?assert(esdb_filter_nif:nif_regex_match(<<"test123">>, <<"^test\\d+$">>)),
    ?assertNot(esdb_filter_nif:nif_regex_match(<<"test">>, <<"^test\\d+$">>)).

nif_filter_by_wildcard_works() ->
    Items = [<<"a-1">>, <<"a-2">>, <<"b-1">>],
    Matching = esdb_filter_nif:nif_filter_by_wildcard(Items, <<"a-*">>),
    ?assertEqual(2, length(Matching)).

nif_has_prefix_works() ->
    ?assert(esdb_filter_nif:nif_has_prefix(<<"prefix-value">>, <<"prefix">>)),
    ?assertNot(esdb_filter_nif:nif_has_prefix(<<"other">>, <<"prefix">>)).

nif_has_suffix_works() ->
    ?assert(esdb_filter_nif:nif_has_suffix(<<"value-suffix">>, <<"suffix">>)),
    ?assertNot(esdb_filter_nif:nif_has_suffix(<<"value">>, <<"other">>)).

nif_filter_by_prefix_works() ->
    Items = [<<"pre-a">>, <<"pre-b">>, <<"other">>],
    Matching = esdb_filter_nif:nif_filter_by_prefix(Items, <<"pre-">>),
    ?assertEqual(2, length(Matching)).

nif_match_indices_works() ->
    Items = [<<"x">>, <<"y">>, <<"x">>],
    Indices = esdb_filter_nif:nif_match_indices(Items, <<"x">>),
    ?assertEqual([0, 2], Indices).

nif_count_matches_works() ->
    Items = [<<"a">>, <<"b">>, <<"a">>],
    Count = esdb_filter_nif:nif_count_matches(Items, <<"a">>),
    ?assertEqual(2, Count).

%%====================================================================
%% Equivalence Tests (Enterprise builds only)
%%====================================================================

equivalence_tests_when_available_test_() ->
    case esdb_filter_nif:is_nif_loaded() of
        true ->
            [
                {"Equivalence: wildcard_match", fun equivalence_wildcard_match/0},
                {"Equivalence: filter_by_wildcard", fun equivalence_filter_by_wildcard/0},
                {"Equivalence: has_prefix", fun equivalence_has_prefix/0}
            ];
        false ->
            []
    end.

equivalence_wildcard_match() ->
    TestCases = [
        {<<"orders-123">>, <<"orders-*">>},
        {<<"users-abc">>, <<"users-???">>},
        {<<"data">>, <<"*">>}
    ],
    lists:foreach(fun({Text, Pattern}) ->
        ErlResult = esdb_filter_nif:erlang_wildcard_match(Text, Pattern),
        NifResult = esdb_filter_nif:nif_wildcard_match(Text, Pattern),
        ?assertEqual(ErlResult, NifResult)
    end, TestCases).

equivalence_filter_by_wildcard() ->
    Items = stream_ids(),
    Pattern = <<"orders-*">>,
    ErlResult = lists:sort(esdb_filter_nif:erlang_filter_by_wildcard(Items, Pattern)),
    NifResult = lists:sort(esdb_filter_nif:nif_filter_by_wildcard(Items, Pattern)),
    ?assertEqual(ErlResult, NifResult).

equivalence_has_prefix() ->
    TestCases = [
        {<<"orders-123">>, <<"orders">>},
        {<<"abc">>, <<"abcd">>},
        {<<"test">>, <<>>}
    ],
    lists:foreach(fun({Text, Prefix}) ->
        ErlResult = esdb_filter_nif:erlang_has_prefix(Text, Prefix),
        NifResult = esdb_filter_nif:nif_has_prefix(Text, Prefix),
        ?assertEqual(ErlResult, NifResult)
    end, TestCases).

%%====================================================================
%% Benchmark Tests (Only When NIF Available)
%%====================================================================

benchmark_test_() ->
    case esdb_filter_nif:is_nif_loaded() of
        true ->
            [{"Benchmark filtering", fun benchmark_filtering/0}];
        false ->
            []
    end.

benchmark_filtering() ->
    %% Create a larger dataset for benchmarking
    Items = [<<"stream-", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 1000)],
    Pattern = <<"stream-1*">>,
    Iterations = 100,

    %% Benchmark Erlang path
    {TimeErl, _} = timer:tc(fun() ->
        [esdb_filter_nif:erlang_filter_by_wildcard(Items, Pattern)
         || _ <- lists:seq(1, Iterations)]
    end),

    %% Benchmark NIF path
    {TimeNif, _} = timer:tc(fun() ->
        [esdb_filter_nif:nif_filter_by_wildcard(Items, Pattern)
         || _ <- lists:seq(1, Iterations)]
    end),

    %% Log results
    logger:info("Filter benchmark (~p items, ~p iterations): Erlang=~pus, NIF=~pus, Speedup=~.2fx",
               [length(Items), Iterations, TimeErl, TimeNif, TimeErl / max(TimeNif, 1)]),

    %% NIF should be competitive
    ?assert(TimeNif =< TimeErl * 3).
