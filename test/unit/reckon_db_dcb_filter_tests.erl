%%% @doc Unit tests for reckon_db_dcb_filter.
%%%
%%% Pure-function tests via a mock seqs_provider. No Khepri runtime
%%% needed — `seqs_for_tag/1` is exercised in P3.2 integration tests.
%%% @end
-module(reckon_db_dcb_filter_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Mock seqs provider
%%====================================================================

%% Returns a provider over the given Tag => [Seq] mapping.
mock_provider(MockData) ->
    fun(Tag) ->
        case lists:keyfind(Tag, 1, MockData) of
            {Tag, Seqs} -> Seqs;
            false       -> []
        end
    end.

%%====================================================================
%% match_seqs/2 — algebra
%%====================================================================

any_of_unions_tag_subtrees_test() ->
    P = mock_provider([
        {<<"a">>, [1, 2, 3]},
        {<<"b">>, [3, 4, 5]},
        {<<"c">>, [10]}
    ]),
    Result = reckon_db_dcb_filter:match_seqs({any_of, [<<"a">>, <<"b">>]}, P),
    ?assertEqual([1, 2, 3, 4, 5], lists:sort(sets:to_list(Result))).

any_of_single_tag_test() ->
    P = mock_provider([{<<"a">>, [7, 11]}]),
    Result = reckon_db_dcb_filter:match_seqs({any_of, [<<"a">>]}, P),
    ?assertEqual([7, 11], lists:sort(sets:to_list(Result))).

any_of_missing_tag_yields_other_tags_only_test() ->
    P = mock_provider([{<<"a">>, [1]}]),
    Result = reckon_db_dcb_filter:match_seqs(
               {any_of, [<<"a">>, <<"missing">>]}, P),
    ?assertEqual([1], lists:sort(sets:to_list(Result))).

any_of_empty_tag_list_test() ->
    P = mock_provider([{<<"a">>, [1]}]),
    Result = reckon_db_dcb_filter:match_seqs({any_of, []}, P),
    ?assertEqual([], sets:to_list(Result)).

all_of_intersects_tag_subtrees_test() ->
    %% Event 3 is the only one tagged with BOTH a and b.
    P = mock_provider([
        {<<"a">>, [1, 2, 3]},
        {<<"b">>, [3, 4, 5]}
    ]),
    Result = reckon_db_dcb_filter:match_seqs({all_of, [<<"a">>, <<"b">>]}, P),
    ?assertEqual([3], lists:sort(sets:to_list(Result))).

all_of_three_way_intersection_test() ->
    P = mock_provider([
        {<<"a">>, [1, 2, 3, 4]},
        {<<"b">>, [2, 3, 4, 5]},
        {<<"c">>, [3, 4, 6]}
    ]),
    Result = reckon_db_dcb_filter:match_seqs(
               {all_of, [<<"a">>, <<"b">>, <<"c">>]}, P),
    ?assertEqual([3, 4], lists:sort(sets:to_list(Result))).

all_of_disjoint_tags_yields_empty_test() ->
    P = mock_provider([
        {<<"a">>, [1, 2]},
        {<<"b">>, [3, 4]}
    ]),
    Result = reckon_db_dcb_filter:match_seqs({all_of, [<<"a">>, <<"b">>]}, P),
    ?assertEqual([], sets:to_list(Result)).

all_of_single_tag_equals_seqs_for_that_tag_test() ->
    P = mock_provider([{<<"a">>, [5, 6, 7]}]),
    Result = reckon_db_dcb_filter:match_seqs({all_of, [<<"a">>]}, P),
    ?assertEqual([5, 6, 7], lists:sort(sets:to_list(Result))).

all_of_empty_tag_list_test() ->
    P = mock_provider([{<<"a">>, [1]}]),
    Result = reckon_db_dcb_filter:match_seqs({all_of, []}, P),
    ?assertEqual([], sets:to_list(Result)).

or_unions_subfilter_results_test() ->
    %% (any_of [a]) OR (any_of [b]) ==  any_of [a, b]
    P = mock_provider([
        {<<"a">>, [1, 2]},
        {<<"b">>, [3, 4]}
    ]),
    Result = reckon_db_dcb_filter:match_seqs(
               {or_, [{any_of, [<<"a">>]}, {any_of, [<<"b">>]}]}, P),
    ?assertEqual([1, 2, 3, 4], lists:sort(sets:to_list(Result))).

or_empty_filter_list_test() ->
    P = mock_provider([{<<"a">>, [1]}]),
    Result = reckon_db_dcb_filter:match_seqs({or_, []}, P),
    ?assertEqual([], sets:to_list(Result)).

and_intersects_subfilter_results_test() ->
    %% (any_of [a]) AND (any_of [b]) - per-event AND = same as all_of([a,b])
    P = mock_provider([
        {<<"a">>, [1, 2, 3]},
        {<<"b">>, [3, 4, 5]}
    ]),
    Result = reckon_db_dcb_filter:match_seqs(
               {and_, [{any_of, [<<"a">>]}, {any_of, [<<"b">>]}]}, P),
    ?assertEqual([3], lists:sort(sets:to_list(Result))).

and_empty_filter_list_test() ->
    P = mock_provider([{<<"a">>, [1]}]),
    Result = reckon_db_dcb_filter:match_seqs({and_, []}, P),
    ?assertEqual([], sets:to_list(Result)).

nested_compound_filter_test() ->
    %% (any_of [a] OR all_of [b, c]) AND (any_of [d])
    %% any_of [a]       -> {1, 2}
    %% all_of [b, c]    -> {5}    (b={5,6}, c={4,5})
    %% OR               -> {1, 2, 5}
    %% any_of [d]       -> {2, 5}
    %% AND              -> {2, 5}
    P = mock_provider([
        {<<"a">>, [1, 2]},
        {<<"b">>, [5, 6]},
        {<<"c">>, [4, 5]},
        {<<"d">>, [2, 5]}
    ]),
    Filter = {and_, [
        {or_, [{any_of, [<<"a">>]}, {all_of, [<<"b">>, <<"c">>]}]},
        {any_of, [<<"d">>]}
    ]},
    Result = reckon_db_dcb_filter:match_seqs(Filter, P),
    ?assertEqual([2, 5], lists:sort(sets:to_list(Result))).

%%====================================================================
%% match_any_above_cutoff/3 — cutoff comparison
%%====================================================================

cutoff_zero_matches_any_event_above_test() ->
    P = mock_provider([{<<"a">>, [1, 5, 7]}]),
    ?assertEqual({true, 7},
        reckon_db_dcb_filter:match_any_above_cutoff(
            {any_of, [<<"a">>]}, 0, P)).

cutoff_above_all_yields_false_test() ->
    P = mock_provider([{<<"a">>, [1, 5, 7]}]),
    ?assertEqual(false,
        reckon_db_dcb_filter:match_any_above_cutoff(
            {any_of, [<<"a">>]}, 100, P)).

cutoff_at_boundary_test() ->
    %% Cutoff = 5. Seqs > 5: only 7.
    P = mock_provider([{<<"a">>, [1, 5, 7]}]),
    ?assertEqual({true, 7},
        reckon_db_dcb_filter:match_any_above_cutoff(
            {any_of, [<<"a">>]}, 5, P)).

cutoff_at_boundary_exclusive_test() ->
    %% Cutoff = 7 means "any seq STRICTLY greater than 7". Seqs = [1, 5, 7]
    %% so nothing matches.
    P = mock_provider([{<<"a">>, [1, 5, 7]}]),
    ?assertEqual(false,
        reckon_db_dcb_filter:match_any_above_cutoff(
            {any_of, [<<"a">>]}, 7, P)).

empty_match_yields_false_test() ->
    P = mock_provider([]),
    ?assertEqual(false,
        reckon_db_dcb_filter:match_any_above_cutoff(
            {any_of, [<<"a">>]}, 0, P)).

returns_max_seq_not_arbitrary_test() ->
    %% Must return the MAX seq above cutoff, not e.g. the first or any.
    %% This matters for context_changed semantics — callers report the
    %% latest conflicting seq.
    P = mock_provider([{<<"a">>, [10, 20, 5, 15]}]),
    ?assertEqual({true, 20},
        reckon_db_dcb_filter:match_any_above_cutoff(
            {any_of, [<<"a">>]}, 0, P)).

all_of_with_cutoff_test() ->
    %% Intersection {3}, cutoff 0 → {true, 3}; cutoff 3 → false.
    P = mock_provider([
        {<<"a">>, [1, 2, 3]},
        {<<"b">>, [3, 4]}
    ]),
    ?assertEqual({true, 3},
        reckon_db_dcb_filter:match_any_above_cutoff(
            {all_of, [<<"a">>, <<"b">>]}, 0, P)),
    ?assertEqual(false,
        reckon_db_dcb_filter:match_any_above_cutoff(
            {all_of, [<<"a">>, <<"b">>]}, 3, P)).

negative_cutoff_rejected_test() ->
    P = mock_provider([{<<"a">>, [1]}]),
    ?assertError(function_clause,
        reckon_db_dcb_filter:match_any_above_cutoff(
            {any_of, [<<"a">>]}, -1, P)).
