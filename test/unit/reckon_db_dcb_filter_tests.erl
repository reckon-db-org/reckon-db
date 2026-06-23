%%% @doc Unit tests for reckon_db_dcb_filter.
%%%
%%% Pure-function tests via a mock seqs_provider. No Khepri runtime
%%% needed — `seqs_for_tag/1` is exercised in P3.2 integration tests.
%%% @end
-module(reckon_db_dcb_filter_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../../include/reckon_db.hrl").

%%====================================================================
%% Mock seqs provider
%%====================================================================

%% Returns a provider over a Key => [Seq] mapping where Key is either
%% a binary tag or {event_type, binary()}.
mock_provider(MockData) ->
    fun(Key) ->
        case lists:keyfind(Key, 1, MockData) of
            {Key, Seqs} -> Seqs;
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

cutoff_minus_one_matches_any_event_test() ->
    %% Cutoff = -1 means "I saw nothing yet". Any matching event with
    %% seq >= 0 is a conflict. This is the empty-initial-state idiom
    %% used by uniqueness checks: "ensure no event with this tag exists."
    P = mock_provider([{<<"a">>, [0, 5, 7]}]),
    ?assertEqual({true, 7},
        reckon_db_dcb_filter:match_any_above_cutoff(
            {any_of, [<<"a">>]}, -1, P)),
    %% And an empty store still yields false even with cutoff=-1.
    PEmpty = mock_provider([]),
    ?assertEqual(false,
        reckon_db_dcb_filter:match_any_above_cutoff(
            {any_of, [<<"a">>]}, -1, PEmpty)).

%%====================================================================
%% event_type filter
%%====================================================================

event_type_matches_indexed_seqs_test() ->
    P = mock_provider([{{event_type, <<"user_registered_v1">>}, [3, 7, 11]}]),
    Result = reckon_db_dcb_filter:match_seqs(
               {event_type, <<"user_registered_v1">>}, P),
    ?assertEqual([3, 7, 11], lists:sort(sets:to_list(Result))).

event_type_no_matching_events_test() ->
    P = mock_provider([{{event_type, <<"other_event_v1">>}, [1, 2]}]),
    Result = reckon_db_dcb_filter:match_seqs(
               {event_type, <<"user_registered_v1">>}, P),
    ?assertEqual([], sets:to_list(Result)).

event_type_composes_with_any_of_via_and_test() ->
    %% Canonical DCB "query item": type AND tag.
    %% event_type = user_registered_v1 -> {1, 2, 3}
    %% any_of email:alice              -> {2, 4}
    %% AND                             -> {2}
    P = mock_provider([
        {{event_type, <<"user_registered_v1">>}, [1, 2, 3]},
        {<<"email:alice@example.com">>, [2, 4]}
    ]),
    Filter = {and_, [
        {event_type, <<"user_registered_v1">>},
        {any_of, [<<"email:alice@example.com">>]}
    ]},
    Result = reckon_db_dcb_filter:match_seqs(Filter, P),
    ?assertEqual([2], lists:sort(sets:to_list(Result))).

event_type_or_tag_test() ->
    %% "Match either this type OR this tag" — broader consistency window.
    P = mock_provider([
        {{event_type, <<"order_placed_v1">>}, [1, 3]},
        {<<"customer:42">>, [2, 3]}
    ]),
    Filter = {or_, [
        {event_type, <<"order_placed_v1">>},
        {any_of, [<<"customer:42">>]}
    ]},
    Result = reckon_db_dcb_filter:match_seqs(Filter, P),
    ?assertEqual([1, 2, 3], lists:sort(sets:to_list(Result))).

event_type_with_cutoff_test() ->
    P = mock_provider([{{event_type, <<"user_registered_v1">>}, [1, 5, 9]}]),
    ?assertEqual({true, 9},
        reckon_db_dcb_filter:match_any_above_cutoff(
            {event_type, <<"user_registered_v1">>}, 0, P)),
    ?assertEqual(false,
        reckon_db_dcb_filter:match_any_above_cutoff(
            {event_type, <<"user_registered_v1">>}, 9, P)).

event_type_cutoff_minus_one_test() ->
    P = mock_provider([{{event_type, <<"user_registered_v1">>}, [0]}]),
    ?assertEqual({true, 0},
        reckon_db_dcb_filter:match_any_above_cutoff(
            {event_type, <<"user_registered_v1">>}, -1, P)).

%%====================================================================
%% payload_match filter (CCC, 5.3.0+)
%%====================================================================

payload_match_returns_indexed_seqs_test() ->
    P = mock_provider([{{payload, <<"account_id">>, <<"acc-42">>}, [1, 5, 9]}]),
    Result = reckon_db_dcb_filter:match_seqs(
               {payload_match, <<"account_id">>, <<"acc-42">>}, P),
    ?assertEqual([1, 5, 9], lists:sort(sets:to_list(Result))).

payload_match_no_matching_events_test() ->
    P = mock_provider([{{payload, <<"account_id">>, <<"acc-42">>}, [1, 2]}]),
    Result = reckon_db_dcb_filter:match_seqs(
               {payload_match, <<"account_id">>, <<"acc-99">>}, P),
    ?assertEqual([], sets:to_list(Result)).

payload_match_composes_with_event_type_via_and_test() ->
    %% event_type="seat_reserved_v1" AND payload account_id="acc-1"
    %% type: {1,2,3}, payload: {2,4} → AND → {2}
    P = mock_provider([
        {{event_type, <<"seat_reserved_v1">>}, [1, 2, 3]},
        {{payload, <<"account_id">>, <<"acc-1">>}, [2, 4]}
    ]),
    Filter = {and_, [
        {event_type, <<"seat_reserved_v1">>},
        {payload_match, <<"account_id">>, <<"acc-1">>}
    ]},
    Result = reckon_db_dcb_filter:match_seqs(Filter, P),
    ?assertEqual([2], lists:sort(sets:to_list(Result))).

payload_match_with_cutoff_test() ->
    P = mock_provider([{{payload, <<"user_id">>, <<"u1">>}, [1, 3, 7]}]),
    ?assertEqual({true, 7},
        reckon_db_dcb_filter:match_any_above_cutoff(
            {payload_match, <<"user_id">>, <<"u1">>}, 3, P)),
    ?assertEqual(false,
        reckon_db_dcb_filter:match_any_above_cutoff(
            {payload_match, <<"user_id">>, <<"u1">>}, 7, P)).

%%====================================================================
%% payload_hash_match_pre filter (CCC, 5.3.0+)
%%====================================================================

payload_hash_match_pre_returns_indexed_seqs_test() ->
    Hash = reckon_db_dcb_paths:payload_combo_hash(
        [<<"flight_id">>, <<"seat_no">>], [<<"FL001">>, <<"12A">>]),
    P = mock_provider([{{payload_hash_pre, Hash}, [2, 8]}]),
    Result = reckon_db_dcb_filter:match_seqs(
               {payload_hash_match_pre, Hash}, P),
    ?assertEqual([2, 8], lists:sort(sets:to_list(Result))).

payload_hash_match_pre_no_match_test() ->
    Hash = reckon_db_dcb_paths:payload_combo_hash([<<"k">>], [<<"v">>]),
    P = mock_provider([{{payload_hash_pre, Hash}, []}]),
    Result = reckon_db_dcb_filter:match_seqs({payload_hash_match_pre, Hash}, P),
    ?assertEqual([], sets:to_list(Result)).

%%====================================================================
%% preprocess_filter/1 (CCC, 5.3.0+)
%%====================================================================

preprocess_filter_replaces_payload_hash_match_test() ->
    Keys = [<<"flight_id">>, <<"seat_no">>],
    Values = [<<"FL001">>, <<"12A">>],
    Expected = {payload_hash_match_pre,
                reckon_db_dcb_paths:payload_combo_hash(Keys, Values)},
    ?assertEqual(Expected,
        reckon_db_dcb_filter:preprocess_filter(
            {payload_hash_match, Keys, Values})).

preprocess_filter_is_order_independent_test() ->
    %% Swapping key-value order must produce the same pre-computed hash.
    F1 = reckon_db_dcb_filter:preprocess_filter(
           {payload_hash_match, [<<"a">>, <<"b">>], [<<"x">>, <<"y">>]}),
    F2 = reckon_db_dcb_filter:preprocess_filter(
           {payload_hash_match, [<<"b">>, <<"a">>], [<<"y">>, <<"x">>]}),
    ?assertEqual(F1, F2).

preprocess_filter_recurses_into_and_test() ->
    Keys = [<<"k">>],
    Values = [<<"v">>],
    Hash = reckon_db_dcb_paths:payload_combo_hash(Keys, Values),
    Filter = {and_, [
        {event_type, <<"t">>},
        {payload_hash_match, Keys, Values}
    ]},
    ?assertEqual(
        {and_, [{event_type, <<"t">>}, {payload_hash_match_pre, Hash}]},
        reckon_db_dcb_filter:preprocess_filter(Filter)).

preprocess_filter_recurses_into_or_test() ->
    Hash = reckon_db_dcb_paths:payload_combo_hash([<<"a">>], [<<"b">>]),
    Filter = {or_, [{payload_hash_match, [<<"a">>], [<<"b">>]}, {any_of, [<<"x">>]}]},
    ?assertEqual(
        {or_, [{payload_hash_match_pre, Hash}, {any_of, [<<"x">>]}]},
        reckon_db_dcb_filter:preprocess_filter(Filter)).

preprocess_filter_passthrough_for_other_filters_test() ->
    ?assertEqual({any_of, [<<"a">>]},
        reckon_db_dcb_filter:preprocess_filter({any_of, [<<"a">>]})),
    ?assertEqual({event_type, <<"t">>},
        reckon_db_dcb_filter:preprocess_filter({event_type, <<"t">>})),
    ?assertEqual({payload_match, <<"k">>, <<"v">>},
        reckon_db_dcb_filter:preprocess_filter({payload_match, <<"k">>, <<"v">>})).
