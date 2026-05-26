%%% @doc Unit tests for reckon_db_dcb_paths.
%%%
%%% Pure-function tests — no Khepri required. The paths module is the
%%% foundation for DCB (Phase 3) so its invariants must be airtight.
%%% @end
-module(reckon_db_dcb_paths_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("../../include/reckon_db.hrl").

%%====================================================================
%% seq_key / seq_from_key
%%====================================================================

seq_key_width_is_fixed_test() ->
    %% Every seq_key has the same width regardless of input magnitude.
    ?assertEqual(?DCB_SEQ_KEY_WIDTH, byte_size(reckon_db_dcb_paths:seq_key(0))),
    ?assertEqual(?DCB_SEQ_KEY_WIDTH, byte_size(reckon_db_dcb_paths:seq_key(1))),
    ?assertEqual(?DCB_SEQ_KEY_WIDTH, byte_size(reckon_db_dcb_paths:seq_key(1234567890))),
    ?assertEqual(?DCB_SEQ_KEY_WIDTH, byte_size(reckon_db_dcb_paths:seq_key(99999999999999999999))).

seq_key_is_zero_padded_decimal_test() ->
    %% Specific known values, decimal not hex.
    Width = ?DCB_SEQ_KEY_WIDTH,
    Zeros = list_to_binary(lists:duplicate(Width, $0)),
    ?assertEqual(Zeros, reckon_db_dcb_paths:seq_key(0)),
    ?assertEqual(
        list_to_binary(lists:duplicate(Width - 1, $0) ++ "1"),
        reckon_db_dcb_paths:seq_key(1)
    ),
    ?assertEqual(
        list_to_binary(lists:duplicate(Width - 2, $0) ++ "42"),
        reckon_db_dcb_paths:seq_key(42)
    ).

seq_key_roundtrip_test() ->
    %% seq_from_key(seq_key(N)) == N for a representative range.
    Cases = [0, 1, 42, 100, 999, 1000, 1234567890, 99999999999999999999],
    lists:foreach(
        fun(N) ->
            ?assertEqual(N, reckon_db_dcb_paths:seq_from_key(
                              reckon_db_dcb_paths:seq_key(N)))
        end,
        Cases).

seq_key_lex_order_matches_numeric_test() ->
    %% Critical DCB invariant. If this fails, the per-tag subtree iteration
    %% would return events in the wrong order and seq > cutoff comparisons
    %% would be incorrect.
    Seqs = [0, 1, 2, 9, 10, 11, 99, 100, 999, 1000, 1234567890],
    Keys = [reckon_db_dcb_paths:seq_key(N) || N <- Seqs],
    %% Sorting the keys lexicographically must produce the same order
    %% as sorting the original integers numerically.
    SortedKeys = lists:sort(Keys),
    ?assertEqual(Keys, SortedKeys),
    %% And the reverse: decoding the sorted keys gives sorted integers.
    DecodedSorted = [reckon_db_dcb_paths:seq_from_key(K) || K <- SortedKeys],
    ?assertEqual(lists:sort(Seqs), DecodedSorted).

seq_key_overflow_raises_test() ->
    %% A seq value too large for the fixed width must raise rather than
    %% silently produce a wrong-width key.
    TooBig = list_to_integer(lists:duplicate(?DCB_SEQ_KEY_WIDTH + 1, $9)),
    ?assertError({seq_overflow, TooBig, ?DCB_SEQ_KEY_WIDTH},
                 reckon_db_dcb_paths:seq_key(TooBig)).

seq_key_rejects_negative_test() ->
    ?assertError(function_clause, reckon_db_dcb_paths:seq_key(-1)).

%%====================================================================
%% event_path
%%====================================================================

event_path_shape_test() ->
    Path = reckon_db_dcb_paths:event_path(42),
    %% Three-element path: events / <<"_dcb">> / seq_key(42)
    ?assertEqual(3, length(Path)),
    [Events, DcbStream, SeqKey] = Path,
    ?assertEqual(events, Events),
    ?assertEqual(?DCB_STREAM, DcbStream),
    ?assertEqual(reckon_db_dcb_paths:seq_key(42), SeqKey).

event_path_rejects_negative_test() ->
    ?assertError(function_clause, reckon_db_dcb_paths:event_path(-1)).

%%====================================================================
%% by_tag_path
%%====================================================================

by_tag_path_shape_test() ->
    Tag = <<"email:foo@bar">>,
    Path = reckon_db_dcb_paths:by_tag_path(Tag, 42),
    ?assertEqual(3, length(Path)),
    [ByTag, T, SeqKey] = Path,
    ?assertEqual(by_tag, ByTag),
    ?assertEqual(Tag, T),
    ?assertEqual(reckon_db_dcb_paths:seq_key(42), SeqKey).

by_tag_path_rejects_non_binary_tag_test() ->
    ?assertError(function_clause, reckon_db_dcb_paths:by_tag_path("not_a_binary", 0)),
    ?assertError(function_clause, reckon_db_dcb_paths:by_tag_path(some_atom, 0)).

%%====================================================================
%% by_tag_pattern
%%====================================================================

by_tag_pattern_shape_test() ->
    Tag = <<"signup-flow">>,
    Pattern = reckon_db_dcb_paths:by_tag_pattern(Tag),
    ?assertEqual(3, length(Pattern)),
    [ByTag, T, Wildcard] = Pattern,
    ?assertEqual(by_tag, ByTag),
    ?assertEqual(Tag, T),
    %% Wildcard is whatever Khepri's STAR macro expands to — verify it
    %% matches the get_many wildcard convention used elsewhere.
    ?assertEqual(?KHEPRI_WILDCARD_STAR, Wildcard).

by_tag_pattern_distinct_from_path_test() ->
    %% The pattern's third element is the wildcard; the path's third
    %% element is a concrete seq key. They must NOT compare equal.
    Tag = <<"x">>,
    Pattern = reckon_db_dcb_paths:by_tag_pattern(Tag),
    Path = reckon_db_dcb_paths:by_tag_path(Tag, 42),
    ?assertNotEqual(Pattern, Path).
