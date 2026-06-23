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
    %% Three-element path: streams / <<"_dcb">> / seq_key(42)
    %% DCB events live under ?STREAMS_PATH so they're visible to all
    %% existing read APIs (read_all_global, read_by_event_types, ...).
    ?assertEqual(3, length(Path)),
    [Streams, DcbStream, SeqKey] = Path,
    ?assertEqual(streams, Streams),
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

%%====================================================================
%% by_payload_path / by_payload_pattern (CCC, 5.3.0+)
%%====================================================================

by_payload_path_shape_test() ->
    Key = <<"account_id">>,
    Value = <<"acc-42">>,
    Path = reckon_db_dcb_paths:by_payload_path(Key, Value, 7),
    ?assertEqual(4, length(Path)),
    [ByPayload, K, V, SeqKey] = Path,
    ?assertEqual(by_payload, ByPayload),
    ?assertEqual(Key, K),
    ?assertEqual(Value, V),
    ?assertEqual(reckon_db_dcb_paths:seq_key(7), SeqKey).

by_payload_pattern_shape_test() ->
    Key = <<"email">>,
    Value = <<"alice@example.com">>,
    Pattern = reckon_db_dcb_paths:by_payload_pattern(Key, Value),
    ?assertEqual(4, length(Pattern)),
    [ByPayload, K, V, Wildcard] = Pattern,
    ?assertEqual(by_payload, ByPayload),
    ?assertEqual(Key, K),
    ?assertEqual(Value, V),
    ?assertEqual(?KHEPRI_WILDCARD_STAR, Wildcard).

by_payload_pattern_distinct_from_path_test() ->
    Key = <<"k">>,
    Value = <<"v">>,
    ?assertNotEqual(
        reckon_db_dcb_paths:by_payload_pattern(Key, Value),
        reckon_db_dcb_paths:by_payload_path(Key, Value, 0)).

%%====================================================================
%% by_payload_hash_path / by_payload_hash_pattern (CCC, 5.3.0+)
%%====================================================================

by_payload_hash_path_shape_test() ->
    Hash = reckon_db_dcb_paths:payload_combo_hash(
        [<<"flight_id">>, <<"seat_no">>], [<<"FL001">>, <<"12A">>]),
    Path = reckon_db_dcb_paths:by_payload_hash_path(Hash, 3),
    ?assertEqual(3, length(Path)),
    [ByHash, H, SeqKey] = Path,
    ?assertEqual(by_payload_hash, ByHash),
    ?assertEqual(Hash, H),
    ?assertEqual(reckon_db_dcb_paths:seq_key(3), SeqKey).

by_payload_hash_pattern_shape_test() ->
    Hash = reckon_db_dcb_paths:payload_combo_hash([<<"a">>], [<<"b">>]),
    Pattern = reckon_db_dcb_paths:by_payload_hash_pattern(Hash),
    ?assertEqual(3, length(Pattern)),
    [ByHash, H, Wildcard] = Pattern,
    ?assertEqual(by_payload_hash, ByHash),
    ?assertEqual(Hash, H),
    ?assertEqual(?KHEPRI_WILDCARD_STAR, Wildcard).

%%====================================================================
%% payload_combo_hash (CCC, 5.3.0+)
%%====================================================================

payload_combo_hash_is_32_bytes_test() ->
    Hash = reckon_db_dcb_paths:payload_combo_hash(
        [<<"flight_id">>, <<"seat_no">>], [<<"FL001">>, <<"12A">>]),
    ?assertEqual(32, byte_size(Hash)).

payload_combo_hash_order_independent_test() ->
    %% The hash must be the same regardless of the order Keys/Values are given.
    Hash1 = reckon_db_dcb_paths:payload_combo_hash(
        [<<"a">>, <<"b">>], [<<"x">>, <<"y">>]),
    Hash2 = reckon_db_dcb_paths:payload_combo_hash(
        [<<"b">>, <<"a">>], [<<"y">>, <<"x">>]),
    ?assertEqual(Hash1, Hash2).

payload_combo_hash_different_values_differ_test() ->
    Hash1 = reckon_db_dcb_paths:payload_combo_hash([<<"k">>], [<<"v1">>]),
    Hash2 = reckon_db_dcb_paths:payload_combo_hash([<<"k">>], [<<"v2">>]),
    ?assertNotEqual(Hash1, Hash2).

payload_combo_hash_different_keys_differ_test() ->
    Hash1 = reckon_db_dcb_paths:payload_combo_hash([<<"k1">>], [<<"v">>]),
    Hash2 = reckon_db_dcb_paths:payload_combo_hash([<<"k2">>], [<<"v">>]),
    ?assertNotEqual(Hash1, Hash2).

payload_combo_hash_deterministic_test() ->
    %% Same input always produces the same hash (no randomness).
    Keys = [<<"flight_id">>, <<"seat_no">>],
    Values = [<<"FL001">>, <<"12A">>],
    Hash = reckon_db_dcb_paths:payload_combo_hash(Keys, Values),
    ?assertEqual(Hash, reckon_db_dcb_paths:payload_combo_hash(Keys, Values)).
