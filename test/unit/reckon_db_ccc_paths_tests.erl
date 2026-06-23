%%% @doc Unit tests for reckon_db_ccc_paths.
%%%
%%% Pure-function tests — no Khepri required. Verifies that payload
%%% index paths have the correct structure and ordering properties,
%%% and that payload_combo_hash is stable and order-independent.
%%% @end
-module(reckon_db_ccc_paths_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("../../include/reckon_db.hrl").

%%====================================================================
%% by_payload_path / by_payload_pattern
%%====================================================================

by_payload_path_shape_test() ->
    Key = <<"account_id">>,
    Value = <<"acc-42">>,
    Path = reckon_db_ccc_paths:by_payload_path(Key, Value, 7),
    ?assertEqual(4, length(Path)),
    [ByPayload, K, V, SeqKey] = Path,
    ?assertEqual(by_payload, ByPayload),
    ?assertEqual(Key, K),
    ?assertEqual(Value, V),
    ?assertEqual(reckon_db_dcb_paths:seq_key(7), SeqKey).

by_payload_path_zero_seq_test() ->
    Path = reckon_db_ccc_paths:by_payload_path(<<"k">>, <<"v">>, 0),
    [_, _, _, SeqKey] = Path,
    ?assertEqual(reckon_db_dcb_paths:seq_key(0), SeqKey).

by_payload_pattern_shape_test() ->
    Key = <<"email">>,
    Value = <<"alice@example.com">>,
    Pattern = reckon_db_ccc_paths:by_payload_pattern(Key, Value),
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
        reckon_db_ccc_paths:by_payload_pattern(Key, Value),
        reckon_db_ccc_paths:by_payload_path(Key, Value, 0)).

by_payload_path_rejects_non_binary_key_test() ->
    ?assertError(function_clause,
                 reckon_db_ccc_paths:by_payload_path(not_a_binary, <<"v">>, 0)),
    ?assertError(function_clause,
                 reckon_db_ccc_paths:by_payload_path("string_key", <<"v">>, 0)).

by_payload_path_rejects_negative_seq_test() ->
    ?assertError(function_clause,
                 reckon_db_ccc_paths:by_payload_path(<<"k">>, <<"v">>, -1)).

%%====================================================================
%% by_payload_hash_path / by_payload_hash_pattern
%%====================================================================

by_payload_hash_path_shape_test() ->
    Hash = reckon_db_ccc_paths:payload_combo_hash(
        [<<"flight_id">>, <<"seat_no">>], [<<"FL001">>, <<"12A">>]),
    Path = reckon_db_ccc_paths:by_payload_hash_path(Hash, 3),
    ?assertEqual(3, length(Path)),
    [ByHash, H, SeqKey] = Path,
    ?assertEqual(by_payload_hash, ByHash),
    ?assertEqual(Hash, H),
    ?assertEqual(reckon_db_dcb_paths:seq_key(3), SeqKey).

by_payload_hash_pattern_shape_test() ->
    Hash = reckon_db_ccc_paths:payload_combo_hash([<<"a">>], [<<"b">>]),
    Pattern = reckon_db_ccc_paths:by_payload_hash_pattern(Hash),
    ?assertEqual(3, length(Pattern)),
    [ByHash, H, Wildcard] = Pattern,
    ?assertEqual(by_payload_hash, ByHash),
    ?assertEqual(Hash, H),
    ?assertEqual(?KHEPRI_WILDCARD_STAR, Wildcard).

by_payload_hash_pattern_distinct_from_path_test() ->
    Hash = reckon_db_ccc_paths:payload_combo_hash([<<"k">>], [<<"v">>]),
    ?assertNotEqual(
        reckon_db_ccc_paths:by_payload_hash_pattern(Hash),
        reckon_db_ccc_paths:by_payload_hash_path(Hash, 0)).

%%====================================================================
%% payload_combo_hash
%%====================================================================

payload_combo_hash_is_32_bytes_test() ->
    Hash = reckon_db_ccc_paths:payload_combo_hash(
        [<<"flight_id">>, <<"seat_no">>], [<<"FL001">>, <<"12A">>]),
    ?assertEqual(32, byte_size(Hash)).

payload_combo_hash_order_independent_test() ->
    Hash1 = reckon_db_ccc_paths:payload_combo_hash(
        [<<"a">>, <<"b">>], [<<"x">>, <<"y">>]),
    Hash2 = reckon_db_ccc_paths:payload_combo_hash(
        [<<"b">>, <<"a">>], [<<"y">>, <<"x">>]),
    ?assertEqual(Hash1, Hash2).

payload_combo_hash_different_values_differ_test() ->
    Hash1 = reckon_db_ccc_paths:payload_combo_hash([<<"k">>], [<<"v1">>]),
    Hash2 = reckon_db_ccc_paths:payload_combo_hash([<<"k">>], [<<"v2">>]),
    ?assertNotEqual(Hash1, Hash2).

payload_combo_hash_different_keys_differ_test() ->
    Hash1 = reckon_db_ccc_paths:payload_combo_hash([<<"k1">>], [<<"v">>]),
    Hash2 = reckon_db_ccc_paths:payload_combo_hash([<<"k2">>], [<<"v">>]),
    ?assertNotEqual(Hash1, Hash2).

payload_combo_hash_deterministic_test() ->
    Keys = [<<"flight_id">>, <<"seat_no">>],
    Values = [<<"FL001">>, <<"12A">>],
    Hash = reckon_db_ccc_paths:payload_combo_hash(Keys, Values),
    ?assertEqual(Hash, reckon_db_ccc_paths:payload_combo_hash(Keys, Values)).

payload_combo_hash_three_fields_test() ->
    Hash = reckon_db_ccc_paths:payload_combo_hash(
        [<<"a">>, <<"b">>, <<"c">>],
        [<<"x">>, <<"y">>, <<"z">>]),
    ?assertEqual(32, byte_size(Hash)),
    HashReordered = reckon_db_ccc_paths:payload_combo_hash(
        [<<"c">>, <<"a">>, <<"b">>],
        [<<"z">>, <<"x">>, <<"y">>]),
    ?assertEqual(Hash, HashReordered).
