%% @doc Unit tests for esdb_crypto_nif module.
%%
%% Tests both the pure Erlang implementations (always tested) and the NIF
%% implementations (tested only when available in Enterprise builds).
%%
%% @author rgfaber

-module(esdb_crypto_nif_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Generate a test keypair for Ed25519 tests
generate_test_keypair() ->
    {PubKey, PrivKey} = crypto:generate_key(eddsa, ed25519),
    {PubKey, PrivKey}.

%% Sign a message with the test keypair
sign_message(Message, PrivKey) ->
    crypto:sign(eddsa, none, Message, [PrivKey, ed25519]).

%%====================================================================
%% Pure Erlang Implementation Tests (Always Run)
%%====================================================================

erlang_verify_ed25519_valid_test() ->
    {PubKey, PrivKey} = generate_test_keypair(),
    Message = <<"Hello, World!">>,
    Signature = sign_message(Message, PrivKey),
    ?assert(esdb_crypto_nif:erlang_verify_ed25519(Message, Signature, PubKey)).

erlang_verify_ed25519_invalid_sig_test() ->
    {PubKey, _PrivKey} = generate_test_keypair(),
    Message = <<"Hello, World!">>,
    FakeSig = crypto:strong_rand_bytes(64),
    ?assertNot(esdb_crypto_nif:erlang_verify_ed25519(Message, FakeSig, PubKey)).

erlang_verify_ed25519_wrong_message_test() ->
    {PubKey, PrivKey} = generate_test_keypair(),
    Message1 = <<"Hello, World!">>,
    Message2 = <<"Goodbye, World!">>,
    Signature = sign_message(Message1, PrivKey),
    ?assertNot(esdb_crypto_nif:erlang_verify_ed25519(Message2, Signature, PubKey)).

erlang_verify_ed25519_wrong_key_test() ->
    {_PubKey1, PrivKey1} = generate_test_keypair(),
    {PubKey2, _PrivKey2} = generate_test_keypair(),
    Message = <<"Hello, World!">>,
    Signature = sign_message(Message, PrivKey1),
    ?assertNot(esdb_crypto_nif:erlang_verify_ed25519(Message, Signature, PubKey2)).

erlang_hash_sha256_test() ->
    Data = <<"test data">>,
    Expected = crypto:hash(sha256, Data),
    ?assertEqual(Expected, esdb_crypto_nif:erlang_hash_sha256(Data)).

erlang_hash_sha256_empty_test() ->
    Data = <<>>,
    Expected = crypto:hash(sha256, Data),
    ?assertEqual(Expected, esdb_crypto_nif:erlang_hash_sha256(Data)).

erlang_hash_sha256_large_test() ->
    Data = crypto:strong_rand_bytes(10000),
    Expected = crypto:hash(sha256, Data),
    ?assertEqual(Expected, esdb_crypto_nif:erlang_hash_sha256(Data)).

erlang_hash_sha256_base64_test() ->
    Data = <<"test data">>,
    Hash = crypto:hash(sha256, Data),
    Expected = base64:encode(Hash, #{mode => urlsafe, padding => false}),
    ?assertEqual(Expected, esdb_crypto_nif:erlang_hash_sha256_base64(Data)).

erlang_base64_encode_urlsafe_test() ->
    Data = <<1, 2, 3, 4, 5>>,
    Expected = base64:encode(Data, #{mode => urlsafe, padding => false}),
    ?assertEqual(Expected, esdb_crypto_nif:erlang_base64_encode_urlsafe(Data)).

erlang_base64_decode_urlsafe_test() ->
    Original = <<1, 2, 3, 4, 5>>,
    Encoded = base64:encode(Original, #{mode => urlsafe, padding => false}),
    ?assertEqual({ok, Original}, esdb_crypto_nif:erlang_base64_decode_urlsafe(Encoded)).

erlang_base64_decode_invalid_test() ->
    Invalid = <<"!!!not valid base64!!!">>,
    ?assertEqual({error, invalid_base64}, esdb_crypto_nif:erlang_base64_decode_urlsafe(Invalid)).

erlang_secure_compare_equal_test() ->
    A = <<"secret_token_123">>,
    B = <<"secret_token_123">>,
    ?assert(esdb_crypto_nif:erlang_secure_compare(A, B)).

erlang_secure_compare_not_equal_test() ->
    A = <<"secret_token_123">>,
    B = <<"secret_token_456">>,
    ?assertNot(esdb_crypto_nif:erlang_secure_compare(A, B)).

erlang_secure_compare_different_length_test() ->
    A = <<"short">>,
    B = <<"much_longer_string">>,
    ?assertNot(esdb_crypto_nif:erlang_secure_compare(A, B)).

erlang_secure_compare_empty_test() ->
    ?assert(esdb_crypto_nif:erlang_secure_compare(<<>>, <<>>)).

%%====================================================================
%% Public API Tests (Uses Dispatch)
%%====================================================================

api_verify_ed25519_test() ->
    {PubKey, PrivKey} = generate_test_keypair(),
    Message = <<"API test message">>,
    Signature = sign_message(Message, PrivKey),
    ?assert(esdb_crypto_nif:verify_ed25519(Message, Signature, PubKey)).

api_hash_sha256_test() ->
    Data = <<"API test data">>,
    Expected = crypto:hash(sha256, Data),
    ?assertEqual(Expected, esdb_crypto_nif:hash_sha256(Data)).

api_hash_sha256_base64_test() ->
    Data = <<"API test data">>,
    Result = esdb_crypto_nif:hash_sha256_base64(Data),
    ?assert(is_binary(Result)),
    %% Should be valid base64 and decode to 32 bytes (SHA256)
    {ok, Decoded} = esdb_crypto_nif:base64_decode_urlsafe(Result),
    ?assertEqual(32, byte_size(Decoded)).

api_base64_roundtrip_test() ->
    Original = crypto:strong_rand_bytes(100),
    Encoded = esdb_crypto_nif:base64_encode_urlsafe(Original),
    {ok, Decoded} = esdb_crypto_nif:base64_decode_urlsafe(Encoded),
    ?assertEqual(Original, Decoded).

api_secure_compare_test() ->
    Token = crypto:strong_rand_bytes(32),
    ?assert(esdb_crypto_nif:secure_compare(Token, Token)),
    ?assertNot(esdb_crypto_nif:secure_compare(Token, crypto:strong_rand_bytes(32))).

%%====================================================================
%% Introspection Tests
%%====================================================================

implementation_returns_atom_test() ->
    Impl = esdb_crypto_nif:implementation(),
    ?assert(Impl =:= nif orelse Impl =:= erlang).

is_nif_loaded_returns_boolean_test() ->
    Loaded = esdb_crypto_nif:is_nif_loaded(),
    ?assert(is_boolean(Loaded)).

%%====================================================================
%% NIF Implementation Tests (Only When Available)
%%====================================================================

nif_tests_when_available_test_() ->
    case esdb_crypto_nif:is_nif_loaded() of
        true ->
            %% NIF is loaded - run NIF-specific tests
            [
                {"NIF verify_ed25519 works", fun nif_verify_ed25519_works/0},
                {"NIF hash_sha256 works", fun nif_hash_sha256_works/0},
                {"NIF hash_sha256_base64 works", fun nif_hash_sha256_base64_works/0},
                {"NIF base64 roundtrip works", fun nif_base64_roundtrip_works/0},
                {"NIF secure_compare works", fun nif_secure_compare_works/0}
            ];
        false ->
            %% NIF not loaded (community edition) - skip NIF tests
            []
    end.

nif_verify_ed25519_works() ->
    {PubKey, PrivKey} = generate_test_keypair(),
    Message = <<"NIF test message">>,
    Signature = sign_message(Message, PrivKey),
    ?assert(esdb_crypto_nif:nif_verify_ed25519(Message, Signature, PubKey)).

nif_hash_sha256_works() ->
    Data = <<"NIF test data">>,
    Expected = crypto:hash(sha256, Data),
    ?assertEqual(Expected, esdb_crypto_nif:nif_hash_sha256(Data)).

nif_hash_sha256_base64_works() ->
    Data = <<"NIF test data">>,
    Result = esdb_crypto_nif:nif_hash_sha256_base64(Data),
    ?assert(is_binary(Result)),
    ?assert(byte_size(Result) > 0).

nif_base64_roundtrip_works() ->
    Original = crypto:strong_rand_bytes(100),
    Encoded = esdb_crypto_nif:nif_base64_encode_urlsafe(Original),
    {ok, Decoded} = esdb_crypto_nif:nif_base64_decode_urlsafe(Encoded),
    ?assertEqual(Original, Decoded).

nif_secure_compare_works() ->
    Token = crypto:strong_rand_bytes(32),
    ?assert(esdb_crypto_nif:nif_secure_compare(Token, Token)),
    ?assertNot(esdb_crypto_nif:nif_secure_compare(Token, crypto:strong_rand_bytes(32))).

%%====================================================================
%% Equivalence Tests (Only When NIF Available)
%%====================================================================

equivalence_tests_test_() ->
    case esdb_crypto_nif:is_nif_loaded() of
        true ->
            [
                {"Ed25519 equivalence", fun equivalence_verify_ed25519/0},
                {"SHA256 equivalence", fun equivalence_hash_sha256/0},
                {"SHA256+Base64 equivalence", fun equivalence_hash_sha256_base64/0},
                {"Base64 encode equivalence", fun equivalence_base64_encode/0},
                {"Secure compare equivalence", fun equivalence_secure_compare/0}
            ];
        false ->
            []
    end.

equivalence_verify_ed25519() ->
    {PubKey, PrivKey} = generate_test_keypair(),
    Message = <<"Equivalence test">>,
    Signature = sign_message(Message, PrivKey),

    ErlResult = esdb_crypto_nif:erlang_verify_ed25519(Message, Signature, PubKey),
    NifResult = esdb_crypto_nif:nif_verify_ed25519(Message, Signature, PubKey),
    ?assertEqual(ErlResult, NifResult).

equivalence_hash_sha256() ->
    Data = crypto:strong_rand_bytes(1000),
    ErlResult = esdb_crypto_nif:erlang_hash_sha256(Data),
    NifResult = esdb_crypto_nif:nif_hash_sha256(Data),
    ?assertEqual(ErlResult, NifResult).

equivalence_hash_sha256_base64() ->
    Data = crypto:strong_rand_bytes(1000),
    ErlResult = esdb_crypto_nif:erlang_hash_sha256_base64(Data),
    NifResult = esdb_crypto_nif:nif_hash_sha256_base64(Data),
    ?assertEqual(ErlResult, NifResult).

equivalence_base64_encode() ->
    Data = crypto:strong_rand_bytes(100),
    ErlResult = esdb_crypto_nif:erlang_base64_encode_urlsafe(Data),
    NifResult = esdb_crypto_nif:nif_base64_encode_urlsafe(Data),
    ?assertEqual(ErlResult, NifResult).

equivalence_secure_compare() ->
    A = crypto:strong_rand_bytes(32),
    B = crypto:strong_rand_bytes(32),

    ErlEq = esdb_crypto_nif:erlang_secure_compare(A, A),
    NifEq = esdb_crypto_nif:nif_secure_compare(A, A),
    ?assertEqual(ErlEq, NifEq),

    ErlNeq = esdb_crypto_nif:erlang_secure_compare(A, B),
    NifNeq = esdb_crypto_nif:nif_secure_compare(A, B),
    ?assertEqual(ErlNeq, NifNeq).

%%====================================================================
%% Benchmark Tests (Only When NIF Available)
%%====================================================================

benchmark_test_() ->
    case esdb_crypto_nif:is_nif_loaded() of
        true ->
            [{"Benchmark verify_ed25519", fun benchmark_verify_ed25519/0}];
        false ->
            []
    end.

benchmark_verify_ed25519() ->
    {PubKey, PrivKey} = generate_test_keypair(),
    Message = crypto:strong_rand_bytes(256),
    Signature = sign_message(Message, PrivKey),

    Iterations = 1000,

    %% Benchmark Erlang path
    {TimeErl, _} = timer:tc(fun() ->
        [esdb_crypto_nif:erlang_verify_ed25519(Message, Signature, PubKey)
         || _ <- lists:seq(1, Iterations)]
    end),

    %% Benchmark NIF path
    {TimeNif, _} = timer:tc(fun() ->
        [esdb_crypto_nif:nif_verify_ed25519(Message, Signature, PubKey)
         || _ <- lists:seq(1, Iterations)]
    end),

    %% Log results
    logger:info("Ed25519 verify benchmark (~p iterations): Erlang=~pus, NIF=~pus, Speedup=~.2fx",
               [Iterations, TimeErl, TimeNif, TimeErl / max(TimeNif, 1)]),

    %% NIF should be faster (or at least not slower)
    ?assert(TimeNif =< TimeErl * 1.5).  %% Allow some variance
