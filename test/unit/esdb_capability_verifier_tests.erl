%% @doc Unit tests for esdb_capability_verifier
%%
%% Tests server-side capability token verification including:
%% - Signature verification
%% - Expiration checking
%% - Permission matching
%% - Revocation integration
%%
%% @author Macula.io

-module(esdb_capability_verifier_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("reckon_db_gater/include/esdb_capability_types.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

verifier_test_() ->
    {foreach,
        fun setup/0,
        fun cleanup/1,
        [
            {"verify valid token", fun verify_valid_token/0},
            {"verify expired token", fun verify_expired_token/0},
            {"verify not yet valid token", fun verify_not_yet_valid_token/0},
            {"verify unsigned token fails", fun verify_unsigned_token/0},
            {"verify tampered signature fails", fun verify_tampered_signature/0},
            {"authorize with matching grant", fun authorize_matching_grant/0},
            {"authorize with wildcard grant", fun authorize_wildcard_grant/0},
            {"authorize with prefix grant", fun authorize_prefix_grant/0},
            {"authorize without permission denied", fun authorize_permission_denied/0},
            {"authorize wildcard action", fun authorize_wildcard_action/0},
            {"check permission exact match", fun check_permission_exact/0},
            {"check permission insufficient", fun check_permission_insufficient/0},
            {"token CID extraction", fun token_cid_extraction/0}
        ]
    }.

setup() ->
    %% Ensure crypto is started
    ok = application:ensure_started(crypto),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Helpers
%%====================================================================

%% Create a test identity
create_test_identity() ->
    esdb_identity:generate().

%% Create a valid capability with specific grants
create_test_capability(Issuer, Audience, Grants, TTL) ->
    esdb_capability:create(Issuer, Audience, Grants, #{ttl => TTL}).

%% Create an expired capability (expired 1 hour ago)
create_expired_capability(Issuer, Audience, Grants) ->
    Cap = esdb_capability:create(Issuer, Audience, Grants, #{ttl => 60}),
    %% Manually set expiration to the past
    PastExp = erlang:system_time(second) - 3600,
    Cap#capability{exp = PastExp, iat = PastExp - 60}.

%% Create a not-yet-valid capability (valid in 1 hour)
create_future_capability(Issuer, Audience, Grants) ->
    Cap = esdb_capability:create(Issuer, Audience, Grants, #{ttl => 3600}),
    FutureNbf = erlang:system_time(second) + 3600,
    Cap#capability{nbf = FutureNbf}.

%%====================================================================
%% Verification Tests
%%====================================================================

verify_valid_token() ->
    Issuer = create_test_identity(),
    Audience = esdb_identity:did(create_test_identity()),

    Grants = [
        esdb_capability:grant(<<"esdb://test/stream/*">>, ?ACTION_STREAM_READ)
    ],

    Cap = create_test_capability(Issuer, Audience, Grants, 900),
    SignedCap = esdb_capability:sign(Cap, esdb_identity:private_key(Issuer)),
    Token = esdb_capability:encode(SignedCap, binary),

    %% Verify should succeed
    Result = esdb_capability_verifier:verify(Token),
    ?assertMatch({ok, #capability{}}, Result),
    {ok, VerifiedCap} = Result,
    ?assertEqual(esdb_identity:did(Issuer), VerifiedCap#capability.iss),
    ?assertEqual(Audience, VerifiedCap#capability.aud).

verify_expired_token() ->
    Issuer = create_test_identity(),
    Audience = esdb_identity:did(create_test_identity()),

    Grants = [esdb_capability:grant(<<"esdb://test/*">>, <<"*">>)],

    Cap = create_expired_capability(Issuer, Audience, Grants),
    SignedCap = esdb_capability:sign(Cap, esdb_identity:private_key(Issuer)),
    Token = esdb_capability:encode(SignedCap, binary),

    %% Verify should fail with expired error
    Result = esdb_capability_verifier:verify(Token),
    ?assertMatch({error, {expired, _}}, Result).

verify_not_yet_valid_token() ->
    Issuer = create_test_identity(),
    Audience = esdb_identity:did(create_test_identity()),

    Grants = [esdb_capability:grant(<<"esdb://test/*">>, <<"*">>)],

    Cap = create_future_capability(Issuer, Audience, Grants),
    SignedCap = esdb_capability:sign(Cap, esdb_identity:private_key(Issuer)),
    Token = esdb_capability:encode(SignedCap, binary),

    %% Verify should fail with not_yet_valid error
    Result = esdb_capability_verifier:verify(Token),
    ?assertMatch({error, {not_yet_valid, _}}, Result).

verify_unsigned_token() ->
    Issuer = create_test_identity(),
    Audience = esdb_identity:did(create_test_identity()),

    Grants = [esdb_capability:grant(<<"esdb://test/*">>, <<"*">>)],

    %% Create but don't sign
    Cap = create_test_capability(Issuer, Audience, Grants, 900),

    %% esdb_capability:encode/2 throws error for unsigned tokens,
    %% so we test the verify directly on a hand-crafted binary token
    Token = term_to_binary(Cap, [compressed]),

    %% Verify should fail with invalid_signature (sig = undefined)
    Result = esdb_capability_verifier:verify(Token),
    ?assertMatch({error, {invalid_signature, _}}, Result).

verify_tampered_signature() ->
    Issuer = create_test_identity(),
    Audience = esdb_identity:did(create_test_identity()),

    Grants = [esdb_capability:grant(<<"esdb://test/*">>, <<"*">>)],

    Cap = create_test_capability(Issuer, Audience, Grants, 900),
    SignedCap = esdb_capability:sign(Cap, esdb_identity:private_key(Issuer)),

    %% Tamper with the signature
    TamperedSig = crypto:strong_rand_bytes(64),
    TamperedCap = SignedCap#capability{sig = TamperedSig},
    Token = esdb_capability:encode(TamperedCap, binary),

    %% Verify should fail
    Result = esdb_capability_verifier:verify(Token),
    ?assertMatch({error, {invalid_signature, _}}, Result).

%%====================================================================
%% Authorization Tests
%%====================================================================

authorize_matching_grant() ->
    Issuer = create_test_identity(),
    Audience = esdb_identity:did(create_test_identity()),

    Grants = [
        esdb_capability:grant(<<"esdb://myapp/stream/orders">>, ?ACTION_STREAM_APPEND)
    ],

    Cap = create_test_capability(Issuer, Audience, Grants, 900),
    SignedCap = esdb_capability:sign(Cap, esdb_identity:private_key(Issuer)),
    Token = esdb_capability:encode(SignedCap, binary),

    %% Authorize for exact resource should succeed
    Result = esdb_capability_verifier:authorize(
        Token,
        <<"esdb://myapp/stream/orders">>,
        ?ACTION_STREAM_APPEND
    ),
    ?assertMatch({ok, #verification_result{}}, Result).

authorize_wildcard_grant() ->
    Issuer = create_test_identity(),
    Audience = esdb_identity:did(create_test_identity()),

    Grants = [
        esdb_capability:grant(<<"esdb://myapp/stream/*">>, ?ACTION_STREAM_READ)
    ],

    Cap = create_test_capability(Issuer, Audience, Grants, 900),
    SignedCap = esdb_capability:sign(Cap, esdb_identity:private_key(Issuer)),
    Token = esdb_capability:encode(SignedCap, binary),

    %% Authorize for any stream should succeed
    Result1 = esdb_capability_verifier:authorize(
        Token,
        <<"esdb://myapp/stream/orders">>,
        ?ACTION_STREAM_READ
    ),
    ?assertMatch({ok, #verification_result{}}, Result1),

    Result2 = esdb_capability_verifier:authorize(
        Token,
        <<"esdb://myapp/stream/users">>,
        ?ACTION_STREAM_READ
    ),
    ?assertMatch({ok, #verification_result{}}, Result2).

authorize_prefix_grant() ->
    Issuer = create_test_identity(),
    Audience = esdb_identity:did(create_test_identity()),

    Grants = [
        esdb_capability:grant(<<"esdb://myapp/stream/orders-*">>, ?ACTION_STREAM_SUBSCRIBE)
    ],

    Cap = create_test_capability(Issuer, Audience, Grants, 900),
    SignedCap = esdb_capability:sign(Cap, esdb_identity:private_key(Issuer)),
    Token = esdb_capability:encode(SignedCap, binary),

    %% Authorize for matching prefix should succeed
    Result1 = esdb_capability_verifier:authorize(
        Token,
        <<"esdb://myapp/stream/orders-123">>,
        ?ACTION_STREAM_SUBSCRIBE
    ),
    ?assertMatch({ok, #verification_result{}}, Result1),

    %% Non-matching prefix should fail
    Result2 = esdb_capability_verifier:authorize(
        Token,
        <<"esdb://myapp/stream/users-123">>,
        ?ACTION_STREAM_SUBSCRIBE
    ),
    ?assertMatch({error, {insufficient_permissions, _}}, Result2).

authorize_permission_denied() ->
    Issuer = create_test_identity(),
    Audience = esdb_identity:did(create_test_identity()),

    Grants = [
        esdb_capability:grant(<<"esdb://myapp/stream/orders">>, ?ACTION_STREAM_READ)
    ],

    Cap = create_test_capability(Issuer, Audience, Grants, 900),
    SignedCap = esdb_capability:sign(Cap, esdb_identity:private_key(Issuer)),
    Token = esdb_capability:encode(SignedCap, binary),

    %% Authorize for different action should fail
    Result = esdb_capability_verifier:authorize(
        Token,
        <<"esdb://myapp/stream/orders">>,
        ?ACTION_STREAM_APPEND  %% We only have read permission
    ),
    ?assertMatch({error, {insufficient_permissions, _}}, Result).

authorize_wildcard_action() ->
    Issuer = create_test_identity(),
    Audience = esdb_identity:did(create_test_identity()),

    Grants = [
        esdb_capability:grant(<<"esdb://myapp/*">>, ?ACTION_ADMIN_ALL)
    ],

    Cap = create_test_capability(Issuer, Audience, Grants, 900),
    SignedCap = esdb_capability:sign(Cap, esdb_identity:private_key(Issuer)),
    Token = esdb_capability:encode(SignedCap, binary),

    %% Wildcard action should match any action
    Result1 = esdb_capability_verifier:authorize(
        Token,
        <<"esdb://myapp/stream/orders">>,
        ?ACTION_STREAM_APPEND
    ),
    ?assertMatch({ok, #verification_result{}}, Result1),

    Result2 = esdb_capability_verifier:authorize(
        Token,
        <<"esdb://myapp/channel/events">>,
        ?ACTION_CHANNEL_PUBLISH
    ),
    ?assertMatch({ok, #verification_result{}}, Result2).

%%====================================================================
%% Permission Checking Tests
%%====================================================================

check_permission_exact() ->
    Cap = #capability{
        iss = <<"did:key:test">>,
        aud = <<"did:key:other">>,
        att = [
            #{with => <<"esdb://test/stream/orders">>, can => ?ACTION_STREAM_READ}
        ]
    },

    ?assertEqual(ok, esdb_capability_verifier:check_permission(
        Cap,
        <<"esdb://test/stream/orders">>,
        ?ACTION_STREAM_READ
    )).

check_permission_insufficient() ->
    Cap = #capability{
        iss = <<"did:key:test">>,
        aud = <<"did:key:other">>,
        att = [
            #{with => <<"esdb://test/stream/orders">>, can => ?ACTION_STREAM_READ}
        ]
    },

    Result = esdb_capability_verifier:check_permission(
        Cap,
        <<"esdb://test/stream/users">>,  %% Different resource
        ?ACTION_STREAM_READ
    ),
    ?assertMatch({error, {insufficient_permissions, _}}, Result).

%%====================================================================
%% Token CID Tests
%%====================================================================

token_cid_extraction() ->
    Cap1 = #capability{
        iss = <<"did:key:issuer1">>,
        aud = <<"did:key:aud1">>,
        exp = 1234567890,
        nnc = <<"nonce1">>
    },
    Cap2 = #capability{
        iss = <<"did:key:issuer2">>,
        aud = <<"did:key:aud1">>,
        exp = 1234567890,
        nnc = <<"nonce1">>
    },

    CID1 = esdb_capability_verifier:extract_token_cid(Cap1),
    CID2 = esdb_capability_verifier:extract_token_cid(Cap2),

    %% Different issuers should produce different CIDs
    ?assertNotEqual(CID1, CID2),

    %% Same token should produce same CID
    CID1Again = esdb_capability_verifier:extract_token_cid(Cap1),
    ?assertEqual(CID1, CID1Again).

%%====================================================================
%% Resource Matching Tests (exported for testing)
%%====================================================================

match_resource_test_() ->
    [
        ?_assert(esdb_capability_verifier:match_resource(
            <<"esdb://test/stream/orders">>,
            <<"esdb://test/stream/orders">>
        )),
        ?_assertNot(esdb_capability_verifier:match_resource(
            <<"esdb://test/stream/orders">>,
            <<"esdb://test/stream/users">>
        )),
        ?_assert(esdb_capability_verifier:match_resource(
            <<"esdb://test/*">>,
            <<"esdb://test/anything">>
        )),
        ?_assert(esdb_capability_verifier:match_resource(
            <<"esdb://test/stream/order-*">>,
            <<"esdb://test/stream/order-123">>
        )),
        ?_assertNot(esdb_capability_verifier:match_resource(
            <<"esdb://test/stream/order-*">>,
            <<"esdb://test/stream/user-123">>
        ))
    ].

match_action_test_() ->
    [
        ?_assert(esdb_capability_verifier:match_action(
            ?ACTION_STREAM_READ,
            ?ACTION_STREAM_READ
        )),
        ?_assertNot(esdb_capability_verifier:match_action(
            ?ACTION_STREAM_READ,
            ?ACTION_STREAM_APPEND
        )),
        ?_assert(esdb_capability_verifier:match_action(
            ?ACTION_ADMIN_ALL,
            ?ACTION_STREAM_APPEND
        )),
        ?_assert(esdb_capability_verifier:match_action(
            <<"*">>,
            ?ACTION_CHANNEL_PUBLISH
        ))
    ].
