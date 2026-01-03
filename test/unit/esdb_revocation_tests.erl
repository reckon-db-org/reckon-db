%% @doc Unit tests for esdb_revocation
%%
%% Tests token revocation management including:
%% - Token revocation
%% - Issuer revocation
%% - Revocation expiration
%%
%% @author rgfaber

-module(esdb_revocation_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

revocation_test_() ->
    {foreach,
        fun setup/0,
        fun cleanup/1,
        [
            {"revoke token", fun revoke_token/0},
            {"revoke token with reason", fun revoke_token_with_reason/0},
            {"check non-revoked token", fun check_non_revoked/0},
            {"revoke issuer", fun revoke_issuer/0},
            {"get revocations", fun get_revocations/0},
            {"clear revocations", fun clear_revocations/0}
        ]
    }.

setup() ->
    %% Start the revocation server
    {ok, Pid} = esdb_revocation:start_link(),
    %% Clear any existing revocations
    esdb_revocation:clear(),
    Pid.

cleanup(Pid) ->
    %% Stop the server
    gen_server:stop(Pid),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

revoke_token() ->
    TokenCID = <<"token-cid-123">>,

    %% Initially not revoked
    ?assertNot(esdb_revocation:is_revoked(TokenCID)),

    %% Revoke the token
    ok = esdb_revocation:revoke(TokenCID),

    %% Should now be revoked
    ?assert(esdb_revocation:is_revoked(TokenCID)).

revoke_token_with_reason() ->
    TokenCID = <<"token-cid-456">>,
    Reason = <<"key_compromised">>,

    %% Revoke with reason
    ok = esdb_revocation:revoke(TokenCID, Reason),

    %% Should be revoked
    ?assert(esdb_revocation:is_revoked(TokenCID)).

check_non_revoked() ->
    TokenCID = <<"never-revoked-token">>,

    %% Should not be revoked
    ?assertNot(esdb_revocation:is_revoked(TokenCID)).

revoke_issuer() ->
    IssuerDID = <<"did:key:z6MkcompromisedKey">>,

    %% Initially not revoked
    ?assertNot(esdb_revocation:is_issuer_revoked(IssuerDID)),

    %% Revoke the issuer
    ok = esdb_revocation:revoke_issuer(IssuerDID),

    %% Should now be revoked
    ?assert(esdb_revocation:is_issuer_revoked(IssuerDID)).

get_revocations() ->
    Token1 = <<"token-1">>,
    Token2 = <<"token-2">>,
    Issuer1 = <<"did:key:z6Mkissuer1">>,

    %% Add some revocations
    ok = esdb_revocation:revoke(Token1),
    ok = esdb_revocation:revoke(Token2),
    ok = esdb_revocation:revoke_issuer(Issuer1),

    %% Get all revocations
    #{tokens := Tokens, issuers := Issuers} = esdb_revocation:get_revocations(),

    %% Check tokens
    ?assert(lists:member(Token1, Tokens)),
    ?assert(lists:member(Token2, Tokens)),
    ?assertEqual(2, length(Tokens)),

    %% Check issuers
    ?assert(lists:member(Issuer1, Issuers)),
    ?assertEqual(1, length(Issuers)).

clear_revocations() ->
    Token1 = <<"token-to-clear">>,
    Issuer1 = <<"did:key:z6MktoClear">>,

    %% Add revocations
    ok = esdb_revocation:revoke(Token1),
    ok = esdb_revocation:revoke_issuer(Issuer1),

    %% Verify they're revoked
    ?assert(esdb_revocation:is_revoked(Token1)),
    ?assert(esdb_revocation:is_issuer_revoked(Issuer1)),

    %% Clear all
    ok = esdb_revocation:clear(),

    %% Should no longer be revoked
    ?assertNot(esdb_revocation:is_revoked(Token1)),
    ?assertNot(esdb_revocation:is_issuer_revoked(Issuer1)),

    %% Revocations should be empty
    #{tokens := Tokens, issuers := Issuers} = esdb_revocation:get_revocations(),
    ?assertEqual([], Tokens),
    ?assertEqual([], Issuers).
