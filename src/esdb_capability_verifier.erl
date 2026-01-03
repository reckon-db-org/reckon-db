%% @doc Server-side capability token verification for reckon-db.
%%
%% Verifies UCAN-inspired capability tokens for authorization decisions.
%% Tokens are created client-side (reckon-gater) and verified server-side here.
%%
%% Verification steps:
%% <ol>
%% <li>Decode token (JWT or binary format, auto-detected)</li>
%% <li>Verify Ed25519 signature using issuer's public key from DID</li>
%% <li>Check token is not expired (exp less than now)</li>
%% <li>Check token is active (nbf less than or equal to now, if present)</li>
%% <li>Check token is not revoked (via gossip list)</li>
%% <li>Match resource URI against request</li>
%% <li>Match action against permitted actions</li>
%% </ol>
%%
%% @author rgfaber
%% @see esdb_capability
%% @see esdb_identity

-module(esdb_capability_verifier).

-include_lib("reckon_gater/include/esdb_capability_types.hrl").

%% API
-export([verify/1, verify/2]).
-export([authorize/3, authorize/4]).
-export([check_permission/3]).
-export([is_revoked/1]).
-export([extract_token_cid/1]).

%% For testing
-export([match_resource/2, match_action/2]).

%% Verification options
-type verify_opts() :: #{
    skip_signature => boolean(),      %% Skip signature verification (testing only)
    skip_revocation => boolean(),     %% Skip revocation check
    now => integer()                  %% Override current time (testing)
}.

%%====================================================================
%% API
%%====================================================================

%% @doc Verify a capability token
%%
%% Decodes the token and verifies:
%% - Signature is valid (Ed25519)
%% - Token is not expired
%% - Token is not revoked
%%
%% Does NOT check permissions against a specific resource/action.
%% Use authorize/3 for full authorization.
-spec verify(binary()) -> {ok, capability()} | {error, capability_error()}.
verify(Token) ->
    verify(Token, #{}).

%% @doc Verify a capability token with options
-spec verify(binary(), verify_opts()) -> {ok, capability()} | {error, capability_error()}.
verify(Token, Opts) ->
    case esdb_capability:decode(Token) of
        {ok, Cap} ->
            verify_capability(Cap, Opts);
        {error, Reason} ->
            {error, {parse_error, Reason}}
    end.

%% @doc Authorize a request with a capability token
%%
%% Verifies the token AND checks it grants permission for the
%% specified resource and action.
-spec authorize(binary(), binary(), binary()) ->
    {ok, verification_result()} | {error, capability_error()}.
authorize(Token, Resource, Action) ->
    authorize(Token, Resource, Action, #{}).

%% @doc Authorize a request with options
-spec authorize(binary(), binary(), binary(), verify_opts()) ->
    {ok, verification_result()} | {error, capability_error()}.
authorize(Token, Resource, Action, Opts) ->
    case verify(Token, Opts) of
        {ok, Cap} ->
            case check_permission(Cap, Resource, Action) of
                ok ->
                    Result = #verification_result{
                        capability = Cap,
                        issuer_chain = [Cap#capability.iss],
                        resource = Resource,
                        action = Action,
                        verified_at = erlang:system_time(second)
                    },
                    {ok, Result};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Check if a verified capability grants permission for resource/action
%%
%% The capability should already be verified (signature, expiration).
%% This function only checks the grants against the requested resource/action.
-spec check_permission(capability(), binary(), binary()) ->
    ok | {error, capability_error()}.
check_permission(#capability{att = Grants}, Resource, Action) ->
    case find_matching_grant(Grants, Resource, Action) of
        {ok, _Grant} ->
            ok;
        not_found ->
            {error, {insufficient_permissions,
                iolist_to_binary([
                    <<"No grant found for ">>, Resource, <<" / ">>, Action
                ])}}
    end.

%% @doc Check if a token CID is revoked
%%
%% Currently returns false (not revoked) as revocation gossip is not yet implemented.
%% This will be integrated with a gossip-based revocation list in Phase 4.
-spec is_revoked(binary()) -> boolean().
is_revoked(_TokenCID) ->
    %% TODO: Implement gossip-based revocation check
    %% For now, tokens are not considered revoked
    %% This will be replaced with:
    %% esdb_revocation:is_revoked(TokenCID)
    false.

%% @doc Extract a content-addressed identifier for a token
%%
%% Uses SHA-256 hash of the token's core fields (excluding signature).
%% This CID can be used for revocation.
-spec extract_token_cid(capability()) -> binary().
extract_token_cid(#capability{iss = Iss, aud = Aud, exp = Exp, nnc = Nnc}) ->
    %% Hash core identifying fields
    Data = term_to_binary({Iss, Aud, Exp, Nnc}),
    Hash = crypto:hash(sha256, Data),
    %% Return base64url encoded hash
    base64:encode(Hash, #{mode => urlsafe, padding => false}).

%%====================================================================
%% Internal - Verification
%%====================================================================

%% @private
-spec verify_capability(capability(), verify_opts()) ->
    {ok, capability()} | {error, capability_error()}.
verify_capability(Cap, Opts) ->
    Now = maps:get(now, Opts, erlang:system_time(second)),

    %% Check expiration first (fast path for expired tokens)
    case check_expiration(Cap, Now) of
        ok ->
            case check_not_before(Cap, Now) of
                ok ->
                    case maybe_verify_signature(Cap, Opts) of
                        ok ->
                            case maybe_check_revocation(Cap, Opts) of
                                ok -> {ok, Cap};
                                {error, _} = E -> E
                            end;
                        {error, _} = E -> E
                    end;
                {error, _} = E -> E
            end;
        {error, _} = E -> E
    end.

%% @private
-spec check_expiration(capability(), integer()) ->
    ok | {error, capability_error()}.
check_expiration(#capability{exp = Exp}, Now) when Exp > Now ->
    ok;
check_expiration(#capability{exp = Exp}, _Now) ->
    {error, {expired, Exp}}.

%% @private
-spec check_not_before(capability(), integer()) ->
    ok | {error, capability_error()}.
check_not_before(#capability{nbf = undefined}, _Now) ->
    ok;
check_not_before(#capability{nbf = Nbf}, Now) when Nbf =< Now ->
    ok;
check_not_before(#capability{nbf = Nbf}, _Now) ->
    {error, {not_yet_valid, Nbf}}.

%% @private
-spec maybe_verify_signature(capability(), verify_opts()) ->
    ok | {error, capability_error()}.
maybe_verify_signature(_Cap, #{skip_signature := true}) ->
    ok;
maybe_verify_signature(Cap, _Opts) ->
    verify_signature(Cap).

%% @private
-spec verify_signature(capability()) -> ok | {error, capability_error()}.
verify_signature(#capability{sig = undefined}) ->
    {error, {invalid_signature, <<"Token is not signed">>}};
verify_signature(#capability{iss = Iss, sig = Sig} = Cap) ->
    %% Extract public key from issuer DID
    case esdb_identity:public_key_from_did(Iss) of
        {ok, PubKey} ->
            %% Reconstruct the message that was signed
            Message = capability_to_signable(Cap),
            %% Verify Ed25519 signature
            case crypto:verify(eddsa, none, Message, Sig, [PubKey, ed25519]) of
                true -> ok;
                false -> {error, {invalid_signature, <<"Signature verification failed">>}}
            end;
        {error, Reason} ->
            {error, {invalid_signature, iolist_to_binary([
                <<"Cannot extract public key from DID: ">>,
                io_lib:format("~p", [Reason])
            ])}}
    end.

%% @private
%% Reconstruct the signable message from a capability
%% Must match the format used in esdb_capability:sign/2 (encode_payload_for_signing)
-spec capability_to_signable(capability()) -> binary().
capability_to_signable(#capability{} = Cap) ->
    %% Create the same payload format as esdb_capability:encode_payload_for_signing/1
    %% This is a map of all fields except signature, serialized as sorted pairs
    Payload = #{
        alg => Cap#capability.alg,
        typ => Cap#capability.typ,
        iss => Cap#capability.iss,
        aud => Cap#capability.aud,
        nbf => Cap#capability.nbf,
        exp => Cap#capability.exp,
        iat => Cap#capability.iat,
        nnc => Cap#capability.nnc,
        att => Cap#capability.att,
        fct => Cap#capability.fct,
        prf => Cap#capability.prf
    },
    %% Sort keys for deterministic serialization (must match esdb_capability)
    SortedPairs = lists:sort(maps:to_list(Payload)),
    term_to_binary(SortedPairs).

%% @private
-spec maybe_check_revocation(capability(), verify_opts()) ->
    ok | {error, capability_error()}.
maybe_check_revocation(_Cap, #{skip_revocation := true}) ->
    ok;
maybe_check_revocation(Cap, _Opts) ->
    CID = extract_token_cid(Cap),
    case is_revoked(CID) of
        false -> ok;
        true -> {error, {revoked, CID}}
    end.

%%====================================================================
%% Internal - Permission Checking
%%====================================================================

%% @private
-spec find_matching_grant([capability_grant()], binary(), binary()) ->
    {ok, capability_grant()} | not_found.
find_matching_grant([], _Resource, _Action) ->
    not_found;
find_matching_grant([Grant | Rest], Resource, Action) ->
    With = maps:get(with, Grant),
    Can = maps:get(can, Grant),
    case match_resource(With, Resource) andalso match_action(Can, Action) of
        true -> {ok, Grant};
        false -> find_matching_grant(Rest, Resource, Action)
    end.

%% @doc Match a resource pattern against a specific resource URI.
%%
%% Patterns support:
%% <ul>
%% <li>Exact match: esdb://realm/stream/orders matches only that path</li>
%% <li>Wildcard suffix: esdb://realm/stream/* matches any stream</li>
%% <li>Prefix wildcard: esdb://realm/stream/orders-* matches orders-123, orders-abc</li>
%% </ul>
%%
%% @private exported for testing
-spec match_resource(binary(), binary()) -> boolean().
match_resource(Pattern, Resource) when Pattern =:= Resource ->
    true;
match_resource(Pattern, Resource) ->
    case binary:last(Pattern) of
        $* ->
            %% Pattern ends with *, check prefix
            Prefix = binary:part(Pattern, 0, byte_size(Pattern) - 1),
            binary:match(Resource, Prefix) =:= {0, byte_size(Prefix)};
        _ ->
            false
    end.

%% @doc Match an action pattern against a specific action.
%%
%% Actions support:
%% <ul>
%% <li>Exact match: stream/append matches only append</li>
%% <li>Wildcard: * matches any action</li>
%% </ul>
%%
%% @private exported for testing
-spec match_action(binary(), binary()) -> boolean().
match_action(<<"*">>, _Action) ->
    true;
match_action(Pattern, Action) ->
    Pattern =:= Action.

%%====================================================================
%% Tests (inline eunit)
%%====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

match_resource_exact_test() ->
    ?assert(match_resource(<<"esdb://realm/stream/orders">>, <<"esdb://realm/stream/orders">>)),
    ?assertNot(match_resource(<<"esdb://realm/stream/orders">>, <<"esdb://realm/stream/users">>)).

match_resource_wildcard_test() ->
    ?assert(match_resource(<<"esdb://realm/stream/*">>, <<"esdb://realm/stream/orders">>)),
    ?assert(match_resource(<<"esdb://realm/stream/*">>, <<"esdb://realm/stream/users">>)),
    ?assertNot(match_resource(<<"esdb://realm/stream/*">>, <<"esdb://realm/channel/events">>)).

match_resource_prefix_test() ->
    ?assert(match_resource(<<"esdb://realm/stream/orders-*">>, <<"esdb://realm/stream/orders-123">>)),
    ?assert(match_resource(<<"esdb://realm/stream/orders-*">>, <<"esdb://realm/stream/orders-abc">>)),
    ?assertNot(match_resource(<<"esdb://realm/stream/orders-*">>, <<"esdb://realm/stream/users-123">>)).

match_action_exact_test() ->
    ?assert(match_action(<<"stream/append">>, <<"stream/append">>)),
    ?assertNot(match_action(<<"stream/append">>, <<"stream/read">>)).

match_action_wildcard_test() ->
    ?assert(match_action(<<"*">>, <<"stream/append">>)),
    ?assert(match_action(<<"*">>, <<"channel/publish">>)).

-endif.
