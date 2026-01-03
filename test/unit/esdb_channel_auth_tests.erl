%% @doc Unit tests for channel capability authorization
%%
%% Tests capability-based authorization for PubSub channels.
%% Tests run in reckon-db where both channel server and verifier are available.
%%
%% @author R. Lefever

-module(esdb_channel_auth_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("reckon_db_gater/include/esdb_capability_types.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

channel_auth_test_() ->
    {foreach,
        fun setup/0,
        fun cleanup/1,
        [
            {"publish with valid capability succeeds", fun publish_with_valid_cap/0},
            {"publish with invalid capability fails", fun publish_with_invalid_cap/0},
            {"publish with expired capability fails", fun publish_with_expired_cap/0},
            {"publish with wrong action fails", fun publish_with_wrong_action/0},
            {"publish with wrong resource fails", fun publish_with_wrong_resource/0},
            {"subscribe with valid capability succeeds", fun subscribe_with_valid_cap/0},
            {"subscribe with invalid capability fails", fun subscribe_with_invalid_cap/0},
            {"wildcard resource matches any topic", fun wildcard_resource_matches/0}
        ]
    }.

setup() ->
    %% Ensure required apps are started
    ok = application:ensure_started(crypto),

    %% Set capability mode to required for auth tests
    esdb_gater_config:set_capability_mode(required),

    %% Start pg scope for channel server
    case pg:start(esdb_channel_scope) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,

    %% Start a test channel
    {ok, Pid} = esdb_channel_server:start_link(test_channel_impl, #{
        name => test_auth_channel,
        realm => <<"test">>
    }),
    Pid.

cleanup(Pid) ->
    %% Stop the channel
    gen_server:stop(Pid),
    %% Reset capability mode to default
    esdb_gater_config:set_capability_mode(disabled),
    ok.

%%====================================================================
%% Test Helpers
%%====================================================================

create_test_identity() ->
    esdb_identity:generate().

create_test_capability(Issuer, Resource, Action, TTL) ->
    Audience = esdb_identity:did(create_test_identity()),
    Grants = [esdb_capability:grant(Resource, Action)],
    Cap = esdb_capability:create(Issuer, Audience, Grants, #{ttl => TTL}),
    SignedCap = esdb_capability:sign(Cap, esdb_identity:private_key(Issuer)),
    esdb_capability:encode(SignedCap, binary).

create_expired_capability(Issuer, Resource, Action) ->
    Audience = esdb_identity:did(create_test_identity()),
    Grants = [esdb_capability:grant(Resource, Action)],
    Cap = esdb_capability:create(Issuer, Audience, Grants, #{ttl => 60}),
    %% Manually set expiration to the past
    ExpiredCap = Cap#capability{
        exp = erlang:system_time(second) - 3600,
        iat = erlang:system_time(second) - 3660
    },
    SignedCap = esdb_capability:sign(ExpiredCap, esdb_identity:private_key(Issuer)),
    esdb_capability:encode(SignedCap, binary).

%%====================================================================
%% Publish Tests
%%====================================================================

publish_with_valid_cap() ->
    Issuer = create_test_identity(),
    Resource = <<"esdb://test/channel/test_auth_channel/events.user.created">>,
    Token = create_test_capability(Issuer, Resource, ?ACTION_CHANNEL_PUBLISH, 900),

    Result = esdb_channel_server:publish(
        test_auth_channel,
        <<"events.user.created">>,
        #{event => <<"user_created">>},
        Token
    ),
    ?assertEqual(ok, Result).

publish_with_invalid_cap() ->
    %% Create a token with tampered signature
    Issuer = create_test_identity(),
    Resource = <<"esdb://test/channel/test_auth_channel/events.*">>,
    Grants = [esdb_capability:grant(Resource, ?ACTION_CHANNEL_PUBLISH)],
    Audience = esdb_identity:did(create_test_identity()),
    Cap = esdb_capability:create(Issuer, Audience, Grants, #{ttl => 900}),
    SignedCap = esdb_capability:sign(Cap, esdb_identity:private_key(Issuer)),

    %% Tamper with signature
    TamperedCap = SignedCap#capability{sig = crypto:strong_rand_bytes(64)},
    Token = esdb_capability:encode(TamperedCap, binary),

    Result = esdb_channel_server:publish(
        test_auth_channel,
        <<"events.user.created">>,
        #{event => <<"test">>},
        Token
    ),
    ?assertMatch({error, {unauthorized, _}}, Result).

publish_with_expired_cap() ->
    Issuer = create_test_identity(),
    Resource = <<"esdb://test/channel/test_auth_channel/*">>,
    Token = create_expired_capability(Issuer, Resource, ?ACTION_CHANNEL_PUBLISH),

    Result = esdb_channel_server:publish(
        test_auth_channel,
        <<"events.test">>,
        #{event => <<"test">>},
        Token
    ),
    ?assertMatch({error, {unauthorized, {expired, _}}}, Result).

publish_with_wrong_action() ->
    Issuer = create_test_identity(),
    Resource = <<"esdb://test/channel/test_auth_channel/*">>,
    %% Token grants subscribe, not publish
    Token = create_test_capability(Issuer, Resource, ?ACTION_CHANNEL_SUBSCRIBE, 900),

    Result = esdb_channel_server:publish(
        test_auth_channel,
        <<"events.test">>,
        #{event => <<"test">>},
        Token
    ),
    ?assertMatch({error, {unauthorized, {insufficient_permissions, _}}}, Result).

publish_with_wrong_resource() ->
    Issuer = create_test_identity(),
    %% Token grants access to different channel
    Resource = <<"esdb://test/channel/other_channel/*">>,
    Token = create_test_capability(Issuer, Resource, ?ACTION_CHANNEL_PUBLISH, 900),

    Result = esdb_channel_server:publish(
        test_auth_channel,
        <<"events.test">>,
        #{event => <<"test">>},
        Token
    ),
    ?assertMatch({error, {unauthorized, {insufficient_permissions, _}}}, Result).

%%====================================================================
%% Subscribe Tests
%%====================================================================

subscribe_with_valid_cap() ->
    Issuer = create_test_identity(),
    Resource = <<"esdb://test/channel/test_auth_channel/events.*">>,
    Token = create_test_capability(Issuer, Resource, ?ACTION_CHANNEL_SUBSCRIBE, 900),

    Result = esdb_channel_server:subscribe(
        test_auth_channel,
        <<"events.user.created">>,
        self(),
        Token
    ),
    ?assertEqual(ok, Result),

    %% Cleanup - unsubscribe
    esdb_channel_server:unsubscribe(test_auth_channel, <<"events.user.created">>, self()).

subscribe_with_invalid_cap() ->
    %% Empty/invalid token
    Token = <<"invalid_token">>,

    Result = esdb_channel_server:subscribe(
        test_auth_channel,
        <<"events.test">>,
        self(),
        Token
    ),
    ?assertMatch({error, {unauthorized, _}}, Result).

%%====================================================================
%% Wildcard Tests
%%====================================================================

wildcard_resource_matches() ->
    Issuer = create_test_identity(),
    %% Wildcard grants access to all topics on this channel
    Resource = <<"esdb://test/channel/test_auth_channel/*">>,
    Token = create_test_capability(Issuer, Resource, ?ACTION_CHANNEL_PUBLISH, 900),

    %% Should work for any topic
    Result1 = esdb_channel_server:publish(
        test_auth_channel,
        <<"events.user.created">>,
        #{event => <<"test1">>},
        Token
    ),
    ?assertEqual(ok, Result1),

    Result2 = esdb_channel_server:publish(
        test_auth_channel,
        <<"metrics.cpu.usage">>,
        #{event => <<"test2">>},
        Token
    ),
    ?assertEqual(ok, Result2).
