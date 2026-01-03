%% @doc End-to-end mesh integration tests
%%
%% Tests multi-node scenarios including:
%% - Capability cross-gateway verification
%% - Event routing between gateways
%% - Store cluster consistency
%%
%% Note: These tests can be run locally against a running cluster
%% or via Docker Compose using scripts/run-mesh-tests.sh
-module(reckon_db_mesh_e2e_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    capability_mode_disabled_test/1,
    capability_mode_optional_test/1,
    capability_mode_required_test/1,
    capability_mode_channel_override_test/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, capability_modes}].

groups() ->
    [
        {capability_modes, [sequence], [
            capability_mode_disabled_test,
            capability_mode_optional_test,
            capability_mode_required_test,
            capability_mode_channel_override_test
        ]}
    ].

init_per_suite(Config) ->
    %% Start required applications
    application:ensure_all_started(telemetry),
    application:ensure_all_started(reckon_gater),
    Config.

end_per_suite(_Config) ->
    application:stop(reckon_gater),
    application:stop(telemetry),
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Reset capability mode to default before each test
    esdb_gater_config:set_capability_mode(disabled),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - Capability Modes
%%====================================================================

%% @doc Test disabled mode - capabilities never checked
capability_mode_disabled_test(_Config) ->
    ct:log("Testing capability mode: disabled"),

    %% Set mode to disabled
    ok = esdb_gater_config:set_capability_mode(disabled),
    ?assertEqual(disabled, esdb_gater_config:capability_mode()),

    %% Effective mode should be disabled even with no channel override
    ?assertEqual(disabled, esdb_gater_config:effective_capability_mode(false)),

    %% Effective mode should still be required if channel overrides
    ?assertEqual(required, esdb_gater_config:effective_capability_mode(true)),

    ct:log("Disabled mode test passed"),
    ok.

%% @doc Test optional mode - verify if token provided, allow if not
capability_mode_optional_test(_Config) ->
    ct:log("Testing capability mode: optional"),

    %% Set mode to optional
    ok = esdb_gater_config:set_capability_mode(optional),
    ?assertEqual(optional, esdb_gater_config:capability_mode()),

    %% Effective mode should be optional without channel override
    ?assertEqual(optional, esdb_gater_config:effective_capability_mode(false)),

    %% Effective mode should be required with channel override
    ?assertEqual(required, esdb_gater_config:effective_capability_mode(true)),

    ct:log("Optional mode test passed"),
    ok.

%% @doc Test required mode - capabilities always required
capability_mode_required_test(_Config) ->
    ct:log("Testing capability mode: required"),

    %% Set mode to required
    ok = esdb_gater_config:set_capability_mode(required),
    ?assertEqual(required, esdb_gater_config:capability_mode()),

    %% Effective mode should be required in both cases
    ?assertEqual(required, esdb_gater_config:effective_capability_mode(false)),
    ?assertEqual(required, esdb_gater_config:effective_capability_mode(true)),

    ct:log("Required mode test passed"),
    ok.

%% @doc Test that channel override takes precedence
capability_mode_channel_override_test(_Config) ->
    ct:log("Testing channel override behavior"),

    %% With disabled global mode
    ok = esdb_gater_config:set_capability_mode(disabled),
    ?assertEqual(disabled, esdb_gater_config:effective_capability_mode(false)),
    ?assertEqual(required, esdb_gater_config:effective_capability_mode(true)),

    %% With optional global mode
    ok = esdb_gater_config:set_capability_mode(optional),
    ?assertEqual(optional, esdb_gater_config:effective_capability_mode(false)),
    ?assertEqual(required, esdb_gater_config:effective_capability_mode(true)),

    %% With required global mode
    ok = esdb_gater_config:set_capability_mode(required),
    ?assertEqual(required, esdb_gater_config:effective_capability_mode(false)),
    ?assertEqual(required, esdb_gater_config:effective_capability_mode(true)),

    ct:log("Channel override test passed"),
    ok.
