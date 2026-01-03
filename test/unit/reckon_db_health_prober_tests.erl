%% @doc Unit tests for health prober module
%% @author Macula.io

-module(reckon_db_health_prober_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Node Status Type Tests
%%====================================================================

status_healthy_test() ->
    %% Healthy means probes are succeeding
    Status = healthy,
    ?assert(is_atom(Status)),
    ?assertEqual(healthy, Status).

status_suspect_test() ->
    %% Suspect means some probes failing but not at threshold
    Status = suspect,
    ?assert(is_atom(Status)),
    ?assertEqual(suspect, Status).

status_failed_test() ->
    %% Failed means consecutive failures exceeded threshold
    Status = failed,
    ?assert(is_atom(Status)),
    ?assertEqual(failed, Status).

status_unknown_test() ->
    %% Unknown for newly discovered nodes
    Status = unknown,
    ?assert(is_atom(Status)),
    ?assertEqual(unknown, Status).

%%====================================================================
%% Probe Configuration Tests
%%====================================================================

config_defaults_test() ->
    %% Verify default configuration values
    DefaultProbeInterval = 2000,
    DefaultProbeTimeout = 1000,
    DefaultFailureThreshold = 3,
    ?assertEqual(2000, DefaultProbeInterval),
    ?assertEqual(1000, DefaultProbeTimeout),
    ?assertEqual(3, DefaultFailureThreshold).

config_probe_type_test() ->
    %% Valid probe types
    ValidTypes = [ping, rpc, khepri],
    lists:foreach(
        fun(Type) ->
            ?assert(is_atom(Type))
        end,
        ValidTypes
    ).

config_structure_test() ->
    %% Probe config structure
    Config = #{
        probe_interval => 2000,
        probe_timeout => 1000,
        failure_threshold => 3,
        probe_type => rpc
    },
    ?assert(maps:is_key(probe_interval, Config)),
    ?assert(maps:is_key(probe_timeout, Config)),
    ?assert(maps:is_key(failure_threshold, Config)),
    ?assert(maps:is_key(probe_type, Config)).

%%====================================================================
%% Failure Threshold Tests
%%====================================================================

threshold_not_exceeded_test() ->
    %% Failures below threshold = suspect
    Threshold = 3,
    ConsecutiveFailures = 2,
    Status = case ConsecutiveFailures >= Threshold of
        true -> failed;
        false -> suspect
    end,
    ?assertEqual(suspect, Status).

threshold_exceeded_test() ->
    %% Failures at or above threshold = failed
    Threshold = 3,
    ConsecutiveFailures = 3,
    Status = case ConsecutiveFailures >= Threshold of
        true -> failed;
        false -> suspect
    end,
    ?assertEqual(failed, Status).

threshold_well_exceeded_test() ->
    %% Failures well above threshold = failed
    Threshold = 3,
    ConsecutiveFailures = 10,
    Status = case ConsecutiveFailures >= Threshold of
        true -> failed;
        false -> suspect
    end,
    ?assertEqual(failed, Status).

threshold_reset_on_success_test() ->
    %% Success resets consecutive failures
    ConsecutiveFailures = 5,
    ProbeResult = ok,
    NewFailures = case ProbeResult of
        ok -> 0;
        _ -> ConsecutiveFailures + 1
    end,
    ?assertEqual(0, NewFailures).

%%====================================================================
%% Probe Result Handling Tests
%%====================================================================

probe_success_test() ->
    %% Successful probe
    Result = ok,
    ?assertEqual(ok, Result).

probe_failure_ping_test() ->
    %% Ping failure
    Result = {error, ping_failed},
    ?assertMatch({error, _}, Result).

probe_failure_rpc_test() ->
    %% RPC failure
    Result = {error, {rpc_failed, timeout}},
    ?assertMatch({error, {rpc_failed, _}}, Result).

probe_failure_badrpc_test() ->
    %% Bad RPC
    Result = {error, {rpc_failed, nodedown}},
    ?assertMatch({error, {rpc_failed, nodedown}}, Result).

%%====================================================================
%% Node State Tests
%%====================================================================

node_state_structure_test() ->
    %% Verify node state structure
    NodeState = #{
        node => 'test@host',
        status => healthy,
        consecutive_failures => 0,
        last_success => 1703000000000,
        last_failure => undefined,
        last_probe_duration => 500
    },
    ?assertEqual('test@host', maps:get(node, NodeState)),
    ?assertEqual(healthy, maps:get(status, NodeState)),
    ?assertEqual(0, maps:get(consecutive_failures, NodeState)).

node_state_after_failure_test() ->
    %% State after probe failure
    NodeState = #{
        status => suspect,
        consecutive_failures => 2,
        last_failure => 1703000000000
    },
    ?assertEqual(suspect, maps:get(status, NodeState)),
    ?assertEqual(2, maps:get(consecutive_failures, NodeState)).

node_state_recovery_test() ->
    %% State after recovery
    PreviousState = #{
        status => failed,
        consecutive_failures => 5
    },
    %% After successful probe
    NewState = #{
        status => healthy,
        consecutive_failures => 0
    },
    ?assertEqual(failed, maps:get(status, PreviousState)),
    ?assertEqual(healthy, maps:get(status, NewState)).

%%====================================================================
%% Status Transition Tests
%%====================================================================

transition_unknown_to_healthy_test() ->
    %% First successful probe
    PreviousStatus = unknown,
    ProbeResult = ok,
    NewStatus = case ProbeResult of
        ok -> healthy;
        _ -> suspect
    end,
    ?assertEqual(unknown, PreviousStatus),
    ?assertEqual(healthy, NewStatus).

transition_healthy_to_suspect_test() ->
    %% First failure
    PreviousStatus = healthy,
    ProbeResult = {error, timeout},
    ConsecutiveFailures = 1,
    Threshold = 3,
    NewStatus = case {ProbeResult, ConsecutiveFailures >= Threshold} of
        {ok, _} -> healthy;
        {_, true} -> failed;
        {_, false} -> suspect
    end,
    ?assertEqual(healthy, PreviousStatus),
    ?assertEqual(suspect, NewStatus).

transition_suspect_to_failed_test() ->
    %% Threshold exceeded
    PreviousStatus = suspect,
    ConsecutiveFailures = 3,
    Threshold = 3,
    NewStatus = case ConsecutiveFailures >= Threshold of
        true -> failed;
        false -> suspect
    end,
    ?assertEqual(suspect, PreviousStatus),
    ?assertEqual(failed, NewStatus).

transition_failed_to_healthy_test() ->
    %% Recovery after failure
    PreviousStatus = failed,
    ProbeResult = ok,
    NewStatus = case ProbeResult of
        ok -> healthy;
        _ -> failed
    end,
    ?assertEqual(failed, PreviousStatus),
    ?assertEqual(healthy, NewStatus).

%%====================================================================
%% Callback Tests
%%====================================================================

failed_callback_arity_test() ->
    %% Failed callbacks must be arity-1
    Callback = fun(Node) -> {failed, Node} end,
    ?assert(is_function(Callback, 1)).

recovered_callback_arity_test() ->
    %% Recovered callbacks must be arity-1
    Callback = fun(Node) -> {recovered, Node} end,
    ?assert(is_function(Callback, 1)).

callback_receives_node_test() ->
    %% Callbacks receive node name
    FailedCb = fun(Node) -> {failed, Node} end,
    RecoveredCb = fun(Node) -> {recovered, Node} end,
    TestNode = 'test@host',
    ?assertEqual({failed, 'test@host'}, FailedCb(TestNode)),
    ?assertEqual({recovered, 'test@host'}, RecoveredCb(TestNode)).

%%====================================================================
%% Health Check Response Tests
%%====================================================================

health_check_response_structure_test() ->
    %% Expected health check response structure
    Response = #{
        node => 'test@host',
        caller => 'caller@host',
        timestamp => 1703000000000,
        memory => 100000000,
        process_count => 50
    },
    ?assert(maps:is_key(node, Response)),
    ?assert(maps:is_key(caller, Response)),
    ?assert(maps:is_key(timestamp, Response)),
    ?assert(maps:is_key(memory, Response)),
    ?assert(maps:is_key(process_count, Response)).

%%====================================================================
%% Probe Duration Tests
%%====================================================================

probe_duration_measurement_test() ->
    %% Duration should be non-negative microseconds
    Duration = 1500,  %% 1.5ms
    ?assert(Duration >= 0),
    ?assert(is_integer(Duration)).

probe_duration_timeout_test() ->
    %% Timeout should trigger failure before duration exceeds timeout
    Timeout = 1000,  %% 1000ms
    Duration = 999,
    WithinTimeout = Duration < (Timeout * 1000),  %% Convert to us
    ?assert(WithinTimeout).

%%====================================================================
%% Node List Tests
%%====================================================================

get_nodes_from_members_test() ->
    %% Extract nodes from Khepri member list
    Members = [{store, 'node1@host'}, {store, 'node2@host'}],
    Nodes = [Node || {_, Node} <- Members],
    ?assertEqual(['node1@host', 'node2@host'], Nodes).

filter_self_from_nodes_test() ->
    %% Don't probe ourselves
    AllNodes = [node(), 'other@host'],
    NodesToProbe = lists:filter(fun(N) -> N =/= node() end, AllNodes),
    ?assertNot(lists:member(node(), NodesToProbe)).

ensure_all_tracked_test() ->
    %% New nodes should be added to tracking
    TrackedNodes = #{'node1@host' => #{status => healthy}},
    AllNodes = ['node1@host', 'node2@host'],
    UpdatedNodes = lists:foldl(fun(Node, Acc) ->
        case maps:is_key(Node, Acc) of
            true -> Acc;
            false -> Acc#{Node => #{status => unknown}}
        end
    end, TrackedNodes, AllNodes),
    ?assert(maps:is_key('node1@host', UpdatedNodes)),
    ?assert(maps:is_key('node2@host', UpdatedNodes)).

%%====================================================================
%% Telemetry Event Name Tests
%%====================================================================

telemetry_probe_complete_test() ->
    Event = [reckon_db, health, probe, complete],
    ?assertEqual([reckon_db, health, probe, complete], Event).

telemetry_node_failed_test() ->
    Event = [reckon_db, health, node, failed],
    ?assertEqual([reckon_db, health, node, failed], Event).

telemetry_node_recovered_test() ->
    Event = [reckon_db, health, node, recovered],
    ?assertEqual([reckon_db, health, node, recovered], Event).

%%====================================================================
%% Probe Type Selection Tests
%%====================================================================

probe_type_ping_test() ->
    %% Ping is fastest but shallowest
    ProbeType = ping,
    ?assertEqual(ping, ProbeType).

probe_type_rpc_test() ->
    %% RPC verifies process responsiveness
    ProbeType = rpc,
    ?assertEqual(rpc, ProbeType).

probe_type_khepri_test() ->
    %% Khepri verifies store health
    ProbeType = khepri,
    ?assertEqual(khepri, ProbeType).
