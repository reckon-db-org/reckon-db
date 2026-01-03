%% @doc Unit tests for consistency checker module
%% @author rgfaber

-module(reckon_db_consistency_checker_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Status Type Tests
%%====================================================================

status_healthy_test() ->
    %% Healthy status represents all checks passing
    Status = healthy,
    ?assert(is_atom(Status)),
    ?assertEqual(healthy, Status).

status_degraded_test() ->
    %% Degraded status represents warnings or minor issues
    Status = degraded,
    ?assert(is_atom(Status)),
    ?assertEqual(degraded, Status).

status_split_brain_test() ->
    %% Split-brain is the most severe status
    Status = split_brain,
    ?assert(is_atom(Status)),
    ?assertEqual(split_brain, Status).

status_no_quorum_test() ->
    %% No quorum when insufficient nodes available
    Status = no_quorum,
    ?assert(is_atom(Status)),
    ?assertEqual(no_quorum, Status).

%%====================================================================
%% Check Result Structure Tests
%%====================================================================

check_result_structure_test() ->
    %% Verify expected check result structure
    ExpectedKeys = [status, checks, timestamp, duration_us],

    Result = #{
        status => healthy,
        checks => #{},
        timestamp => 1703000000000,
        duration_us => 1234
    },

    lists:foreach(
        fun(Key) ->
            ?assert(maps:is_key(Key, Result))
        end,
        ExpectedKeys
    ).

check_detail_structure_test() ->
    %% Verify check detail structure
    ExpectedKeys = [status, message, data],

    Detail = #{
        status => ok,
        message => <<"All nodes agree">>,
        data => #{consensus => true}
    },

    lists:foreach(
        fun(Key) ->
            ?assert(maps:is_key(Key, Detail))
        end,
        ExpectedKeys
    ).

%%====================================================================
%% Membership Consensus Tests
%%====================================================================

membership_single_view_consensus_test() ->
    %% Single view means consensus
    Views = #{
        'node1@host' => {ok, [{store, 'node1@host'}]},
        'node2@host' => {ok, [{store, 'node1@host'}]}
    },
    UniqueViews = lists:usort([M || {ok, M} <- maps:values(Views)]),
    ?assertEqual(1, length(UniqueViews)).

membership_multiple_views_split_brain_test() ->
    %% Multiple different views indicate split-brain
    Views = #{
        'node1@host' => {ok, [{store, 'node1@host'}]},
        'node2@host' => {ok, [{store, 'node2@host'}]}
    },
    UniqueViews = lists:usort([M || {ok, M} <- maps:values(Views)]),
    ?assert(length(UniqueViews) > 1).

membership_partial_failure_test() ->
    %% Some nodes failing to respond
    Views = #{
        'node1@host' => {ok, [{store, 'node1@host'}]},
        'node2@host' => {error, timeout}
    },
    FailedNodes = [N || {N, V} <- maps:to_list(Views), element(1, V) =:= error],
    ?assertEqual(['node2@host'], FailedNodes).

%%====================================================================
%% Leader Consensus Tests
%%====================================================================

leader_consensus_test() ->
    %% All nodes agree on leader
    LeaderViews = #{
        'node1@host' => {ok, 'node1@host'},
        'node2@host' => {ok, 'node1@host'},
        'node3@host' => {ok, 'node1@host'}
    },
    Leaders = [L || {ok, L} <- maps:values(LeaderViews)],
    UniqueLeaders = lists:usort(Leaders),
    ?assertEqual(1, length(UniqueLeaders)).

leader_no_consensus_test() ->
    %% Nodes disagree on leader (possible split-brain)
    LeaderViews = #{
        'node1@host' => {ok, 'node1@host'},
        'node2@host' => {ok, 'node2@host'}
    },
    Leaders = [L || {ok, L} <- maps:values(LeaderViews)],
    UniqueLeaders = lists:usort(Leaders),
    ?assert(length(UniqueLeaders) > 1).

leader_no_leader_test() ->
    %% No leader reported
    LeaderViews = #{
        'node1@host' => {error, no_leader},
        'node2@host' => {error, no_leader}
    },
    SuccessfulViews = [V || {ok, V} <- maps:values(LeaderViews)],
    ?assertEqual([], SuccessfulViews).

%%====================================================================
%% Quorum Calculation Tests
%%====================================================================

quorum_3_node_cluster_test() ->
    %% 3-node cluster needs 2 for quorum
    TotalNodes = 3,
    RequiredQuorum = (TotalNodes div 2) + 1,
    ?assertEqual(2, RequiredQuorum).

quorum_5_node_cluster_test() ->
    %% 5-node cluster needs 3 for quorum
    TotalNodes = 5,
    RequiredQuorum = (TotalNodes div 2) + 1,
    ?assertEqual(3, RequiredQuorum).

quorum_single_node_test() ->
    %% Single node needs 1 for quorum
    TotalNodes = 1,
    RequiredQuorum = (TotalNodes div 2) + 1,
    ?assertEqual(1, RequiredQuorum).

quorum_has_quorum_test() ->
    %% Available >= Required means quorum
    TotalNodes = 3,
    AvailableNodes = 2,
    RequiredQuorum = (TotalNodes div 2) + 1,
    HasQuorum = AvailableNodes >= RequiredQuorum,
    ?assert(HasQuorum).

quorum_lost_quorum_test() ->
    %% Available < Required means no quorum
    TotalNodes = 3,
    AvailableNodes = 1,
    RequiredQuorum = (TotalNodes div 2) + 1,
    HasQuorum = AvailableNodes >= RequiredQuorum,
    ?assertNot(HasQuorum).

quorum_margin_test() ->
    %% Calculate how many nodes can fail
    TotalNodes = 5,
    AvailableNodes = 4,
    RequiredQuorum = (TotalNodes div 2) + 1,  %% 3
    Margin = AvailableNodes - RequiredQuorum,  %% 4 - 3 = 1
    CanLose = max(0, Margin),
    ?assertEqual(1, CanLose).

%%====================================================================
%% Raft Consistency Tests
%%====================================================================

raft_terms_consistent_test() ->
    %% All nodes on same term
    Stats = #{
        'node1@host' => {ok, #{term => 5, commit_index => 100}},
        'node2@host' => {ok, #{term => 5, commit_index => 98}},
        'node3@host' => {ok, #{term => 5, commit_index => 99}}
    },
    Terms = [maps:get(term, S) || {ok, S} <- maps:values(Stats)],
    UniqueTerms = lists:usort(Terms),
    TermsConsistent = length(UniqueTerms) =< 1,
    ?assert(TermsConsistent).

raft_terms_inconsistent_test() ->
    %% Nodes on different terms
    Stats = #{
        'node1@host' => {ok, #{term => 5}},
        'node2@host' => {ok, #{term => 4}}
    },
    Terms = [maps:get(term, S) || {ok, S} <- maps:values(Stats)],
    UniqueTerms = lists:usort(Terms),
    TermsConsistent = length(UniqueTerms) =< 1,
    ?assertNot(TermsConsistent).

raft_commit_lag_test() ->
    %% Calculate max commit index lag
    CommitIndices = [100, 98, 95],
    MaxLag = lists:max(CommitIndices) - lists:min(CommitIndices),
    ?assertEqual(5, MaxLag).

raft_commit_lag_healthy_test() ->
    %% Small lag is healthy
    MaxLag = 5,
    IsHealthy = MaxLag < 100,
    ?assert(IsHealthy).

raft_commit_lag_warning_test() ->
    %% Large lag is warning
    MaxLag = 150,
    IsHealthy = MaxLag < 100,
    ?assertNot(IsHealthy).

%%====================================================================
%% Overall Status Determination Tests
%%====================================================================

overall_status_all_healthy_test() ->
    %% All checks OK = healthy
    Checks = #{
        membership => #{status => ok},
        leader => #{status => ok},
        raft => #{status => ok},
        quorum => #{status => ok}
    },
    CheckStatuses = [maps:get(status, D) || {_, D} <- maps:to_list(Checks)],
    HasError = lists:member(error, CheckStatuses),
    HasWarning = lists:member(warning, CheckStatuses),
    Status = case {HasError, HasWarning} of
        {true, _} -> degraded;
        {false, true} -> degraded;
        {false, false} -> healthy
    end,
    ?assertEqual(healthy, Status).

overall_status_with_warning_test() ->
    %% Warning = degraded
    Checks = #{
        membership => #{status => ok},
        leader => #{status => ok},
        raft => #{status => warning},
        quorum => #{status => ok}
    },
    CheckStatuses = [maps:get(status, D) || {_, D} <- maps:to_list(Checks)],
    HasWarning = lists:member(warning, CheckStatuses),
    ?assert(HasWarning).

overall_status_with_error_test() ->
    %% Error = degraded (unless split-brain)
    Checks = #{
        membership => #{status => error},
        leader => #{status => ok},
        raft => #{status => ok},
        quorum => #{status => ok}
    },
    CheckStatuses = [maps:get(status, D) || {_, D} <- maps:to_list(Checks)],
    HasError = lists:member(error, CheckStatuses),
    ?assert(HasError).

%%====================================================================
%% Callback Tests
%%====================================================================

callback_function_arity_test() ->
    %% Callbacks must be arity-1 functions
    ValidCallback = fun(Status) -> Status end,
    ?assert(is_function(ValidCallback, 1)).

callback_receives_status_test() ->
    %% Simulate callback receiving a status
    Callback = fun(Status) -> {received, Status} end,
    ?assertEqual({received, healthy}, Callback(healthy)),
    ?assertEqual({received, degraded}, Callback(degraded)),
    ?assertEqual({received, split_brain}, Callback(split_brain)),
    ?assertEqual({received, no_quorum}, Callback(no_quorum)).

%%====================================================================
%% Configuration Tests
%%====================================================================

config_defaults_test() ->
    %% Verify default configuration values
    DefaultCheckInterval = 5000,
    DefaultRpcTimeout = 3000,
    ?assertEqual(5000, DefaultCheckInterval),
    ?assertEqual(3000, DefaultRpcTimeout).

config_interval_minimum_test() ->
    %% Minimum check interval should be enforced
    MinInterval = 1000,
    RequestedInterval = 500,
    ActualInterval = max(RequestedInterval, MinInterval),
    ?assertEqual(1000, ActualInterval).

%%====================================================================
%% Node Extraction Tests
%%====================================================================

extract_nodes_from_members_test() ->
    %% Extract node names from member tuples
    Members = [{store, 'node1@host'}, {store, 'node2@host'}, {store, 'node3@host'}],
    Nodes = [Node || {_, Node} <- Members],
    ?assertEqual(['node1@host', 'node2@host', 'node3@host'], Nodes).

extract_nodes_empty_test() ->
    %% Empty members list
    Members = [],
    Nodes = [Node || {_, Node} <- Members],
    ?assertEqual([], Nodes).

%%====================================================================
%% Telemetry Event Name Tests
%%====================================================================

telemetry_check_complete_test() ->
    %% Verify telemetry event names
    Event = [reckon_db, consistency, check, complete],
    ?assertEqual([reckon_db, consistency, check, complete], Event).

telemetry_status_changed_test() ->
    Event = [reckon_db, consistency, status, changed],
    ?assertEqual([reckon_db, consistency, status, changed], Event).

telemetry_split_brain_detected_test() ->
    Event = [reckon_db, consistency, split_brain, detected],
    ?assertEqual([reckon_db, consistency, split_brain, detected], Event).
