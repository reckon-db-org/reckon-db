%% @doc Integration test for single-node quorum health reporting
%%
%% Found live on macula-realm's production deployment (2026-09-09):
%% `reckon_db_cluster:health_check/1' reported `status => no_quorum',
%% `available_nodes => 0' against `total_nodes => 1' for a store that
%% was, by every other measure, completely healthy (elected leader,
%% recovered in 2ms). Numbers were not transient/still-catching-up --
%% structurally wrong for the single-node case, permanently blocking a
%% health-gated feature that must return `true' eventually.
%%
%% Root cause: `is_node_available/1' trusts an exact `Node =:= node()'
%% match for the local node, falling back to `net_adm:ping/1'
%% otherwise. A genuinely single-node deployment's persisted Ra
%% membership entry can drift from the CURRENT `node()' across a
%% restart -- most commonly a container whose hostname changes on
%% every recreate, which `RELEASE_NODE' folds straight into the
%% distributed node name. The local Ra server can be perfectly healthy
%% under its new identity while BOTH the exact-match AND the ping (which
%% needs the OLD, now-nonexistent node to answer) fail -- `available_nodes'
%% then reads 0 forever, with no possible transient recovery, since
%% nothing ever un-drifts the persisted identity.
%%
%% The fix: for a single-member cluster, skip node-identity matching
%% entirely and ask the local Ra server directly whether it responds --
%% the same bounded liveness probe `reckon_db_cluster:local_healthy/1'
%% already uses for ITS local check (which itself doesn't fit here,
%% since it separately requires `length(Members) > 1' -- a genuinely
%% single-node deployment never satisfies that either).
%%
%% @author rgfaber

-module(reckon_db_single_node_quorum_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

%% CT callbacks
-export([
    all/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    healthy_single_node_reports_quorum/1,
    single_node_available_despite_node_identity_mismatch/1,
    single_node_unavailable_when_store_id_unknown_to_ra/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {seconds, 30}}].

all() ->
    [
        healthy_single_node_reports_quorum,
        single_node_available_despite_node_identity_mismatch,
        single_node_unavailable_when_store_id_unknown_to_ra
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    RaDataDir = "/tmp/reckon_db_single_node_quorum_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),

    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),

    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    RaDataDir = proplists:get_value(ra_data_dir, Config),
    os:cmd("rm -rf " ++ RaDataDir),
    ok.

init_per_testcase(_TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_single_node_quorum_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom("single_node_quorum_" ++ Rand),

    {ok, _} = khepri:start(DataDir, StoreId),
    ok = wait_for_leader(StoreId, 5000),

    [{data_dir, DataDir}, {store_id, StoreId} | Config].

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    DataDir = proplists:get_value(data_dir, Config),
    catch khepri:stop(StoreId),
    os:cmd("rm -rf " ++ DataDir),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc GIVEN a genuinely single-node store with an elected leader
%%      WHEN we check its quorum status
%%      THEN it reports has_quorum, status healthy, available_nodes 1
%%
%%      Sanity check for the common case -- the local node's identity
%%      DOES match the persisted membership entry here (same process,
%%      just started it), so this passed before the fix too. Guards
%%      against a future change breaking the unremarkable path while
%%      fixing the drifted-identity one.
healthy_single_node_reports_quorum(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    {ok, Status} = reckon_db_consistency_checker:get_quorum_status(StoreId),

    ?assertEqual(true, maps:get(has_quorum, Status)),
    ?assertEqual(1, maps:get(total_nodes, Status)),
    ?assertEqual(1, maps:get(available_nodes, Status)),
    ok.

%% @doc GIVEN a real, locally-running single-node store
%%      WHEN count_available_nodes/2 is asked about a member list
%%           naming a node that is NOT the current node() (simulating
%%           a container hostname that changed since the membership
%%           entry was persisted)
%%      THEN it still reports the node as available, because it
%%           consults the LOCAL Ra server directly instead of matching
%%           identities
%%
%%      This is the core regression test for the production bug: before
%%      the fix, this returned 0 (neither `Node =:= node()' nor
%%      `net_adm:ping/1' can succeed against a node that was never
%%      distributed or no longer exists), permanently reporting
%%      no_quorum for a store that was actually completely healthy.
single_node_available_despite_node_identity_mismatch(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    DriftedMembers = [{StoreId, 'stale_hostname_from_a_previous_container@nowhere'}],

    ?assertEqual(1, reckon_db_consistency_checker:count_available_nodes(StoreId, DriftedMembers)),
    ok.

%% @doc GIVEN a store id Ra has never heard of
%%      WHEN count_available_nodes/2 is asked about a single-member
%%           list for it
%%      THEN it reports 0 available -- the local-probe fallback must
%%           still fail closed for a store that genuinely isn't there,
%%           not report every unknown store id as trivially healthy
single_node_unavailable_when_store_id_unknown_to_ra(_Config) ->
    UnknownStoreId = never_started_single_node_quorum_store,
    Members = [{UnknownStoreId, node()}],

    ?assertEqual(0, reckon_db_consistency_checker:count_available_nodes(UnknownStoreId, Members)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

wait_for_leader(StoreId, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_leader_loop(StoreId, Deadline).

wait_for_leader_loop(StoreId, Deadline) ->
    case ra_leaderboard:lookup_leader(StoreId) of
        undefined ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true -> ct:fail("Leader did not activate within timeout (store: ~p)", [StoreId]);
                false ->
                    timer:sleep(100),
                    wait_for_leader_loop(StoreId, Deadline)
            end;
        _Leader ->
            ok
    end.
