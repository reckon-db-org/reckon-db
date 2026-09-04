%% @doc Cluster-level health and consistency facade.
%%
%% Stable surface for cluster-wide checks that
%% `reckon_db_gateway_worker' (and therefore the gateway's
%% `HealthService' RPCs) consumes. Delegates to the underlying
%% primitives in [[reckon_db_consistency_checker]] and ra/khepri.
%%
%% This module deliberately does NOT depend on the
%% `reckon_db_consistency_checker' gen_server being supervised — the
%% per-call entry points (`verify_membership_consensus/1', etc.) are
%% pure functions that gather state from ra/khepri on demand. That
%% makes the facade safe to call in both `single' and `cluster' modes,
%% and avoids a dependency on the periodic-checker actually running.
%%
%% Historical note: prior to reckon_db 2.2.1, `reckon_db_gateway_worker'
%% referenced this module's four functions, but the module itself was
%% never extracted from the legacy `esdb_cluster' rename in 2.0.0.
%% The dangling references caused `HealthService.Check' and the three
%% `VerifyXxx' RPCs to hang in the retry loop until the gRPC client
%% timed out. 2.2.1 ships this facade and gets those handlers working.
-module(reckon_db_cluster).

-export([
    health_check/1,
    local_healthy/1,
    verify_consistency/1,
    verify_membership/1,
    check_log_consistency/1
]).

-define(LOCAL_PROBE_TIMEOUT, 2000).

%% @doc Quick health check — does the store have quorum and a leader?
%%
%% Cheap. No RPCs to other nodes. Suitable for liveness probes and
%% the gateway's `HealthService.Check' RPC.
%%
%% Returns `{ok, #{status => healthy | degraded | no_quorum, ...}}'
%% on success, `{error, Reason}' when the store isn't reachable at
%% all (e.g. coordinator not started, store not known to ra).
-spec health_check(atom()) -> {ok, map()} | {error, term()}.
health_check(StoreId) ->
    case reckon_db_consistency_checker:get_quorum_status(StoreId) of
        {ok, #{has_quorum := HasQuorum} = Quorum} ->
            LeaderStatus = leader_status(ra_leaderboard:lookup_leader(StoreId)),
            Status = classify_health(HasQuorum, LeaderStatus),
            {ok, Quorum#{status => Status, leader => LeaderStatus,
                         self_healing => reckon_db_store_healer:status(StoreId)}};
        {error, Reason} ->
            {error, Reason}
    end.

leader_status(undefined) -> no_leader;
leader_status(_Leader) -> has_leader.

%% @doc Wedge-proof LOCAL health predicate.
%%
%% True when there is an elected leader that THIS node is locally clustered
%% with (>1 members), and the local ra server answers a members query within a
%% hard bound. Uses ONLY the lock-free `ra_leaderboard' ETS table plus a
%% bounded `ra:members/2' liveness probe — it never calls
%% `khepri_cluster:members' / `get_quorum_status', which do an unbounded
%% `statem_call' into a possibly-wedged local ra server. Callers that poll
%% health in a hot loop (the coordinator's reconcile, the healer) MUST use this
%% rather than `health_check/1', or they inherit the wedge of the very store
%% they are trying to fix.
-spec local_healthy(atom()) -> boolean().
local_healthy(StoreId) ->
    case ra_leaderboard:lookup_leader(StoreId) of
        undefined ->
            false;
        Leader ->
            Members = ra_leaderboard:lookup_members(StoreId),
            is_list(Members)
                andalso length(Members) > 1
                andalso lists:member(Leader, Members)
                andalso local_responsive(StoreId)
    end.

%% @private Does the local ra server answer a members query within a hard
%% bound? A wedged server yields `false' fast rather than blocking the caller.
-spec local_responsive(atom()) -> boolean().
local_responsive(StoreId) ->
    case try ra:members({StoreId, node()}, ?LOCAL_PROBE_TIMEOUT)
         catch _:_ -> probe_failed
         end of
        {ok, _Members, _Leader} -> true;
        _                       -> false
    end.

%% @doc Full cluster consistency check.
%%
%% Combines membership consensus + leader consensus into a single
%% verdict. Quorum is checked first as a cheap precondition. Used by
%% the gateway's `HealthService.VerifyClusterConsistency' RPC.
%%
%% Returns `{ok, #{status => healthy | degraded | split_brain | no_quorum,
%% membership => ..., leader => ...}}' or `{error, Reason}'.
-spec verify_consistency(atom()) -> {ok, map()} | {error, term()}.
verify_consistency(StoreId) ->
    case reckon_db_consistency_checker:get_quorum_status(StoreId) of
        {ok, #{has_quorum := false} = Q} ->
            {ok, Q#{status => no_quorum}};
        {ok, Quorum} ->
            Membership = normalize(reckon_db_consistency_checker:verify_membership_consensus(StoreId)),
            Leader = normalize(reckon_db_consistency_checker:verify_leader_consensus(StoreId)),
            Status = overall_status(Membership, Leader),
            {ok, #{
                status     => Status,
                quorum     => Quorum,
                membership => check_to_map(Membership),
                leader     => check_to_map(Leader)
            }};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Verify membership consensus.
%%
%% Collects each node's view of cluster membership via RPC and
%% confirms they agree. Used by the gateway's
%% `HealthService.VerifyMembershipConsensus' RPC.
-spec verify_membership(atom()) -> {ok, map()} | {error, term()}.
verify_membership(StoreId) ->
    normalize(reckon_db_consistency_checker:verify_membership_consensus(StoreId)).

%% @doc Verify Raft log consistency across followers.
%%
%% Collects per-follower term/index stats and checks for replication
%% divergence. Used by the gateway's `HealthService.CheckRaftLogConsistency'
%% RPC.
-spec check_log_consistency(atom()) -> {ok, map()} | {error, term()}.
check_log_consistency(StoreId) ->
    normalize(reckon_db_consistency_checker:verify_raft_consistency(StoreId)).

%%====================================================================
%% Internal
%%====================================================================

classify_health(true,  has_leader) -> healthy;
classify_health(true,  no_leader)  -> degraded;
classify_health(false, _)          -> no_quorum.

overall_status({ok, #{status := split_brain}}, _) -> split_brain;
overall_status(_, {ok, #{status := split_brain}}) -> split_brain;
overall_status({ok, #{status := healthy}}, {ok, #{status := healthy}}) -> healthy;
overall_status({error, _}, _) -> degraded;
overall_status(_, {error, _}) -> degraded;
overall_status(_, _) -> degraded.

%% @private The consistency_checker uses `consensus' to mean
%% "all nodes agree" and `no_consensus' to mean "they don't". This
%% facade exposes the gateway's vocabulary: `healthy | degraded |
%% split_brain | no_quorum'. Rewrite the status atom in-place.
normalize({ok, #{status := Status} = Map}) ->
    {ok, Map#{status => normalize_status(Status)}};
normalize(Other) ->
    Other.

normalize_status(consensus)    -> healthy;
normalize_status(no_consensus) -> split_brain;
normalize_status(Other)        -> Other.

check_to_map({ok, Map}) -> Map;
check_to_map({error, Reason}) -> #{error => Reason}.
