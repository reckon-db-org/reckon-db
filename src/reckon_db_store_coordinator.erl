%% @doc Store coordinator for reckon-db
%%
%% Coordinates cluster join operations and prevents split-brain scenarios.
%%
%% Responsibilities:
%% - Detecting existing clusters via RPC
%% - Coordinator election (lowest node name)
%% - Coordinated cluster joining
%% - Split-brain prevention
%%
%% @author rgfaber

-module(reckon_db_store_coordinator).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([start_link/1]).
-export([join_cluster/1, join_cluster/2]).
-export([should_handle_nodeup/1]).
-export([members/1]).
-export([leader/1]).
-export([is_leader/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-ifdef(TEST).
-export([elect_coordinator/2, has_persisted_cluster/1]).
-endif.

-define(JOIN_TIMEOUT, 30000).
-define(RPC_TIMEOUT, 5000).
%% khepri_cluster:join/2 defaults to khepri's `default_timeout' which
%% is `infinity'. Combined with a global lock acquired during the
%% join, a simultaneous-boot cluster can wedge a node forever waiting
%% on the lock. Pass an explicit timeout less than the outer
%% gen_server:call's JOIN_TIMEOUT so a stuck join surfaces as an
%% error instead of an infinite hang.
-define(KHEPRI_JOIN_TIMEOUT, 20000).
%% Retry delay when initial join finds nothing to join yet (all peers
%% are still booting), or when the join failed transiently. With
%% jitter, simultaneous boots can stagger their join attempts.
-define(JOIN_RETRY_BASE_MS, 3000).
-define(JOIN_RETRY_MAX_MS,  8000).

-record(state, {
    store_id :: atom(),
    config :: store_config(),
    current_leader :: node() | undefined,
    join_status :: idle | joining | joined | coordinating
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:coordinator_name(StoreId),
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

%% @doc Join the Khepri cluster using coordinated approach.
%%
%% Returns {error, not_started} if the coordinator process isn't registered
%% yet (startup race or after a store stop). gen_server:call/3 throws
%% {exit, {noproc}} as an exception when the target process doesn't exist
%% — the guard converts that to an error tuple.
-spec join_cluster(atom()) -> ok | coordinator | no_nodes | waiting | failed | {error, term()}.
join_cluster(StoreId) ->
    Name = reckon_db_naming:coordinator_name(StoreId),
    case whereis(Name) of
        undefined -> {error, not_started};
        _Pid -> gen_server:call(Name, {join_cluster, StoreId}, ?JOIN_TIMEOUT)
    end.

%% @doc Join a specific node's cluster.
%%
%% Returns {error, not_started} if the coordinator process isn't registered.
-spec join_cluster(atom(), node()) -> ok | {error, term()}.
join_cluster(StoreId, TargetNode) ->
    Name = reckon_db_naming:coordinator_name(StoreId),
    case whereis(Name) of
        undefined -> {error, not_started};
        _Pid -> gen_server:call(Name, {join_cluster_node, StoreId, TargetNode}, ?JOIN_TIMEOUT)
    end.

%% @doc Check if this node should handle nodeup events.
%%
%% Returns false if the coordinator isn't running — a store without a
%% coordinator should not handle nodeup events.
-spec should_handle_nodeup(atom()) -> boolean().
should_handle_nodeup(StoreId) ->
    Name = reckon_db_naming:coordinator_name(StoreId),
    case whereis(Name) of
        undefined -> false;
        _Pid -> gen_server:call(Name, {should_handle_nodeup, StoreId}, 5000)
    end.

%% @doc Get cluster members
-spec members(atom()) -> {ok, [term()]} | {error, term()}.
members(StoreId) ->
    khepri_cluster:members(StoreId).

%% @doc Get current leader node
-spec leader(atom()) -> {ok, node()} | {error, no_leader}.
leader(StoreId) ->
    case ra_leaderboard:lookup_leader(StoreId) of
        {_, LeaderNode} -> {ok, LeaderNode};
        _ -> {error, no_leader}
    end.

%% @doc Check if this node is the leader
-spec is_leader(atom()) -> boolean().
is_leader(StoreId) ->
    case leader(StoreId) of
        {ok, LeaderNode} -> node() =:= LeaderNode;
        _ -> false
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(#store_config{store_id = StoreId} = Config) ->
    process_flag(trap_exit, true),
    logger:info("Store coordinator started (store: ~p)", [StoreId]),
    State = #state{
        store_id = StoreId,
        config = Config,
        current_leader = undefined,
        join_status = idle
    },
    {ok, State}.

handle_call({join_cluster, StoreId}, _From, State) ->
    Result = do_join_cluster(StoreId),
    {reply, Result, post_join_result(Result, State)};

handle_call({join_cluster_node, StoreId, TargetNode}, _From, State) ->
    Result = join_existing_cluster(StoreId, TargetNode),
    NewState = case Result of
        ok -> State#state{join_status = joined};
        _ -> State
    end,
    {reply, Result, NewState};

handle_call({should_handle_nodeup, StoreId}, _From, State) ->
    Result = should_handle_nodeup_internal(StoreId),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({retry_join, StoreId}, State) ->
    %% Stop only when we are ACTUALLY part of a multi-member cluster — not
    %% merely because we once returned `coordinator'. A self-elected
    %% coordinator (a cluster of one) keeps reconciling so a peer that
    %% self-elected on a partial boot view, or a lower store-runner that
    %% connects late, still converges to a single cluster.
    case is_multi_member(StoreId) of
        true ->
            logger:debug("retry_join: converged to multi-member cluster (store: ~p)", [StoreId]),
            {noreply, State#state{join_status = joined}};
        false ->
            logger:info("retry_join: re-attempting cluster join/reconcile (store: ~p)", [StoreId]),
            Result = do_join_cluster(StoreId),
            {noreply, post_join_result(Result, State)}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%% @private Fold a join outcome into the next state. `ok' means we joined
%% an existing cluster (done). `coordinator' means we are a cluster of one:
%% in cluster mode keep reconciling until multi-member (see retry_join);
%% in single mode we are done. Everything else retries.
-spec post_join_result(ok | coordinator | no_nodes | waiting | failed | term(),
                       #state{}) -> #state{}.
post_join_result(ok, State)          -> State#state{join_status = joined};
post_join_result(coordinator, State) -> reconcile_or_done(State);
post_join_result(waiting, State)     -> schedule_retry(State);
post_join_result(no_nodes, State)    -> schedule_retry(State);
post_join_result(failed, State)      -> schedule_retry(State);
post_join_result(_, State)           -> State.

%% @private A cluster-mode coordinator keeps reconciling (it is only a
%% cluster of one until joiners arrive); a single-mode store is done.
reconcile_or_done(#state{config = #store_config{mode = cluster}} = State) ->
    schedule_retry(State#state{join_status = coordinating});
reconcile_or_done(State) ->
    State#state{join_status = joined}.

%% @private True when this node's store is a joined, HEALTHY multi-member
%% cluster. Both the old `khepri_cluster:members' (configured set, stays at 3
%% even when isolated) and `reckon_db_cluster:health_check' (get_quorum_status)
%% do an UNBOUNDED `statem_call' into the local ra server — so when that server
%% wedged, this reconcile froze forever (the live bug: coordinator + healer
%% both stuck on `do_query_members'). Use the wedge-proof predicate instead:
%% lock-free `ra_leaderboard' ETS + a bounded liveness probe. An isolated,
%% split, or wedged replica reads as unhealthy and keeps reconciling.
-spec is_multi_member(atom()) -> boolean().
is_multi_member(StoreId) ->
    reckon_db_cluster:local_healthy(StoreId).

%% @private Schedule a single retry of the cluster-join sequence.
%% Uses rand jitter so simultaneous boots don't keep colliding on the
%% same retry tick.
schedule_retry(#state{store_id = StoreId} = State) ->
    Jitter = rand:uniform(?JOIN_RETRY_MAX_MS - ?JOIN_RETRY_BASE_MS),
    Delay = ?JOIN_RETRY_BASE_MS + Jitter,
    erlang:send_after(Delay, self(), {retry_join, StoreId}),
    State.

terminate(Reason, #state{store_id = StoreId}) ->
    logger:info("Store coordinator terminating (store: ~p, reason: ~p)", [StoreId, Reason]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Join cluster via connected nodes.
%%
%% First guard: if this node ALREADY holds a persisted multi-member Ra config,
%% it was a member before this restart — `khepri:start' has restarted its server
%% and it rejoins the existing cluster natively (the ex-esdb / native-Ra
%% behaviour), so there is nothing to do here. Re-running the form/join election
%% would `khepri_cluster:join' (which RESETS local data); when the whole cluster
%% rolls at once and no leader is momentarily visible, that split it into
%% singletons. Trust the persisted membership + native rejoin; the self-healer
%% covers genuine orphans/divergence. Only a fresh replica (single-member config)
%% actually forms or joins.
-spec do_join_cluster(atom()) -> ok | coordinator | no_nodes | waiting | failed.
do_join_cluster(StoreId) ->
    case already_clustered_locally(StoreId) of
        true ->
            logger:info("Already a configured cluster member; relying on native "
                        "Ra rejoin, skipping re-formation (store: ~p)", [StoreId]),
            ok;
        false ->
            do_form_or_join(StoreId)
    end.

%% @private Persisted-membership guard. Distinct from is_multi_member/1, which
%% checks CURRENT health (a live leader) and is false mid-roll: this checks the
%% CONFIGURED member set, which survives a restart intact even before a leader
%% is re-elected. `khepri_cluster:members' returns the configured set.
already_clustered_locally(StoreId) ->
    try khepri_cluster:members(StoreId) of
        {ok, M}           -> has_persisted_cluster(M);
        M when is_list(M) -> has_persisted_cluster(M);
        _                 -> false
    catch _:_ -> false end.

%% @private True when the configured member set names more than this node.
-spec has_persisted_cluster(term()) -> boolean().
has_persisted_cluster(Members) when is_list(Members) -> length(Members) > 1;
has_persisted_cluster(_) -> false.

do_form_or_join(StoreId) ->
    ConnectedNodes = nodes(),
    case ConnectedNodes of
        [] ->
            logger:info("No connected nodes found, starting as single node (store: ~p)", [StoreId]),
            no_nodes;
        _ ->
            logger:info("Attempting cluster join via nodes: ~p (store: ~p)",
                       [ConnectedNodes, StoreId]),
            %% Find nodes with existing clusters
            ClusterNodes = find_existing_cluster_nodes(StoreId, ConnectedNodes),
            handle_cluster_nodes(StoreId, ClusterNodes, ConnectedNodes)
    end.

%% @private Handle found cluster nodes
-spec handle_cluster_nodes(atom(), [node()], [node()]) ->
    ok | coordinator | waiting | failed.
handle_cluster_nodes(StoreId, [], ConnectedNodes) ->
    %% No existing clusters found, check if we should be coordinator
    handle_no_existing_clusters(StoreId, ConnectedNodes);
handle_cluster_nodes(StoreId, [TargetNode | _], _ConnectedNodes) ->
    %% Found existing cluster, join it
    join_existing_cluster(StoreId, TargetNode).

%% @private Handle case when no existing multi-node cluster is yet
%% formed (cold-start case).
%%
%% Deterministic election: AllNodes are sorted by name, the lowest
%% becomes the coordinator. The coordinator stays as its own
%% standalone Khepri cluster (quorum of 1); the others actively join
%% IT. Once any non-coordinator joins, the coordinator's cluster
%% has 2 members and subsequent has_active_cluster checks against
%% the coordinator return true, so retries from any remaining
%% non-coordinators join cleanly.
%%
%% Without this, every node stayed as a standalone cluster
%% indefinitely — election picked a coordinator but no one ever
%% acted on the election to actually grow the cluster.
-spec handle_no_existing_clusters(atom(), [node()]) -> ok | coordinator | waiting | failed.
handle_no_existing_clusters(StoreId, ConnectedNodes) ->
    %% Elect among the nodes that actually RUN this store, NOT every
    %% connected node. On a dist mesh that runs many single-store nodes
    %% (e.g. parksim: 12 nodes, one tenant store each), electing over all
    %% connected nodes picked the globally-lowest node NAME — which may not
    %% run this store — so every join for this store failed forever and the
    %% replicas stayed split. Restricting the candidate set to store-runners
    %% makes the election converge on the right coordinator.
    StoreRunners = store_runner_nodes(StoreId, ConnectedNodes),
    case elect_coordinator(node(), StoreRunners) of
        coordinator ->
            logger:info("Elected coordinator among store-runners ~p (store: ~p)",
                        [StoreRunners, StoreId]),
            telemetry:execute(
                ?CLUSTER_LEADER_ELECTED,
                #{system_time => erlang:system_time(millisecond)},
                #{store_id => StoreId, leader => node(), member_count => 1}
            ),
            coordinator;
        {join, Coordinator} ->
            logger:info(
                "Joining cold-start cluster via coordinator ~p (store: ~p)",
                [Coordinator, StoreId]),
            join_existing_cluster(StoreId, Coordinator)
    end.

%% @private Deterministic coordinator election. The lowest node name among
%% self + the store-running peers is the coordinator; everyone else joins
%% it. Pure so it can be unit-tested without a cluster.
-spec elect_coordinator(node(), [node()]) -> coordinator | {join, node()}.
elect_coordinator(Self, StoreRunners) ->
    case lists:sort([Self | lists:delete(Self, StoreRunners)]) of
        [Self | _]   -> coordinator;
        [Lowest | _] -> {join, Lowest}
    end.

%% @private The connected nodes that run this store (have a local Khepri
%% cluster for it, even a standalone 1-member one), as distinct from
%% sibling nodes in a shared dist mesh that run other stores.
-spec store_runner_nodes(atom(), [node()]) -> [node()].
store_runner_nodes(StoreId, Nodes) ->
    [N || N <- Nodes, runs_store(N, StoreId)].

-spec runs_store(node(), atom()) -> boolean().
runs_store(Node, StoreId) ->
    case rpc:call(Node, khepri_cluster, members, [StoreId], ?RPC_TIMEOUT) of
        {ok, _Members} -> true;
        _              -> false
    end.

%% @private Join an existing cluster
-spec join_existing_cluster(atom(), node()) -> ok | failed.
join_existing_cluster(StoreId, TargetNode) ->
    logger:info("Joining cluster via ~p (store: ~p)", [TargetNode, StoreId]),
    %% The local Ra server must be registered before khepri_cluster:join
    %% can merge it into the target. `khepri_cluster:join' RESETS the local
    %% store as part of joining, so a join interrupted mid-reset (the
    %% timeout guard kills the joiner) can leave the local Ra server gone.
    %% Self-heal by restarting the local store, so the retry loop recovers
    %% instead of looping forever on "not registered".
    case ensure_local_ra_server(StoreId) of
        ok     -> do_join_with_timeout(StoreId, TargetNode);
        failed -> failed
    end.

%% @private Ensure the local Ra server for the store is registered, healing
%% a torn-down store via the store worker. Returns `failed' (→ retry) if it
%% still cannot be brought up this attempt.
-spec ensure_local_ra_server(atom()) -> ok | failed.
ensure_local_ra_server(StoreId) ->
    case erlang:whereis(StoreId) of
        undefined ->
            logger:warning(
                "Local Ra server for store ~p is not registered "
                "(likely an interrupted join reset); restarting the local "
                "store before joining.", [StoreId]),
            heal_local_store(StoreId);
        _Pid ->
            ok
    end.

-spec heal_local_store(atom()) -> ok | failed.
heal_local_store(StoreId) ->
    case reckon_db_store:ensure_khepri_started(StoreId) of
        ok ->
            case erlang:whereis(StoreId) of
                undefined -> failed;   %% still down — retry on the next tick
                _Pid      -> ok
            end;
        {error, Reason} ->
            logger:error("Could not restart local store ~p for join: ~p",
                         [StoreId, Reason]),
            failed
    end.

do_join_with_timeout(StoreId, TargetNode) ->
    %% khepri_cluster:join/3 exists in the source but is NOT exported
    %% in the installed khepri version (0.17.2 exports only join/1
    %% and join/2). The 2-arg form internally calls
    %% khepri_app:get_default_timeout/0, which defaults to `infinity'
    %% — so a stuck join hangs forever. Setting that app-wide env
    %% would also affect every other khepri operation, so we wrap
    %% the call in a side process and kill it on timeout.
    Parent = self(),
    Ref = make_ref(),
    Joiner = spawn(fun() ->
        Parent ! {join_result, Ref, khepri_cluster:join(StoreId, TargetNode)}
    end),
    MRef = erlang:monitor(process, Joiner),
    receive
        {join_result, Ref, ok} ->
            erlang:demonitor(MRef, [flush]),
            logger:info("Successfully joined cluster via ~p (store: ~p)",
                       [TargetNode, StoreId]),
            verify_cluster_membership(StoreId);
        {join_result, Ref, {error, Reason}} ->
            erlang:demonitor(MRef, [flush]),
            logger:warning("Failed to join cluster via ~p: ~p (store: ~p)",
                          [TargetNode, Reason, StoreId]),
            failed;
        {'DOWN', MRef, process, Joiner, Reason} ->
            logger:warning("Join helper crashed via ~p: ~p (store: ~p)",
                          [TargetNode, Reason, StoreId]),
            failed
    after ?KHEPRI_JOIN_TIMEOUT ->
        exit(Joiner, kill),
        receive {'DOWN', MRef, _, _, _} -> ok after 100 -> ok end,
        logger:warning(
            "Join via ~p timed out after ~bms (store: ~p). The remote "
            "node is reachable but cluster membership change is stuck. "
            "Will retry. If the node has stale Ra state, recover with "
            "scripts/wipe-and-rejoin.sh.",
            [TargetNode, ?KHEPRI_JOIN_TIMEOUT, StoreId]),
        failed
    end.

%% @private Verify cluster membership after join
-spec verify_cluster_membership(atom()) -> ok.
verify_cluster_membership(StoreId) ->
    case khepri_cluster:members(StoreId) of
        {ok, Members} when length(Members) > 1 ->
            logger:info("Cluster join verified, now part of ~p-node cluster (store: ~p)",
                       [length(Members), StoreId]),
            telemetry:execute(
                ?CLUSTER_NODE_UP,
                #{system_time => erlang:system_time(millisecond)},
                #{store_id => StoreId, node => node(), member_count => length(Members)}
            ),
            ok;
        {ok, [_Single]} ->
            logger:warning("Join appeared successful but still only 1 member (store: ~p)", [StoreId]),
            ok;
        {error, Reason} ->
            logger:warning("Join succeeded but verification failed: ~p (store: ~p)",
                          [Reason, StoreId]),
            ok
    end.

%% @private Find nodes with existing clusters
-spec find_existing_cluster_nodes(atom(), [node()]) -> [node()].
find_existing_cluster_nodes(StoreId, Nodes) ->
    lists:filter(fun(Node) -> has_active_cluster(Node, StoreId) end, Nodes).

%% @private Check if a node has an active (multi-node) cluster.
%%
%% A freshly-started Khepri node reports itself as a 1-member
%% cluster — that's the default standalone configuration. If we
%% accept that as "active cluster" during simultaneous boots, every
%% node sees every other node as a cluster and they all race to
%% join each other under the same global lock. Only treat a node
%% as having an active cluster when it has MORE than one member —
%% i.e. it has actually been joined.
-spec has_active_cluster(node(), atom()) -> boolean().
has_active_cluster(Node, StoreId) ->
    case rpc:call(Node, khepri_cluster, members, [StoreId], ?RPC_TIMEOUT) of
        {ok, Members} when length(Members) > 1 ->
            logger:debug("Found existing multi-node cluster on ~p with ~p members",
                        [Node, length(Members)]),
            true;
        {ok, _} ->
            %% Single-member (standalone) or empty — not an active cluster.
            false;
        {badrpc, Reason} ->
            logger:debug("RPC to ~p failed: ~p", [Node, Reason]),
            false;
        _ ->
            false
    end.

%% @private Check if should handle nodeup events
-spec should_handle_nodeup_internal(atom()) -> boolean().
should_handle_nodeup_internal(StoreId) ->
    %% Handle nodeup unless we are already in a HEALTHY multi-member cluster.
    %% Using authoritative health (quorum + leader) rather than the configured
    %% member count means an isolated-but-configured replica still reacts to a
    %% peer reappearing and attempts to rejoin, instead of ignoring it.
    not is_multi_member(StoreId).
