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
    join_status :: idle | joining | joined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:coordinator_name(StoreId),
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

%% @doc Join the Khepri cluster using coordinated approach
-spec join_cluster(atom()) -> ok | coordinator | no_nodes | waiting | failed.
join_cluster(StoreId) ->
    Name = reckon_db_naming:coordinator_name(StoreId),
    gen_server:call(Name, {join_cluster, StoreId}, ?JOIN_TIMEOUT).

%% @doc Join a specific node's cluster
-spec join_cluster(atom(), node()) -> ok | {error, term()}.
join_cluster(StoreId, TargetNode) ->
    Name = reckon_db_naming:coordinator_name(StoreId),
    gen_server:call(Name, {join_cluster_node, StoreId, TargetNode}, ?JOIN_TIMEOUT).

%% @doc Check if this node should handle nodeup events
-spec should_handle_nodeup(atom()) -> boolean().
should_handle_nodeup(StoreId) ->
    Name = reckon_db_naming:coordinator_name(StoreId),
    gen_server:call(Name, {should_handle_nodeup, StoreId}, 5000).

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
    NewState = case Result of
        ok          -> State#state{join_status = joined};
        coordinator -> State#state{join_status = joined};
        waiting     -> schedule_retry(State);
        no_nodes    -> schedule_retry(State);
        failed      -> schedule_retry(State);
        _           -> State
    end,
    {reply, Result, NewState};

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

handle_info({retry_join, StoreId}, #state{join_status = joined} = State) ->
    %% Already joined while we were waiting — drop the retry.
    logger:debug("retry_join: already joined (store: ~p)", [StoreId]),
    {noreply, State};
handle_info({retry_join, StoreId}, State) ->
    logger:info("retry_join: re-attempting cluster join (store: ~p)", [StoreId]),
    Result = do_join_cluster(StoreId),
    NewState = case Result of
        ok          -> State#state{join_status = joined};
        coordinator -> State#state{join_status = joined};
        waiting     -> schedule_retry(State);
        no_nodes    -> schedule_retry(State);
        failed      -> schedule_retry(State);
        _           -> State
    end,
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

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

%% @private Join cluster via connected nodes
-spec do_join_cluster(atom()) -> ok | coordinator | no_nodes | waiting | failed.
do_join_cluster(StoreId) ->
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

%% @private Handle case when no existing clusters are found
-spec handle_no_existing_clusters(atom(), [node()]) -> coordinator | waiting.
handle_no_existing_clusters(StoreId, ConnectedNodes) ->
    case should_be_coordinator(ConnectedNodes) of
        true ->
            logger:info("Elected as cluster coordinator (store: ~p)", [StoreId]),
            telemetry:execute(
                ?CLUSTER_LEADER_ELECTED,
                #{system_time => erlang:system_time(millisecond)},
                #{store_id => StoreId, leader => node(), member_count => 1}
            ),
            coordinator;
        false ->
            logger:info("Waiting for cluster coordinator (store: ~p)", [StoreId]),
            waiting
    end.

%% @private Join an existing cluster
-spec join_existing_cluster(atom(), node()) -> ok | failed.
join_existing_cluster(StoreId, TargetNode) ->
    logger:info("Joining cluster via ~p (store: ~p)", [TargetNode, StoreId]),
    %% Verify the local Ra server is registered before attempting
    %% join. If not, khepri_cluster:join will block waiting on
    %% infrastructure that never arrives. Explicit fail-fast lets
    %% the operator run wipe-and-rejoin instead of waiting on a
    %% silent hang.
    case erlang:whereis(StoreId) of
        undefined ->
            logger:error(
                "Local Ra server for store ~p is not registered. "
                "This typically means the local data dir has stale "
                "membership state from a previous cluster generation. "
                "Recover with reckon-cluster-compose/scripts/wipe-and-rejoin.sh.",
                [StoreId]),
            failed;
        _Pid ->
            do_join_with_timeout(StoreId, TargetNode)
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

%% @private Determine if this node should be the coordinator
%% Uses deterministic election: lowest node name becomes coordinator
-spec should_be_coordinator([node()]) -> boolean().
should_be_coordinator(ConnectedNodes) ->
    AllNodes = lists:sort([node() | ConnectedNodes]),
    node() =:= hd(AllNodes).

%% @private Check if should handle nodeup events
-spec should_handle_nodeup_internal(atom()) -> boolean().
should_handle_nodeup_internal(StoreId) ->
    %% Check if we're already part of a multi-node cluster
    case khepri_cluster:members(StoreId) of
        {ok, Members} when length(Members) > 1 ->
            %% Already in a cluster, no need to handle nodeup
            false;
        _ ->
            %% Not in a cluster or only have ourselves, should handle nodeup
            true
    end.
