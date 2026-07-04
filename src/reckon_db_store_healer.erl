%% @doc Store healer for reckon-db — continuous self-healing of a store's
%% Ra cluster.
%%
%% One healer per store (cluster mode only). It closes the loop that the
%% coordinator + consistency facade leave open: detection existed, but
%% nothing took corrective action, so a replica that boots into its own
%% singleton (the classic deploy join-race) or drifts out of quorum stays
%% orphaned until an operator runs a wipe-and-rejoin script by hand.
%%
%% The healer runs an always-armed periodic audit. Each tick it asks the
%% authoritative, reachability-based facade [[reckon_db_cluster]] whether
%% THIS replica is in a healthy, quorum-holding cluster. If not, it looks
%% for a majority cluster (one with an elected leader — which by Raft
%% necessarily holds quorum) that this replica is NOT part of, and, if the
%% data-safety gate passes, resets the local diverged state and rejoins.
%%
%% == Data-safety gate ==
%%
%% A reset is destructive (it discards the local Ra/Khepri state), so
%% `safe_to_reset/1' (== `is_orphan/1') permits it ONLY when:
%%   1. a majority cluster with an elected leader exists among the peers,
%%   2. this node is NOT that leader, and
%%   3. EITHER this node is not locally clustered with that leader (diverged
%%      into its own cluster) OR its local ra server is wedged
%%      (`local_responsive => false').
%%
%% All probes are lock-free (ra_leaderboard ETS) or hard timeout-bounded; the
%% audit must NEVER call `khepri_cluster:members' / `get_quorum_status', which
%% do an unbounded `statem_call' into the local ra server — the very server
%% that is wedged in the case we most need to heal. A monitor that calls the
%% monitored server's synchronous API inherits its wedge. A node still
%% clustered with the leader AND responsive is a transient partition, left to
%% Ra; we never reset the majority itself.
%%
%% Set `self_heal => alarm_only' in the store config `options' to detect
%% and emit telemetry without ever taking destructive action.
%%
%% @author rgfaber

-module(reckon_db_store_healer).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([start_link/1]).
-export([status/1]).
-export([audit_now/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-ifdef(TEST).
-export([safe_to_reset/1, classify/2, majority_view/1]).
-endif.

%% First audit is delayed so cold-boot cluster formation (the coordinator's
%% own fast retry loop) has time to converge before the healer weighs in.
-define(AUDIT_STARTUP_MS, 60000).
-define(AUDIT_INTERVAL_MS, 30000).
-define(AUDIT_JITTER_MS, 10000).
-define(RPC_TIMEOUT, 5000).
%% Hard bound on the LOCAL ra members liveness probe. Short: a healthy server
%% answers in ms; a wedged one must fail fast, not stall the audit.
-define(LOCAL_PROBE_TIMEOUT, 2000).
%% khepri_cluster:reset/1 inherits khepri's default `infinity' timeout, so
%% guard it in a killable side process (same pattern the coordinator uses
%% for join).
-define(RESET_TIMEOUT, 20000).

-record(state, {
    store_id :: atom(),
    mode :: auto | alarm_only,
    heal_count = 0 :: non_neg_integer(),
    last_heal_at :: integer() | undefined,
    last_action :: atom() | undefined,
    last_verdict :: atom() | undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:healer_name(StoreId),
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

%% @doc Self-healing status for the store — surfaced by
%% reckon_db_cluster:health_check/1 for the admin dashboard.
%%
%% Reads a persistent_term the healer publishes on every audit. A plain term
%% read (no gen_server call) is essential: health_check/1 is called by both
%% the coordinator and the healer itself while they hold each other's calls,
%% so routing this through the healer's mailbox would deadlock.
-spec status(atom()) -> #{atom() => term()}.
status(StoreId) ->
    persistent_term:get({?MODULE, StoreId}, #{mode => disabled, heal_count => 0}).

%% @doc Force an immediate audit (for tests / operators).
-spec audit_now(atom()) -> ok.
audit_now(StoreId) ->
    Name = reckon_db_naming:healer_name(StoreId),
    gen_server:cast(Name, audit).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(#store_config{store_id = StoreId, options = Options}) ->
    process_flag(trap_exit, true),
    Mode = maps:get(self_heal, Options, auto),
    logger:info("Store healer started (store: ~p, mode: ~p)", [StoreId, Mode]),
    State = #state{store_id = StoreId, mode = Mode},
    publish_status(State),
    schedule_audit(?AUDIT_STARTUP_MS),
    {ok, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(audit, State) ->
    {noreply, run_audit(State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(audit, State) ->
    NewState = run_audit(State),
    schedule_audit(next_delay()),
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #state{store_id = StoreId}) ->
    logger:info("Store healer terminating (store: ~p, reason: ~p)", [StoreId, Reason]),
    persistent_term:erase({?MODULE, StoreId}),
    ok.

%%====================================================================
%% Audit loop
%%====================================================================

-spec run_audit(#state{}) -> #state{}.
run_audit(#state{store_id = StoreId} = State) ->
    NewState =
        try
            Facts = gather_facts(StoreId),
            Verdict = classify(maps:get(self_status, Facts), Facts),
            act_on(Verdict, Facts, State#state{last_verdict = Verdict})
        catch
            Class:Reason:Stack ->
                logger:warning("Healer audit crashed (store: ~p): ~p:~p~n~p",
                               [StoreId, Class, Reason, Stack]),
                State
        end,
    publish_status(NewState),
    NewState.

%% @private Turn self-health + majority view into a verdict. `orphaned' (the
%% actionable verdict) is exactly `is_orphan/1'; otherwise healthy self-status
%% is `healthy' and anything else is unhealed `drift' (alarm, no reset).
-spec classify(atom(), map()) -> healthy | orphaned | drift.
classify(SelfStatus, Facts) ->
    case is_orphan(Facts) of
        true                          -> orphaned;
        false when SelfStatus =:= healthy -> healthy;
        false                         -> drift
    end.

%% @private The single orphan predicate, shared by classify/2 and
%% safe_to_reset/1. We are an orphan the majority can safely re-absorb when a
%% real (leader-having) majority exists, we are not its leader, and EITHER we
%% are not locally clustered with that leader (diverged into our own cluster)
%% OR our local ra server is wedged (cannot answer within the bound). Both are
%% faults the majority holds authoritative data for, so reset+rejoin is safe.
%% A node still locally clustered with the leader AND responsive is a transient
%% partition -> left to Ra, never reset.
-spec is_orphan(map()) -> boolean().
is_orphan(#{majority_present := true,
            self_is_leader := false,
            self_clustered_with_leader := Clustered,
            local_responsive := Responsive}) ->
    (not Clustered) orelse (not Responsive);
is_orphan(_) ->
    false.

-spec act_on(healthy | orphaned | drift, map(), #state{}) -> #state{}.
act_on(healthy, _Facts, State) ->
    State#state{last_action = none};
act_on(drift, Facts, #state{store_id = StoreId} = State) ->
    %% Unhealthy, but no safe majority to rejoin (e.g. genuine quorum loss,
    %% or we ARE the authoritative side). Alarm, take no destructive action.
    emit(?CLUSTER_DRIFT_DETECTED, StoreId, #{verdict => maps:get(self_status, Facts)}),
    emit(?CLUSTER_HEAL_BLOCKED, StoreId, #{reason => no_safe_majority}),
    State#state{last_action = alarmed};
act_on(orphaned, Facts, #state{store_id = StoreId, mode = Mode} = State) ->
    emit(?CLUSTER_DRIFT_DETECTED, StoreId, #{verdict => orphaned}),
    heal_if_safe(Mode, safe_to_reset(Facts), Facts, State).

-spec heal_if_safe(auto | alarm_only, boolean(), map(), #state{}) -> #state{}.
heal_if_safe(alarm_only, _Safe, _Facts, #state{store_id = StoreId} = State) ->
    emit(?CLUSTER_HEAL_BLOCKED, StoreId, #{reason => alarm_only_mode}),
    State#state{last_action = alarmed};
heal_if_safe(auto, false, Facts, #state{store_id = StoreId} = State) ->
    emit(?CLUSTER_HEAL_BLOCKED, StoreId, #{reason => safety_gate,
                                           facts => redact(Facts)}),
    State#state{last_action = blocked};
heal_if_safe(auto, true, Facts, State) ->
    do_heal(Facts, State).

%%====================================================================
%% Healing
%%====================================================================

-spec do_heal(map(), #state{}) -> #state{}.
do_heal(#{majority_leader := LeaderNode}, #state{store_id = StoreId} = State) ->
    emit(?CLUSTER_HEAL_STARTED, StoreId, #{target_leader => LeaderNode}),
    logger:notice("Healer: replica orphaned; resetting + rejoining via ~p "
                  "(store: ~p)", [LeaderNode, StoreId]),
    case reset_and_rejoin(StoreId, LeaderNode) of
        ok ->
            Count = State#state.heal_count + 1,
            emit(?CLUSTER_RESET_PERFORMED, StoreId, #{}),
            telemetry:execute(?CLUSTER_HEAL_SUCCEEDED,
                              #{system_time => erlang:system_time(millisecond),
                                heal_count => Count},
                              #{store_id => StoreId, node => node()}),
            logger:notice("Healer: replica rejoined cluster (store: ~p, "
                          "heal_count: ~p)", [StoreId, Count]),
            State#state{heal_count = Count,
                        last_heal_at = erlang:system_time(millisecond),
                        last_action = healed};
        {error, Reason} ->
            emit(?CLUSTER_HEAL_FAILED, StoreId, #{reason => Reason}),
            logger:warning("Healer: reset/rejoin failed (store: ~p): ~p — "
                           "will retry next tick", [StoreId, Reason]),
            State#state{last_action = heal_failed}
    end.

%% @private Clear the diverged/wedged local state, then rejoin the majority.
%%
%% Escalates: the graceful `khepri_cluster:reset` is a `statem_call` into the
%% LOCAL ra server, which is exactly the thing that is wedged in the case we
%% most need to heal (an unresponsive server never replies, so a bounded reset
%% just times out). So on reset failure we escalate to `ra:force_delete_server`,
%% which tears the server + its data down WITHOUT the server's cooperation, then
%% restart it fresh and rejoin. A monitor's remedy must never require the sick
%% component to cooperate.
-spec reset_and_rejoin(atom(), node()) -> ok | {error, term()}.
reset_and_rejoin(StoreId, LeaderNode) ->
    ok = clear_local_state(StoreId),
    _ = reckon_db_store:ensure_khepri_started(StoreId),
    case reckon_db_store_coordinator:join_cluster(StoreId, LeaderNode) of
        ok  -> verify_rejoined(StoreId, LeaderNode);
        Err -> {error, {join_failed, Err}}
    end.

%% @private Try a graceful reset first; if it does not cleanly succeed (a
%% wedged server never answers), force-delete the local server + its data.
-spec clear_local_state(atom()) -> ok.
clear_local_state(StoreId) ->
    case reset_local(StoreId) of
        ok ->
            ok;
        {error, Reason} ->
            logger:warning("Healer: graceful reset failed (~p); force-deleting "
                           "the wedged local server (store: ~p)", [Reason, StoreId]),
            force_delete_local(StoreId),
            ok
    end.

%% @private Nuke the local ra server + its on-disk data without needing it to
%% respond. `ra:force_delete_server/2' works on a wedged server.
-spec force_delete_local(atom()) -> ok.
force_delete_local(StoreId) ->
    System = reckon_db_store:ra_system_name(StoreId),
    _ = catch ra:force_delete_server(System, {StoreId, node()}),
    ok.

%% @private khepri_cluster:reset/1 with a killable timeout guard (it
%% otherwise inherits khepri's default `infinity').
-spec reset_local(atom()) -> ok | {error, term()}.
reset_local(StoreId) ->
    Parent = self(),
    Ref = make_ref(),
    Pid = spawn(fun() -> Parent ! {reset_result, Ref, catch khepri_cluster:reset(StoreId)} end),
    MRef = erlang:monitor(process, Pid),
    receive
        {reset_result, Ref, ok} ->
            erlang:demonitor(MRef, [flush]), ok;
        {reset_result, Ref, {error, _} = E} ->
            erlang:demonitor(MRef, [flush]), E;
        {reset_result, Ref, Other} ->
            erlang:demonitor(MRef, [flush]), {error, {reset_failed, Other}};
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, {reset_crashed, Reason}}
    after ?RESET_TIMEOUT ->
        exit(Pid, kill),
        receive {'DOWN', MRef, _, _, _} -> ok after 100 -> ok end,
        {error, reset_timeout}
    end.

%% @private Confirm the rejoin took, WITHOUT the wedging health facade
%% (which does a statem member query). Lock-free ETS: are we now locally
%% clustered with the leader?
-spec verify_rejoined(atom(), node()) -> ok | {error, term()}.
verify_rejoined(StoreId, LeaderNode) ->
    case lists:member(LeaderNode, leaderboard_member_nodes(StoreId)) of
        true  -> ok;
        false -> {error, still_not_clustered_with_leader}
    end.

%%====================================================================
%% Fact gathering
%%====================================================================

%% @private Gather the facts the verdict + safety gate need.
%%
%% CRITICAL: every probe here is lock-free (ra_leaderboard ETS reads) or hard
%% timeout-bounded. It must NEVER route through `khepri_cluster:members' /
%% `get_quorum_status', which do an unbounded `statem_call' into the local ra
%% server — the very server that is wedged in the case we most need to heal.
%% A monitor that calls the monitored server's synchronous API inherits its
%% wedge (the bug this fixes: coordinator + healer both froze on that call).
-spec gather_facts(atom()) -> map().
gather_facts(StoreId) ->
    SelfLocalMembers = leaderboard_member_nodes(StoreId),
    LocalResponsive = local_responsive(StoreId),
    MajView = majority_view(peer_memberships(StoreId)),
    MajMembers = maps:get(members, MajView, []),
    MajLeader = maps:get(leader, MajView, undefined),
    Facts = #{store_id => StoreId,
              majority_leader => MajLeader,
              majority_members => MajMembers,
              %% A cluster that ELECTED a leader necessarily holds quorum (Raft),
              %% so "leader present AND >= 2 members" identifies the authoritative
              %% majority without needing the configured size.
              majority_present => MajLeader =/= undefined andalso length(MajMembers) >= 2,
              self_is_leader => MajLeader =:= node(),
              %% Is the majority's leader in the cluster WE are locally part of?
              %% (leaderboard ETS view). A diverged singleton has [self] (leader
              %% absent); a transient-partition member still has the full set.
              self_clustered_with_leader =>
                  MajLeader =/= undefined andalso lists:member(MajLeader, SelfLocalMembers),
              %% Does our LOCAL ra server answer a membership query within a hard
              %% bound? `false' = wedged (the exact fault we saw): a fault to heal
              %% when a majority exists, even if our config still looks correct.
              local_responsive => LocalResponsive},
    Facts#{self_status => self_status(Facts)}.

%% @private This node's LOCAL member view from the ra_leaderboard ETS table
%% (lock-free, wedge-proof), as node names. `[]' if unknown.
-spec leaderboard_member_nodes(atom()) -> [node()].
leaderboard_member_nodes(StoreId) ->
    case ra_leaderboard:lookup_members(StoreId) of
        Members when is_list(Members) -> [node_of(M) || M <- Members];
        _                             -> []
    end.

%% @private Liveness probe: does the local ra server answer a members query
%% within a hard bound? Uses ra:members/2 (bounded), so a wedged server
%% yields `false' fast instead of blocking the whole audit forever.
-spec local_responsive(atom()) -> boolean().
local_responsive(StoreId) ->
    case catch ra:members({StoreId, node()}, ?LOCAL_PROBE_TIMEOUT) of
        {ok, _Members, _Leader} -> true;
        _                       -> false
    end.

%% @private This replica's own health, derived purely from the lock-free
%% leaderboard (NOT get_quorum_status, which wedges). Present leader -> healthy.
-spec self_status(map()) -> healthy | no_leader.
self_status(#{store_id := StoreId}) ->
    case ra_leaderboard:lookup_leader(StoreId) of
        undefined -> no_leader;
        _Leader   -> healthy
    end.

%% @private Ask every connected node that runs this store for its Ra membership
%% view via the lock-free ra_leaderboard ETS table (wedge-proof on the peer
%% side too), bounded by the rpc timeout: `{Node, LeaderNode, [MemberNode]}'.
-spec peer_memberships(atom()) -> [{node(), node() | undefined, [node()]}].
peer_memberships(StoreId) ->
    lists:filtermap(
        fun(N) ->
            Leader = rpc:call(N, ra_leaderboard, lookup_leader, [StoreId], ?RPC_TIMEOUT),
            Members = rpc:call(N, ra_leaderboard, lookup_members, [StoreId], ?RPC_TIMEOUT),
            case Members of
                Ms when is_list(Ms) ->
                    {true, {N, leader_node(Leader), [node_of(M) || M <- Ms]}};
                _ ->
                    false
            end
        end,
        nodes()).

%% @private Pick the authoritative majority view among peer responses: the
%% largest member set that has an elected leader.
-spec majority_view([{node(), node() | undefined, [node()]}]) ->
    #{leader => node() | undefined, members => [node()]}.
majority_view(Peers) ->
    WithLeader = [{Leader, Members} || {_N, Leader, Members} <- Peers,
                                       Leader =/= undefined, Members =/= []],
    case WithLeader of
        [] ->
            #{leader => undefined, members => []};
        _ ->
            {Leader, Members} = hd(lists:sort(
                fun({_, A}, {_, B}) -> length(A) >= length(B) end, WithLeader)),
            #{leader => Leader, members => Members}
    end.

%%====================================================================
%% The data-safety gate (pure — unit-tested)
%%====================================================================

%% @doc The data-safety gate == the orphan predicate. Permits a destructive
%% reset ONLY for a replica that has diverged into its own cluster OR whose
%% local ra server is wedged, while a real (leader-having) majority exists to
%% rejoin. Never the leader, never a responsive node still clustered with it.
-spec safe_to_reset(map()) -> boolean().
safe_to_reset(Facts) -> is_orphan(Facts).

%%====================================================================
%% Helpers
%%====================================================================

-spec node_of(term()) -> node() | undefined.
node_of({_Name, Node}) when is_atom(Node) -> Node;
node_of(Node) when is_atom(Node) -> Node;
node_of(_) -> undefined.

%% @private Extract the leader node from ra_leaderboard:lookup_leader/1, which
%% over rpc may also come back as `undefined' or `{badrpc, _}'. `{badrpc, R}'
%% must NOT be read as a `{Name, Node}' server id.
-spec leader_node(term()) -> node() | undefined.
leader_node({badrpc, _}) -> undefined;
leader_node(undefined)   -> undefined;
leader_node({_Name, Node}) when is_atom(Node) -> Node;
leader_node(_) -> undefined.

schedule_audit(DelayMs) ->
    erlang:send_after(DelayMs, self(), audit).

next_delay() ->
    ?AUDIT_INTERVAL_MS + rand:uniform(?AUDIT_JITTER_MS).

status_map(#state{mode = Mode, heal_count = C, last_heal_at = At,
                  last_action = Action, last_verdict = Verdict}) ->
    #{mode => Mode,
      heal_count => C,
      last_heal_at => At,
      last_action => Action,
      last_verdict => Verdict}.

%% @private Publish the current status to a persistent_term for lock-free
%% reads by status/1 (see the deadlock note there). Written only on audit
%% ticks + heals, so the persistent_term update cost is negligible.
publish_status(State) ->
    persistent_term:put({?MODULE, State#state.store_id}, status_map(State)).

redact(Facts) ->
    maps:with([self_status, majority_leader, majority_present, self_is_leader,
               self_clustered_with_leader, local_responsive], Facts).

emit(Event, StoreId, Meta) ->
    telemetry:execute(Event,
                      #{system_time => erlang:system_time(millisecond)},
                      Meta#{store_id => StoreId, node => node()}).
