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
%% `safe_to_reset/1' permits it ONLY when ALL hold:
%%   1. a majority cluster with an elected leader exists among the peers,
%%   2. this node is NOT that leader, and
%%   3. this node is NOT a member of that majority cluster.
%%
%% (3) is the key invariant: we only ever reset a replica the majority has
%% never accepted (the never-joined singleton) — never a member of the
%% authoritative cluster, and never the majority itself. A transient
%% network partition of a real member is left to Ra to heal on its own.
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

%% @private Turn the self health status + majority view into a verdict.
%%
%% Orphan detection is driven by the MAJORITY-EXCLUSION signal, not local
%% self-health: a replica that split into its own singleton is LOCALLY a
%% healthy 1-node cluster (quorum 1/1, leader = self), so keying on
%% `self_status' would never heal the exact split we are here to fix. If a
%% real majority (a leader-having >= 2 cluster) exists that we are neither
%% the leader of nor a member of, we are an orphan — regardless of how
%% healthy we look to ourselves. Only when no such majority exists do we
%% fall back to self-health (healthy -> nothing; otherwise -> unhealed drift).
-spec classify(atom(), map()) -> healthy | orphaned | drift.
classify(_SelfStatus, #{majority_present := true,
                        self_is_leader := false,
                        self_in_majority := false}) -> orphaned;
classify(healthy, _Facts) -> healthy;
classify(_Unhealthy, _Facts) -> drift.

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

%% @private Reset the diverged local state, then rejoin the majority via the
%% coordinator's existing join primitive. reset/1 is the Raft-correct "forget
%% my local state" op; it is safe here precisely because the safety gate
%% proved the majority never accepted this replica.
-spec reset_and_rejoin(atom(), node()) -> ok | {error, term()}.
reset_and_rejoin(StoreId, LeaderNode) ->
    case reset_local(StoreId) of
        ok ->
            _ = reckon_db_store:ensure_khepri_started(StoreId),
            case reckon_db_store_coordinator:join_cluster(StoreId, LeaderNode) of
                ok  -> verify_rejoined(StoreId);
                Err -> {error, {join_failed, Err}}
            end;
        {error, _} = E ->
            E
    end.

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

-spec verify_rejoined(atom()) -> ok | {error, term()}.
verify_rejoined(StoreId) ->
    case reckon_db_cluster:health_check(StoreId) of
        {ok, #{status := healthy}} -> ok;
        {ok, #{status := Other}}   -> {error, {still_unhealthy, Other}};
        {error, Reason}            -> {error, Reason}
    end.

%%====================================================================
%% Fact gathering
%%====================================================================

%% @private Gather the facts the verdict + safety gate need. `self_status'
%% is the authoritative local verdict; the majority view is derived by
%% asking each connected store-runner peer for its Ra membership.
-spec gather_facts(atom()) -> map().
gather_facts(StoreId) ->
    SelfStatus = self_status(StoreId),
    MajView = majority_view(peer_memberships(StoreId)),
    MajMembers = maps:get(members, MajView, []),
    MajLeader = maps:get(leader, MajView, undefined),
    #{store_id => StoreId,
      self_status => SelfStatus,
      majority_leader => MajLeader,
      majority_members => MajMembers,
      %% A cluster that has ELECTED a leader necessarily holds quorum (Raft),
      %% so "leader present AND >= 2 members" identifies the authoritative
      %% majority without needing to know the configured size.
      majority_present => MajLeader =/= undefined andalso length(MajMembers) >= 2,
      self_is_leader => MajLeader =:= node(),
      self_in_majority => lists:member(node(), MajMembers)}.

%% @private This replica's own authoritative health, computed directly from
%% the pure primitives (NOT via reckon_db_cluster:health_check/1, which now
%% folds in the healer's own status and would recurse). Mirrors
%% reckon_db_cluster:classify_health/2.
-spec self_status(atom()) -> healthy | degraded | no_quorum | unreachable.
self_status(StoreId) ->
    case reckon_db_consistency_checker:get_quorum_status(StoreId) of
        {ok, #{has_quorum := true}} ->
            case ra_leaderboard:lookup_leader(StoreId) of
                undefined -> degraded;
                _Leader   -> healthy
            end;
        {ok, #{has_quorum := false}} -> no_quorum;
        {error, _}                   -> unreachable
    end.

%% @private Ask every connected node that runs this store for its Ra
%% membership view: `{Node, LeaderNode | undefined, [MemberNode]}'.
-spec peer_memberships(atom()) -> [{node(), node() | undefined, [node()]}].
peer_memberships(StoreId) ->
    lists:filtermap(
        fun(N) ->
            case rpc:call(N, ra, members, [{StoreId, N}], ?RPC_TIMEOUT) of
                {ok, Members, Leader} ->
                    {true, {N, node_of(Leader), [node_of(M) || M <- Members]}};
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

%% @doc Permit a destructive reset ONLY for a replica the majority has never
%% accepted. Never resets the leader, never resets a member of the majority,
%% and requires a real majority (leader-having) cluster to rejoin.
-spec safe_to_reset(map()) -> boolean().
safe_to_reset(#{majority_present := true,
                self_is_leader := false,
                self_in_majority := false}) -> true;
safe_to_reset(_) -> false.

%%====================================================================
%% Helpers
%%====================================================================

-spec node_of(term()) -> node() | undefined.
node_of({_Name, Node}) -> Node;
node_of(Node) when is_atom(Node) -> Node;
node_of(_) -> undefined.

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
    maps:with([self_status, majority_leader, majority_present,
               self_is_leader, self_in_majority], Facts).

emit(Event, StoreId, Meta) ->
    telemetry:execute(Event,
                      #{system_time => erlang:system_time(millisecond)},
                      Meta#{store_id => StoreId, node => node()}).
