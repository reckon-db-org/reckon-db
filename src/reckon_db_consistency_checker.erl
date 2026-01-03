%% @doc Cluster consistency checker for reckon-db
%%
%% Provides active split-brain detection and cluster health verification.
%% Implements multi-layer consistency checking:
%%
%% 1. Membership Consensus - All nodes agree on cluster membership
%% 2. Raft Log Consistency - Log terms and indices match across followers
%% 3. Leader Consensus - All nodes agree on who the leader is
%% 4. Quorum Verification - Sufficient nodes available for operations
%%
%% Split-Brain Detection:
%%
%% Split-brain occurs when network partitions cause nodes to form
%% independent clusters. This module detects such scenarios by:
%%
%% - Collecting membership views from all nodes via RPC
%% - Comparing views to find inconsistencies
%% - Detecting when nodes report different leaders
%% - Identifying when quorum is at risk
%%
%% Academic References:
%%
%% - Ongaro, D. and Ousterhout, J. (2014). In Search of an Understandable
%%   Consensus Algorithm (Raft). USENIX ATC 2014.
%% - Brewer, E. (2012). CAP Twelve Years Later: How the "Rules" Have Changed.
%%   IEEE Computer, 45(2), 23-29.
%%
%% @author rgfaber
%% @see reckon_db_health_prober

-module(reckon_db_consistency_checker).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([start_link/1]).
-export([check_now/1]).
-export([get_status/1]).
-export([verify_membership_consensus/1]).
-export([verify_leader_consensus/1]).
-export([verify_raft_consistency/1]).
-export([get_quorum_status/1]).
-export([on_status_change/2]).
-export([remove_callback/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Internal exports (for RPC)
-export([get_local_raft_stats/1]).

%% Status types
-type consistency_status() :: healthy | degraded | split_brain | no_quorum.
-type check_result() :: #{
    status := consistency_status(),
    checks := #{atom() => check_detail()},
    timestamp := integer(),
    duration_us := non_neg_integer()
}.
-type check_detail() :: #{
    status := ok | warning | error,
    message := binary(),
    data := term()
}.

-export_type([consistency_status/0, check_result/0]).

-define(DEFAULT_CHECK_INTERVAL, 5000).     %% 5 seconds
-define(RPC_TIMEOUT, 3000).                %% 3 seconds for RPC calls
-define(MIN_CHECK_INTERVAL, 1000).         %% Minimum 1 second

-record(state, {
    store_id :: atom(),
    config :: store_config(),
    check_interval :: pos_integer(),
    last_status :: consistency_status(),
    last_result :: check_result() | undefined,
    callbacks :: #{reference() => fun((consistency_status()) -> any())},
    consecutive_failures :: non_neg_integer()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the consistency checker
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = consistency_checker_name(StoreId),
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

%% @doc Force an immediate consistency check
-spec check_now(atom()) -> check_result().
check_now(StoreId) ->
    Name = consistency_checker_name(StoreId),
    gen_server:call(Name, check_now, 30000).

%% @doc Get current consistency status
-spec get_status(atom()) -> {ok, consistency_status()} | {error, not_running}.
get_status(StoreId) ->
    Name = consistency_checker_name(StoreId),
    try
        gen_server:call(Name, get_status, 5000)
    catch
        exit:{noproc, _} -> {error, not_running}
    end.

%% @doc Verify membership consensus across all cluster nodes
%%
%% Collects membership views from each node and compares them.
%% Returns consensus if all nodes agree, split_brain if they disagree.
-spec verify_membership_consensus(atom()) -> {ok, map()} | {error, term()}.
verify_membership_consensus(StoreId) ->
    case get_cluster_members(StoreId) of
        {ok, LocalMembers} ->
            Nodes = extract_nodes_from_members(LocalMembers),
            Views = collect_membership_views(StoreId, Nodes),
            analyze_membership_consensus(Views, LocalMembers);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Verify all nodes agree on the current leader
-spec verify_leader_consensus(atom()) -> {ok, map()} | {error, term()}.
verify_leader_consensus(StoreId) ->
    case get_cluster_members(StoreId) of
        {ok, Members} ->
            Nodes = extract_nodes_from_members(Members),
            LeaderViews = collect_leader_views(StoreId, Nodes),
            analyze_leader_consensus(LeaderViews);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Verify Raft log consistency across cluster
%%
%% Checks that follower nodes have consistent log terms and indices.
%% Significant divergence may indicate replication issues.
-spec verify_raft_consistency(atom()) -> {ok, map()} | {error, term()}.
verify_raft_consistency(StoreId) ->
    case ra:members({StoreId, node()}) of
        {ok, Members, Leader} ->
            RaftStats = collect_raft_stats(StoreId, Members),
            analyze_raft_consistency(RaftStats, Leader);
        {error, Reason} ->
            {error, Reason};
        {timeout, _} ->
            {error, timeout}
    end.

%% @doc Get current quorum status
%%
%% Returns quorum availability and margin information.
-spec get_quorum_status(atom()) -> {ok, map()} | {error, term()}.
get_quorum_status(StoreId) ->
    case get_cluster_members(StoreId) of
        {ok, Members} ->
            TotalNodes = length(Members),
            RequiredQuorum = (TotalNodes div 2) + 1,
            AvailableNodes = count_available_nodes(StoreId, Members),
            HasQuorum = AvailableNodes >= RequiredQuorum,
            Margin = AvailableNodes - RequiredQuorum,
            {ok, #{
                has_quorum => HasQuorum,
                total_nodes => TotalNodes,
                available_nodes => AvailableNodes,
                required_quorum => RequiredQuorum,
                quorum_margin => Margin,
                can_lose => max(0, Margin)
            }};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Register a callback for status changes
-spec on_status_change(atom(), fun((consistency_status()) -> any())) -> reference().
on_status_change(StoreId, Callback) when is_function(Callback, 1) ->
    Name = consistency_checker_name(StoreId),
    gen_server:call(Name, {register_callback, Callback}, 5000).

%% @doc Remove a previously registered callback
-spec remove_callback(atom(), reference()) -> ok.
remove_callback(StoreId, Ref) ->
    Name = consistency_checker_name(StoreId),
    gen_server:call(Name, {remove_callback, Ref}, 5000).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(#store_config{store_id = StoreId, mode = Mode} = Config) ->
    process_flag(trap_exit, true),

    CheckInterval = application:get_env(reckon_db, consistency_check_interval,
                                        ?DEFAULT_CHECK_INTERVAL),

    logger:info("Consistency checker started (store: ~p, interval: ~pms)",
                [StoreId, CheckInterval]),

    State = #state{
        store_id = StoreId,
        config = Config,
        check_interval = max(CheckInterval, ?MIN_CHECK_INTERVAL),
        last_status = healthy,
        last_result = undefined,
        callbacks = #{},
        consecutive_failures = 0
    },

    %% Only run periodic checks in cluster mode
    case Mode of
        cluster ->
            schedule_check(State#state.check_interval);
        single ->
            ok
    end,

    {ok, State}.

handle_call(check_now, _From, State) ->
    {Result, NewState} = perform_consistency_check(State),
    {reply, Result, NewState};

handle_call(get_status, _From, #state{last_status = Status} = State) ->
    {reply, {ok, Status}, State};

handle_call({register_callback, Callback}, _From, #state{callbacks = Callbacks} = State) ->
    Ref = make_ref(),
    NewCallbacks = Callbacks#{Ref => Callback},
    {reply, Ref, State#state{callbacks = NewCallbacks}};

handle_call({remove_callback, Ref}, _From, #state{callbacks = Callbacks} = State) ->
    NewCallbacks = maps:remove(Ref, Callbacks),
    {reply, ok, State#state{callbacks = NewCallbacks}};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(run_check, #state{check_interval = Interval} = State) ->
    {_Result, NewState} = perform_consistency_check(State),
    schedule_check(Interval),
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #state{store_id = StoreId}) ->
    logger:info("Consistency checker terminating (store: ~p, reason: ~p)",
                [StoreId, Reason]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Perform all consistency checks
-spec perform_consistency_check(#state{}) -> {check_result(), #state{}}.
perform_consistency_check(#state{store_id = StoreId, last_status = PreviousStatus,
                                  callbacks = Callbacks} = State) ->
    StartTime = erlang:monotonic_time(microsecond),

    %% Run all checks
    MembershipResult = safe_check(fun() -> verify_membership_consensus(StoreId) end),
    LeaderResult = safe_check(fun() -> verify_leader_consensus(StoreId) end),
    RaftResult = safe_check(fun() -> verify_raft_consistency(StoreId) end),
    QuorumResult = safe_check(fun() -> get_quorum_status(StoreId) end),

    EndTime = erlang:monotonic_time(microsecond),
    Duration = EndTime - StartTime,

    %% Determine overall status
    Checks = #{
        membership => format_check_result(MembershipResult),
        leader => format_check_result(LeaderResult),
        raft => format_check_result(RaftResult),
        quorum => format_check_result(QuorumResult)
    },

    OverallStatus = determine_overall_status(Checks),

    Result = #{
        status => OverallStatus,
        checks => Checks,
        timestamp => erlang:system_time(millisecond),
        duration_us => Duration
    },

    %% Emit telemetry
    emit_telemetry(StoreId, Result, PreviousStatus),

    %% Notify callbacks if status changed
    NewState = case OverallStatus of
        PreviousStatus ->
            State#state{last_result = Result, consecutive_failures = 0};
        _ ->
            logger:notice("Cluster consistency status changed: ~p -> ~p (store: ~p)",
                         [PreviousStatus, OverallStatus, StoreId]),
            notify_callbacks(Callbacks, OverallStatus),
            State#state{last_result = Result, last_status = OverallStatus,
                        consecutive_failures = 0}
    end,

    {Result, NewState}.

%% @private Execute check safely
-spec safe_check(fun(() -> {ok, map()} | {error, term()})) -> {ok, map()} | {error, term()}.
safe_check(Fun) ->
    try
        Fun()
    catch
        Class:Reason:Stack ->
            logger:warning("Consistency check failed: ~p:~p~n~p", [Class, Reason, Stack]),
            {error, {check_failed, {Class, Reason}}}
    end.

%% @private Format check result for output
-spec format_check_result({ok, map()} | {error, term()}) -> check_detail().
format_check_result({ok, #{status := split_brain} = Data}) ->
    #{status => error, message => <<"Split-brain detected">>, data => Data};
format_check_result({ok, #{status := no_consensus} = Data}) ->
    #{status => error, message => <<"No consensus">>, data => Data};
format_check_result({ok, #{has_quorum := false} = Data}) ->
    #{status => error, message => <<"No quorum">>, data => Data};
format_check_result({ok, #{status := warning} = Data}) ->
    #{status => warning, message => maps:get(message, Data, <<"Warning">>), data => Data};
format_check_result({ok, Data}) ->
    #{status => ok, message => <<"OK">>, data => Data};
format_check_result({error, Reason}) ->
    #{status => error, message => iolist_to_binary(io_lib:format("~p", [Reason])),
      data => #{error => Reason}}.

%% @private Determine overall status from individual checks
-spec determine_overall_status(#{atom() => check_detail()}) -> consistency_status().
determine_overall_status(Checks) ->
    CheckStatuses = [maps:get(status, Detail) || {_, Detail} <- maps:to_list(Checks)],

    %% Check for split-brain first (highest severity)
    MembershipData = maps:get(data, maps:get(membership, Checks, #{data => #{}}), #{}),
    case maps:get(status, MembershipData, undefined) of
        split_brain -> split_brain;
        _ ->
            %% Check for no quorum
            QuorumData = maps:get(data, maps:get(quorum, Checks, #{data => #{}}), #{}),
            case maps:get(has_quorum, QuorumData, true) of
                false -> no_quorum;
                _ ->
                    %% Check for any errors or warnings
                    case lists:member(error, CheckStatuses) of
                        true -> degraded;
                        false ->
                            case lists:member(warning, CheckStatuses) of
                                true -> degraded;
                                false -> healthy
                            end
                    end
            end
    end.

%% @private Get cluster members
-spec get_cluster_members(atom()) -> {ok, [term()]} | {error, term()}.
get_cluster_members(StoreId) ->
    khepri_cluster:members(StoreId).

%% @private Extract node names from member tuples
-spec extract_nodes_from_members([term()]) -> [node()].
extract_nodes_from_members(Members) ->
    lists:map(fun({_StoreId, Node}) -> Node end, Members).

%% @private Collect membership views from all nodes
-spec collect_membership_views(atom(), [node()]) -> #{node() => {ok, [term()]} | {error, term()}}.
collect_membership_views(StoreId, Nodes) ->
    lists:foldl(fun(Node, Acc) ->
        Result = case Node of
            N when N =:= node() ->
                khepri_cluster:members(StoreId);
            _ ->
                rpc:call(Node, khepri_cluster, members, [StoreId], ?RPC_TIMEOUT)
        end,
        NormalizedResult = case Result of
            {ok, Members} -> {ok, lists:sort(Members)};
            {badrpc, Reason} -> {error, {rpc_failed, Reason}};
            Other -> Other
        end,
        Acc#{Node => NormalizedResult}
    end, #{}, Nodes).

%% @private Analyze membership consensus
-spec analyze_membership_consensus(#{node() => term()}, [term()]) -> {ok, map()}.
analyze_membership_consensus(Views, LocalMembers) ->
    SuccessfulViews = maps:filter(fun(_, V) ->
        case V of {ok, _} -> true; _ -> false end
    end, Views),

    FailedNodes = maps:keys(maps:filter(fun(_, V) ->
        case V of {ok, _} -> false; _ -> true end
    end, Views)),

    %% Extract member lists and find unique views
    MemberLists = [M || {ok, M} <- maps:values(SuccessfulViews)],
    UniqueViews = lists:usort(MemberLists),

    NodesChecked = maps:size(Views),
    NodesResponded = maps:size(SuccessfulViews),

    case length(UniqueViews) of
        0 ->
            {ok, #{status => error, message => <<"No nodes responded">>,
                   nodes_checked => NodesChecked, nodes_responded => 0}};
        1 ->
            %% All nodes agree
            {ok, #{status => consensus,
                   nodes_checked => NodesChecked,
                   nodes_responded => NodesResponded,
                   failed_nodes => FailedNodes,
                   consistent_view => LocalMembers}};
        N when N > 1 ->
            %% Split-brain detected - nodes have different views
            {ok, #{status => split_brain,
                   conflicting_views => N,
                   nodes_checked => NodesChecked,
                   nodes_responded => NodesResponded,
                   views => SuccessfulViews,
                   failed_nodes => FailedNodes}}
    end.

%% @private Collect leader views from all nodes
-spec collect_leader_views(atom(), [node()]) -> #{node() => term()}.
collect_leader_views(StoreId, Nodes) ->
    lists:foldl(fun(Node, Acc) ->
        Result = case Node of
            N when N =:= node() ->
                ra_leaderboard:lookup_leader(StoreId);
            _ ->
                rpc:call(Node, ra_leaderboard, lookup_leader, [StoreId], ?RPC_TIMEOUT)
        end,
        NormalizedResult = case Result of
            {badrpc, Reason} -> {error, {rpc_failed, Reason}};
            undefined -> {error, no_leader};
            {_, LeaderNode} when is_atom(LeaderNode) -> {ok, LeaderNode};
            Other -> {error, Other}
        end,
        Acc#{Node => NormalizedResult}
    end, #{}, Nodes).

%% @private Analyze leader consensus
-spec analyze_leader_consensus(#{node() => term()}) -> {ok, map()}.
analyze_leader_consensus(Views) ->
    SuccessfulViews = maps:filter(fun(_, V) ->
        case V of {ok, _} -> true; _ -> false end
    end, Views),

    Leaders = [L || {ok, L} <- maps:values(SuccessfulViews)],
    UniqueLeaders = lists:usort(Leaders),

    NodesChecked = maps:size(Views),
    NodesResponded = maps:size(SuccessfulViews),

    case {length(UniqueLeaders), NodesResponded} of
        {0, _} ->
            {ok, #{status => no_leader,
                   nodes_checked => NodesChecked,
                   nodes_responded => NodesResponded}};
        {1, _} ->
            [Leader] = UniqueLeaders,
            {ok, #{status => consensus,
                   leader => Leader,
                   nodes_checked => NodesChecked,
                   nodes_responded => NodesResponded}};
        {N, _} when N > 1 ->
            %% Multiple leaders reported - possible split-brain
            {ok, #{status => no_consensus,
                   leaders_reported => UniqueLeaders,
                   nodes_checked => NodesChecked,
                   nodes_responded => NodesResponded,
                   views => Views}}
    end.

%% @private Collect Raft statistics from members
-spec collect_raft_stats(atom(), [term()]) -> #{node() => term()}.
collect_raft_stats(StoreId, Members) ->
    lists:foldl(fun({_, Node}, Acc) ->
        Result = case Node of
            N when N =:= node() ->
                get_local_raft_stats(StoreId);
            _ ->
                rpc:call(Node, ?MODULE, get_local_raft_stats, [StoreId], ?RPC_TIMEOUT)
        end,
        NormalizedResult = case Result of
            {badrpc, Reason} -> {error, {rpc_failed, Reason}};
            Other -> Other
        end,
        Acc#{Node => NormalizedResult}
    end, #{}, Members).

%% @private Get local Raft statistics (exported for RPC)
-spec get_local_raft_stats(atom()) -> {ok, map()} | {error, term()}.
get_local_raft_stats(StoreId) ->
    try
        case ra:member_overview({StoreId, node()}) of
            #{current_term := Term, commit_index := CommitIdx,
              last_applied := LastApplied, state := RaftState} ->
                {ok, #{
                    term => Term,
                    commit_index => CommitIdx,
                    last_applied => LastApplied,
                    state => RaftState
                }};
            Other ->
                {ok, #{raw => Other}}
        end
    catch
        _:Reason ->
            {error, Reason}
    end.

%% @private Analyze Raft consistency
-spec analyze_raft_consistency(#{node() => term()}, term()) -> {ok, map()}.
analyze_raft_consistency(Stats, Leader) ->
    SuccessfulStats = maps:filter(fun(_, V) ->
        case V of {ok, _} -> true; _ -> false end
    end, Stats),

    case maps:size(SuccessfulStats) of
        0 ->
            {ok, #{status => error, message => <<"No Raft stats available">>}};
        _ ->
            %% Extract terms and check consistency
            Terms = [maps:get(term, S, 0) || {ok, S} <- maps:values(SuccessfulStats)],
            CommitIndices = [maps:get(commit_index, S, 0) || {ok, S} <- maps:values(SuccessfulStats)],

            TermsConsistent = length(lists:usort(Terms)) =< 1,
            MaxCommitDiff = lists:max(CommitIndices) - lists:min(CommitIndices),

            Status = case {TermsConsistent, MaxCommitDiff} of
                {true, Diff} when Diff < 100 -> consensus;
                {true, _} -> warning;
                {false, _} -> warning
            end,

            {ok, #{
                status => Status,
                leader => Leader,
                terms => lists:usort(Terms),
                terms_consistent => TermsConsistent,
                commit_index_range => {lists:min(CommitIndices), lists:max(CommitIndices)},
                max_commit_lag => MaxCommitDiff,
                nodes_checked => maps:size(Stats),
                nodes_responded => maps:size(SuccessfulStats)
            }}
    end.

%% @private Count available nodes
-spec count_available_nodes(atom(), [term()]) -> non_neg_integer().
count_available_nodes(_StoreId, Members) ->
    Nodes = extract_nodes_from_members(Members),
    length(lists:filter(fun(Node) ->
        case Node of
            N when N =:= node() -> true;
            _ ->
                case net_adm:ping(Node) of
                    pong -> true;
                    pang -> false
                end
        end
    end, Nodes)).

%% @private Emit telemetry event
-spec emit_telemetry(atom(), check_result(), consistency_status()) -> ok.
emit_telemetry(StoreId, #{status := Status, duration_us := Duration} = Result, PreviousStatus) ->
    %% Always emit check completed
    telemetry:execute(
        ?CONSISTENCY_CHECK_COMPLETE,
        #{duration_us => Duration},
        #{store_id => StoreId, status => Status, checks => maps:get(checks, Result)}
    ),

    %% Emit status change if changed
    case Status of
        PreviousStatus -> ok;
        _ ->
            telemetry:execute(
                ?CONSISTENCY_STATUS_CHANGED,
                #{system_time => erlang:system_time(millisecond)},
                #{store_id => StoreId, old_status => PreviousStatus, new_status => Status}
            )
    end,

    %% Emit split-brain alert if detected
    case Status of
        split_brain ->
            telemetry:execute(
                ?SPLIT_BRAIN_DETECTED,
                #{system_time => erlang:system_time(millisecond)},
                #{store_id => StoreId, result => Result}
            );
        _ -> ok
    end,

    ok.

%% @private Notify registered callbacks
-spec notify_callbacks(#{reference() => fun()}, consistency_status()) -> ok.
notify_callbacks(Callbacks, Status) ->
    maps:foreach(fun(_Ref, Callback) ->
        spawn(fun() ->
            try
                Callback(Status)
            catch
                Class:Reason:Stack ->
                    logger:warning("Consistency callback failed: ~p:~p~n~p",
                                  [Class, Reason, Stack])
            end
        end)
    end, Callbacks),
    ok.

%% @private Schedule next check
-spec schedule_check(pos_integer()) -> reference().
schedule_check(Interval) ->
    erlang:send_after(Interval, self(), run_check).

%% @private Get process name for store
-spec consistency_checker_name(atom()) -> atom().
consistency_checker_name(StoreId) ->
    list_to_atom("reckon_db_consistency_checker_" ++ atom_to_list(StoreId)).
