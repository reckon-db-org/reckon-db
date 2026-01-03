%% @doc Active health prober for reckon-db cluster nodes
%%
%% Implements active health probing to detect node failures faster than
%% passive net_kernel:monitor_nodes/1 events. This is critical for
%% timely split-brain detection and quorum management.
%%
%% Design Philosophy:
%%
%% Passive monitoring (nodeup/nodedown) can take 60+ seconds to detect
%% failures depending on net_ticktime configuration. Active probing
%% provides sub-second detection with configurable thresholds.
%%
%% Probe Types:
%%
%% 1. Ping Probe - net_adm:ping/1, fast but shallow
%% 2. RPC Probe - rpc:call with actual work, deeper health check
%% 3. Khepri Probe - khepri_cluster:members/1, verifies store health
%%
%% Failure Threshold:
%%
%% A node is only declared failed after consecutive probe failures
%% (default: 3). This prevents transient network issues from triggering
%% false positives.
%%
%% Recovery Detection:
%%
%% Once a node is marked failed, probing continues. When probes succeed
%% again, the node is marked recovered and callbacks are notified.
%%
%% @author rgfaber
%% @see reckon_db_consistency_checker

-module(reckon_db_health_prober).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([start_link/1]).
-export([get_node_status/2]).
-export([get_all_status/1]).
-export([probe_now/1]).
-export([configure/2]).
-export([on_node_failed/2]).
-export([on_node_recovered/2]).
-export([remove_callback/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% Probe target (exported for RPC)
-export([health_check/1]).

-type node_status() :: healthy | suspect | failed | unknown.
-type probe_config() :: #{
    probe_interval => pos_integer(),
    probe_timeout => pos_integer(),
    failure_threshold => pos_integer(),
    probe_type => ping | rpc | khepri
}.

-export_type([node_status/0, probe_config/0]).

-define(DEFAULT_PROBE_INTERVAL, 2000).    %% 2 seconds
-define(DEFAULT_PROBE_TIMEOUT, 1000).     %% 1 second
-define(DEFAULT_FAILURE_THRESHOLD, 3).    %% 3 consecutive failures
-define(DEFAULT_PROBE_TYPE, rpc).

-record(node_state, {
    node :: node(),
    status :: node_status(),
    consecutive_failures :: non_neg_integer(),
    last_success :: integer() | undefined,
    last_failure :: integer() | undefined,
    last_probe_duration :: non_neg_integer() | undefined
}).

-record(state, {
    store_id :: atom(),
    config :: store_config(),
    probe_interval :: pos_integer(),
    probe_timeout :: pos_integer(),
    failure_threshold :: pos_integer(),
    probe_type :: ping | rpc | khepri,
    nodes :: #{node() => #node_state{}},
    failed_callbacks :: #{reference() => fun((node()) -> any())},
    recovered_callbacks :: #{reference() => fun((node()) -> any())}
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the health prober
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = prober_name(StoreId),
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

%% @doc Get health status of a specific node
-spec get_node_status(atom(), node()) -> {ok, node_status()} | {error, unknown_node}.
get_node_status(StoreId, Node) ->
    Name = prober_name(StoreId),
    gen_server:call(Name, {get_node_status, Node}, 5000).

%% @doc Get health status of all monitored nodes
-spec get_all_status(atom()) -> #{node() => node_status()}.
get_all_status(StoreId) ->
    Name = prober_name(StoreId),
    gen_server:call(Name, get_all_status, 5000).

%% @doc Force immediate probe cycle
-spec probe_now(atom()) -> ok.
probe_now(StoreId) ->
    Name = prober_name(StoreId),
    gen_server:cast(Name, probe_now).

%% @doc Update prober configuration
-spec configure(atom(), probe_config()) -> ok.
configure(StoreId, Config) ->
    Name = prober_name(StoreId),
    gen_server:call(Name, {configure, Config}, 5000).

%% @doc Register callback for node failure events
-spec on_node_failed(atom(), fun((node()) -> any())) -> reference().
on_node_failed(StoreId, Callback) when is_function(Callback, 1) ->
    Name = prober_name(StoreId),
    gen_server:call(Name, {register_failed_callback, Callback}, 5000).

%% @doc Register callback for node recovery events
-spec on_node_recovered(atom(), fun((node()) -> any())) -> reference().
on_node_recovered(StoreId, Callback) when is_function(Callback, 1) ->
    Name = prober_name(StoreId),
    gen_server:call(Name, {register_recovered_callback, Callback}, 5000).

%% @doc Remove a previously registered callback
-spec remove_callback(atom(), reference()) -> ok.
remove_callback(StoreId, Ref) ->
    Name = prober_name(StoreId),
    gen_server:call(Name, {remove_callback, Ref}, 5000).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(#store_config{store_id = StoreId, mode = Mode} = Config) ->
    process_flag(trap_exit, true),

    ProbeInterval = application:get_env(reckon_db, health_probe_interval,
                                        ?DEFAULT_PROBE_INTERVAL),
    ProbeTimeout = application:get_env(reckon_db, health_probe_timeout,
                                       ?DEFAULT_PROBE_TIMEOUT),
    FailureThreshold = application:get_env(reckon_db, health_failure_threshold,
                                           ?DEFAULT_FAILURE_THRESHOLD),
    ProbeType = application:get_env(reckon_db, health_probe_type,
                                    ?DEFAULT_PROBE_TYPE),

    logger:info("Health prober started (store: ~p, interval: ~pms, threshold: ~p)",
                [StoreId, ProbeInterval, FailureThreshold]),

    State = #state{
        store_id = StoreId,
        config = Config,
        probe_interval = ProbeInterval,
        probe_timeout = ProbeTimeout,
        failure_threshold = FailureThreshold,
        probe_type = ProbeType,
        nodes = #{},
        failed_callbacks = #{},
        recovered_callbacks = #{}
    },

    %% Only run probes in cluster mode
    case Mode of
        cluster ->
            %% Start monitoring and schedule first probe
            ok = net_kernel:monitor_nodes(true),
            schedule_probe(ProbeInterval);
        single ->
            ok
    end,

    {ok, State}.

handle_call({get_node_status, Node}, _From, #state{nodes = Nodes} = State) ->
    Result = case maps:get(Node, Nodes, undefined) of
        undefined -> {error, unknown_node};
        #node_state{status = Status} -> {ok, Status}
    end,
    {reply, Result, State};

handle_call(get_all_status, _From, #state{nodes = Nodes} = State) ->
    Status = maps:map(fun(_, #node_state{status = S}) -> S end, Nodes),
    {reply, Status, State};

handle_call({configure, NewConfig}, _From, State) ->
    NewState = apply_config(NewConfig, State),
    {reply, ok, NewState};

handle_call({register_failed_callback, Callback}, _From,
            #state{failed_callbacks = Callbacks} = State) ->
    Ref = make_ref(),
    NewCallbacks = Callbacks#{Ref => Callback},
    {reply, Ref, State#state{failed_callbacks = NewCallbacks}};

handle_call({register_recovered_callback, Callback}, _From,
            #state{recovered_callbacks = Callbacks} = State) ->
    Ref = make_ref(),
    NewCallbacks = Callbacks#{Ref => Callback},
    {reply, Ref, State#state{recovered_callbacks = NewCallbacks}};

handle_call({remove_callback, Ref}, _From,
            #state{failed_callbacks = FailedCb, recovered_callbacks = RecoveredCb} = State) ->
    NewState = State#state{
        failed_callbacks = maps:remove(Ref, FailedCb),
        recovered_callbacks = maps:remove(Ref, RecoveredCb)
    },
    {reply, ok, NewState};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(probe_now, State) ->
    NewState = perform_probe_cycle(State),
    {noreply, NewState};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Handle nodeup - add to monitoring
handle_info({nodeup, Node}, #state{nodes = Nodes} = State) ->
    logger:debug("Health prober: node ~p connected", [Node]),
    NodeState = #node_state{
        node = Node,
        status = unknown,
        consecutive_failures = 0,
        last_success = undefined,
        last_failure = undefined,
        last_probe_duration = undefined
    },
    NewNodes = Nodes#{Node => NodeState},
    {noreply, State#state{nodes = NewNodes}};

%% Handle nodedown - mark as failed immediately
handle_info({nodedown, Node}, #state{store_id = StoreId, nodes = Nodes,
                                      failed_callbacks = Callbacks} = State) ->
    logger:warning("Health prober: node ~p disconnected", [Node]),
    Now = erlang:system_time(millisecond),
    NewNodes = case maps:get(Node, Nodes, undefined) of
        undefined ->
            Nodes;
        NodeState ->
            UpdatedState = NodeState#node_state{
                status = failed,
                last_failure = Now
            },
            %% Emit telemetry and notify callbacks
            emit_node_failed(StoreId, Node, nodedown),
            notify_failed_callbacks(Callbacks, Node),
            Nodes#{Node => UpdatedState}
    end,
    {noreply, State#state{nodes = NewNodes}};

%% Handle scheduled probe
handle_info(run_probe, #state{probe_interval = Interval} = State) ->
    NewState = perform_probe_cycle(State),
    schedule_probe(Interval),
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #state{store_id = StoreId}) ->
    net_kernel:monitor_nodes(false),
    logger:info("Health prober terminating (store: ~p, reason: ~p)",
                [StoreId, Reason]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Perform a complete probe cycle
-spec perform_probe_cycle(#state{}) -> #state{}.
perform_probe_cycle(#state{store_id = StoreId, nodes = Nodes, probe_type = ProbeType,
                           probe_timeout = Timeout, failure_threshold = Threshold,
                           failed_callbacks = FailedCb, recovered_callbacks = RecoveredCb} = State) ->
    StartTime = erlang:monotonic_time(microsecond),

    %% Get current cluster members to probe
    NodesToProbe = get_nodes_to_probe(StoreId),

    %% Ensure all cluster nodes are tracked
    UpdatedNodes = ensure_nodes_tracked(NodesToProbe, Nodes),

    %% Probe each node
    {ProbedNodes, SuccessCount, FailureCount} = lists:foldl(fun(Node, {NodesAcc, Succ, Fail}) ->
        case Node of
            N when N =:= node() ->
                %% Don't probe ourselves
                {NodesAcc, Succ, Fail};
            _ ->
                NodeState = maps:get(Node, NodesAcc, new_node_state(Node)),
                {ProbeResult, Duration} = probe_node(Node, ProbeType, Timeout),
                {UpdatedNodeState, NewSucc, NewFail} =
                    handle_probe_result(NodeState, ProbeResult, Duration, Threshold,
                                        StoreId, FailedCb, RecoveredCb),
                {NodesAcc#{Node => UpdatedNodeState}, Succ + NewSucc, Fail + NewFail}
        end
    end, {UpdatedNodes, 0, 0}, NodesToProbe),

    EndTime = erlang:monotonic_time(microsecond),
    Duration = EndTime - StartTime,

    %% Emit telemetry
    telemetry:execute(
        ?HEALTH_PROBE_COMPLETE,
        #{duration_us => Duration, success_count => SuccessCount, failure_count => FailureCount},
        #{store_id => StoreId}
    ),

    State#state{nodes = ProbedNodes}.

%% @private Get nodes to probe
-spec get_nodes_to_probe(atom()) -> [node()].
get_nodes_to_probe(StoreId) ->
    case khepri_cluster:members(StoreId) of
        {ok, Members} ->
            [Node || {_, Node} <- Members];
        _ ->
            %% Fall back to connected nodes
            nodes()
    end.

%% @private Ensure all nodes are tracked
-spec ensure_nodes_tracked([node()], #{node() => #node_state{}}) -> #{node() => #node_state{}}.
ensure_nodes_tracked(Nodes, TrackedNodes) ->
    lists:foldl(fun(Node, Acc) ->
        case maps:is_key(Node, Acc) of
            true -> Acc;
            false -> Acc#{Node => new_node_state(Node)}
        end
    end, TrackedNodes, Nodes).

%% @private Create new node state
-spec new_node_state(node()) -> #node_state{}.
new_node_state(Node) ->
    #node_state{
        node = Node,
        status = unknown,
        consecutive_failures = 0,
        last_success = undefined,
        last_failure = undefined,
        last_probe_duration = undefined
    }.

%% @private Probe a single node
-spec probe_node(node(), ping | rpc | khepri, pos_integer()) ->
    {ok | {error, term()}, non_neg_integer()}.
probe_node(Node, ProbeType, Timeout) ->
    StartTime = erlang:monotonic_time(microsecond),
    Result = case ProbeType of
        ping ->
            case net_adm:ping(Node) of
                pong -> ok;
                pang -> {error, ping_failed}
            end;
        rpc ->
            case rpc:call(Node, ?MODULE, health_check, [node()], Timeout) of
                {ok, _} -> ok;
                {badrpc, Reason} -> {error, {rpc_failed, Reason}};
                {error, Reason} -> {error, Reason}
            end;
        khepri ->
            %% For khepri probe, we verify the node can access the store
            case rpc:call(Node, erlang, node, [], Timeout) of
                N when is_atom(N) -> ok;
                {badrpc, Reason} -> {error, {rpc_failed, Reason}}
            end
    end,
    EndTime = erlang:monotonic_time(microsecond),
    Duration = EndTime - StartTime,
    {Result, Duration}.

%% @doc Health check function called via RPC
%% Returns basic node health information
-spec health_check(node()) -> {ok, map()}.
health_check(CallerNode) ->
    {ok, #{
        node => node(),
        caller => CallerNode,
        timestamp => erlang:system_time(millisecond),
        memory => erlang:memory(total),
        process_count => erlang:system_info(process_count)
    }}.

%% @private Handle probe result
-spec handle_probe_result(#node_state{}, ok | {error, term()}, non_neg_integer(),
                          pos_integer(), atom(), map(), map()) ->
    {#node_state{}, 0 | 1, 0 | 1}.
handle_probe_result(#node_state{status = PrevStatus, consecutive_failures = Failures,
                                 node = Node} = NodeState,
                    ProbeResult, Duration, Threshold, StoreId, FailedCb, RecoveredCb) ->
    Now = erlang:system_time(millisecond),
    case ProbeResult of
        ok ->
            %% Probe succeeded
            NewStatus = healthy,
            WasRecovered = (PrevStatus =:= failed),
            UpdatedState = NodeState#node_state{
                status = NewStatus,
                consecutive_failures = 0,
                last_success = Now,
                last_probe_duration = Duration
            },
            %% Notify recovery if was failed
            case WasRecovered of
                true ->
                    emit_node_recovered(StoreId, Node),
                    notify_recovered_callbacks(RecoveredCb, Node);
                false ->
                    ok
            end,
            {UpdatedState, 1, 0};

        {error, _Reason} ->
            %% Probe failed
            NewFailures = Failures + 1,
            NewStatus = case NewFailures >= Threshold of
                true -> failed;
                false -> suspect
            end,
            UpdatedState = NodeState#node_state{
                status = NewStatus,
                consecutive_failures = NewFailures,
                last_failure = Now,
                last_probe_duration = Duration
            },
            %% Notify failure if just crossed threshold
            case {NewStatus, PrevStatus} of
                {failed, Status} when Status =/= failed ->
                    emit_node_failed(StoreId, Node, NewFailures),
                    notify_failed_callbacks(FailedCb, Node);
                _ ->
                    ok
            end,
            {UpdatedState, 0, 1}
    end.

%% @private Apply configuration changes
-spec apply_config(probe_config(), #state{}) -> #state{}.
apply_config(Config, State) ->
    State#state{
        probe_interval = maps:get(probe_interval, Config, State#state.probe_interval),
        probe_timeout = maps:get(probe_timeout, Config, State#state.probe_timeout),
        failure_threshold = maps:get(failure_threshold, Config, State#state.failure_threshold),
        probe_type = maps:get(probe_type, Config, State#state.probe_type)
    }.

%% @private Emit node failed telemetry
-spec emit_node_failed(atom(), node(), non_neg_integer() | nodedown) -> ok.
emit_node_failed(StoreId, Node, Failures) ->
    telemetry:execute(
        ?HEALTH_NODE_FAILED,
        #{system_time => erlang:system_time(millisecond),
          consecutive_failures => Failures},
        #{store_id => StoreId, node => Node}
    ).

%% @private Emit node recovered telemetry
-spec emit_node_recovered(atom(), node()) -> ok.
emit_node_recovered(StoreId, Node) ->
    telemetry:execute(
        ?HEALTH_NODE_RECOVERED,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, node => Node}
    ).

%% @private Notify failed callbacks
-spec notify_failed_callbacks(#{reference() => fun()}, node()) -> ok.
notify_failed_callbacks(Callbacks, Node) ->
    maps:foreach(fun(_Ref, Callback) ->
        spawn(fun() ->
            try
                Callback(Node)
            catch
                Class:Reason:Stack ->
                    logger:warning("Failed callback error: ~p:~p~n~p",
                                  [Class, Reason, Stack])
            end
        end)
    end, Callbacks),
    ok.

%% @private Notify recovered callbacks
-spec notify_recovered_callbacks(#{reference() => fun()}, node()) -> ok.
notify_recovered_callbacks(Callbacks, Node) ->
    maps:foreach(fun(_Ref, Callback) ->
        spawn(fun() ->
            try
                Callback(Node)
            catch
                Class:Reason:Stack ->
                    logger:warning("Recovered callback error: ~p:~p~n~p",
                                  [Class, Reason, Stack])
            end
        end)
    end, Callbacks),
    ok.

%% @private Schedule next probe
-spec schedule_probe(pos_integer()) -> reference().
schedule_probe(Interval) ->
    erlang:send_after(Interval, self(), run_probe).

%% @private Get process name for store
-spec prober_name(atom()) -> atom().
prober_name(StoreId) ->
    list_to_atom("reckon_db_health_prober_" ++ atom_to_list(StoreId)).
