%% @doc Node monitor for reckon-db
%%
%% Monitors cluster node health and handles node up/down events.
%%
%% Responsibilities:
%% - Monitor node connectivity via net_kernel:monitor_nodes/1
%% - Trigger cluster join attempts on nodeup events
%% - Track cluster membership changes
%% - Emit telemetry on node events
%% - Periodic leader checks
%%
%% @author rgfaber

-module(reckon_db_node_monitor).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([start_link/1]).
-export([get_members/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(LEADER_CHECK_INTERVAL, 5000).
-define(MEMBERSHIP_CHECK_INTERVAL, 10000).

-record(state, {
    store_id :: atom(),
    config :: store_config(),
    current_leader :: node() | undefined,
    previous_members :: [term()],
    mode :: single | cluster
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:node_monitor_name(StoreId),
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

%% @doc Get current cluster members
-spec get_members(atom()) -> {ok, [term()]} | {error, term()}.
get_members(StoreId) ->
    reckon_db_store_coordinator:members(StoreId).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(#store_config{store_id = StoreId, mode = Mode} = Config) ->
    %% Monitor node events
    ok = net_kernel:monitor_nodes(true),

    logger:info("Node monitor started (store: ~p, mode: ~p)", [StoreId, Mode]),

    State = #state{
        store_id = StoreId,
        config = Config,
        current_leader = undefined,
        previous_members = [],
        mode = Mode
    },

    %% Schedule periodic checks based on mode
    case Mode of
        cluster ->
            %% In cluster mode, check leader and membership periodically
            schedule_leader_check(?LEADER_CHECK_INTERVAL),
            schedule_membership_check(?MEMBERSHIP_CHECK_INTERVAL);
        single ->
            %% In single mode, check leader once to activate
            schedule_leader_check(1000)
    end,

    {ok, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Handle node up event
handle_info({nodeup, Node}, #state{store_id = StoreId, mode = Mode} = State) ->
    logger:info("Node ~p connected (store: ~p)", [Node, StoreId]),
    telemetry:execute(
        ?CLUSTER_NODE_UP,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, node => Node, member_count => length(nodes()) + 1}
    ),

    %% In cluster mode, try to join cluster on nodeup
    case Mode of
        cluster ->
            handle_nodeup_cluster_join(StoreId);
        single ->
            ok
    end,

    {noreply, State};

%% Handle node down event
handle_info({nodedown, Node}, #state{store_id = StoreId} = State) ->
    logger:warning("Node ~p disconnected (store: ~p)", [Node, StoreId]),
    telemetry:execute(
        ?CLUSTER_NODE_DOWN,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, node => Node, reason => nodedown,
          member_count => length(nodes()) + 1}
    ),

    %% Trigger immediate leader and membership checks
    self() ! check_leader,
    self() ! check_members,

    {noreply, State};

%% Handle periodic leader check
handle_info(check_leader, #state{store_id = StoreId, current_leader = PreviousLeader,
                                 mode = Mode} = State) ->
    NewState = case reckon_db_store_coordinator:leader(StoreId) of
        {ok, LeaderNode} ->
            handle_leader_detected(LeaderNode, PreviousLeader, StoreId, State);
        {error, no_leader} ->
            handle_no_leader(PreviousLeader, StoreId, State)
    end,

    %% Schedule next check
    case Mode of
        cluster ->
            schedule_leader_check(?LEADER_CHECK_INTERVAL);
        single ->
            %% In single mode, keep retrying until leader is detected.
            %% Ra leader election may not complete before the first check.
            %% Once detected, no more checks needed (no leadership changes
            %% in single-node mode).
            case NewState#state.current_leader of
                undefined ->
                    schedule_leader_check(?LEADER_CHECK_INTERVAL);
                _ ->
                    ok
            end
    end,

    {noreply, NewState};

%% Handle periodic membership check
handle_info(check_members, #state{store_id = StoreId, previous_members = PreviousMembers,
                                  current_leader = Leader, mode = Mode} = State) ->
    NewState = case reckon_db_store_coordinator:members(StoreId) of
        {ok, Members} ->
            handle_membership_change(Members, PreviousMembers, Leader, StoreId, State);
        {error, _Reason} ->
            State
    end,

    %% Schedule next check in cluster mode
    case Mode of
        cluster -> schedule_membership_check(?MEMBERSHIP_CHECK_INTERVAL);
        single -> ok
    end,

    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    net_kernel:monitor_nodes(false),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Handle nodeup in cluster mode
-spec handle_nodeup_cluster_join(atom()) -> ok.
handle_nodeup_cluster_join(StoreId) ->
    case reckon_db_store_coordinator:should_handle_nodeup(StoreId) of
        true ->
            logger:info("Attempting cluster join due to nodeup (store: ~p)", [StoreId]),
            spawn(fun() ->
                case reckon_db_store_coordinator:join_cluster(StoreId) of
                    ok ->
                        logger:info("Cluster join successful (store: ~p)", [StoreId]);
                    coordinator ->
                        logger:info("Acting as coordinator (store: ~p)", [StoreId]);
                    _ ->
                        ok
                end
            end);
        false ->
            logger:debug("Already in cluster, ignoring nodeup (store: ~p)", [StoreId])
    end,
    ok.

%% @private Handle leader detected
-spec handle_leader_detected(node(), node() | undefined, atom(), #state{}) -> #state{}.
handle_leader_detected(LeaderNode, LeaderNode, _StoreId, State) ->
    %% No change
    State;
handle_leader_detected(LeaderNode, undefined, StoreId, State) ->
    %% First leader detection
    logger:info("Leader detected: ~p (store: ~p)", [LeaderNode, StoreId]),
    maybe_activate_leader(LeaderNode, StoreId),
    State#state{current_leader = LeaderNode};
handle_leader_detected(NewLeader, PreviousLeader, StoreId, State) ->
    %% Leadership change
    logger:info("Leadership changed: ~p -> ~p (store: ~p)",
               [PreviousLeader, NewLeader, StoreId]),
    telemetry:execute(
        ?CLUSTER_LEADER_ELECTED,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, leader => NewLeader, previous_leader => PreviousLeader}
    ),
    maybe_activate_leader(NewLeader, StoreId),
    State#state{current_leader = NewLeader}.

%% @private Handle no leader
-spec handle_no_leader(node() | undefined, atom(), #state{}) -> #state{}.
handle_no_leader(undefined, _StoreId, State) ->
    State;
handle_no_leader(PreviousLeader, StoreId, State) ->
    logger:warning("Leadership lost: was ~p (store: ~p)", [PreviousLeader, StoreId]),
    State#state{current_leader = undefined}.

%% @private Maybe activate leader worker
-spec maybe_activate_leader(node(), atom()) -> ok.
maybe_activate_leader(LeaderNode, StoreId) ->
    case node() =:= LeaderNode of
        true ->
            logger:info("This node is leader, activating (store: ~p)", [StoreId]),
            reckon_db_leader:activate(StoreId);
        false ->
            ok
    end.

%% @private Handle membership change
-spec handle_membership_change([term()], [term()], node() | undefined, atom(), #state{}) ->
    #state{}.
handle_membership_change(Members, Members, _Leader, _StoreId, State) ->
    %% No change
    State;
handle_membership_change(CurrentMembers, PreviousMembers, _Leader, StoreId, State) ->
    %% Membership changed
    Added = CurrentMembers -- PreviousMembers,
    Removed = PreviousMembers -- CurrentMembers,

    case Added of
        [] -> ok;
        _ -> logger:info("Members joined: ~p (store: ~p)", [Added, StoreId])
    end,

    case Removed of
        [] -> ok;
        _ -> logger:info("Members left: ~p (store: ~p)", [Removed, StoreId])
    end,

    logger:info("Current cluster membership: ~p members (store: ~p)",
               [length(CurrentMembers), StoreId]),

    State#state{previous_members = CurrentMembers}.

%% @private Schedule leader check
-spec schedule_leader_check(non_neg_integer()) -> reference().
schedule_leader_check(Interval) ->
    erlang:send_after(Interval, self(), check_leader).

%% @private Schedule membership check
-spec schedule_membership_check(non_neg_integer()) -> reference().
schedule_membership_check(Interval) ->
    erlang:send_after(Interval, self(), check_members).
