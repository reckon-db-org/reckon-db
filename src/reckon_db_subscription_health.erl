%% @doc Subscription health monitor for reckon-db
%%
%% Periodic health checks that detect and clean up:
%% - Stale subscriptions: subscriber process is dead
%% - Orphaned emitter pools: pool running without a subscription
%% - Missing emitter pools: subscription exists but no pool on leader
%%
%% Runs as a child of notification_sup (after leader_sup and emitter_sup).
%% Only performs cleanup when this node is the Ra leader.
%%
%% @author rgfaber

-module(reckon_db_subscription_health).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([start_link/1]).
-export([health_check/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DEFAULT_CHECK_INTERVAL, 60000).  %% 60 seconds
-define(MAX_CLEANUP_RETRIES, 3).

-record(state, {
    store_id :: atom(),
    config :: store_config(),
    check_interval :: pos_integer(),
    cleanup_count :: non_neg_integer()
}).

-record(health_report, {
    stale_subscriptions = [] :: [{binary(), binary()}],
    orphaned_pools = [] :: [binary()],
    missing_pools = [] :: [binary()],
    healthy_count = 0 :: non_neg_integer(),
    total_subscriptions = 0 :: non_neg_integer(),
    total_pools = 0 :: non_neg_integer()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:health_monitor_name(StoreId),
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

%% @doc Run an on-demand health check and return the report
-spec health_check(atom()) -> {ok, map()} | {error, term()}.
health_check(StoreId) ->
    Name = reckon_db_naming:health_monitor_name(StoreId),
    case whereis(Name) of
        undefined -> {error, not_running};
        _Pid -> gen_server:call(Name, health_check, 10000)
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(#store_config{store_id = StoreId, options = Options} = Config) ->
    process_flag(trap_exit, true),

    Interval = get_check_interval(Options),

    logger:info("Subscription health monitor started (store: ~p, interval: ~pms)",
                [StoreId, Interval]),

    %% Schedule first check after a delay to let the system stabilize
    schedule_check(Interval * 2),

    State = #state{
        store_id = StoreId,
        config = Config,
        check_interval = Interval,
        cleanup_count = 0
    },
    {ok, State}.

handle_call(health_check, _From, #state{store_id = StoreId} = State) ->
    Report = run_health_check(StoreId),
    {reply, {ok, report_to_map(Report)}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_health, #state{store_id = StoreId,
                                  check_interval = Interval} = State) ->
    NewState = case reckon_db_store_coordinator:is_leader(StoreId) of
        true ->
            Report = run_health_check(StoreId),
            CleanedUp = maybe_cleanup(StoreId, Report),
            maybe_log_report(StoreId, Report, CleanedUp),
            State#state{cleanup_count = State#state.cleanup_count + CleanedUp};
        false ->
            State
    end,

    schedule_check(Interval),
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #state{store_id = StoreId}) ->
    logger:info("Subscription health monitor terminating (store: ~p, reason: ~p)",
                [StoreId, Reason]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Run a complete health check
-spec run_health_check(atom()) -> #health_report{}.
run_health_check(StoreId) ->
    %% Get all subscriptions
    Subscriptions = case reckon_db_subscriptions:list(StoreId) of
        {ok, Subs} -> Subs;
        {error, _} -> []
    end,

    %% Get all running emitter pool names
    RunningPools = get_running_pools(StoreId),

    %% Check each subscription
    {Stale, Missing, Healthy} = check_subscriptions(StoreId, Subscriptions, RunningPools),

    %% Find orphaned pools (running but no subscription)
    SubscriptionKeys = [Sub#subscription.id || Sub <- Subscriptions],
    Orphaned = find_orphaned_pools(StoreId, RunningPools, SubscriptionKeys),

    #health_report{
        stale_subscriptions = Stale,
        orphaned_pools = Orphaned,
        missing_pools = Missing,
        healthy_count = Healthy,
        total_subscriptions = length(Subscriptions),
        total_pools = length(RunningPools)
    }.

%% @private Check all subscriptions for health issues
-spec check_subscriptions(atom(), [subscription()], [binary()]) ->
    {[{binary(), binary()}], [binary()], non_neg_integer()}.
check_subscriptions(StoreId, Subscriptions, RunningPools) ->
    lists:foldl(
        fun(#subscription{id = SubId, subscription_name = Name,
                          subscriber_pid = SubscriberPid}, {StaleAcc, MissingAcc, HealthyAcc}) ->
            IsStale = is_subscriber_dead(SubscriberPid),
            HasPool = has_running_pool(StoreId, SubId, RunningPools),

            case {IsStale, HasPool} of
                {true, _} ->
                    {[{SubId, Name} | StaleAcc], MissingAcc, HealthyAcc};
                {false, false} ->
                    {StaleAcc, [SubId | MissingAcc], HealthyAcc};
                {false, true} ->
                    {StaleAcc, MissingAcc, HealthyAcc + 1}
            end
        end,
        {[], [], 0},
        Subscriptions
    ).

%% @private Check if a subscriber PID is dead
-spec is_subscriber_dead(pid() | undefined) -> boolean().
is_subscriber_dead(undefined) ->
    false;
is_subscriber_dead(Pid) when is_pid(Pid) ->
    not erlang:is_process_alive(Pid);
is_subscriber_dead(_) ->
    false.

%% @private Check if an emitter pool is running for a subscription
-spec has_running_pool(atom(), binary(), [binary()]) -> boolean().
has_running_pool(StoreId, SubId, _RunningPools) ->
    PoolName = reckon_db_emitter_pool:name(StoreId, SubId),
    whereis(PoolName) =/= undefined.

%% @private Get all running emitter pool subscription IDs
-spec get_running_pools(atom()) -> [binary()].
get_running_pools(StoreId) ->
    SupName = reckon_db_naming:emitter_sup_name(StoreId),
    case whereis(SupName) of
        undefined ->
            [];
        _Pid ->
            Children = supervisor:which_children(SupName),
            [extract_sub_id(Id) || {Id, Child, _, _} <- Children,
                                    Child =/= undefined,
                                    Child =/= restarting]
    end.

%% @private Extract subscription ID from emitter pool child ID
-spec extract_sub_id(term()) -> binary().
extract_sub_id(Id) when is_atom(Id) ->
    %% Pool names are atoms like 'reckon_db_emitter_pool_StoreId_SubId'
    %% We need the SubId part
    IdStr = atom_to_list(Id),
    %% Find the last underscore-separated segment
    case string:split(IdStr, "_", trailing) of
        [_, SubId] -> list_to_binary(SubId);
        _ -> list_to_binary(IdStr)
    end;
extract_sub_id(Id) ->
    %% Fallback
    list_to_binary(io_lib:format("~p", [Id])).

%% @private Find orphaned pools (running without a subscription)
-spec find_orphaned_pools(atom(), [binary()], [binary()]) -> [binary()].
find_orphaned_pools(_StoreId, RunningPools, SubscriptionKeys) ->
    [PoolId || PoolId <- RunningPools,
               not lists:member(PoolId, SubscriptionKeys)].

%% @private Perform cleanup actions
-spec maybe_cleanup(atom(), #health_report{}) -> non_neg_integer().
maybe_cleanup(StoreId, #health_report{
    stale_subscriptions = Stale,
    orphaned_pools = Orphaned,
    missing_pools = Missing
}) ->
    CleanedStale = cleanup_stale_subscriptions(StoreId, Stale),
    CleanedOrphaned = cleanup_orphaned_pools(StoreId, Orphaned),
    StartedMissing = start_missing_pools(StoreId, Missing),
    CleanedStale + CleanedOrphaned + StartedMissing.

%% @private Clean up stale subscriptions (dead subscriber)
-spec cleanup_stale_subscriptions(atom(), [{binary(), binary()}]) -> non_neg_integer().
cleanup_stale_subscriptions(StoreId, Stale) ->
    lists:foldl(
        fun({SubId, Name}, Count) ->
            logger:warning("Cleaning up stale subscription ~s (~s) in store ~p",
                          [Name, SubId, StoreId]),
            %% Stop the emitter pool first
            reckon_db_emitter_pool:stop(StoreId, SubId),
            %% Remove the subscription
            case reckon_db_subscriptions:unsubscribe(StoreId, SubId) of
                ok -> Count + 1;
                {error, _} -> Count
            end
        end,
        0,
        Stale
    ).

%% @private Clean up orphaned emitter pools
-spec cleanup_orphaned_pools(atom(), [binary()]) -> non_neg_integer().
cleanup_orphaned_pools(StoreId, Orphaned) ->
    lists:foldl(
        fun(PoolId, Count) ->
            logger:warning("Stopping orphaned emitter pool ~s in store ~p",
                          [PoolId, StoreId]),
            case reckon_db_emitter_pool:stop(StoreId, PoolId) of
                ok -> Count + 1;
                {error, _} -> Count
            end
        end,
        0,
        Orphaned
    ).

%% @private Start missing emitter pools
-spec start_missing_pools(atom(), [binary()]) -> non_neg_integer().
start_missing_pools(StoreId, Missing) ->
    lists:foldl(
        fun(SubId, Count) ->
            case reckon_db_subscriptions:get(StoreId, SubId) of
                {ok, Subscription} ->
                    logger:info("Starting missing emitter pool for ~s in store ~p",
                               [SubId, StoreId]),
                    case reckon_db_emitter_pool:start_emitter(StoreId, Subscription) of
                        {ok, _Pid} -> Count + 1;
                        {error, {already_started, _}} -> Count;
                        {error, _} -> Count
                    end;
                {error, _} ->
                    Count
            end
        end,
        0,
        Missing
    ).

%% @private Log health report if there are issues
-spec maybe_log_report(atom(), #health_report{}, non_neg_integer()) -> ok.
maybe_log_report(StoreId, #health_report{
    stale_subscriptions = Stale,
    orphaned_pools = Orphaned,
    missing_pools = Missing,
    healthy_count = Healthy,
    total_subscriptions = Total
}, CleanedUp) ->
    case {Stale, Orphaned, Missing} of
        {[], [], []} when Total > 0 ->
            logger:debug("Subscription health OK: ~p/~p healthy (store: ~p)",
                         [Healthy, Total, StoreId]);
        {[], [], []} ->
            ok;
        _ ->
            logger:info("Subscription health check (store: ~p): "
                        "~p stale, ~p orphaned, ~p missing, ~p healthy of ~p total. "
                        "Cleaned up: ~p",
                        [StoreId, length(Stale), length(Orphaned),
                         length(Missing), Healthy, Total, CleanedUp])
    end,
    ok.

%% @private Convert report record to map for API response
-spec report_to_map(#health_report{}) -> map().
report_to_map(#health_report{
    stale_subscriptions = Stale,
    orphaned_pools = Orphaned,
    missing_pools = Missing,
    healthy_count = Healthy,
    total_subscriptions = TotalSubs,
    total_pools = TotalPools
}) ->
    #{
        stale_subscriptions => [{Id, Name} || {Id, Name} <- Stale],
        orphaned_pools => Orphaned,
        missing_pools => Missing,
        healthy_count => Healthy,
        total_subscriptions => TotalSubs,
        total_pools => TotalPools,
        status => case {Stale, Orphaned, Missing} of
            {[], [], []} -> healthy;
            _ -> degraded
        end
    }.

%% @private Schedule the next health check
-spec schedule_check(pos_integer()) -> reference().
schedule_check(Interval) ->
    erlang:send_after(Interval, self(), check_health).

%% @private Get the check interval from options
-spec get_check_interval(map() | undefined) -> pos_integer().
get_check_interval(Options) when is_map(Options) ->
    maps:get(health_check_interval, Options, ?DEFAULT_CHECK_INTERVAL);
get_check_interval(_) ->
    ?DEFAULT_CHECK_INTERVAL.
