%% @doc Memory pressure monitoring for reckon-db
%%
%% Monitors system memory usage and triggers adaptive behavior
%% when memory pressure increases. Components can register callbacks
%% to be notified of pressure changes.
%%
%% Pressure levels:
%% - `normal`: Full caching, all features enabled
%% - `elevated`: Reduce cache sizes, flush more often
%% - `critical`: Pause non-essential operations, aggressive cleanup
%%
%% Usage:
%% ```
%% %% Start monitoring
%% reckon_db_memory:start_link().
%%
%% %% Check current level
%% normal = reckon_db_memory:level().
%%
%% %% Register callback
%% reckon_db_memory:on_pressure_change(fun(Level) ->
%%     logger:info("Memory pressure: ~p", [Level])
%% end).
%% '''
%%
%% @author rgfaber

-module(reckon_db_memory).
-behaviour(gen_server).

%% API
-export([
    start_link/0,
    start_link/1,
    level/0,
    level/1,
    configure/1,
    on_pressure_change/1,
    remove_callback/1,
    get_config/0,
    get_stats/0,
    check_now/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%%====================================================================
%% Types
%%====================================================================

-type pressure_level() :: normal | elevated | critical.
-type callback_ref() :: reference().
-type callback_fun() :: fun((pressure_level()) -> any()).

-type config() :: #{
    elevated_threshold => float(),  %% 0.0 - 1.0
    critical_threshold => float(),  %% 0.0 - 1.0
    check_interval => pos_integer() %% milliseconds
}.

-export_type([pressure_level/0, callback_ref/0, callback_fun/0, config/0]).

%%====================================================================
%% Defaults
%%====================================================================

-define(DEFAULT_ELEVATED_THRESHOLD, 0.70).
-define(DEFAULT_CRITICAL_THRESHOLD, 0.85).
-define(DEFAULT_CHECK_INTERVAL, 10000).  %% 10 seconds
-define(SERVER, ?MODULE).

%%====================================================================
%% State
%%====================================================================

-record(state, {
    level = normal :: pressure_level(),
    callbacks = #{} :: #{callback_ref() => callback_fun()},
    elevated_threshold :: float(),
    critical_threshold :: float(),
    check_interval :: pos_integer(),
    timer_ref :: reference() | undefined,
    last_check :: integer() | undefined,
    memory_used :: non_neg_integer() | undefined,
    memory_total :: non_neg_integer() | undefined
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the memory monitor with default configuration.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(#{}).

%% @doc Start the memory monitor with custom configuration.
-spec start_link(config()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

%% @doc Get current memory pressure level.
-spec level() -> pressure_level().
level() ->
    gen_server:call(?SERVER, level).

%% @doc Get pressure level for a given memory usage ratio.
%% This is a pure function useful for testing.
-spec level(float()) -> pressure_level().
level(UsageRatio) when is_float(UsageRatio) ->
    level(UsageRatio, ?DEFAULT_ELEVATED_THRESHOLD, ?DEFAULT_CRITICAL_THRESHOLD).

%% @doc Update configuration.
-spec configure(config()) -> ok.
configure(Config) ->
    gen_server:call(?SERVER, {configure, Config}).

%% @doc Register callback for pressure level changes.
%% Returns a reference that can be used to remove the callback.
-spec on_pressure_change(callback_fun()) -> callback_ref().
on_pressure_change(Fun) when is_function(Fun, 1) ->
    gen_server:call(?SERVER, {register_callback, Fun}).

%% @doc Remove a registered callback.
-spec remove_callback(callback_ref()) -> ok.
remove_callback(Ref) ->
    gen_server:call(?SERVER, {remove_callback, Ref}).

%% @doc Get current configuration.
-spec get_config() -> config().
get_config() ->
    gen_server:call(?SERVER, get_config).

%% @doc Get current memory statistics.
-spec get_stats() -> map().
get_stats() ->
    gen_server:call(?SERVER, get_stats).

%% @doc Force an immediate memory check.
-spec check_now() -> pressure_level().
check_now() ->
    gen_server:call(?SERVER, check_now).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    ElevatedThreshold = maps:get(elevated_threshold, Config, ?DEFAULT_ELEVATED_THRESHOLD),
    CriticalThreshold = maps:get(critical_threshold, Config, ?DEFAULT_CRITICAL_THRESHOLD),
    CheckInterval = maps:get(check_interval, Config, ?DEFAULT_CHECK_INTERVAL),

    State = #state{
        elevated_threshold = ElevatedThreshold,
        critical_threshold = CriticalThreshold,
        check_interval = CheckInterval
    },

    %% Do initial check
    State2 = do_check(State),

    %% Schedule periodic checks
    TimerRef = schedule_check(CheckInterval),

    {ok, State2#state{timer_ref = TimerRef}}.

handle_call(level, _From, #state{level = Level} = State) ->
    {reply, Level, State};

handle_call({configure, Config}, _From, State) ->
    %% Cancel existing timer
    cancel_timer(State#state.timer_ref),

    ElevatedThreshold = maps:get(elevated_threshold, Config, State#state.elevated_threshold),
    CriticalThreshold = maps:get(critical_threshold, Config, State#state.critical_threshold),
    CheckInterval = maps:get(check_interval, Config, State#state.check_interval),

    NewState = State#state{
        elevated_threshold = ElevatedThreshold,
        critical_threshold = CriticalThreshold,
        check_interval = CheckInterval
    },

    %% Re-check and reschedule
    NewState2 = do_check(NewState),
    TimerRef = schedule_check(CheckInterval),

    {reply, ok, NewState2#state{timer_ref = TimerRef}};

handle_call({register_callback, Fun}, _From, #state{callbacks = Callbacks} = State) ->
    Ref = make_ref(),
    NewCallbacks = maps:put(Ref, Fun, Callbacks),
    {reply, Ref, State#state{callbacks = NewCallbacks}};

handle_call({remove_callback, Ref}, _From, #state{callbacks = Callbacks} = State) ->
    NewCallbacks = maps:remove(Ref, Callbacks),
    {reply, ok, State#state{callbacks = NewCallbacks}};

handle_call(get_config, _From, State) ->
    Config = #{
        elevated_threshold => State#state.elevated_threshold,
        critical_threshold => State#state.critical_threshold,
        check_interval => State#state.check_interval
    },
    {reply, Config, State};

handle_call(get_stats, _From, State) ->
    Stats = #{
        level => State#state.level,
        memory_used => State#state.memory_used,
        memory_total => State#state.memory_total,
        last_check => State#state.last_check,
        callback_count => maps:size(State#state.callbacks)
    },
    {reply, Stats, State};

handle_call(check_now, _From, State) ->
    NewState = do_check(State),
    {reply, NewState#state.level, NewState};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_memory, State) ->
    NewState = do_check(State),
    TimerRef = schedule_check(NewState#state.check_interval),
    {noreply, NewState#state{timer_ref = TimerRef}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{timer_ref = TimerRef}) ->
    cancel_timer(TimerRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Check memory and update state
-spec do_check(#state{}) -> #state{}.
do_check(State) ->
    {Used, Total} = get_memory_info(),
    UsageRatio = Used / max(Total, 1),

    NewLevel = level(UsageRatio,
                     State#state.elevated_threshold,
                     State#state.critical_threshold),

    OldLevel = State#state.level,

    %% Notify callbacks if level changed
    case NewLevel =/= OldLevel of
        true ->
            notify_callbacks(OldLevel, NewLevel, State#state.callbacks),
            emit_telemetry(OldLevel, NewLevel, UsageRatio);
        false ->
            ok
    end,

    State#state{
        level = NewLevel,
        memory_used = Used,
        memory_total = Total,
        last_check = erlang:system_time(millisecond)
    }.

%% @private Calculate pressure level from usage ratio
-spec level(float(), float(), float()) -> pressure_level().
level(UsageRatio, _ElevatedThreshold, CriticalThreshold) when UsageRatio >= CriticalThreshold ->
    critical;
level(UsageRatio, ElevatedThreshold, _CriticalThreshold) when UsageRatio >= ElevatedThreshold ->
    elevated;
level(_UsageRatio, _ElevatedThreshold, _CriticalThreshold) ->
    normal.

%% @private Get memory usage info
-spec get_memory_info() -> {Used :: non_neg_integer(), Total :: non_neg_integer()}.
get_memory_info() ->
    MemData = memsup:get_system_memory_data(),
    case MemData of
        [] ->
            %% memsup not available, fallback to erlang:memory()
            ErlangMem = erlang:memory(),
            Used = proplists:get_value(total, ErlangMem, 0),
            %% Estimate total as used * 2 when we can't get real total
            {Used, Used * 2};
        _ ->
            Total = proplists:get_value(total_memory, MemData, 0),
            Free = proplists:get_value(free_memory, MemData, 0),
            Cached = proplists:get_value(cached_memory, MemData, 0),
            Buffered = proplists:get_value(buffered_memory, MemData, 0),
            %% Available memory = free + cached + buffers
            Available = Free + Cached + Buffered,
            Used = Total - Available,
            {Used, Total}
    end.

%% @private Notify all registered callbacks
-spec notify_callbacks(pressure_level(), pressure_level(), #{callback_ref() => callback_fun()}) -> ok.
notify_callbacks(_OldLevel, NewLevel, Callbacks) ->
    maps:foreach(
        fun(_Ref, Fun) ->
            try
                Fun(NewLevel)
            catch
                Class:Reason:Stacktrace ->
                    logger:warning("Memory pressure callback failed: ~p:~p~n~p",
                                  [Class, Reason, Stacktrace])
            end
        end,
        Callbacks
    ),
    ok.

%% @private Emit telemetry for pressure change
-spec emit_telemetry(pressure_level(), pressure_level(), float()) -> ok.
emit_telemetry(OldLevel, NewLevel, UsageRatio) ->
    telemetry:execute(
        [reckon_db, memory, pressure_changed],
        #{usage_ratio => UsageRatio},
        #{old_level => OldLevel, new_level => NewLevel}
    ),
    ok.

%% @private Schedule periodic check
-spec schedule_check(pos_integer()) -> reference().
schedule_check(Interval) ->
    erlang:send_after(Interval, self(), check_memory).

%% @private Cancel timer if set
-spec cancel_timer(reference() | undefined) -> ok.
cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref), ok.
