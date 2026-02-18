%% @doc Persistence worker for reckon-db
%%
%% A GenServer that handles periodic disk persistence operations.
%% This worker batches and schedules flush operations to ensure data is
%% persisted to disk without blocking event append operations.
%%
%% Features:
%% - Configurable persistence interval (default: 5 seconds)
%% - Batching of flush operations to reduce disk I/O
%% - Graceful shutdown with final persistence
%% - Per-store persistence workers
%%
%% NOTE: The actual flush operation is currently DISABLED as Khepri/Ra
%% handles persistence internally via Raft consensus. This module exists
%% for future optimization and to match the architecture of ex-esdb.
%%
%% @author rgfaber

-module(reckon_db_persistence_worker).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([
    start_link/1,
    request_persistence/1,
    force_persistence/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(DEFAULT_PERSISTENCE_INTERVAL, 5000). %% 5 seconds

-record(state, {
    store_id :: atom(),
    persistence_interval :: pos_integer(),
    timer_ref :: reference() | undefined,
    pending_stores :: sets:set(atom()),
    last_persistence_time :: integer()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start a persistence worker for a specific store.
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:persistence_worker_name(StoreId),
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

%% @doc Request that a store's data be persisted to disk.
%%
%% This is a non-blocking call that queues the store for persistence.
%% Returns ok immediately; actual persistence happens asynchronously.
-spec request_persistence(atom()) -> ok | {error, not_found}.
request_persistence(StoreId) ->
    WorkerName = reckon_db_naming:persistence_worker_name(StoreId),
    case whereis(WorkerName) of
        undefined ->
            logger:warning("PersistenceWorker for store ~p not found", [StoreId]),
            {error, not_found};
        Pid ->
            gen_server:cast(Pid, {request_persistence, StoreId}),
            ok
    end.

%% @doc Force immediate persistence of all pending stores.
%%
%% This is a synchronous call that blocks until persistence is complete.
-spec force_persistence(atom()) -> ok | {error, term()}.
force_persistence(StoreId) ->
    WorkerName = reckon_db_naming:persistence_worker_name(StoreId),
    case whereis(WorkerName) of
        undefined ->
            logger:warning("PersistenceWorker for store ~p not found", [StoreId]),
            {error, not_found};
        Pid ->
            gen_server:call(Pid, force_persistence, 30000)
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init(#store_config{store_id = StoreId} = Config) ->
    PersistenceInterval = get_persistence_interval(Config),

    %% Schedule the first persistence check
    TimerRef = erlang:send_after(PersistenceInterval, self(), persist_data),

    State = #state{
        store_id = StoreId,
        persistence_interval = PersistenceInterval,
        timer_ref = TimerRef,
        pending_stores = sets:new(),
        last_persistence_time = erlang:system_time(millisecond)
    },

    logger:info("PersistenceWorker for store ~p started (interval: ~pms)",
                [StoreId, PersistenceInterval]),

    %% Emit telemetry
    telemetry:execute(
        [reckon_db, persistence_worker, started],
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, persistence_interval => PersistenceInterval}
    ),

    {ok, State}.

%% @private
handle_call(force_persistence, _From, State) ->
    StartTime = erlang:system_time(millisecond),
    PendingCount = sets:size(State#state.pending_stores),

    %% Immediately persist all pending stores
    Result = persist_pending_stores(State#state.pending_stores),

    EndTime = erlang:system_time(millisecond),
    Duration = EndTime - StartTime,

    %% Emit telemetry
    telemetry:execute(
        [reckon_db, persistence_worker, forced],
        #{duration => Duration, pending_count => PendingCount},
        #{store_id => State#state.store_id, result => Result}
    ),

    %% Clear pending stores and update last persistence time
    NewState = State#state{
        pending_stores = sets:new(),
        last_persistence_time = EndTime
    },

    {reply, Result, NewState};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast({request_persistence, StoreId}, State) ->
    %% Add store to pending persistence set
    UpdatedPending = sets:add_element(StoreId, State#state.pending_stores),
    {noreply, State#state{pending_stores = UpdatedPending}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(persist_data, State) ->
    StartTime = erlang:system_time(millisecond),
    PendingCount = sets:size(State#state.pending_stores),

    %% Persist any pending stores
    Result = case PendingCount > 0 of
        true -> persist_pending_stores(State#state.pending_stores);
        false -> ok
    end,

    EndTime = erlang:system_time(millisecond),
    Duration = EndTime - StartTime,

    %% Emit telemetry
    telemetry:execute(
        [reckon_db, persistence_worker, cycle],
        #{duration => Duration, pending_count => PendingCount},
        #{store_id => State#state.store_id, result => Result}
    ),

    %% Schedule next persistence
    TimerRef = erlang:send_after(State#state.persistence_interval, self(), persist_data),

    NewState = State#state{
        timer_ref = TimerRef,
        pending_stores = sets:new(),
        last_persistence_time = EndTime
    },

    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, State) ->
    %% Cancel the timer
    case State#state.timer_ref of
        undefined -> ok;
        Ref -> erlang:cancel_timer(Ref)
    end,

    %% Final persistence of any pending stores
    case sets:size(State#state.pending_stores) > 0 of
        true ->
            persist_pending_stores(State#state.pending_stores);
        false ->
            ok
    end,

    logger:info("PersistenceWorker for store ~p stopped", [State#state.store_id]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Get persistence interval from config or application env
-spec get_persistence_interval(store_config()) -> pos_integer().
get_persistence_interval(#store_config{options = Options}) when is_map(Options) ->
    case maps:get(persistence_interval, Options, undefined) of
        undefined ->
            application:get_env(reckon_db, persistence_interval, ?DEFAULT_PERSISTENCE_INTERVAL);
        Interval ->
            Interval
    end;
get_persistence_interval(#store_config{}) ->
    application:get_env(reckon_db, persistence_interval, ?DEFAULT_PERSISTENCE_INTERVAL).

%% @private Persist all pending stores
-spec persist_pending_stores(sets:set(atom())) -> ok | {error, term()}.
persist_pending_stores(PendingStores) ->
    Results = [persist_store(StoreId) || StoreId <- sets:to_list(PendingStores)],

    {SuccessCount, ErrorCount} = lists:foldl(
        fun(ok, {S, E}) -> {S + 1, E};
           ({error, _}, {S, E}) -> {S, E + 1}
        end,
        {0, 0},
        Results
    ),

    case ErrorCount of
        0 ->
            ok;
        _ ->
            logger:warning("Persisted ~p stores, ~p errors", [SuccessCount, ErrorCount]),
            {error, {partial_success, SuccessCount, ErrorCount}}
    end.

%% @private Persist a single store
-spec persist_store(atom()) -> ok | {error, term()}.
persist_store(StoreId) ->
    %% Use non-blocking flush
    case flush_async(StoreId) of
        ok ->
            ok;
        {error, Reason} = Error ->
            logger:error("Failed to request persistence for store ~p: ~p",
                        [StoreId, Reason]),
            Error
    end.

%% @private Perform async flush
%%
%% DISABLED: Flush operations are disabled to prevent Khepri tree corruption.
%% Khepri/Ra handles persistence internally via Raft consensus.
%%
%% Previous attempts to implement custom flush commands at specific paths
%% caused conflicts with the existing tree structure.
%%
%% This function exists for future optimization if needed.
-spec flush_async(atom()) -> ok | {error, term()}.
flush_async(_StoreId) ->
    %% DISABLED: Flush operations disabled - Khepri/Ra handles persistence
    %% via Raft consensus. This is a no-op placeholder.
    %%
    %% If flush is needed in the future, options include:
    %% 1. khepri:fence(StoreId) - synchronous, may cause timeouts
    %% 2. Custom flush trigger at a dedicated path
    %% 3. Ra machine command for fsync
    ok.
