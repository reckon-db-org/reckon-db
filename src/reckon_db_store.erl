%% @doc Khepri store lifecycle management for reckon-db
%%
%% Manages the Khepri store instance, including:
%% - Starting and stopping the store
%% - Cluster formation (in cluster mode)
%% - Health checks
%%
%% @author rgfaber

-module(reckon_db_store).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([start_link/1]).
-export([get_store/1, is_ready/1, get_leader/1, ensure_khepri_started/1]).
-export([ra_system_name/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    store_id :: atom(),
    config :: store_config(),
    started_at :: integer()
}).

%% Upper bound for the synchronous ensure_khepri_started/1 call — must
%% exceed the internal khepri:start + await_store_ready work.
-define(ENSURE_STARTED_TIMEOUT, 30000).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the store worker
%% IMPORTANT: We use store_worker_name/1 for gen_server registration to avoid
%% conflicting with Khepri's internal naming. Khepri uses the StoreId for
%% its Ra cluster and process registration.
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    WorkerName = reckon_db_naming:store_worker_name(StoreId),
    gen_server:start_link({local, WorkerName}, ?MODULE, Config, []).

%% @doc Get the store name (for use with khepri operations)
-spec get_store(atom()) -> atom().
get_store(StoreId) ->
    StoreId.

%% @doc Check if the store is ready
-spec is_ready(atom()) -> boolean().
is_ready(StoreId) ->
    try
        khepri:exists(StoreId, [])
    catch
        _:_ -> false
    end.

%% @doc Idempotently (re)start this store's local Khepri/Ra server.
%%
%% The cluster coordinator calls this to self-heal a local store that was
%% torn down by a reset-during-join: `khepri_cluster:join' resets the local
%% store as part of joining, so a join interrupted mid-reset (e.g. the
%% coordinator's timeout guard killed it) can leave the local Ra server
%% gone — with the coordinator then looping forever on "not registered".
%% A no-op when the server is already registered.
-spec ensure_khepri_started(atom()) -> ok | {error, term()}.
ensure_khepri_started(StoreId) ->
    WorkerName = reckon_db_naming:store_worker_name(StoreId),
    try gen_server:call(WorkerName, ensure_khepri_started, ?ENSURE_STARTED_TIMEOUT)
    catch exit:Reason -> {error, {store_worker_unavailable, Reason}}
    end.

%% @doc Get the current leader node for the store
-spec get_leader(atom()) -> {ok, node()} | {error, term()}.
get_leader(StoreId) ->
    case khepri_cluster:get_store_ids() of
        [] ->
            {error, not_started};
        _ ->
            query_ra_leader(StoreId)
    end.

%% @private
query_ra_leader(StoreId) ->
    try ra:members({StoreId, node()}) of
        {ok, _Members, Leader} when is_tuple(Leader) ->
            {ok, element(2, Leader)};
        {ok, _, _} ->
            {error, no_leader};
        Error ->
            Error
    catch
        _:Reason -> {error, Reason}
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init(#store_config{store_id = StoreId, data_dir = DataDir, mode = Mode} = Config) ->
    process_flag(trap_exit, true),

    StartTime = erlang:system_time(millisecond),

    %% Ensure data directory exists
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    %% Start Khepri store
    case start_khepri_store(StoreId, DataDir, Mode) of
        ok ->
            %% Initialize store paths
            ok = init_store_paths(StoreId),

            %% Load tamper-resistance HMAC key into persistent_term.
            %% If integrity is enabled but the key cannot be loaded
            %% (missing env var, bad base64, wrong file mode, wrong
            %% size, etc.) the store refuses to start. This is
            %% deliberate fail-fast — a misconfigured key would
            %% silently leave the store unable to write or verify
            %% events, which is strictly worse than not starting.
            post_key_load(reckon_db_integrity_key:load(Config),
                          StoreId, Mode, DataDir, Config, StartTime);
        {error, Reason} = Error ->
            logger:error("Failed to start Khepri store ~p: ~p", [StoreId, Reason]),
            {stop, Error}
    end.

%% @private After Khepri is up, load indexes and continue — or fail fast
%% if the integrity key could not be loaded.
post_key_load(ok, StoreId, Mode, DataDir, Config, StartTime) ->
    %% Install the store's declared secondary indexes so the append path
    %% can read them. Always succeeds (a [] list just means no indexes).
    ok = reckon_db_index_config:load(Config),
    do_post_khepri_init(StoreId, Mode, DataDir, Config, StartTime);
post_key_load({error, KeyReason} = KeyError, StoreId, _Mode, _DataDir, _Config, _StartTime) ->
    logger:error("Failed to load integrity key for store ~p: ~p", [StoreId, KeyReason]),
    {stop, KeyError}.

%% @private
do_post_khepri_init(StoreId, Mode, DataDir, Config, StartTime) ->
    telemetry:execute(
        ?STORE_STARTED,
        #{system_time => StartTime},
        #{store_id => StoreId, mode => Mode, data_dir => DataDir}
    ),

    logger:info("Khepri store ~p started in ~p mode", [StoreId, Mode]),

    %% Announce store to the distributed registry
    ok = reckon_db_store_registry:announce_store(StoreId, Config),

    State = #state{
        store_id = StoreId,
        config = Config,
        started_at = StartTime
    },
    {ok, State}.

%% @private
handle_call(get_state, _From, State) ->
    {reply, State, State};

handle_call(ensure_khepri_started, _From,
            #state{config = #store_config{store_id = StoreId,
                                          data_dir  = DataDir,
                                          mode      = Mode}} = State) ->
    {reply, restart_local_khepri(StoreId, DataDir, Mode), State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(Reason, #state{store_id = StoreId, started_at = StartedAt}) ->
    Uptime = erlang:system_time(millisecond) - StartedAt,

    %% Unannounce store from the distributed registry
    _ = (try reckon_db_store_registry:unannounce_store(StoreId) catch _:_ -> ok end),

    %% Emit telemetry
    telemetry:execute(
        ?STORE_STOPPED,
        #{system_time => erlang:system_time(millisecond), uptime_ms => Uptime},
        #{store_id => StoreId, reason => Reason}
    ),

    %% Clear tamper-resistance state from persistent_term so a future
    %% reincarnation of this store ID does not inherit stale key
    %% material.
    _ = (try reckon_db_integrity_key:clear(StoreId) catch _:_ -> ok end),
    _ = (try reckon_db_index_config:clear(StoreId) catch _:_ -> ok end),

    %% Stop Khepri store
    case khepri:stop(StoreId) of
        ok ->
            logger:info("Khepri store ~p stopped (uptime: ~pms)", [StoreId, Uptime]);
        {error, StopReason} ->
            logger:warning("Error stopping Khepri store ~p: ~p", [StoreId, StopReason])
    end,
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Re-run the local khepri:start for a store whose Ra server has
%% gone (torn down by an interrupted join reset). Idempotent: a no-op when
%% the server is already registered. Re-inits the base paths so a freshly
%% reset 1-member store is ready to be joined again.
-spec restart_local_khepri(atom(), string(), single | cluster) -> ok | {error, term()}.
restart_local_khepri(StoreId, DataDir, Mode) ->
    case erlang:whereis(StoreId) of
        undefined ->
            logger:warning("Restarting torn-down local Khepri store ~p", [StoreId]),
            case start_khepri_store(StoreId, DataDir, Mode) of
                ok ->
                    _ = (try init_store_paths(StoreId) catch _:_ -> ok end),
                    ok;
                {error, _} = Error ->
                    logger:error("Failed to restart local Khepri store ~p: ~p",
                                 [StoreId, Error]),
                    Error
            end;
        _Pid ->
            ok
    end.

%% @private
-spec start_khepri_store(atom(), string(), single | cluster) -> ok | {error, term()}.
start_khepri_store(StoreId, DataDir, single) ->
    %% Single node mode — each store gets its own Ra system.
    %%
    %% Without this, khepri:start(DataDir, StoreId) uses the default 'khepri'
    %% Ra system. The first store's DataDir becomes the WAL directory for ALL
    %% stores, mixing event data from separate bounded contexts into one WAL.
    %%
    %% Fix: create a dedicated Ra system per store (named after the StoreId),
    %% each with its own DataDir, WAL, and segments.
    Timeout = application:get_env(reckon_db, default_timeout, ?DEFAULT_TIMEOUT),
    RaSystemName = ra_system_name(StoreId),
    case ensure_ra_system(RaSystemName, DataDir) of
        ok ->
            start_khepri_single(RaSystemName, StoreId, Timeout);
        {error, _} = Error ->
            Error
    end;

start_khepri_store(StoreId, DataDir, cluster) ->
    %% Cluster mode — starts identically to single mode.
    %% The Ra store starts as a single-node cluster (quorum of 1).
    %% reckon_db_store_coordinator handles joining multi-node clusters
    %% when other nodes appear.
    Timeout = application:get_env(reckon_db, default_timeout, ?DEFAULT_TIMEOUT),
    RaSystemName = ra_system_name(StoreId),
    case ensure_ra_system(RaSystemName, DataDir) of
        ok ->
            start_khepri_single(RaSystemName, StoreId, Timeout);
        {error, _} = Error ->
            Error
    end.

start_khepri_single(RaSystemName, StoreId, Timeout) ->
    case khepri:start(RaSystemName, StoreId, Timeout) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        Error -> Error
    end.

%% @private Derive a unique Ra system name from a store ID.
-spec ra_system_name(atom()) -> atom().
ra_system_name(StoreId) ->
    list_to_atom("ra_" ++ atom_to_list(StoreId)).

%% @private Start a dedicated Ra system for a store.
-spec ensure_ra_system(atom(), string()) -> ok | {error, term()}.
ensure_ra_system(RaSystemName, DataDir) ->
    DefaultConfig = ra_system:default_config(),
    RaSystemConfig = DefaultConfig#{
        name => RaSystemName,
        data_dir => DataDir,
        wal_data_dir => DataDir,
        names => ra_system:derive_names(RaSystemName)
    },
    case ra_system:start(RaSystemConfig) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%% @private
-spec init_store_paths(atom()) -> ok.
init_store_paths(StoreId) ->
    %% Wait for Ra leader election before querying Khepri.
    %% In cluster mode, the Ra server needs time to elect a leader
    %% after khepri:start returns.
    ok = await_store_ready(StoreId, 10),
    %% Ensure base paths exist
    Paths = [
        ?STREAMS_PATH,
        ?SNAPSHOTS_PATH,
        ?SUBSCRIPTIONS_PATH,
        ?METADATA_PATH
    ],
    lists:foreach(fun(Path) -> ensure_path(StoreId, Path) end, Paths),
    ok.

%% @private Create a tree path with an empty map if it doesn't exist.
ensure_path(StoreId, Path) ->
    case khepri:exists(StoreId, Path) of
        false -> khepri:put(StoreId, Path, #{});
        true -> ok
    end.

%% @private Wait for the Khepri store to be queryable (Ra leader elected).
-spec await_store_ready(atom(), non_neg_integer()) -> ok | {error, timeout}.
await_store_ready(_StoreId, 0) ->
    {error, timeout};
await_store_ready(StoreId, Retries) ->
    case khepri:exists(StoreId, []) of
        true -> ok;
        false -> ok;
        {error, noproc} ->
            timer:sleep(500),
            await_store_ready(StoreId, Retries - 1);
        {error, {timeout, _}} ->
            timer:sleep(500),
            await_store_ready(StoreId, Retries - 1);
        _ -> ok
    end.
