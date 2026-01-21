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
-export([get_store/1, is_ready/1, get_leader/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    store_id :: atom(),
    config :: store_config(),
    started_at :: integer()
}).

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

%% @doc Get the current leader node for the store
-spec get_leader(atom()) -> {ok, node()} | {error, term()}.
get_leader(StoreId) ->
    case khepri_cluster:get_store_ids() of
        [] ->
            {error, not_started};
        _ ->
            try
                case ra:members({StoreId, node()}) of
                    {ok, _Members, Leader} when is_tuple(Leader) ->
                        {_LeaderName, LeaderNode} = Leader,
                        {ok, LeaderNode};
                    {ok, _, _} ->
                        {error, no_leader};
                    Error ->
                        Error
                end
            catch
                _:Reason -> {error, Reason}
            end
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

            %% Emit telemetry
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
            {ok, State};
        {error, Reason} = Error ->
            logger:error("Failed to start Khepri store ~p: ~p", [StoreId, Reason]),
            {stop, Error}
    end.

%% @private
handle_call(get_state, _From, State) ->
    {reply, State, State};

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
    catch reckon_db_store_registry:unannounce_store(StoreId),

    %% Emit telemetry
    telemetry:execute(
        ?STORE_STOPPED,
        #{system_time => erlang:system_time(millisecond), uptime_ms => Uptime},
        #{store_id => StoreId, reason => Reason}
    ),

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

%% @private
-spec start_khepri_store(atom(), string(), single | cluster) -> ok | {error, term()}.
start_khepri_store(StoreId, DataDir, single) ->
    %% Single node mode - simple khepri start
    %% IMPORTANT: First argument MUST be DataDir (string/binary), not StoreId (atom).
    %% When khepri:start/2 receives a path as first arg, it auto-creates the Ra system.
    %% When it receives an atom, it assumes that Ra system already exists and fails
    %% with :system_not_started if it doesn't.
    Timeout = application:get_env(reckon_db, default_timeout, ?DEFAULT_TIMEOUT),
    case khepri:start(DataDir, StoreId, Timeout) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        Error -> Error
    end;

start_khepri_store(StoreId, DataDir, cluster) ->
    %% Cluster mode - start with Ra cluster configuration
    %% IMPORTANT: First argument MUST be DataDir (string/binary), not StoreId (atom).
    %% When khepri:start/2 receives a path as first arg, it auto-creates the Ra system.
    %% When it receives an atom, it assumes that Ra system already exists and fails
    %% with :system_not_started if it doesn't.
    RaServerConfig = #{
        cluster_name => StoreId,
        id => {StoreId, node()},
        uid => atom_to_binary(StoreId, utf8),
        initial_members => [{StoreId, node()}],
        log_init_args => #{uid => atom_to_binary(StoreId, utf8)},
        machine => {module, khepri_machine, #{store_id => StoreId}}
    },
    KhepriOpts = #{
        store_id => StoreId,
        ra_server_config => RaServerConfig
    },
    case khepri:start(DataDir, KhepriOpts) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        Error -> Error
    end.

%% @private
-spec init_store_paths(atom()) -> ok.
init_store_paths(StoreId) ->
    %% Ensure base paths exist
    Paths = [
        ?STREAMS_PATH,
        ?SNAPSHOTS_PATH,
        ?SUBSCRIPTIONS_PATH,
        ?METADATA_PATH
    ],
    lists:foreach(
        fun(Path) ->
            case khepri:exists(StoreId, Path) of
                false ->
                    khepri:put(StoreId, Path, #{});
                true ->
                    ok
            end
        end,
        Paths
    ),
    ok.
