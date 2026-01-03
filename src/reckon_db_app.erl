%% @doc Application behaviour for reckon-db
%% @author rgfaber

-module(reckon_db_app).
-behaviour(application).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%% API
-export([
    start/0,
    stop/0
]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the reckon_db application
-spec start() -> {ok, pid()} | {error, term()}.
start() ->
    application:ensure_all_started(reckon_db).

%% @doc Stop the reckon_db application
-spec stop() -> ok | {error, term()}.
stop() ->
    application:stop(reckon_db).

%%====================================================================
%% Application callbacks
%%====================================================================

%% @private
-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    %% Ensure process group scope is started
    ok = ensure_pg_scope(),

    %% Start telemetry handlers
    ok = start_telemetry_handlers(),

    %% Start top-level supervisor
    case reckon_db_sup:start_link() of
        {ok, Pid} ->
            logger:info("reckon-db v~s started", [?RECKON_DB_VERSION]),
            emit_start_telemetry(),
            {ok, Pid};
        {error, Reason} = Error ->
            logger:error("Failed to start reckon-db: ~p", [Reason]),
            Error
    end.

%% @private
-spec stop(term()) -> ok.
stop(_State) ->
    emit_stop_telemetry(),
    logger:info("reckon-db stopped"),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec ensure_pg_scope() -> ok.
ensure_pg_scope() ->
    case pg:start_link(?RECKON_DB_PG_SCOPE) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok
    end.

%% @private
-spec start_telemetry_handlers() -> ok.
start_telemetry_handlers() ->
    Handlers = application:get_env(reckon_db, telemetry_handlers, [logger]),
    lists:foreach(fun attach_handler/1, Handlers),
    ok.

%% @private
-spec attach_handler(atom()) -> ok.
attach_handler(logger) ->
    %% Basic logger handler for telemetry events
    Events = [
        ?STREAM_WRITE_STOP,
        ?STREAM_WRITE_ERROR,
        ?STREAM_READ_STOP,
        ?SUBSCRIPTION_CREATED,
        ?SUBSCRIPTION_DELETED,
        ?CLUSTER_NODE_UP,
        ?CLUSTER_NODE_DOWN,
        ?CLUSTER_LEADER_ELECTED,
        ?STORE_STARTED,
        ?STORE_STOPPED
    ],
    telemetry:attach_many(
        reckon_db_logger_handler,
        Events,
        fun reckon_db_telemetry:handle_event/4,
        #{}
    ),
    ok;
attach_handler(opentelemetry) ->
    %% OpenTelemetry handler - optional, for datacenter deployments
    %% This would be implemented in reckon_db_telemetry_otel module
    logger:info("OpenTelemetry handler requested but not yet implemented"),
    ok;
attach_handler(Handler) ->
    logger:warning("Unknown telemetry handler: ~p", [Handler]),
    ok.

%% @private
-spec emit_start_telemetry() -> ok.
emit_start_telemetry() ->
    telemetry:execute(
        ?STORE_STARTED,
        #{system_time => erlang:system_time(millisecond)},
        #{application => reckon_db, version => ?RECKON_DB_VERSION}
    ),
    ok.

%% @private
-spec emit_stop_telemetry() -> ok.
emit_stop_telemetry() ->
    telemetry:execute(
        ?STORE_STOPPED,
        #{system_time => erlang:system_time(millisecond)},
        #{application => reckon_db, reason => normal}
    ),
    ok.
