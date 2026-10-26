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
    %% Start telemetry handlers
    ok = start_telemetry_handlers(),

    %% Draw this VM's boot token before any subscriber can race for it
    _ = reckon_db_subscriptions:this_boot(),

    %% Start top-level supervisor
    case reckon_db_sup:start_link() of
        {ok, Pid} ->
            logger:info("reckon-db v~s started", [release_version()]),
            {ok, Pid};
        {error, Reason} = Error ->
            logger:error("Failed to start reckon-db: ~p", [Reason]),
            Error
    end.

%% @private
-spec stop(term()) -> ok.
stop(_State) ->
    logger:info("reckon-db stopped"),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================


%% @private The version this VM is running, asked of the application rather
%% than kept in a second place. ?RECKON_DB_VERSION in reckon_db.hrl was a
%% hand-kept copy of src/reckon_db.app.src, and it drifted: the startup line
%% named 5.11.13 while the release was 5.11.14 (reckon-db#31). The app is
%% always loaded when its own start callback runs, so the match cannot miss.
-spec release_version() -> string().
release_version() ->
    {ok, Vsn} = application:get_key(reckon_db, vsn),
    Vsn.

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

