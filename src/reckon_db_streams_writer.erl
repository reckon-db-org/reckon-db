%% @doc Streams writer worker for reckon-db
%%
%% A gen_server that handles write operations for streams.
%% Writers are temporary processes that terminate after a period of inactivity.
%%
%% Features:
%% - Partitioned by stream_id for concurrent writes to different streams
%% - Idle timeout to free up resources
%% - Swarm-like registration via pg groups
%%
%% @author rgfaber

-module(reckon_db_streams_writer).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([start_link/1]).
-export([append/4]).
-export([get_writer/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DEFAULT_IDLE_TIMEOUT_MS, 10000).  %% 10 seconds (matches ex-esdb)

-record(state, {
    store_id :: atom(),
    stream_id :: binary(),
    partition :: non_neg_integer(),
    idle_timeout :: pos_integer(),
    last_activity :: integer()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start a writer worker
-spec start_link({atom(), binary(), non_neg_integer()}) -> {ok, pid()} | {error, term()}.
start_link({StoreId, StreamId, Partition}) ->
    gen_server:start_link(?MODULE, {StoreId, StreamId, Partition}, []).

%% @doc Append events to a stream via a writer worker
-spec append(atom(), binary(), integer(), [map()]) ->
    {ok, non_neg_integer()} | {error, term()}.
append(StoreId, StreamId, ExpectedVersion, Events) ->
    Writer = get_writer(StoreId, StreamId),
    gen_server:call(Writer, {append, StoreId, StreamId, ExpectedVersion, Events}, infinity).

%% @doc Get or create a writer for a stream
-spec get_writer(atom(), binary()) -> pid().
get_writer(StoreId, StreamId) ->
    GroupKey = writer_group_key(StoreId, StreamId),
    case pg:get_members(?RECKON_DB_PG_SCOPE, GroupKey) of
        [] ->
            start_new_writer(StoreId, StreamId);
        Writers ->
            %% Pick a random writer if multiple exist
            lists:nth(rand:uniform(length(Writers)), Writers)
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init({StoreId, StreamId, Partition}) ->
    process_flag(trap_exit, true),

    %% Join the pg group for this stream
    GroupKey = writer_group_key(StoreId, StreamId),
    ok = pg:join(?RECKON_DB_PG_SCOPE, GroupKey, self()),

    IdleTimeout = application:get_env(reckon_db, writer_idle_timeout_ms, ?DEFAULT_IDLE_TIMEOUT_MS),

    %% Schedule idle check
    schedule_idle_check(IdleTimeout),

    logger:debug("Streams writer started: store=~p stream=~s partition=~p",
                [StoreId, StreamId, Partition]),

    State = #state{
        store_id = StoreId,
        stream_id = StreamId,
        partition = Partition,
        idle_timeout = IdleTimeout,
        last_activity = erlang:system_time(millisecond)
    },
    {ok, State}.

%% @private
handle_call({append, StoreId, StreamId, ExpectedVersion, Events}, _From, State) ->
    Result = reckon_db_streams:do_append(StoreId, StreamId, ExpectedVersion, Events),
    NewState = State#state{last_activity = erlang:system_time(millisecond)},
    {reply, Result, NewState};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(check_idle, #state{idle_timeout = Timeout, last_activity = LastActivity} = State) ->
    Now = erlang:system_time(millisecond),
    IdleDuration = Now - LastActivity,

    case IdleDuration >= Timeout of
        true ->
            logger:debug("Streams writer idle timeout reached, stopping"),
            {stop, normal, State};
        false ->
            %% Schedule next check
            schedule_idle_check(Timeout),
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, #state{store_id = StoreId, stream_id = StreamId}) ->
    GroupKey = writer_group_key(StoreId, StreamId),
    pg:leave(?RECKON_DB_PG_SCOPE, GroupKey, self()),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec writer_group_key(atom(), binary()) -> term().
writer_group_key(StoreId, StreamId) ->
    {StoreId, StreamId, streams_writer}.

%% @private
-spec start_new_writer(atom(), binary()) -> pid().
start_new_writer(StoreId, StreamId) ->
    Partition = partition_for(StoreId, StreamId),
    SupName = reckon_db_naming:streams_sup_name(StoreId),

    ChildSpec = #{
        id => make_ref(),
        start => {?MODULE, start_link, [{StoreId, StreamId, Partition}]},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    },

    case supervisor:start_child(SupName, ChildSpec) of
        {ok, Pid} -> Pid;
        {error, {already_started, Pid}} -> Pid;
        {error, Reason} ->
            logger:error("Failed to start streams writer: ~p", [Reason]),
            error({failed_to_start_writer, Reason})
    end.

%% @private
-spec partition_for(atom(), binary()) -> non_neg_integer().
partition_for(StoreId, StreamId) ->
    Partitions = erlang:system_info(schedulers_online),
    erlang:phash2({StoreId, StreamId}, Partitions).

%% @private
-spec schedule_idle_check(pos_integer()) -> reference().
schedule_idle_check(Timeout) ->
    erlang:send_after(Timeout, self(), check_idle).
