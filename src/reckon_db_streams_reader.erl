%% @doc Streams reader worker for reckon-db
%%
%% A gen_server that handles read operations for streams.
%% Readers are temporary processes that terminate after a period of inactivity.
%%
%% Features:
%% - Partitioned by stream_id for concurrent reads from different streams
%% - Idle timeout to free up resources
%% - Registration via pg groups
%%
%% @author rgfaber

-module(reckon_db_streams_reader).
-behaviour(gen_server).

-include("reckon_db.hrl").

%% API
-export([start_link/1]).
-export([read/5]).
-export([get_streams/1]).
-export([get_reader/2]).

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

%% @doc Start a reader worker
-spec start_link({atom(), binary(), non_neg_integer()}) -> {ok, pid()} | {error, term()}.
start_link({StoreId, StreamId, Partition}) ->
    gen_server:start_link(?MODULE, {StoreId, StreamId, Partition}, []).

%% @doc Read events from a stream via a reader worker
-spec read(atom(), binary(), non_neg_integer(), pos_integer(), atom()) ->
    {ok, [event()]} | {error, term()}.
read(StoreId, StreamId, StartVersion, Count, Direction) ->
    Reader = get_reader(StoreId, StreamId),
    gen_server:call(Reader, {read, StoreId, StreamId, StartVersion, Count, Direction}).

%% @doc Get all streams in the store
-spec get_streams(atom()) -> {ok, [binary()]} | {error, term()}.
get_streams(StoreId) ->
    %% Use a general reader for store-wide operations
    Reader = get_reader(StoreId, <<"$meta">>),
    gen_server:call(Reader, {get_streams, StoreId}).

%% @doc Get or create a reader for a stream
-spec get_reader(atom(), binary()) -> pid().
get_reader(StoreId, StreamId) ->
    GroupKey = reader_group_key(StoreId, StreamId),
    case pg:get_members(?RECKON_DB_PG_SCOPE, GroupKey) of
        [] ->
            start_new_reader(StoreId, StreamId);
        Readers ->
            %% Pick a random reader if multiple exist
            lists:nth(rand:uniform(length(Readers)), Readers)
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init({StoreId, StreamId, Partition}) ->
    process_flag(trap_exit, true),

    %% Join the pg group for this stream
    GroupKey = reader_group_key(StoreId, StreamId),
    ok = pg:join(?RECKON_DB_PG_SCOPE, GroupKey, self()),

    IdleTimeout = application:get_env(reckon_db, reader_idle_timeout_ms, ?DEFAULT_IDLE_TIMEOUT_MS),

    %% Schedule idle check
    schedule_idle_check(IdleTimeout),

    logger:debug("Streams reader started: store=~p stream=~s partition=~p",
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
handle_call({read, StoreId, StreamId, StartVersion, Count, Direction}, _From, State) ->
    Result = reckon_db_streams:do_read(StoreId, StreamId, StartVersion, Count, Direction),
    NewState = State#state{last_activity = erlang:system_time(millisecond)},
    {reply, Result, NewState};

handle_call({get_streams, StoreId}, _From, State) ->
    Result = reckon_db_streams:list_streams(StoreId),
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
            logger:debug("Streams reader idle timeout reached, stopping"),
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
    GroupKey = reader_group_key(StoreId, StreamId),
    pg:leave(?RECKON_DB_PG_SCOPE, GroupKey, self()),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec reader_group_key(atom(), binary()) -> term().
reader_group_key(StoreId, StreamId) ->
    {StoreId, StreamId, streams_reader}.

%% @private
-spec start_new_reader(atom(), binary()) -> pid().
start_new_reader(StoreId, StreamId) ->
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
            logger:error("Failed to start streams reader: ~p", [Reason]),
            error({failed_to_start_reader, Reason})
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
