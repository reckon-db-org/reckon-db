%% @doc Store manager for reckon-db
%%
%% Coordinates store lifecycle and provides store-level operations.
%%
%% @author rgfaber

-module(reckon_db_store_mgr).
-behaviour(gen_server).

-include("reckon_db.hrl").

%% API
-export([start_link/1]).
-export([get_info/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    store_id :: atom(),
    config :: store_config()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the store manager
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:store_mgr_name(StoreId),
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

%% @doc Get store information
-spec get_info(atom()) -> {ok, map()} | {error, term()}.
get_info(StoreId) ->
    Name = reckon_db_naming:store_mgr_name(StoreId),
    gen_server:call(Name, get_info).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init(#store_config{store_id = StoreId} = Config) ->
    logger:debug("Starting store manager for ~p", [StoreId]),
    State = #state{
        store_id = StoreId,
        config = Config
    },
    {ok, State}.

%% @private
handle_call(get_info, _From, #state{store_id = StoreId, config = Config} = State) ->
    Info = #{
        store_id => StoreId,
        mode => Config#store_config.mode,
        data_dir => Config#store_config.data_dir,
        ready => reckon_db_store:is_ready(StoreId)
    },
    {reply, {ok, Info}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.
