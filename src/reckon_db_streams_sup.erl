%% @doc Streams supervisor for reckon-db
%%
%% Manages stream reader and writer pools for concurrent operations.
%% Uses partitioned workers for high-throughput stream access.
%%
%% @author rgfaber

-module(reckon_db_streams_sup).
-behaviour(supervisor).

-include("reckon_db.hrl").

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the streams supervisor
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:streams_sup_name(StoreId),
    supervisor:start_link({local, Name}, ?MODULE, Config).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
-spec init(store_config()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(#store_config{store_id = StoreId} = _Config) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },

    %% No child workers needed - Khepri/Ra handles all concurrency
    %% through Raft consensus. Write serialization and consistent reads
    %% are provided by the underlying Raft implementation.
    %% This supervisor exists for future extensibility (e.g., caching workers).
    Children = [],

    logger:debug("Starting streams supervisor for store ~p", [StoreId]),

    {ok, {SupFlags, Children}}.
