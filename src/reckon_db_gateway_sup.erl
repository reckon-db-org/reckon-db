%% @doc Gateway supervisor for reckon-db
%%
%% Manages a pool of gateway workers that provide the external interface for the
%% event store. Starts last in the supervision hierarchy to ensure the
%% system is fully operational before accepting external requests.
%%
%% == Gateway Worker Pool ==
%%
%% The supervisor starts gateway_pool_size workers (default 1) for load
%% distribution. Each worker registers with reckon-gater independently,
%% allowing round-robin load balancing across all workers.
%%
%% == Gateway Integration ==
%%
%% Gateway workers register themselves with reckon-gater (when available)
%% to enable load-balanced, distributed access to the event store.
%%
%% When reckon-gater is not available, the gateway workers still run
%% locally but are not registered for external load balancing.
%%
%% @author rgfaber

-module(reckon_db_gateway_sup).
-behaviour(supervisor).

-include("reckon_db.hrl").

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the gateway supervisor
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:gateway_sup_name(StoreId),
    supervisor:start_link({local, Name}, ?MODULE, Config).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
-spec init(store_config()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(#store_config{store_id = StoreId, gateway_pool_size = PoolSize} = Config) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 15,      %% max 15 restarts
        period => 60          %% in 60 seconds
    },

    %% Start gateway worker pool
    Children = [gateway_worker_spec(Config, N) || N <- lists:seq(1, PoolSize)],

    logger:info("Starting gateway supervisor for store ~p with ~p workers",
               [StoreId, PoolSize]),

    {ok, {SupFlags, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec gateway_worker_spec(store_config(), pos_integer()) -> supervisor:child_spec().
gateway_worker_spec(#store_config{store_id = StoreId} = Config, WorkerNum) ->
    WorkerId = list_to_atom(
        atom_to_list(reckon_db_naming:gateway_worker_name(StoreId)) ++
        "_" ++ integer_to_list(WorkerNum)
    ),
    #{
        id => WorkerId,
        start => {reckon_db_gateway_worker, start_link, [Config]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [reckon_db_gateway_worker]
    }.
