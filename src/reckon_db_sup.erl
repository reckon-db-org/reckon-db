%% @doc Top-level supervisor for reckon-db
%%
%% This supervisor manages all store instances configured in the application
%% environment. Each store gets its own system supervisor subtree.
%%
%% @author rgfaber

-module(reckon_db_sup).
-behaviour(supervisor).

-include("reckon_db.hrl").

%% API
-export([start_link/0]).
-export([start_store/1, stop_store/1]).
-export([which_stores/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the top-level supervisor
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start a store dynamically
-spec start_store(atom() | store_config()) -> {ok, pid()} | {error, term()}.
start_store(StoreId) when is_atom(StoreId) ->
    case reckon_db_config:get_store_config(StoreId) of
        {ok, Config} ->
            start_store(Config);
        {error, _} = Error ->
            Error
    end;
start_store(#store_config{store_id = StoreId} = Config) ->
    ChildSpec = store_child_spec(StoreId, Config),
    supervisor:start_child(?SERVER, ChildSpec).

%% @doc Stop a store dynamically
-spec stop_store(atom()) -> ok | {error, term()}.
stop_store(StoreId) when is_atom(StoreId) ->
    ChildId = reckon_db_naming:system_sup_name(StoreId),
    case supervisor:terminate_child(?SERVER, ChildId) of
        ok ->
            supervisor:delete_child(?SERVER, ChildId);
        Error ->
            Error
    end.

%% @doc Get list of running stores
-spec which_stores() -> [atom()].
which_stores() ->
    Children = supervisor:which_children(?SERVER),
    [extract_store_id(Id) || {Id, _Pid, _Type, _Mods} <- Children,
                             is_store_child(Id)].

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },

    %% Store registry must start first (before any stores)
    RegistryChild = #{
        id => reckon_db_store_registry,
        start => {reckon_db_store_registry, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [reckon_db_store_registry]
    },

    %% Get configured stores from application environment
    StoreConfigs = reckon_db_config:get_all_store_configs(),

    %% Create child specs for each store
    StoreChildren = lists:map(
        fun(#store_config{store_id = StoreId} = Config) ->
            store_child_spec(StoreId, Config)
        end,
        StoreConfigs
    ),

    %% Registry first, then stores
    Children = [RegistryChild | StoreChildren],

    {ok, {SupFlags, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec store_child_spec(atom(), store_config()) -> supervisor:child_spec().
store_child_spec(StoreId, Config) ->
    #{
        id => reckon_db_naming:system_sup_name(StoreId),
        start => {reckon_db_system_sup, start_link, [Config]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [reckon_db_system_sup]
    }.

%% @private
-spec is_store_child(term()) -> boolean().
is_store_child(Id) when is_atom(Id) ->
    case atom_to_list(Id) of
        "reckon_db_system_" ++ _ -> true;
        _ -> false
    end;
is_store_child(_) ->
    false.

%% @private
-spec extract_store_id(atom()) -> atom().
extract_store_id(Id) ->
    %% Convert reckon_db_system_<store_id> back to store_id
    case atom_to_list(Id) of
        "reckon_db_system_" ++ StoreIdStr ->
            list_to_atom(StoreIdStr);
        _ ->
            Id
    end.
