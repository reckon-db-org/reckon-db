%% @doc Facade module for reckon-db event store
%%
%% Provides a convenient API for common reckon-db operations.
%% This module delegates to the appropriate internal modules.
%%
%% == Store Discovery ==
%%
%% The list_stores/0 function returns all stores known across the cluster,
%% discovered via the distributed store registry.
%%
%% @author rgfaber

-module(reckon_db).

-include("reckon_db.hrl").

%% Store discovery API
-export([
    list_stores/0,
    get_store_info/1,
    list_stores_on_node/1
]).

%% Store lifecycle API
-export([
    start_store/1,
    stop_store/1,
    which_stores/0
]).

%% Store status API
-export([
    is_ready/1,
    get_leader/1
]).

%%====================================================================
%% Store Discovery API
%%====================================================================

%% @doc List all stores known in the cluster
%%
%% Returns a list of maps containing store information including:
%% - store_id: The store identifier
%% - node: The node where the store is running
%% - mode: single or cluster
%% - data_dir: Data directory path
%% - timeout: Default operation timeout
%% - registered_at: Timestamp when store was registered
%%
%% This queries the distributed store registry which maintains
%% an eventually consistent view of all stores across all nodes.
-spec list_stores() -> {ok, [map()]} | {error, term()}.
list_stores() ->
    reckon_db_store_registry:list_stores().

%% @doc Get detailed information about a specific store
-spec get_store_info(atom()) -> {ok, map()} | {error, not_found}.
get_store_info(StoreId) ->
    reckon_db_store_registry:get_store_info(StoreId).

%% @doc List stores running on a specific node
-spec list_stores_on_node(node()) -> {ok, [map()]} | {error, term()}.
list_stores_on_node(Node) ->
    reckon_db_store_registry:list_stores_on_node(Node).

%%====================================================================
%% Store Lifecycle API
%%====================================================================

%% @doc Start a store dynamically
%%
%% The store must be configured in the application environment,
%% or a store_config record can be provided directly.
-spec start_store(atom() | store_config()) -> {ok, pid()} | {error, term()}.
start_store(StoreIdOrConfig) ->
    reckon_db_sup:start_store(StoreIdOrConfig).

%% @doc Stop a running store
-spec stop_store(atom()) -> ok | {error, term()}.
stop_store(StoreId) ->
    reckon_db_sup:stop_store(StoreId).

%% @doc Get list of stores running on this node (from supervisor)
%%
%% Unlike list_stores/0 which returns all cluster stores from the registry,
%% this returns only stores supervised by the local supervisor.
-spec which_stores() -> [atom()].
which_stores() ->
    reckon_db_sup:which_stores().

%%====================================================================
%% Store Status API
%%====================================================================

%% @doc Check if a store is ready for operations
-spec is_ready(atom()) -> boolean().
is_ready(StoreId) ->
    reckon_db_store:is_ready(StoreId).

%% @doc Get the current leader node for a store (cluster mode)
-spec get_leader(atom()) -> {ok, node()} | {error, term()}.
get_leader(StoreId) ->
    reckon_db_store:get_leader(StoreId).
