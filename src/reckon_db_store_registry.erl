%% @doc Distributed store registry for reckon-db
%%
%% Provides cluster-wide store registration and discovery using Erlang's
%% built-in pg (process groups) module for distributed membership.
%%
%% == Architecture ==
%%
%% Each node runs a store registry GenServer that:
%%
%% - Maintains a local list of known stores (from all nodes)
%%
%% - Announces local stores to registries on other nodes
%%
%% - Receives announcements from other nodes
%%
%% - Uses pg groups for registry discovery
%%
%% == Cluster-Wide Discovery ==
%%
%% When a store starts on any node:
%%
%% 1. Local store calls announce_store/2 with its config
%%
%% 2. Registry adds store to local state
%%
%% 3. Registry broadcasts to all other registries via pg
%%
%% 4. Other registries add the store to their state
%%
%% When a node goes down, pg automatically notifies remaining nodes
%% and registries remove stores from the dead node.
%%
%% @author rgfaber

-module(reckon_db_store_registry).
-behaviour(gen_server).

-include("reckon_db.hrl").

%% API
-export([
    start_link/0,
    announce_store/2,
    unannounce_store/1,
    list_stores/0,
    get_store_info/1,
    list_stores_on_node/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(PG_SCOPE, ?RECKON_DB_PG_SCOPE).
-define(REGISTRY_GROUP, reckon_db_store_registries).

%% Store entry: combines config with node information
-record(store_entry, {
    store_id :: atom(),
    node :: node(),
    config :: store_config(),
    registered_at :: integer()
}).

-type store_entry() :: #store_entry{}.

%%====================================================================
%% API
%%====================================================================

%% @doc Start the store registry
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Announce a store to the cluster
%%
%% Called by reckon_db_store when a store starts. This registers the store
%% locally and broadcasts to all other registries in the cluster.
-spec announce_store(atom(), store_config()) -> ok.
announce_store(StoreId, Config) ->
    gen_server:call(?SERVER, {announce_store, StoreId, Config}).

%% @doc Unannounce a store from the cluster
%%
%% Called when a store is stopping. Removes the store from local registry
%% and broadcasts removal to all other registries.
-spec unannounce_store(atom()) -> ok.
unannounce_store(StoreId) ->
    gen_server:call(?SERVER, {unannounce_store, StoreId}).

%% @doc List all known stores in the cluster
%%
%% Returns stores from all nodes, including their node and config information.
-spec list_stores() -> {ok, [map()]} | {error, term()}.
list_stores() ->
    gen_server:call(?SERVER, list_stores).

%% @doc Get detailed information about a specific store
-spec get_store_info(atom()) -> {ok, map()} | {error, not_found}.
get_store_info(StoreId) ->
    gen_server:call(?SERVER, {get_store_info, StoreId}).

%% @doc List stores on a specific node
-spec list_stores_on_node(node()) -> {ok, [map()]} | {error, term()}.
list_stores_on_node(Node) ->
    gen_server:call(?SERVER, {list_stores_on_node, Node}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    process_flag(trap_exit, true),

    %% Join the registry pg group for cluster-wide discovery
    ok = pg:join(?PG_SCOPE, ?REGISTRY_GROUP, self()),

    logger:info("[store_registry] Started on ~p", [node()]),

    %% Initial state: empty stores list
    State = #{
        stores => []
    },

    {ok, State}.

%% Handle store announcement
handle_call({announce_store, StoreId, Config}, _From, #{stores := Stores} = State) ->
    Entry = #store_entry{
        store_id = StoreId,
        node = node(),
        config = Config,
        registered_at = erlang:system_time(millisecond)
    },

    %% Add to local state (avoiding duplicates)
    NewStores = add_store_entry(Stores, Entry),
    NewState = State#{stores => NewStores},

    %% Broadcast to other registries
    broadcast_announcement(StoreId, Config, node()),

    logger:info("[store_registry] Announced store ~p on ~p", [StoreId, node()]),

    {reply, ok, NewState};

%% Handle store unannouncement
handle_call({unannounce_store, StoreId}, _From, #{stores := Stores} = State) ->
    %% Remove from local state
    NewStores = remove_store_entry(Stores, StoreId, node()),
    NewState = State#{stores => NewStores},

    %% Broadcast removal to other registries
    broadcast_unannouncement(StoreId, node()),

    logger:info("[store_registry] Unannounced store ~p on ~p", [StoreId, node()]),

    {reply, ok, NewState};

%% Handle list stores request
handle_call(list_stores, _From, #{stores := Stores} = State) ->
    StoreList = [store_entry_to_map(E) || E <- Stores],
    {reply, {ok, StoreList}, State};

%% Handle get store info request
handle_call({get_store_info, StoreId}, _From, #{stores := Stores} = State) ->
    case lists:keyfind(StoreId, #store_entry.store_id, Stores) of
        false ->
            {reply, {error, not_found}, State};
        Entry ->
            {reply, {ok, store_entry_to_map(Entry)}, State}
    end;

%% Handle list stores on node request
handle_call({list_stores_on_node, Node}, _From, #{stores := Stores} = State) ->
    NodeStores = [store_entry_to_map(E) || E <- Stores, E#store_entry.node =:= Node],
    {reply, {ok, NodeStores}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% Handle broadcast announcement from another registry
handle_cast({remote_announce, StoreId, Config, FromNode}, #{stores := Stores} = State) ->
    %% Only add if from a different node
    case FromNode =:= node() of
        true ->
            {noreply, State};
        false ->
            Entry = #store_entry{
                store_id = StoreId,
                node = FromNode,
                config = Config,
                registered_at = erlang:system_time(millisecond)
            },
            NewStores = add_store_entry(Stores, Entry),
            logger:debug("[store_registry] Received announcement for ~p from ~p",
                        [StoreId, FromNode]),
            {noreply, State#{stores => NewStores}}
    end;

%% Handle broadcast unannouncement from another registry
handle_cast({remote_unannounce, StoreId, FromNode}, #{stores := Stores} = State) ->
    NewStores = remove_store_entry(Stores, StoreId, FromNode),
    logger:debug("[store_registry] Received unannouncement for ~p from ~p",
                [StoreId, FromNode]),
    {noreply, State#{stores => NewStores}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% Handle pg membership changes (node down)
handle_info({pg, ?PG_SCOPE, ?REGISTRY_GROUP, {leave, _Group, Pids}},
            #{stores := Stores} = State) ->
    %% Find which nodes left and remove their stores
    LeftNodes = [node(Pid) || Pid <- Pids],
    NewStores = lists:filter(
        fun(#store_entry{node = N}) ->
            not lists:member(N, LeftNodes)
        end,
        Stores
    ),
    case length(Stores) - length(NewStores) of
        0 -> ok;
        N -> logger:info("[store_registry] Removed ~p stores from departed nodes", [N])
    end,
    {noreply, State#{stores => NewStores}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    pg:leave(?PG_SCOPE, ?REGISTRY_GROUP, self()),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Add a store entry, replacing if same store_id+node exists
-spec add_store_entry([store_entry()], store_entry()) -> [store_entry()].
add_store_entry(Stores, #store_entry{store_id = StoreId, node = Node} = Entry) ->
    %% Remove existing entry for this store_id+node combo
    Filtered = lists:filter(
        fun(#store_entry{store_id = S, node = N}) ->
            not (S =:= StoreId andalso N =:= Node)
        end,
        Stores
    ),
    [Entry | Filtered].

%% @private Remove a store entry by store_id and node
-spec remove_store_entry([store_entry()], atom(), node()) -> [store_entry()].
remove_store_entry(Stores, StoreId, Node) ->
    lists:filter(
        fun(#store_entry{store_id = S, node = N}) ->
            not (S =:= StoreId andalso N =:= Node)
        end,
        Stores
    ).

%% @private Convert store entry to map for external API
-spec store_entry_to_map(store_entry()) -> map().
store_entry_to_map(#store_entry{
    store_id = StoreId,
    node = Node,
    config = Config,
    registered_at = RegisteredAt
}) ->
    #{
        store_id => StoreId,
        node => Node,
        mode => Config#store_config.mode,
        data_dir => Config#store_config.data_dir,
        timeout => Config#store_config.timeout,
        registered_at => RegisteredAt
    }.

%% @private Broadcast store announcement to all other registries
-spec broadcast_announcement(atom(), store_config(), node()) -> ok.
broadcast_announcement(StoreId, Config, FromNode) ->
    Registries = pg:get_members(?PG_SCOPE, ?REGISTRY_GROUP),
    OtherRegistries = [Pid || Pid <- Registries, Pid =/= self()],
    lists:foreach(
        fun(Pid) ->
            gen_server:cast(Pid, {remote_announce, StoreId, Config, FromNode})
        end,
        OtherRegistries
    ),
    ok.

%% @private Broadcast store unannouncement to all other registries
-spec broadcast_unannouncement(atom(), node()) -> ok.
broadcast_unannouncement(StoreId, FromNode) ->
    Registries = pg:get_members(?PG_SCOPE, ?REGISTRY_GROUP),
    OtherRegistries = [Pid || Pid <- Registries, Pid =/= self()],
    lists:foreach(
        fun(Pid) ->
            gen_server:cast(Pid, {remote_unannounce, StoreId, FromNode})
        end,
        OtherRegistries
    ),
    ok.
