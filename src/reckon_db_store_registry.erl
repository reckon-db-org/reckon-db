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
    list_stores_on_node/1,
    subscribe/1,
    unsubscribe/1
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

%% @doc Subscribe a process to live store-registry events. The
%% subscriber receives messages of the form
%%   `{store_event, announced | retired, EntryMap}'
%% as stores come and go anywhere in the cluster. Used by the gRPC
%% `WatchStores' RPC and any in-BEAM watcher that needs live cluster
%% topology.
%%
%% Subscribers are automatically removed if they crash (we monitor
%% them). Safe to call multiple times — no-op if already subscribed.
-spec subscribe(pid()) -> ok.
subscribe(Pid) when is_pid(Pid) ->
    gen_server:call(?SERVER, {subscribe, Pid}).

-spec unsubscribe(pid()) -> ok.
unsubscribe(Pid) when is_pid(Pid) ->
    gen_server:call(?SERVER, {unsubscribe, Pid}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    process_flag(trap_exit, true),

    %% Join the registry pg group for cluster-wide discovery
    ok = pg:join(?PG_SCOPE, ?REGISTRY_GROUP, self()),

    logger:info("[store_registry] Started on ~p", [node()]),

    %% Initial state: empty stores list + no subscribers.
    %% subscribers is a map of WatcherPid => MonitorRef so we can
    %% drop dead watchers automatically.
    State = #{
        stores => [],
        subscribers => #{}
    },

    %% Kick off bilateral sync with existing peer registries.
    %% Existing announce_store/2 broadcasts new local stores TO peers,
    %% but a freshly-joined registry never asks peers for what they
    %% already announced before this node booted. Without this, each
    %% node ends up knowing only about its own store. Schedule the
    %% sync after init returns so peer calls don't block startup.
    self() ! sync_peers_state,

    {ok, State}.

%% Handle store announcement
handle_call({announce_store, StoreId, Config}, _From,
            #{stores := Stores} = State) ->
    Entry = #store_entry{store_id = StoreId,
                         node = node(),
                         config = Config,
                         registered_at = erlang:system_time(millisecond)},
    notify_subscribers(announced, Entry, State),
    broadcast_announcement(StoreId, Config, node()),
    logger:info("[store_registry] Announced store ~p on ~p", [StoreId, node()]),
    {reply, ok, State#{stores => add_store_entry(Stores, Entry)}};

handle_call({unannounce_store, StoreId}, _From,
            #{stores := Stores} = State) ->
    case find_entry(StoreId, node(), Stores) of
        not_found ->
            {reply, ok, State};
        Entry ->
            notify_subscribers(retired, Entry, State),
            broadcast_unannouncement(StoreId, node()),
            logger:info("[store_registry] Unannounced store ~p on ~p",
                        [StoreId, node()]),
            {reply, ok, State#{stores => remove_store_entry(Stores, StoreId, node())}}
    end;

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

%% Watcher subscribe/unsubscribe for the WatchStores RPC.
handle_call({subscribe, Pid}, _From,
            #{subscribers := Subs} = State) ->
    NewSubs = case maps:is_key(Pid, Subs) of
        true  -> Subs;
        false ->
            MRef = erlang:monitor(process, Pid),
            Subs#{Pid => MRef}
    end,
    {reply, ok, State#{subscribers => NewSubs}};

handle_call({unsubscribe, Pid}, _From,
            #{subscribers := Subs} = State) ->
    NewSubs = case maps:take(Pid, Subs) of
        {MRef, Rest} ->
            erlang:demonitor(MRef, [flush]),
            Rest;
        error ->
            Subs
    end,
    {reply, ok, State#{subscribers => NewSubs}};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% Handle broadcast announcement from a peer registry. Self-broadcast
%% is dropped — a remote_announce from our own node would create a
%% duplicate-entry phantom.
handle_cast({remote_announce, _StoreId, _Config, FromNode}, State)
        when FromNode =:= node() ->
    {noreply, State};
handle_cast({remote_announce, StoreId, Config, FromNode},
            #{stores := Stores} = State) ->
    Entry = #store_entry{store_id = StoreId,
                         node = FromNode,
                         config = Config,
                         registered_at = erlang:system_time(millisecond)},
    case find_entry(StoreId, FromNode, Stores) of
        not_found -> notify_subscribers(announced, Entry, State);
        _Already  -> ok
    end,
    logger:debug("[store_registry] Received announcement for ~p from ~p",
                [StoreId, FromNode]),
    {noreply, State#{stores => add_store_entry(Stores, Entry)}};

%% Handle broadcast unannouncement from another registry
handle_cast({remote_unannounce, StoreId, FromNode}, #{stores := Stores} = State) ->
    RemovedEntry = find_entry(StoreId, FromNode, Stores),
    NewStores = remove_store_entry(Stores, StoreId, FromNode),
    case RemovedEntry of
        not_found -> ok;
        _         -> notify_subscribers(retired, RemovedEntry, State)
    end,
    logger:debug("[store_registry] Received unannouncement for ~p from ~p",
                [StoreId, FromNode]),
    {noreply, State#{stores => NewStores}};

%% Peer asking for our current store list — reply with a cast back.
%% Async fan-out: keep registries decoupled, no blocking gen_server
%% calls between them.
handle_cast({peer_state_request, From}, #{stores := Stores} = State) ->
    gen_server:cast(From, {peer_state_reply, Stores}),
    {noreply, State};

%% Peer's response to our sync_peers_state — merge its entries into
%% ours. Notifications fire for newly-discovered entries so any live
%% WatchStores subscribers see the catch-up.
handle_cast({peer_state_reply, PeerEntries}, #{stores := Stores} = State) ->
    {Merged, NewEntries} = merge_entries(Stores, PeerEntries),
    lists:foreach(fun(E) -> notify_subscribers(announced, E, State) end,
                  NewEntries),
    case NewEntries of
        [] -> ok;
        _  -> logger:info("[store_registry] Discovered ~b new store entries "
                          "from peer registry", [length(NewEntries)])
    end,
    {noreply, State#{stores => Merged}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% On startup, ask every other registry for its store list and
%% merge their entries into ours. Bilateral: existing peers will
%% also receive our broadcast announcements when reckon_db_store
%% calls announce_store/2, so both sides eventually converge.
%%
%% Async fan-out — each peer replies with a `peer_state' cast.
%% No try/catch, no timeouts: a dead peer's `!' silently drops
%% and our merge proceeds without it.
handle_info(sync_peers_state, State) ->
    [gen_server:cast(P, {peer_state_request, self()}) || P <- peer_registries()],
    {noreply, State};

%% Handle pg membership changes (node down)
handle_info({pg, ?PG_SCOPE, ?REGISTRY_GROUP, {leave, _Group, Pids}},
            #{stores := Stores} = State) ->
    %% Find which nodes left and remove their stores
    LeftNodes = [node(Pid) || Pid <- Pids],
    {Retiring, Surviving} = lists:partition(
        fun(#store_entry{node = N}) -> lists:member(N, LeftNodes) end,
        Stores),
    lists:foreach(fun(E) -> notify_subscribers(retired, E, State) end, Retiring),
    case Retiring of
        [] -> ok;
        _  -> logger:info("[store_registry] Removed ~b stores from departed nodes",
                          [length(Retiring)])
    end,
    {noreply, State#{stores => Surviving}};

%% Subscriber went down — drop it without unsubscribe call.
handle_info({'DOWN', MRef, process, Pid, _Reason},
            #{subscribers := Subs} = State) ->
    NewSubs = case maps:get(Pid, Subs, undefined) of
        MRef -> maps:remove(Pid, Subs);
        _    -> Subs
    end,
    {noreply, State#{subscribers => NewSubs}};

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

%% @private Find an entry by the (store_id, node) composite key.
find_entry(StoreId, Node, Stores) ->
    case lists:search(
            fun(#store_entry{store_id = S, node = N}) ->
                S =:= StoreId andalso N =:= Node
            end, Stores) of
        {value, Entry} -> Entry;
        false          -> not_found
    end.

%% @private List of peer registry pids (everyone in the pg group
%% except self).
peer_registries() ->
    [P || P <- pg:get_members(?PG_SCOPE, ?REGISTRY_GROUP), P =/= self()].

%% @private Merge a list of peer-supplied entries into our store
%% list. Returns `{Merged, NewEntries}' so the caller can fire
%% announce notifications for the newly-discovered ones.
merge_entries(Stores, PeerEntries) ->
    lists:foldl(
        fun(#store_entry{store_id = S, node = N} = E, {Acc, NewAcc}) ->
            case find_entry(S, N, Acc) of
                not_found -> {add_store_entry(Acc, E), [E | NewAcc]};
                _Already  -> {Acc, NewAcc}
            end
        end,
        {Stores, []},
        PeerEntries).

%% @private Push a store event to every live subscriber. Send is
%% safe to dead pids — `Pid ! Msg' is silently dropped by the
%% runtime — so no catch needed. Dead subscribers are pruned by
%% the `DOWN' handler.
-spec notify_subscribers(announced | retired, store_entry(), map()) -> ok.
notify_subscribers(EventType, Entry, #{subscribers := Subs}) ->
    EntryMap = store_entry_to_map(Entry),
    Msg = {store_event, EventType, EntryMap},
    maps:foreach(fun(Pid, _Ref) -> Pid ! Msg end, Subs).
