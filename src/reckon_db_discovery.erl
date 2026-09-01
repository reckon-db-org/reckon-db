%% @doc Cluster discovery for reckon-db
%%
%% Handles node discovery via UDP multicast (LAN) or Kubernetes DNS.
%% Ported from LibCluster's gossip strategy.
%%
%% Protocol (v2, since 5.1.0):
%% 1. Broadcast {gossip_v2, NodeBin, Timestamp, Hmac} every
%%    BROADCAST_INTERVAL, where Hmac = HMAC-SHA256 over the node name
%%    and timestamp keyed with the cluster secret. The secret itself
%%    never goes on the wire (v1 broadcast it in cleartext).
%% 2. On receive: safe-decode, constant-time HMAC check, freshness
%%    window, THEN net_kernel:connect_node/1. The node name travels
%%    as a binary and is only atomized after authentication, so
%%    unauthenticated LAN datagrams can neither grow the atom table
%%    nor trigger term decoding of attacker-shaped structures.
%% 3. On node up: trigger Khepri cluster join via StoreCoordinator
%%
%% Discovery requires an explicitly configured cluster secret: the
%% RECKON_DB_CLUSTER_SECRET env var, or the cluster_secret application
%% environment key. Without one, cluster-mode discovery stays passive:
%% no broadcasts, inbound gossip ignored. v1 shipped a hardcoded
%% default secret, which made every unconfigured deployment trust any
%% LAN host.
%%
%% @author rgfaber

-module(reckon_db_discovery).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([start_link/1]).
-export([trigger_discovery/1]).
-export([get_discovered_nodes/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-ifdef(TEST).
-export([encode_gossip_message/2, decode_gossip/2, gossip_mac/3]).
-endif.

-define(DEFAULT_PORT, 45892).
-define(MULTICAST_ADDR, {239, 255, 0, 1}).
-define(BROADCAST_INTERVAL_MS, 5000).
-define(MULTICAST_TTL, 1).
%% Reject gossip whose timestamp is further than this from local time.
%% Bounds replay of captured datagrams; generous enough for LAN clock
%% skew. Replaying a fresh packet only re-announces the legitimate
%% node, which is harmless (dist cookie still gates the connection).
-define(GOSSIP_FRESHNESS_MS, 60_000).
%% Erlang node names are practically bounded; anything bigger is junk.
-define(MAX_NODE_NAME_BYTES, 255).

-record(state, {
    store_id :: atom(),
    config :: store_config(),
    socket :: gen_udp:socket() | undefined,
    port :: non_neg_integer() | undefined,
    multicast_addr :: inet:ip4_address() | undefined,
    cluster_secret :: binary() | undefined,
    broadcast_interval :: non_neg_integer() | undefined,
    discovered_nodes :: [node()]
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId} = Config) ->
    Name = reckon_db_naming:discovery_name(StoreId),
    gen_server:start_link({local, Name}, ?MODULE, Config, []).

%% @doc Trigger immediate discovery broadcast
-spec trigger_discovery(atom()) -> ok.
trigger_discovery(StoreId) ->
    Name = reckon_db_naming:discovery_name(StoreId),
    gen_server:cast(Name, trigger_discovery).

%% @doc Get list of discovered nodes.
%%
%% Returns {error, not_running} if the discovery process isn't registered
%% (single-node mode, or store hasn't started yet). gen_server:call/3
%% throws {exit, {noproc}} when the target doesn't exist — the guard
%% converts that to an error tuple.
-spec get_discovered_nodes(atom()) -> [node()] | {error, not_running}.
get_discovered_nodes(StoreId) ->
    Name = reckon_db_naming:discovery_name(StoreId),
    case whereis(Name) of
        undefined -> {error, not_running};
        _Pid -> gen_server:call(Name, get_discovered_nodes)
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(#store_config{store_id = StoreId, mode = Mode} = Config) ->
    case Mode of
        cluster ->
            init_cluster_mode(StoreId, Config);
        single ->
            %% In single mode, discovery is a no-op
            logger:info("Discovery disabled in single-node mode (store: ~p)", [StoreId]),
            {ok, #state{
                store_id = StoreId,
                config = Config,
                socket = undefined,
                discovered_nodes = []
            }}
    end.

init_cluster_mode(StoreId, Config) ->
    case get_cluster_secret() of
        {ok, ClusterSecret} ->
            init_cluster_discovery(StoreId, Config, ClusterSecret);
        error ->
            %% Fail closed: without an explicit shared secret, anyone
            %% on the LAN multicast segment could have us dial them.
            %% Manual/static cluster joins keep working.
            logger:error(
                "Discovery disabled (store: ~p): no cluster secret configured. "
                "Set RECKON_DB_CLUSTER_SECRET or {reckon_db, cluster_secret} "
                "to enable multicast discovery.", [StoreId]),
            {ok, #state{
                store_id = StoreId,
                config = Config,
                socket = undefined,
                discovered_nodes = []
            }}
    end.

init_cluster_discovery(StoreId, Config, ClusterSecret) ->
    Port = get_config_value(discovery_port, ?DEFAULT_PORT),
    MulticastAddr = get_config_value(multicast_addr, ?MULTICAST_ADDR),
    BroadcastInterval = get_config_value(broadcast_interval, ?BROADCAST_INTERVAL_MS),

    State = #state{
        store_id = StoreId,
        config = Config,
        port = Port,
        multicast_addr = MulticastAddr,
        cluster_secret = ClusterSecret,
        broadcast_interval = BroadcastInterval,
        discovered_nodes = []
    },

    case open_multicast_socket(Port, MulticastAddr) of
        {ok, Socket} ->
            logger:info("Discovery started on port ~p (store: ~p)", [Port, StoreId]),
            %% Schedule first broadcast
            schedule_broadcast(BroadcastInterval),
            {ok, State#state{socket = Socket}};
        {error, Reason} ->
            logger:warning("Failed to open multicast socket: ~p, running in passive mode", [Reason]),
            %% Continue without socket - will use manual discovery
            {ok, State#state{socket = undefined}}
    end.

handle_call(get_discovered_nodes, _From, #state{discovered_nodes = Nodes} = State) ->
    {reply, Nodes, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(trigger_discovery, #state{socket = undefined} = State) ->
    %% No socket, can't broadcast
    {noreply, State};

handle_cast(trigger_discovery, State) ->
    broadcast_presence(State),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(broadcast, #state{socket = undefined} = State) ->
    %% No socket, schedule next attempt
    schedule_broadcast(State#state.broadcast_interval),
    {noreply, State};

handle_info(broadcast, #state{broadcast_interval = Interval} = State) ->
    broadcast_presence(State),
    schedule_broadcast(Interval),
    {noreply, State};

handle_info({udp, _Socket, _IP, _Port, Data}, State) ->
    NewState = handle_gossip_message(Data, State),
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{socket = undefined}) ->
    ok;
terminate(_Reason, #state{socket = Socket}) ->
    gen_udp:close(Socket),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Open UDP multicast socket
-spec open_multicast_socket(non_neg_integer(), inet:ip4_address()) ->
    {ok, gen_udp:socket()} | {error, term()}.
open_multicast_socket(Port, MulticastAddr) ->
    Options = [
        binary,
        {active, true},
        {reuseaddr, true},
        {multicast_ttl, ?MULTICAST_TTL},
        {multicast_loop, false},
        {add_membership, {MulticastAddr, {0, 0, 0, 0}}}
    ],
    gen_udp:open(Port, Options).

%% @private Broadcast presence to multicast group
-spec broadcast_presence(#state{}) -> ok.
broadcast_presence(#state{socket = Socket, port = Port, multicast_addr = Addr,
                          cluster_secret = Secret, store_id = StoreId}) ->
    Message = encode_gossip_message(node(), Secret),
    case gen_udp:send(Socket, Addr, Port, Message) of
        ok ->
            ok;
        {error, Reason} ->
            logger:warning("Failed to broadcast discovery message (store: ~p): ~p",
                          [StoreId, Reason])
    end.

%% @private Encode gossip message (v2: authenticated, atom-free,
%% secret never on the wire).
-spec encode_gossip_message(node(), binary()) -> binary().
encode_gossip_message(Node, Secret) ->
    Timestamp = erlang:system_time(millisecond),
    NodeBin = atom_to_binary(Node, utf8),
    Mac = gossip_mac(NodeBin, Timestamp, Secret),
    term_to_binary({gossip_v2, NodeBin, Timestamp, Mac}).

-spec gossip_mac(binary(), integer(), binary()) -> binary().
gossip_mac(NodeBin, Timestamp, Secret) ->
    crypto:mac(hmac, sha256, Secret,
               <<Timestamp:64/signed-big, NodeBin/binary>>).

%% @private Handle incoming gossip message.
-spec handle_gossip_message(binary(), #state{}) -> #state{}.
handle_gossip_message(_Data, #state{cluster_secret = undefined} = State) ->
    State;
handle_gossip_message(Data, #state{cluster_secret = Secret,
                                   store_id = StoreId,
                                   discovered_nodes = KnownNodes} = State) ->
    case decode_gossip(Data, Secret) of
        {ok, NodeBin} ->
            %% Atomization is safe here: only holders of the cluster
            %% secret reach this point, so atom creation is bounded
            %% to authenticated peers.
            Node = binary_to_atom(NodeBin, utf8),
            maybe_handle_node(Node =:= node(), Node, StoreId, KnownNodes, State);
        reject ->
            State
    end.

%% @private Ignore our own gossip; handle a genuine peer.
maybe_handle_node(true, _Node, _StoreId, _KnownNodes, State) ->
    State;
maybe_handle_node(false, Node, StoreId, KnownNodes, State) ->
    handle_discovered_node(Node, StoreId, KnownNodes, State).

%% @private Decode + authenticate one gossip datagram.
%%
%% The datagram is untrusted LAN input. Decode with [safe] (a plain
%% binary_to_term here allowed remote atom-table exhaustion and
%% arbitrary-term allocation before any authentication), pattern-match
%% the exact v2 shape, verify the HMAC in constant time, then check
%% freshness. The try is required: [safe] raises badarg on unknown
%% atoms / garbage and a hostile packet must degrade to a no-op, not
%% crash discovery.
-spec decode_gossip(binary(), binary()) -> {ok, binary()} | reject.
decode_gossip(Data, Secret) ->
    Decoded = try
                  {term, binary_to_term(Data, [safe])}
              catch
                  error:badarg -> invalid
              end,
    authenticate_gossip_term(Decoded, Secret).

authenticate_gossip_term({term, {gossip_v2, NodeBin, Timestamp, Mac}}, Secret)
        when is_binary(NodeBin), byte_size(NodeBin) > 0,
             byte_size(NodeBin) =< ?MAX_NODE_NAME_BYTES,
             is_integer(Timestamp),
             is_binary(Mac), byte_size(Mac) =:= 32 ->
    Expected = gossip_mac(NodeBin, Timestamp, Secret),
    case crypto:hash_equals(Expected, Mac) andalso is_fresh(Timestamp) of
        true ->
            {ok, NodeBin};
        false ->
            logger:debug("Ignoring gossip with invalid MAC or stale timestamp"),
            reject
    end;
authenticate_gossip_term(_Invalid, _Secret) ->
    reject.

is_fresh(Timestamp) ->
    abs(erlang:system_time(millisecond) - Timestamp) =< ?GOSSIP_FRESHNESS_MS.

%% @private Handle a newly discovered node
-spec handle_discovered_node(node(), atom(), [node()], #state{}) -> #state{}.
handle_discovered_node(Node, StoreId, KnownNodes, State) ->
    case lists:member(Node, KnownNodes) of
        true ->
            State;
        false ->
            logger:info("Discovered new node: ~p (store: ~p)", [Node, StoreId]),
            connect_discovered_node(Node, StoreId, KnownNodes, State)
    end.

connect_discovered_node(Node, StoreId, KnownNodes, State) ->
    case net_kernel:connect_node(Node) of
        true ->
            logger:info("Connected to discovered node: ~p", [Node]),
            telemetry:execute(
                ?CLUSTER_NODE_UP,
                #{system_time => erlang:system_time(millisecond)},
                #{store_id => StoreId, node => Node,
                  member_count => length(nodes()) + 1}
            ),
            trigger_cluster_join(StoreId),
            State#state{discovered_nodes = [Node | KnownNodes]};
        false ->
            logger:warning("Failed to connect to discovered node: ~p", [Node]),
            State
    end.

%% @private Trigger cluster join via store coordinator
-spec trigger_cluster_join(atom()) -> ok.
trigger_cluster_join(StoreId) ->
    %% Use spawn to avoid blocking discovery
    spawn(fun() -> do_cluster_join(StoreId) end),
    ok.

do_cluster_join(StoreId) ->
    try
        reckon_db_store_coordinator:join_cluster(StoreId)
    catch
        _:Reason ->
            logger:warning("Failed to trigger cluster join: ~p", [Reason])
    end.

%% @private Schedule next broadcast
-spec schedule_broadcast(non_neg_integer()) -> reference().
schedule_broadcast(Interval) ->
    erlang:send_after(Interval, self(), broadcast).

%% @private Get cluster secret from environment or config.
%% No default: a hardcoded fallback secret means every unconfigured
%% deployment trusts any LAN host that knows the public source code.
-spec get_cluster_secret() -> {ok, binary()} | error.
get_cluster_secret() ->
    case os:getenv("RECKON_DB_CLUSTER_SECRET") of
        false ->
            get_cluster_secret_from_env();
        Secret ->
            {ok, list_to_binary(Secret)}
    end.

get_cluster_secret_from_env() ->
    case application:get_env(reckon_db, cluster_secret) of
        {ok, Secret} when is_binary(Secret), byte_size(Secret) > 0 ->
            {ok, Secret};
        {ok, Secret} when is_list(Secret), Secret =/= [] ->
            {ok, list_to_binary(Secret)};
        _ ->
            error
    end.

%% @private Get config value with default
-spec get_config_value(atom(), term()) -> term().
get_config_value(Key, Default) ->
    case application:get_env(reckon_db, Key) of
        {ok, Value} -> Value;
        undefined -> Default
    end.
