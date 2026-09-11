%% @doc Cluster discovery for reckon-db
%%
%% Handles node discovery via UDP multicast on the LAN.
%% Ported from LibCluster's gossip strategy.
%%
%% Protocol (v3):
%% 1. Every BROADCAST_INTERVAL a node sends a datagram with a fixed
%%    layout: the prefix RDBG, version 3, a signed 64-bit timestamp in
%%    milliseconds, a one-byte name length, the node name, and a 32-byte
%%    HMAC-SHA256 tag over all the bytes before it, keyed with the cluster
%%    secret. The secret itself never goes on the wire.
%% 2. On receive: the tag is checked in constant time over the raw bytes
%%    before any field is read, then the layout and the freshness window.
%%    No term is decoded from a datagram, and the node name becomes an
%%    atom only after its tag verifies.
%% 3. A verified node is dialled from a monitored process of its own, one
%%    dial per node at a time, so discovery keeps serving while a dial
%%    waits. On connect: trigger Khepri cluster join via StoreCoordinator
%%
%% The socket is bound to the multicast group address.
%%
%% Discovery requires a cluster secret of at least 32 bytes: the
%% RECKON_DB_CLUSTER_SECRET env var, or the cluster_secret application
%% environment key when that variable is unset or empty. Without one,
%% cluster-mode discovery stays passive: no socket, no broadcasts, and a
%% discovery_disabled report in the log giving the reason.
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

%% Runs in a dial process of its own, spawned by the discovery server.
-export([dial_node/1]).

-ifdef(TEST).
-export([encode_gossip_message/2, decode_gossip/2]).
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
%% Bytes of cluster secret discovery needs to start.
-define(MIN_SECRET_BYTES, 32).
%% A datagram starts with this prefix and version.
-define(GOSSIP_PREFIX, "RDBG").
-define(GOSSIP_VERSION, 3).
%% The HMAC-SHA256 tag that ends a datagram.
-define(TAG_BYTES, 32).
%% Prefix, version, timestamp, name length, a 255-byte name and the tag.
-define(MAX_DATAGRAM_BYTES, 4 + 1 + 8 + 1 + 255 + ?TAG_BYTES).

-record(state, {
    store_id :: atom(),
    config :: store_config(),
    socket :: gen_udp:socket() | undefined,
    port :: non_neg_integer() | undefined,
    multicast_addr :: inet:ip4_address() | undefined,
    cluster_secret :: binary() | undefined,
    broadcast_interval :: non_neg_integer() | undefined,
    discovered_nodes :: [node()],
    %% Dials in flight, by the monitor on the process dialling each node
    dialling = #{} :: #{reference() => node()}
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
            {ok, passive_state(StoreId, Config)}
    end.

init_cluster_mode(StoreId, Config) ->
    start_with_secret(secret_check(get_cluster_secret()), StoreId, Config).

%% @private Discovery starts only with a cluster secret of MIN_SECRET_BYTES
%% or more.
-spec secret_check(binary() | undefined) ->
    {ok, binary()}
  | {error, secret_required
          | {secret_too_short, #{bytes := non_neg_integer(), required := ?MIN_SECRET_BYTES}}}.
secret_check(undefined) ->
    {error, secret_required};
secret_check(Secret) when byte_size(Secret) < ?MIN_SECRET_BYTES ->
    {error, {secret_too_short, #{bytes => byte_size(Secret),
                                 required => ?MIN_SECRET_BYTES}}};
secret_check(Secret) ->
    {ok, Secret}.

start_with_secret({ok, ClusterSecret}, StoreId, Config) ->
    init_cluster_discovery(StoreId, Config, ClusterSecret);
start_with_secret({error, Reason}, StoreId, Config) ->
    %% Without a cluster secret discovery stays passive. Manual and static
    %% cluster joins keep working.
    logger:error(#{what => discovery_disabled,
                   store_id => StoreId,
                   reason => Reason,
                   remedy => "set RECKON_DB_CLUSTER_SECRET or {reckon_db, cluster_secret} "
                             "to a secret of at least 32 bytes"}),
    {ok, passive_state(StoreId, Config)}.

passive_state(StoreId, Config) ->
    #state{
        store_id = StoreId,
        config = Config,
        socket = undefined,
        discovered_nodes = []
    }.

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

%% A dial ended: its process exits with the dial's result
handle_info({'DOWN', Ref, process, _Pid, Reason}, #state{dialling = Dialling} = State)
        when is_map_key(Ref, Dialling) ->
    {Node, Rest} = maps:take(Ref, Dialling),
    {noreply, dialled(dial_result(Reason), Node, State#state{dialling = Rest})};

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

%% @private Open UDP multicast socket, bound to the group address so only
%% datagrams sent to the group arrive
-spec open_multicast_socket(non_neg_integer(), inet:ip4_address()) ->
    {ok, gen_udp:socket()} | {error, term()}.
open_multicast_socket(Port, MulticastAddr) ->
    Options = [
        binary,
        {active, true},
        {reuseaddr, true},
        {ip, MulticastAddr},
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

%% @private Encode the datagram announcing Node: the fixed layout, with an
%% HMAC-SHA256 tag over its bytes. The secret never goes on the wire.
-spec encode_gossip_message(node(), binary()) -> binary().
encode_gossip_message(Node, Secret) ->
    NodeBin = atom_to_binary(Node, utf8),
    Body = <<?GOSSIP_PREFIX, ?GOSSIP_VERSION,
             (erlang:system_time(millisecond)):64/signed-big,
             (byte_size(NodeBin)):8, NodeBin/binary>>,
    <<Body/binary, (gossip_tag(Body, Secret))/binary>>.

-spec gossip_tag(binary(), binary()) -> binary().
gossip_tag(Body, Secret) ->
    crypto:mac(hmac, sha256, Secret, Body).

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

%% @private Authenticate and read one datagram, which is untrusted LAN
%% input.
%%
%% The tag is checked in constant time over the raw bytes before any field
%% is read; then the layout must match exactly and the timestamp must be
%% fresh. Nothing on this path raises, and no term is decoded.
-spec decode_gossip(binary(), binary()) -> {ok, binary()} | reject.
decode_gossip(Data, Secret)
        when byte_size(Data) > ?TAG_BYTES, byte_size(Data) =< ?MAX_DATAGRAM_BYTES ->
    BodyBytes = byte_size(Data) - ?TAG_BYTES,
    <<Body:BodyBytes/binary, Tag:?TAG_BYTES/binary>> = Data,
    read_verified(crypto:hash_equals(gossip_tag(Body, Secret), Tag), Body);
decode_gossip(_Data, _Secret) ->
    reject.

read_verified(true, <<?GOSSIP_PREFIX, ?GOSSIP_VERSION, Timestamp:64/signed-big,
                      NameBytes:8, NodeBin:NameBytes/binary>>) when NameBytes > 0 ->
    fresh(is_fresh(Timestamp), NodeBin);
read_verified(true, _Body) ->
    logger:debug("Ignoring gossip with a verified tag and an unknown layout"),
    reject;
read_verified(false, _Body) ->
    logger:debug("Ignoring gossip with an invalid tag"),
    reject.

fresh(true, NodeBin) ->
    {ok, NodeBin};
fresh(false, _NodeBin) ->
    logger:debug("Ignoring gossip with a stale timestamp"),
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
            dial(Node, State)
    end.

%% @private A dial runs in a monitored process of its own, one per node at
%% a time, so a node that is slow to answer never holds up discovery. The
%% process exits with the dial's result.
-spec dial(node(), #state{}) -> #state{}.
dial(Node, #state{dialling = Dialling} = State) ->
    dial_unless_dialling(lists:member(Node, maps:values(Dialling)), Node, State).

dial_unless_dialling(true, _Node, State) ->
    State;
dial_unless_dialling(false, Node, #state{dialling = Dialling} = State) ->
    {_Pid, Ref} = spawn_monitor(?MODULE, dial_node, [Node]),
    State#state{dialling = Dialling#{Ref => Node}}.

%% @private Dials Node and exits with the result, for the discovery server
%% that monitors this process.
-spec dial_node(node()) -> no_return().
dial_node(Node) ->
    exit({dialled, net_kernel:connect_node(Node)}).

dial_result({dialled, Result}) -> Result;
dial_result(_Crashed) -> false.

dialled(true, Node, #state{store_id = StoreId, discovered_nodes = KnownNodes} = State) ->
    logger:info("Connected to discovered node: ~p", [Node]),
    telemetry:execute(
        ?CLUSTER_NODE_UP,
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, node => Node,
          member_count => length(nodes()) + 1}
    ),
    trigger_cluster_join(StoreId),
    State#state{discovered_nodes = [Node | KnownNodes]};
dialled(_NotConnected, Node, State) ->
    logger:warning("Failed to connect to discovered node: ~p", [Node]),
    State.

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

%% @private Get the cluster secret: RECKON_DB_CLUSTER_SECRET, or the
%% cluster_secret application key when that variable is unset or empty.
%% There is no default secret.
-spec get_cluster_secret() -> binary() | undefined.
get_cluster_secret() ->
    env_cluster_secret(os:getenv("RECKON_DB_CLUSTER_SECRET")).

env_cluster_secret(Unset) when Unset =:= false; Unset =:= "" ->
    app_cluster_secret(application:get_env(reckon_db, cluster_secret));
env_cluster_secret(Secret) ->
    list_to_binary(Secret).

app_cluster_secret({ok, Secret}) when is_binary(Secret), byte_size(Secret) > 0 ->
    Secret;
app_cluster_secret({ok, Secret}) when is_list(Secret), Secret =/= [] ->
    list_to_binary(Secret);
app_cluster_secret(_) ->
    undefined.

%% @private Get config value with default
-spec get_config_value(atom(), term()) -> term().
get_config_value(Key, Default) ->
    case application:get_env(reckon_db, Key) of
        {ok, Value} -> Value;
        undefined -> Default
    end.
