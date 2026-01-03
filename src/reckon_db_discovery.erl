%% @doc Cluster discovery for reckon-db
%%
%% Handles node discovery via UDP multicast (LAN) or Kubernetes DNS.
%% Ported from LibCluster's gossip strategy.
%%
%% Protocol:
%% 1. Broadcast {gossip, Node, ClusterSecret, Timestamp} every BROADCAST_INTERVAL
%% 2. On receive: verify secret, call net_kernel:connect_node/1
%% 3. On node up: trigger Khepri cluster join via StoreCoordinator
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

-define(DEFAULT_PORT, 45892).
-define(MULTICAST_ADDR, {239, 255, 0, 1}).
-define(BROADCAST_INTERVAL_MS, 5000).
-define(MULTICAST_TTL, 1).

-record(state, {
    store_id :: atom(),
    config :: store_config(),
    socket :: gen_udp:socket() | undefined,
    port :: non_neg_integer(),
    multicast_addr :: inet:ip4_address(),
    cluster_secret :: binary(),
    broadcast_interval :: non_neg_integer(),
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

%% @doc Get list of discovered nodes
-spec get_discovered_nodes(atom()) -> [node()].
get_discovered_nodes(StoreId) ->
    Name = reckon_db_naming:discovery_name(StoreId),
    gen_server:call(Name, get_discovered_nodes).

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
    Port = get_config_value(discovery_port, ?DEFAULT_PORT),
    MulticastAddr = get_config_value(multicast_addr, ?MULTICAST_ADDR),
    ClusterSecret = get_cluster_secret(),
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

%% @private Encode gossip message
-spec encode_gossip_message(node(), binary()) -> binary().
encode_gossip_message(Node, Secret) ->
    Timestamp = erlang:system_time(millisecond),
    term_to_binary({gossip, Node, Secret, Timestamp}).

%% @private Handle incoming gossip message
-spec handle_gossip_message(binary(), #state{}) -> #state{}.
handle_gossip_message(Data, #state{cluster_secret = OurSecret, store_id = StoreId,
                                   discovered_nodes = KnownNodes} = State) ->
    try binary_to_term(Data) of
        {gossip, Node, Secret, _Timestamp} when Secret =:= OurSecret, Node =/= node() ->
            handle_discovered_node(Node, StoreId, KnownNodes, State);
        {gossip, Node, _Secret, _Timestamp} when Node =/= node() ->
            %% Wrong secret, ignore
            logger:debug("Ignoring gossip from ~p with invalid secret", [Node]),
            State;
        _ ->
            State
    catch
        _:_ ->
            %% Invalid message format
            State
    end.

%% @private Handle a newly discovered node
-spec handle_discovered_node(node(), atom(), [node()], #state{}) -> #state{}.
handle_discovered_node(Node, StoreId, KnownNodes, State) ->
    case lists:member(Node, KnownNodes) of
        true ->
            %% Already known
            State;
        false ->
            logger:info("Discovered new node: ~p (store: ~p)", [Node, StoreId]),
            %% Try to connect
            case net_kernel:connect_node(Node) of
                true ->
                    logger:info("Connected to discovered node: ~p", [Node]),
                    telemetry:execute(
                        ?CLUSTER_NODE_UP,
                        #{system_time => erlang:system_time(millisecond)},
                        #{store_id => StoreId, node => Node,
                          member_count => length(nodes()) + 1}
                    ),
                    %% Trigger cluster join
                    trigger_cluster_join(StoreId),
                    State#state{discovered_nodes = [Node | KnownNodes]};
                false ->
                    logger:warning("Failed to connect to discovered node: ~p", [Node]),
                    State
            end
    end.

%% @private Trigger cluster join via store coordinator
-spec trigger_cluster_join(atom()) -> ok.
trigger_cluster_join(StoreId) ->
    %% Use spawn to avoid blocking discovery
    spawn(fun() ->
        try
            reckon_db_store_coordinator:join_cluster(StoreId)
        catch
            _:Reason ->
                logger:warning("Failed to trigger cluster join: ~p", [Reason])
        end
    end),
    ok.

%% @private Schedule next broadcast
-spec schedule_broadcast(non_neg_integer()) -> reference().
schedule_broadcast(Interval) ->
    erlang:send_after(Interval, self(), broadcast).

%% @private Get cluster secret from environment or config
-spec get_cluster_secret() -> binary().
get_cluster_secret() ->
    case os:getenv("RECKON_DB_CLUSTER_SECRET") of
        false ->
            %% Use default secret (not recommended for production)
            <<"reckon_db_default_secret">>;
        Secret ->
            list_to_binary(Secret)
    end.

%% @private Get config value with default
-spec get_config_value(atom(), term()) -> term().
get_config_value(Key, Default) ->
    case application:get_env(reckon_db, Key) of
        {ok, Value} -> Value;
        undefined -> Default
    end.
