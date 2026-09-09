%% @doc Two-node integration test: a subscription created on a node
%% BEFORE that node joins an existing cluster must survive the join.
%%
%% khepri_cluster:join/2 resets the joiner's local tree and replaces it
%% with the cluster's. A consumer that subscribed while its node was
%% still a standalone cluster of one (the normal boot shape: evoq
%% subscribes as soon as the store is up, discovery joins seconds later)
%% would otherwise keep a live pid and a running emitter pool while its
%% subscription record and Khepri trigger are silently gone.
%%
%% Two real peer nodes are started with OTP peer over standard_io, so
%% the CT node itself does not need to be distributed. The peers are
%% distributed with each other (short names, shared cookie).
%%
%% @author rgfaber

-module(reckon_db_cluster_join_subscriptions_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

%% CT callbacks
-export([
    all/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    pre_join_subscription_survives_join/1,
    leadership_transfer_new_leader_serves_existing_subscription/1,
    leader_shutdown_failover_new_leader_serves_existing_subscription/1,
    subscriber_restart_on_follower_repoints_every_node/1
]).

%% Run ON the peer nodes via peer:call/4,5
-export([
    boot_store/2,
    start_collector/0,
    start_collector/1,
    kill_collector/1,
    collector_loop/1,
    collected/0,
    collected/1
]).

-define(STORE, join_subs_store).
-define(COOKIE, "reckon_db_join_subs").
-define(COLLECTOR, join_subs_collector).
-define(COLLECTOR2, join_subs_collector_2).

%%====================================================================
%% CT Callbacks
%%====================================================================

suite() ->
    [{timetrap, {minutes, 3}}].

all() ->
    [
        pre_join_subscription_survives_join,
        leadership_transfer_new_leader_serves_existing_subscription,
        leader_shutdown_failover_new_leader_serves_existing_subscription,
        subscriber_restart_on_follower_repoints_every_node
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    %% Lower name = elected coordinator = keeps its data; the higher
    %% name joins it and gets reset. The subscription goes on the joiner.
    {PeerA, NodeA, DirA} = start_peer("rdb_join_a_" ++ Rand),
    {PeerB, NodeB, DirB} = start_peer("rdb_join_b_" ++ Rand),
    [{rand, Rand},
     {peer_a, PeerA}, {node_a, NodeA}, {dir_a, DirA},
     {peer_b, PeerB}, {node_b, NodeB}, {dir_b, DirB} | Config].

end_per_testcase(_TestCase, Config) ->
    stop_peer(?config(peer_a, Config)),
    stop_peer(?config(peer_b, Config)),
    os:cmd("rm -rf " ++ ?config(dir_a, Config)),
    os:cmd("rm -rf " ++ ?config(dir_b, Config)),
    %% A third peer, when a case started one, is stopped by the case
    %% itself (it is linked to the case process); its directory is
    %% named from the same random suffix.
    os:cmd("rm -rf /tmp/rdb_join_c_" ++ ?config(rand, Config)),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc GIVEN two standalone single-member stores in cluster mode, and a
%%      live subscription created on the node that is about to JOIN
%%      WHEN that node joins the other node's cluster (its local tree is
%%      reset and replaced by the cluster's)
%%      THEN the subscription still exists afterwards and an event
%%      appended on the other node reaches the pre-join subscriber
pre_join_subscription_survives_join(Config) ->
    PeerA = ?config(peer_a, Config),
    NodeA = ?config(node_a, Config),
    PeerB = ?config(peer_b, Config),
    StreamId = reckon_db_test_helpers:sid(<<"joinsubsstream-001">>),
    SubName = <<"pre_join_subscription">>,

    ok = wait_until(fun() -> peer:call(PeerA, reckon_db_leader, is_active, [?STORE]) end, 20000),
    ok = wait_until(fun() -> peer:call(PeerB, reckon_db_leader, is_active, [?STORE]) end, 20000),

    %% B subscribes while it is still a cluster of one.
    Collector = peer:call(PeerB, ?MODULE, start_collector, []),
    {ok, Key} = peer:call(PeerB, reckon_db_subscriptions, subscribe,
                          [?STORE, stream, StreamId, SubName, #{subscriber => Collector}]),

    %% Sanity: local delivery on B works before the join.
    {ok, _} = peer:call(PeerB, reckon_db_streams, append,
                        [?STORE, StreamId, -2, [event(<<"pre_join_v1">>)]]),
    ok = wait_until(fun() -> received(PeerB, <<"pre_join_v1">>) end, 10000),

    %% B connects to A and joins A's cluster: B's local tree is reset.
    true = peer:call(PeerB, net_kernel, connect_node, [NodeA]),
    ok = peer:call(PeerB, reckon_db_store_coordinator, join_cluster, [?STORE, NodeA], 60000),
    ok = wait_until(fun() -> two_members(PeerA) andalso two_members(PeerB) end, 30000),

    %% The subscription must have survived the join...
    ?assertEqual(true, peer:call(PeerB, reckon_db_subscriptions, exists, [?STORE, Key])),

    %% ...and an event written on the other node must reach it.
    {ok, _} = peer:call(PeerA, reckon_db_streams, append,
                        [?STORE, StreamId, -2, [event(<<"post_join_v1">>)]]),
    ok = wait_until(fun() -> received(PeerB, <<"post_join_v1">>) end, 20000),
    ok.

%% @doc GIVEN a two-member cluster and a subscription created through
%%      the leader whose subscriber lives on the FOLLOWER
%%      WHEN Ra leadership is transferred to the follower (a real,
%%      graceful transfer; both members stay up)
%%      THEN the new leader activates, starts an emitter pool for the
%%      existing subscription, the old leader stops being leader, and
%%      events appended on EITHER node reach the subscriber exactly once
leadership_transfer_new_leader_serves_existing_subscription(Config) ->
    {PeerA, NodeA, PeerB, NodeB} = peers(Config),
    ok = join(PeerB, NodeA, [PeerA, PeerB], 2),
    {LeaderPeer, LeaderNode, FollowerPeer, FollowerNode} = roles(PeerA, NodeA, PeerB, NodeB),
    ok = wait_until(fun() -> peer:call(LeaderPeer, reckon_db_leader, is_active, [?STORE]) end, 20000),

    StreamId = reckon_db_test_helpers:sid(<<"xferstream-001">>),
    SubName = <<"transfer_subscription">>,
    Collector = peer:call(FollowerPeer, ?MODULE, start_collector, [?COLLECTOR]),
    {ok, Key} = peer:call(LeaderPeer, reckon_db_subscriptions, subscribe,
                          [?STORE, stream, StreamId, SubName, #{subscriber => Collector}]),
    ok = append_and_expect(LeaderPeer, StreamId, <<"before_transfer_v1">>, FollowerPeer, ?COLLECTOR),

    ok = peer:call(LeaderPeer, ra, transfer_leadership,
                   [{?STORE, LeaderNode}, {?STORE, FollowerNode}], 30000),
    ok = wait_until(fun() ->
                        leader_of(PeerA) =:= {?STORE, FollowerNode} andalso
                        leader_of(PeerB) =:= {?STORE, FollowerNode}
                    end, 30000),

    %% Gainer: activates and serves the subscription it did not create.
    PoolName = reckon_db_emitter_pool:name(?STORE, Key),
    ok = wait_until(fun() -> peer:call(FollowerPeer, reckon_db_leader, is_active, [?STORE]) end, 30000),
    ok = wait_until(fun() -> peer:call(FollowerPeer, erlang, whereis, [PoolName]) =/= undefined end, 30000),
    ?assertEqual(true, peer:call(FollowerPeer, reckon_db_store_coordinator, is_leader, [?STORE])),
    %% Loser: no longer leader.
    ?assertEqual(false, peer:call(LeaderPeer, reckon_db_store_coordinator, is_leader, [?STORE])),

    %% Exactly-once delivery from both sides of the transfer.
    ViaNew = [<<"via_new_leader_", (integer_to_binary(N))/binary>> || N <- lists:seq(1, 5)],
    ViaOld = [<<"via_old_leader_", (integer_to_binary(N))/binary>> || N <- lists:seq(1, 5)],
    ok = append_all(FollowerPeer, StreamId, ViaNew),
    ok = append_all(LeaderPeer, StreamId, ViaOld),
    ok = wait_until(fun() -> received_all(FollowerPeer, ?COLLECTOR, ViaNew ++ ViaOld) end, 20000),
    Got = peer:call(FollowerPeer, ?MODULE, collected, [?COLLECTOR]),
    lists:foreach(fun(T) -> ?assertEqual({T, 1}, {T, count(T, Got)}) end, ViaNew ++ ViaOld),
    ok.

%% @doc GIVEN a three-member cluster and a subscription created through
%%      the leader whose subscriber lives on another member
%%      WHEN the leader NODE shuts down (rolling restart shape) and the
%%      remaining two elect a new leader
%%      THEN the new leader activates, starts an emitter pool for the
%%      existing subscription, and events reach the subscriber
leader_shutdown_failover_new_leader_serves_existing_subscription(Config) ->
    {PeerA, NodeA, PeerB, NodeB} = peers(Config),
    {PeerC, NodeC, _DirC} = start_peer("rdb_join_c_" ++ ?config(rand, Config)),
    ok = join(PeerB, NodeA, [PeerA, PeerB], 2),
    ok = join(PeerC, NodeA, [PeerA, PeerB, PeerC], 3),
    Members = [{PeerA, NodeA}, {PeerB, NodeB}, {PeerC, NodeC}],
    {?STORE, LeaderNode} = leader_of(PeerA),
    {LeaderPeer, LeaderNode} = lists:keyfind(LeaderNode, 2, Members),
    [{SurvivorPeer, _}, {OtherPeer, _}] = [M || {_, N} = M <- Members, N =/= LeaderNode],
    ok = wait_until(fun() -> peer:call(LeaderPeer, reckon_db_leader, is_active, [?STORE]) end, 20000),

    StreamId = reckon_db_test_helpers:sid(<<"failoverstream-001">>),
    SubName = <<"failover_subscription">>,
    Collector = peer:call(SurvivorPeer, ?MODULE, start_collector, [?COLLECTOR]),
    {ok, Key} = peer:call(LeaderPeer, reckon_db_subscriptions, subscribe,
                          [?STORE, stream, StreamId, SubName, #{subscriber => Collector}]),
    ok = append_and_expect(LeaderPeer, StreamId, <<"before_failover_v1">>, SurvivorPeer, ?COLLECTOR),

    %% The leader node goes away; the other two hold quorum and elect.
    ok = peer:stop(LeaderPeer),
    ok = wait_until(fun() ->
                        case leader_of(SurvivorPeer) of
                            {?STORE, N} when N =/= LeaderNode -> true;
                            _ -> false
                        end
                    end, 60000),
    {?STORE, NewLeaderNode} = leader_of(SurvivorPeer),
    {NewLeaderPeer, NewLeaderNode} = lists:keyfind(NewLeaderNode, 2, Members),

    PoolName = reckon_db_emitter_pool:name(?STORE, Key),
    ok = wait_until(fun() -> peer:call(NewLeaderPeer, reckon_db_leader, is_active, [?STORE]) end, 30000),
    ok = wait_until(fun() -> peer:call(NewLeaderPeer, erlang, whereis, [PoolName]) =/= undefined end, 30000),

    AfterTypes = [<<"after_failover_", (integer_to_binary(N))/binary>> || N <- lists:seq(1, 3)],
    ok = append_all(NewLeaderPeer, StreamId, AfterTypes),
    ok = wait_until(fun() -> received_all(SurvivorPeer, ?COLLECTOR, AfterTypes) end, 20000),
    Got = peer:call(SurvivorPeer, ?MODULE, collected, [?COLLECTOR]),
    lists:foreach(fun(T) -> ?assertEqual({T, 1}, {T, count(T, Got)}) end, AfterTypes),

    stop_peer(OtherPeer),
    stop_peer(NewLeaderPeer),
    stop_peer(PeerC),
    ok.

%% @doc GIVEN a two-member cluster where a subscriber on the FOLLOWER
%%      subscribed through its own node (so the follower has an eager
%%      pool and the leader's tracker started a second pool on the
%%      leader for the same subscription)
%%      WHEN that subscriber dies and its replacement re-subscribes on
%%      the follower
%%      THEN every event appended on the leader reaches the replacement;
%%      none is lost to an emitter on the leader still holding the dead
%%      remote pid (whose delivery is a silent send, never a liveness
%%      check, in reckon_db_emitter:send_to_subscriber/4)
subscriber_restart_on_follower_repoints_every_node(Config) ->
    {PeerA, NodeA, PeerB, NodeB} = peers(Config),
    ok = join(PeerB, NodeA, [PeerA, PeerB], 2),
    {LeaderPeer, _LeaderNode, FollowerPeer, _FollowerNode} = roles(PeerA, NodeA, PeerB, NodeB),
    ok = wait_until(fun() -> peer:call(LeaderPeer, reckon_db_leader, is_active, [?STORE]) end, 20000),

    StreamId = reckon_db_test_helpers:sid(<<"repointstream-001">>),
    SubName = <<"repoint_subscription">>,
    First = peer:call(FollowerPeer, ?MODULE, start_collector, [?COLLECTOR]),
    {ok, Key} = peer:call(FollowerPeer, reckon_db_subscriptions, subscribe,
                          [?STORE, stream, StreamId, SubName, #{subscriber => First}]),
    PoolName = reckon_db_emitter_pool:name(?STORE, Key),
    %% Both nodes end up with a pool for this one subscription.
    ok = wait_until(fun() -> peer:call(FollowerPeer, erlang, whereis, [PoolName]) =/= undefined end, 10000),
    ok = wait_until(fun() -> peer:call(LeaderPeer, erlang, whereis, [PoolName]) =/= undefined end, 20000),
    ok = append_and_expect(LeaderPeer, StreamId, <<"before_restart_v1">>, FollowerPeer, ?COLLECTOR),

    %% Subscriber restarts on the follower.
    ok = peer:call(FollowerPeer, ?MODULE, kill_collector, [?COLLECTOR]),
    Second = peer:call(FollowerPeer, ?MODULE, start_collector, [?COLLECTOR2]),
    ok = peer:call(FollowerPeer, reckon_db_subscriptions, ack, [?STORE, SubName, undefined, 1000000]),
    {ok, Key} = peer:call(FollowerPeer, reckon_db_subscriptions, subscribe,
                          [?STORE, stream, StreamId, SubName, #{subscriber => Second}]),

    Types = [<<"after_restart_", (integer_to_binary(N))/binary>> || N <- lists:seq(1, 20)],
    ok = append_all(LeaderPeer, StreamId, Types),
    Delivered = fun() -> peer:call(FollowerPeer, ?MODULE, collected, [?COLLECTOR2]) end,
    _ = wait_until_or_false(fun() -> lists:all(fun(T) -> lists:member(T, Delivered()) end, Types) end, 15000),
    Got = Delivered(),
    ct:pal("delivered ~p of ~p after subscriber restart on the follower",
           [length([T || T <- Types, lists:member(T, Got)]), length(Types)]),
    lists:foreach(fun(T) -> ?assertEqual({T, delivered}, {T, delivered_or_lost(T, Got)}) end, Types),
    ok.

delivered_or_lost(T, Got) ->
    case lists:member(T, Got) of
        true -> delivered;
        false -> lost
    end.

%%====================================================================
%% Functions executed on the peers
%%====================================================================

%% @doc Start ra, khepri and reckon_db on this peer and one store in
%% cluster mode with its own data directory.
boot_store(StoreId, Dir) ->
    RaDir = filename:join(Dir, "ra"),
    ok = filelib:ensure_dir(filename:join(RaDir, "dummy")),
    application:set_env(ra, data_dir, RaDir),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    {ok, _} = application:ensure_all_started(reckon_db),
    StoreConfig = #store_config{
        store_id = StoreId,
        data_dir = filename:join(Dir, "store"),
        mode = cluster,
        writer_pool_size = 1,
        reader_pool_size = 1,
        gateway_pool_size = 1
    },
    {ok, _} = reckon_db_sup:start_store(StoreConfig),
    ok.

%% @doc A subscriber process that accumulates delivered events.
start_collector() ->
    start_collector(?COLLECTOR).

start_collector(Name) ->
    Pid = spawn(?MODULE, collector_loop, [[]]),
    true = register(Name, Pid),
    Pid.

%% @doc Kill a collector and wait until its name is free again.
kill_collector(Name) ->
    Pid = whereis(Name),
    MRef = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', MRef, process, Pid, _} -> ok end,
    ok.

collector_loop(Acc) ->
    receive
        {events, Events} when is_list(Events) ->
            collector_loop(Acc ++ Events);
        {get, From, Ref} ->
            From ! {Ref, Acc},
            collector_loop(Acc)
    end.

%% @doc Event types delivered to the collector so far (duplicates kept).
collected() ->
    collected(?COLLECTOR).

collected(Name) ->
    Ref = make_ref(),
    Name ! {get, self(), Ref},
    receive
        {Ref, Events} -> [E#event.event_type || E <- Events]
    after 5000 ->
        []
    end.

%%====================================================================
%% Helpers (host side)
%%====================================================================

start_peer(Name) ->
    Dir = "/tmp/" ++ Name,
    os:cmd("rm -rf " ++ Dir),
    {ok, Peer, Node} = peer:start_link(#{
        name => list_to_atom(Name),
        connection => standard_io,
        wait_boot => 60000,
        args => ["-setcookie", ?COOKIE, "-pa" | code:get_path()]
    }),
    ok = peer:call(Peer, ?MODULE, boot_store, [?STORE, Dir], 120000),
    {Peer, Node, Dir}.

stop_peer(Peer) ->
    case is_process_alive(Peer) of
        true -> peer:stop(Peer);
        false -> ok
    end.

event(EventType) ->
    #{event_type => EventType, data => #{<<"key">> => <<"value">>}}.

received(Peer, EventType) ->
    lists:member(EventType, peer:call(Peer, ?MODULE, collected, [])).

received_all(Peer, Collector, EventTypes) ->
    Got = peer:call(Peer, ?MODULE, collected, [Collector]),
    lists:all(fun(T) -> lists:member(T, Got) end, EventTypes).

count(EventType, EventTypes) ->
    length([T || T <- EventTypes, T =:= EventType]).

two_members(Peer) ->
    member_count(Peer) =:= 2.

member_count(Peer) ->
    case peer:call(Peer, khepri_cluster, members, [?STORE]) of
        {ok, Members} -> length(Members);
        _ -> 0
    end.

peers(Config) ->
    {?config(peer_a, Config), ?config(node_a, Config),
     ?config(peer_b, Config), ?config(node_b, Config)}.

%% Joiner connects to TargetNode and joins its cluster; wait until every
%% listed peer sees Expected members.
join(JoinerPeer, TargetNode, AllPeers, Expected) ->
    true = peer:call(JoinerPeer, net_kernel, connect_node, [TargetNode]),
    ok = peer:call(JoinerPeer, reckon_db_store_coordinator, join_cluster, [?STORE, TargetNode], 60000),
    wait_until(fun() -> lists:all(fun(P) -> member_count(P) =:= Expected end, AllPeers) end, 30000).

leader_of(Peer) ->
    peer:call(Peer, ra_leaderboard, lookup_leader, [?STORE]).

%% {LeaderPeer, LeaderNode, FollowerPeer, FollowerNode} for a two-member
%% cluster, read from Ra rather than assumed.
roles(PeerA, NodeA, PeerB, NodeB) ->
    ok = wait_until(fun() -> leader_of(PeerA) =/= undefined andalso leader_of(PeerA) =:= leader_of(PeerB) end, 30000),
    case leader_of(PeerA) of
        {?STORE, NodeA} -> {PeerA, NodeA, PeerB, NodeB};
        {?STORE, NodeB} -> {PeerB, NodeB, PeerA, NodeA}
    end.

append_all(Peer, StreamId, EventTypes) ->
    lists:foreach(
        fun(T) ->
            {ok, _} = peer:call(Peer, reckon_db_streams, append, [?STORE, StreamId, -2, [event(T)]])
        end,
        EventTypes).

append_and_expect(AppendPeer, StreamId, EventType, CollectorPeer, Collector) ->
    ok = append_all(AppendPeer, StreamId, [EventType]),
    wait_until(fun() -> received_all(CollectorPeer, Collector, [EventType]) end, 15000).

wait_until(Pred, Timeout) ->
    case wait_until_or_false(Pred, Timeout) of
        true -> ok;
        false -> ct:fail("condition not met within timeout")
    end.

wait_until_or_false(Pred, Timeout) when Timeout > 0 ->
    case Pred() of
        true -> true;
        _ -> timer:sleep(200), wait_until_or_false(Pred, Timeout - 200)
    end;
wait_until_or_false(_Pred, _Timeout) ->
    false.
