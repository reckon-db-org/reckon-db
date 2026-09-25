%% @doc The DCB re-index (reckon-db #2) on a clustered store.
%%
%% Every member of a cluster opens the store, so the one-time re-index of
%% DCB events written before their [idx] entries existed must run through
%% the Ra leader only: a member that is not the leader answers not_leader
%% and writes nothing, and leader activation runs it. Two real peer nodes
%% are started with OTP peer over standard_io, as in
%% reckon_db_cluster_join_subscriptions_SUITE.
-module(reckon_db_dcb_reindex_cluster_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

-export([all/0, suite/0, init_per_testcase/2, end_per_testcase/2]).
-export([only_the_leader_reindexes/1,
         leader_activation_reindexes/1,
         leader_answers_while_a_large_reindex_runs/1]).
%% Run ON the peer nodes via peer:call/4,5
-export([boot_store/2, append_dcb/1, declare/1, slow_reindex/1]).

-define(STORE, dcb_reindex_store).
-define(COOKIE, "reckon_db_dcb_reindex").
-define(EVENTS, 5).

suite() ->
    [{timetrap, {minutes, 3}}].

all() ->
    [only_the_leader_reindexes, leader_activation_reindexes,
     leader_answers_while_a_large_reindex_runs].

init_per_testcase(_TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    {PeerA, NodeA, DirA} = start_peer("rdb_reidx_a_" ++ Rand),
    {PeerB, _NodeB, DirB} = start_peer("rdb_reidx_b_" ++ Rand),
    ok = join(PeerB, NodeA, [PeerA, PeerB], 2),
    {Leader, Follower} = roles(PeerA, PeerB),
    ok = wait_until(fun() -> peer:call(Leader, reckon_db_leader, is_active, [?STORE]) end, 30000),
    %% Written with no index declared: the shape of a store written
    %% before the fix, DCB events without [idx] entries.
    [ok = peer:call(Leader, ?MODULE, append_dcb, [N]) || N <- lists:seq(1, ?EVENTS)],
    %% The upgrade: every member now declares the tags index.
    [ok = peer:call(P, ?MODULE, declare, [[tags]]) || P <- [PeerA, PeerB]],
    [{leader, Leader}, {follower, Follower},
     {peers, [PeerA, PeerB]}, {dirs, [DirA, DirB]} | Config].

end_per_testcase(_TestCase, Config) ->
    [stop_peer(P) || P <- ?config(peers, Config)],
    [os:cmd("rm -rf " ++ D) || D <- ?config(dirs, Config)],
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc GIVEN a two-member cluster whose DCB events have no [idx] entries
%%      WHEN both members run the re-index at the same time
%%      THEN the follower writes nothing (not_leader), the leader re-indexes
%%      every event once, and both members read them through the index
only_the_leader_reindexes(Config) ->
    Leader = ?config(leader, Config),
    Follower = ?config(follower, Config),
    Self = self(),
    [spawn(fun() -> Self ! {P, peer:call(P, reckon_db_dcb_reindex, run, [?STORE], 60000)} end)
     || P <- [Leader, Follower]],
    Results = maps:from_list([receive {P, R} -> {P, R} after 60000 -> {P, timeout} end
                              || P <- [Leader, Follower]]),
    ?assertEqual({ok, not_leader}, maps:get(Follower, Results)),
    ?assertMatch({ok, #{reindexed := ?EVENTS, kinds := [tags]}}, maps:get(Leader, Results)),
    ok = wait_until(fun() -> tagged(Leader) =:= ?EVENTS andalso tagged(Follower) =:= ?EVENTS end,
                    20000),
    ?assertMatch({ok, #{reindexed := 0}}, peer:call(Leader, reckon_db_dcb_reindex, run, [?STORE])).

%% @doc GIVEN the same cluster
%%      WHEN the leader's leader worker activates (as the node monitor does
%%      on the Ra leader after every start)
%%      THEN the DCB events are re-indexed without anyone calling run/1
leader_activation_reindexes(Config) ->
    Leader = ?config(leader, Config),
    Follower = ?config(follower, Config),
    ok = peer:call(Leader, reckon_db_leader, activate, [?STORE]),
    ok = wait_until(fun() -> tagged(Leader) =:= ?EVENTS andalso tagged(Follower) =:= ?EVENTS end,
                    30000),
    ?assertMatch({ok, #{reindexed := 0}}, peer:call(Leader, reckon_db_dcb_reindex, run, [?STORE])).

%% @doc GIVEN the same cluster, with the re-index made slow (3 s) on the
%%      leader (its leader check sleeps)
%%      WHEN the leader activates
%%      THEN its leader worker keeps answering is_active/1 promptly while
%%      the re-index runs: the node monitor calls it every tick with a 5 s
%%      timeout, and a worker blocked in a long run made that call time out
%%      and the monitor crash, taking the gateway pool with it
leader_answers_while_a_large_reindex_runs(Config) ->
    Leader = ?config(leader, Config),
    ok = peer:call(Leader, ?MODULE, slow_reindex, [3000]),
    ok = peer:call(Leader, reckon_db_leader, activate, [?STORE]),
    Probes = [begin
                  timer:sleep(300),
                  {Us, true} = timer:tc(fun() -> peer:call(Leader, reckon_db_leader,
                                                           is_active, [?STORE], 30000) end),
                  Us
              end || _ <- lists:seq(1, 5)],
    ct:pal("is_active probes while re-indexing (us): ~p", [Probes]),
    %% The run was still going when the probes ran: nothing tagged yet.
    ?assertEqual(0, tagged(Leader)),
    lists:foreach(fun(Us) -> ?assert(Us < 1000000) end, Probes),
    ok = wait_until(fun() -> tagged(Leader) =:= ?EVENTS end, 30000).

%%====================================================================
%% Peer-side helpers
%%====================================================================

boot_store(StoreId, Dir) ->
    RaDir = filename:join(Dir, "ra"),
    ok = filelib:ensure_dir(filename:join(RaDir, "dummy")),
    application:set_env(ra, data_dir, RaDir),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    {ok, _} = application:ensure_all_started(reckon_db),
    {ok, _} = reckon_db_sup:start_store(store_config(StoreId, Dir, [])),
    ok.

store_config(StoreId, Dir, Indexes) ->
    #store_config{store_id = StoreId, data_dir = filename:join(Dir, "store"),
                  mode = cluster, writer_pool_size = 1, reader_pool_size = 1,
                  gateway_pool_size = 1, indexes = Indexes}.

append_dcb(N) ->
    {ok, _} = reckon_db_dcb:append_if_no_tag_matches(
                ?STORE, {any_of, [<<"never-matches">>]}, -1,
                [#{event_type => <<"reidx_v1">>, data => #{n => N}, tags => [<<"reidx">>]}]),
    ok.

%% Make the re-index slow: the leader check its run/1 starts with sleeps
%% first. (Not run/1 itself: Horus cannot extract the transaction funs of a
%% meck-renamed module.) no_link: the mock must outlive the peer:call
%% process that creates it.
slow_reindex(Ms) ->
    ok = meck:new(reckon_db_store_coordinator, [passthrough, no_link]),
    ok = meck:expect(reckon_db_store_coordinator, is_leader,
                     fun(StoreId) -> timer:sleep(Ms), meck:passthrough([StoreId]) end).

declare(Indexes) ->
    reckon_db_index_config:load(#store_config{store_id = ?STORE, indexes = Indexes}).

%%====================================================================
%% Host-side helpers
%%====================================================================

tagged(Peer) ->
    tagged(Peer, 100).

tagged(Peer, Limit) ->
    case peer:call(Peer, reckon_db_streams, read_by_tags, [?STORE, [<<"reidx">>], any, Limit],
                   60000) of
        {ok, Events} -> length(Events);
        _ -> -1
    end.

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

join(JoinerPeer, TargetNode, AllPeers, Expected) ->
    true = peer:call(JoinerPeer, net_kernel, connect_node, [TargetNode]),
    ok = peer:call(JoinerPeer, reckon_db_store_coordinator, join_cluster, [?STORE, TargetNode], 60000),
    wait_until(fun() -> lists:all(fun(P) -> member_count(P) =:= Expected end, AllPeers) end, 30000).

member_count(Peer) ->
    case peer:call(Peer, khepri_cluster, members, [?STORE]) of
        {ok, Members} -> length(Members);
        _ -> 0
    end.

roles(PeerA, PeerB) ->
    ok = wait_until(fun() -> leader_of(PeerA) =/= undefined andalso
                             leader_of(PeerA) =:= leader_of(PeerB) end, 30000),
    {?STORE, LeaderNode} = leader_of(PeerA),
    case peer:call(PeerA, erlang, node, []) of
        LeaderNode -> {PeerA, PeerB};
        _ -> {PeerB, PeerA}
    end.

leader_of(Peer) ->
    peer:call(Peer, ra_leaderboard, lookup_leader, [?STORE]).

wait_until(Fun, Timeout) when Timeout =< 0 ->
    case Fun() of true -> ok; _ -> {error, timeout} end;
wait_until(Fun, Timeout) ->
    case Fun() of
        true -> ok;
        _ -> timer:sleep(200), wait_until(Fun, Timeout - 200)
    end.
