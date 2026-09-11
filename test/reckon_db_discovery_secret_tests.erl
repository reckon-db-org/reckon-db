%% @doc Tests for multicast discovery with a required cluster secret.
%%
%% Discovery in cluster mode needs a cluster secret of at least 32 bytes,
%% from RECKON_DB_CLUSTER_SECRET or the cluster_secret application key, and
%% an empty RECKON_DB_CLUSTER_SECRET counts as unset. Without one it stays
%% passive, with no socket, and logs why. Its socket is bound to the
%% multicast group address. A datagram has a fixed layout whose last 32
%% bytes are an HMAC-SHA256 tag over all the bytes before them, and no term
%% is decoded from it. A node is dialled outside the discovery server, so a
%% host that does not answer leaves the server serving; that scenario runs
%% in a peer node of its own.
-module(reckon_db_discovery_secret_tests).

-include_lib("eunit/include/eunit.hrl").
-include("reckon_db.hrl").

%% Logger handler, capturing the events of a test.
-export([log/2]).
%% Scenario, run in a peer node.
-export([dial_that_blocks/0]).

-define(ENV, "RECKON_DB_CLUSTER_SECRET").
%% 32 bytes.
-define(SECRET, <<"0123456789abcdef0123456789abcdef">>).
-define(GROUP, {239, 255, 0, 1}).
-define(STORE, reckon_db_discovery_secret_tests_store).
-define(HANDLER, reckon_db_discovery_secret_tests_logs).
-define(ANSWER_MS, 200).
%% The peer's test epmd module takes longer than ANSWER_MS to resolve this
%% host.
-define(SLOW_NODE, 'far@slow.invalid').
-define(SCENARIO_TIMEOUT_MS, 60_000).

start_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [{"no secret leaves discovery passive and logs why", fun no_secret/0},
      {"an empty RECKON_DB_CLUSTER_SECRET counts as unset", fun empty_env_secret/0},
      {"an empty RECKON_DB_CLUSTER_SECRET gives way to the cluster_secret key",
       fun empty_env_secret_gives_way/0},
      {"a 31-byte RECKON_DB_CLUSTER_SECRET leaves discovery passive and logs why",
       fun short_env_secret/0},
      {"a 31-byte cluster_secret key leaves discovery passive and logs why",
       fun short_app_secret/0},
      {"a 31-byte RECKON_DB_CLUSTER_SECRET does not give way to the cluster_secret key",
       fun short_env_secret_does_not_give_way/0},
      {"a 32-byte RECKON_DB_CLUSTER_SECRET starts discovery", fun env_secret_starts/0},
      {"the socket is bound to the multicast group address", fun bound_to_the_group/0}]}.

datagram_test_() ->
    [{"a datagram is the prefix, version 3, timestamp, name length, name and a tag over them",
      fun datagram_layout/0},
     {"a datagram built to that layout with a verifying tag decodes to its node name",
      fun built_datagram_decodes/0},
     {"a term-encoded datagram is refused, with a tag that verified before",
      fun term_datagram_refused/0},
     {"a verified datagram whose name length does not match its name is refused",
      fun length_mismatch_refused/0},
     {"no datagram of random bytes decodes or raises", fun random_datagrams_refused/0}].

dial_test_() ->
    {"a dial to a host that does not answer leaves the discovery server serving",
     {timeout, 90, fun dial_does_not_block_the_server/0}}.

%%====================================================================
%% Start
%%====================================================================

no_secret() ->
    Pid = start_discovery(),
    ?assertEqual({passive, [secret_required]}, {mode(Pid), refusals()}).

empty_env_secret() ->
    os:putenv(?ENV, ""),
    Pid = start_discovery(),
    ?assertEqual({passive, [secret_required]}, {mode(Pid), refusals()}).

empty_env_secret_gives_way() ->
    os:putenv(?ENV, ""),
    application:set_env(reckon_db, cluster_secret, ?SECRET),
    log_everything(),
    Pid = start_discovery(),
    Node = peer_node(),
    deliver(Pid, reckon_db_discovery:encode_gossip_message(Node, ?SECRET)),
    ?assertEqual({active, discovered}, {mode(Pid), discovered(Node)}).

short_env_secret() ->
    os:putenv(?ENV, binary_to_list(binary:part(?SECRET, 0, 31))),
    Pid = start_discovery(),
    ?assertEqual({passive, [{secret_too_short, #{bytes => 31, required => 32}}]},
                 {mode(Pid), refusals()}).

short_app_secret() ->
    application:set_env(reckon_db, cluster_secret, binary:part(?SECRET, 0, 31)),
    Pid = start_discovery(),
    ?assertEqual({passive, [{secret_too_short, #{bytes => 31, required => 32}}]},
                 {mode(Pid), refusals()}).

short_env_secret_does_not_give_way() ->
    os:putenv(?ENV, binary_to_list(binary:part(?SECRET, 0, 31))),
    application:set_env(reckon_db, cluster_secret, ?SECRET),
    Pid = start_discovery(),
    ?assertEqual({passive, [{secret_too_short, #{bytes => 31, required => 32}}]},
                 {mode(Pid), refusals()}).

env_secret_starts() ->
    os:putenv(?ENV, binary_to_list(?SECRET)),
    Pid = start_discovery(),
    ?assertEqual({active, []}, {mode(Pid), refusals()}).

bound_to_the_group() ->
    application:set_env(reckon_db, cluster_secret, ?SECRET),
    Pid = start_discovery(),
    {ok, Port} = application:get_env(reckon_db, discovery_port),
    ?assertEqual({ok, {?GROUP, Port}}, inet:sockname(socket_of(Pid))).

%%====================================================================
%% Datagram
%%====================================================================

datagram_layout() ->
    Before = now_ms(),
    Datagram = reckon_db_discovery:encode_gossip_message('peer@host1', ?SECRET),
    After = now_ms(),
    ?assertMatch(<<"RDBG", 3, _:64, 10, "peer@host1", _:32/binary>>, Datagram),
    <<Body:(byte_size(Datagram) - 32)/binary, Tag:32/binary>> = Datagram,
    <<"RDBG", 3, Timestamp:64/signed-big, _/binary>> = Body,
    ?assertEqual({true, true},
                 {Timestamp >= Before andalso Timestamp =< After, Tag =:= tag(Body, ?SECRET)}).

built_datagram_decodes() ->
    ?assertEqual({ok, <<"peer@host1">>},
                 reckon_db_discovery:decode_gossip(datagram(<<"peer@host1">>, now_ms()), ?SECRET)).

term_datagram_refused() ->
    Name = <<"peer@host1">>,
    Timestamp = now_ms(),
    Mac = crypto:mac(hmac, sha256, ?SECRET, <<Timestamp:64/signed-big, Name/binary>>),
    Datagram = term_to_binary({gossip_v2, Name, Timestamp, Mac}),
    ?assertEqual(reject, reckon_db_discovery:decode_gossip(Datagram, ?SECRET)).

length_mismatch_refused() ->
    Body = <<"RDBG", 3, (now_ms()):64/signed-big, 12, "peer@host1">>,
    Datagram = <<Body/binary, (tag(Body, ?SECRET))/binary>>,
    ?assertEqual(reject, reckon_db_discovery:decode_gossip(Datagram, ?SECRET)).

random_datagrams_refused() ->
    Results = [reckon_db_discovery:decode_gossip(random_datagram(N), ?SECRET)
               || N <- lists:seq(1, 5_000)],
    ?assertEqual([], [R || R <- Results, R =/= reject]).

%% Random bytes, or random bytes after the prefix and version.
random_datagram(N) when N rem 2 =:= 0 ->
    crypto:strong_rand_bytes(rand:uniform(120) - 1);
random_datagram(_N) ->
    <<"RDBG", 3, (crypto:strong_rand_bytes(rand:uniform(120) - 1))/binary>>.

%%====================================================================
%% Dial
%%====================================================================

dial_does_not_block_the_server() ->
    ?assertEqual({ok, answered}, in_peer(dial_that_blocks, [])).

%% In a peer node whose epmd module takes longer than ANSWER_MS to resolve
%% the host of SLOW_NODE: discovery accepts a datagram from that node and
%% dials it, and the discovery server still answers within ANSWER_MS.
dial_that_blocks() ->
    os:unsetenv(?ENV),
    application:set_env(reckon_db, cluster_secret, ?SECRET),
    application:set_env(reckon_db, discovery_port, free_udp_port()),
    application:set_env(reckon_db, broadcast_interval, 60_000),
    Pid = start_discovery(),
    Pid ! {udp, socket_of(Pid), {127, 0, 0, 1}, 45_000,
           reckon_db_discovery:encode_gossip_message(?SLOW_NODE, ?SECRET)},
    answered(answer_within(fun() -> reckon_db_discovery:get_discovered_nodes(?STORE) end,
                           ?ANSWER_MS)).

answered({answered, _Value}) -> answered;
answered(no_answer) -> no_answer.

answer_within(Fun, Ms) ->
    Asker = self(),
    Pid = spawn(fun() -> Asker ! {answer, self(), Fun()} end),
    receive
        {answer, Pid, Value} -> {answered, Value}
    after Ms ->
        exit(Pid, kill),
        no_answer
    end.

%%====================================================================
%% Helpers
%%====================================================================

setup() ->
    os:unsetenv(?ENV),
    application:unset_env(reckon_db, cluster_secret),
    application:set_env(reckon_db, discovery_port, free_udp_port()),
    application:set_env(reckon_db, broadcast_interval, 60_000),
    maps:get(level, logger:get_primary_config()).

cleanup(PrimaryLevel) ->
    _ = (catch gen_server:stop(reckon_db_naming:discovery_name(?STORE))),
    _ = logger:remove_handler(?HANDLER),
    ok = logger:set_primary_config(level, PrimaryLevel),
    os:unsetenv(?ENV),
    application:unset_env(reckon_db, cluster_secret),
    application:unset_env(reckon_db, discovery_port),
    application:unset_env(reckon_db, broadcast_interval),
    _ = logged(),
    ok.

%% Starts discovery, and from then on sends this process every logged event.
%% A test runs in a process of its own, not in the one that ran setup, so
%% the handler is added here.
start_discovery() ->
    ok = logger:add_handler(?HANDLER, ?MODULE, #{level => all, config => #{pid => self()}}),
    {ok, Pid} = reckon_db_discovery:start_link(#store_config{store_id = ?STORE, mode = cluster}),
    Pid.

%% Info events reach the handler only when the primary level lets them.
log_everything() ->
    ok = logger:set_primary_config(level, all).

log(#{level := Level, msg := Msg}, #{config := #{pid := Pid}}) ->
    Pid ! {logged, Level, Msg},
    ok.

logged() ->
    receive
        {logged, _Level, _Msg} = Event -> [Event | logged()]
    after 0 ->
        []
    end.

%% The reasons discovery gave for staying passive.
refusals() ->
    [Reason || {logged, error, {report, #{what := discovery_disabled, reason := Reason}}}
                   <- logged()].

%% Whether any event logged so far names Node.
discovered(Node) ->
    Name = atom_to_list(Node),
    named(lists:any(fun({logged, _Level, Msg}) -> string:find(text(Msg), Name) =/= nomatch end,
                    logged())).

named(true) -> discovered;
named(false) -> not_discovered.

text({string, String}) -> unicode:characters_to_list(String);
text({report, Report}) -> lists:flatten(io_lib:format("~p", [Report]));
text({Format, Args}) -> lists:flatten(io_lib:format(Format, Args)).

%% Hands the server a datagram as its socket would, then asks it for the
%% discovered nodes, which it answers once it has handled the datagram.
deliver(Pid, Datagram) ->
    Pid ! {udp, socket_of(Pid), {127, 0, 0, 1}, 45_000, Datagram},
    _ = reckon_db_discovery:get_discovered_nodes(?STORE),
    ok.

mode(Pid) ->
    mode_of(sockets_of(Pid)).

mode_of([]) -> passive;
mode_of([_Socket]) -> active.

socket_of(Pid) ->
    [Socket] = sockets_of(Pid),
    Socket.

sockets_of(Pid) ->
    [Port || Port <- erlang:ports(), port_is(Port, Pid, "udp_inet")].

port_is(Port, Pid, Name) ->
    {erlang:port_info(Port, connected), erlang:port_info(Port, name)} =:=
        {{connected, Pid}, {name, Name}}.

datagram(Name, Timestamp) ->
    Body = <<"RDBG", 3, Timestamp:64/signed-big, (byte_size(Name)), Name/binary>>,
    <<Body/binary, (tag(Body, ?SECRET))/binary>>.

tag(Body, Secret) ->
    crypto:mac(hmac, sha256, Secret, Body).

peer_node() ->
    list_to_atom("peer_" ++ integer_to_list(erlang:unique_integer([positive])) ++ "@127.0.0.1").

now_ms() ->
    erlang:system_time(millisecond).

free_udp_port() ->
    {ok, Sock} = gen_udp:open(0, [binary]),
    {ok, Port} = inet:port(Sock),
    ok = gen_udp:close(Sock),
    Port.

%%====================================================================
%% Peer node
%%====================================================================

in_peer(Scenario, Args) ->
    Name = "discovery_peer_" ++ integer_to_list(erlang:unique_integer([positive])),
    Started = peer:start_link(#{name => Name,
                                host => "127.0.0.1",
                                longnames => true,
                                connection => standard_io,
                                args => ["-epmd_module", "reckon_db_discovery_slow_epmd",
                                         "-start_epmd", "false",
                                         "-pa" | code:get_path()]}),
    Peer = element(2, Started),
    OsPid = peer:call(Peer, os, getpid, [], 5_000),
    try peer:call(Peer, ?MODULE, Scenario, Args, ?SCENARIO_TIMEOUT_MS) of
        Result -> {ok, Result}
    catch
        Class:Reason -> {error, {Class, Reason}}
    after
        _ = os:cmd("kill -9 " ++ OsPid),
        try peer:stop(Peer) catch _:_ -> ok end
    end.
