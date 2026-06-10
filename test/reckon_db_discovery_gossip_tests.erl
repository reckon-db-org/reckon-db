%% @doc Gossip codec security tests for reckon_db_discovery.
%%
%% Covers the 2026-06-10 audit fix: untrusted UDP datagrams must be
%% safe-decoded and HMAC-authenticated before anything is trusted,
%% the cluster secret must never ride the wire, and the legacy v1
%% format (raw secret in cleartext, default fallback secret) must be
%% dead.
-module(reckon_db_discovery_gossip_tests).

-include_lib("eunit/include/eunit.hrl").

-define(SECRET, <<"test_cluster_secret">>).

valid_roundtrip_test() ->
    Msg = reckon_db_discovery:encode_gossip_message('peer@host1', ?SECRET),
    ?assertEqual({ok, <<"peer@host1">>},
                 reckon_db_discovery:decode_gossip(Msg, ?SECRET)).

secret_not_on_wire_test() ->
    Msg = reckon_db_discovery:encode_gossip_message('peer@host1', ?SECRET),
    ?assertEqual(nomatch, binary:match(Msg, ?SECRET)).

wrong_secret_rejected_test() ->
    Msg = reckon_db_discovery:encode_gossip_message('peer@host1', ?SECRET),
    ?assertEqual(reject,
                 reckon_db_discovery:decode_gossip(Msg, <<"other_secret">>)).

tampered_node_name_rejected_test() ->
    NodeBin = <<"peer@host1">>,
    Ts = erlang:system_time(millisecond),
    Mac = reckon_db_discovery:gossip_mac(NodeBin, Ts, ?SECRET),
    Forged = term_to_binary({gossip_v2, <<"evil@host9">>, Ts, Mac}),
    ?assertEqual(reject, reckon_db_discovery:decode_gossip(Forged, ?SECRET)).

stale_timestamp_rejected_test() ->
    NodeBin = <<"peer@host1">>,
    Ts = erlang:system_time(millisecond) - 600_000,
    Mac = reckon_db_discovery:gossip_mac(NodeBin, Ts, ?SECRET),
    Stale = term_to_binary({gossip_v2, NodeBin, Ts, Mac}),
    ?assertEqual(reject, reckon_db_discovery:decode_gossip(Stale, ?SECRET)).

legacy_v1_format_rejected_test() ->
    %% Pre-5.1.0 wire format, including the old hardcoded default
    %% secret. Must be dead.
    V1 = term_to_binary({gossip, 'peer@host1', <<"reckon_db_default_secret">>,
                         erlang:system_time(millisecond)}),
    ?assertEqual(reject,
                 reckon_db_discovery:decode_gossip(V1, <<"reckon_db_default_secret">>)).

garbage_rejected_test() ->
    ?assertEqual(reject, reckon_db_discovery:decode_gossip(<<"not etf">>, ?SECRET)),
    ?assertEqual(reject,
                 reckon_db_discovery:decode_gossip(term_to_binary(#{a => 1}), ?SECRET)).

unknown_atom_payload_rejected_test() ->
    %% Hand-built ETF for a tuple containing an atom that does not
    %% exist on this node: {gossip_v2, 'zz_no_such_atom_zz_xq', 1, <<>>}.
    %% [safe] must refuse to materialize the atom (the pre-fix decode
    %% created it, allowing remote atom-table exhaustion).
    AtomName = <<"zz_no_such_atom_zz_xq">>,
    Payload = <<131, 104, 4,
                119, 9, "gossip_v2",
                119, (byte_size(AtomName)), AtomName/binary,
                97, 1,
                109, 0:32>>,
    ?assertEqual(reject, reckon_db_discovery:decode_gossip(Payload, ?SECRET)).

oversized_node_name_rejected_test() ->
    NodeBin = binary:copy(<<"a">>, 300),
    Ts = erlang:system_time(millisecond),
    Mac = reckon_db_discovery:gossip_mac(NodeBin, Ts, ?SECRET),
    Msg = term_to_binary({gossip_v2, NodeBin, Ts, Mac}),
    ?assertEqual(reject, reckon_db_discovery:decode_gossip(Msg, ?SECRET)).
