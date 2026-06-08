-module(reckon_db_stream_path_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("khepri/include/khepri.hrl").

-define(HEX, <<"018f6a7b8c9d4e5f60718293a4b5c6d7">>).
-define(USER, <<"ride-018f6a7b8c9d4e5f60718293a4b5c6d7">>).
-define(SYS,  <<"$link:high-value-orders">>).

%%====================================================================
%% event_path/2
%%====================================================================

event_path_user_test() ->
    ?assertEqual([streams, <<"ride">>, ?HEX, <<"000000000001">>],
        reckon_db_stream_path:event_path(?USER, <<"000000000001">>)).

event_path_system_test() ->
    %% System type node keeps the leading $ ; name is the id node.
    ?assertEqual([streams, <<"$link">>, <<"high-value-orders">>, <<"000000000000">>],
        reckon_db_stream_path:event_path(?SYS, <<"000000000000">>)).

event_path_rejects_malformed_test() ->
    ?assertError({invalid_stream_id, <<"_dcb">>},
        reckon_db_stream_path:event_path(<<"_dcb">>, <<"0">>)),
    ?assertError({invalid_stream_id, <<"nope">>},
        reckon_db_stream_path:event_path(<<"nope">>, <<"0">>)).

%%====================================================================
%% stream_path / versions_pattern / type_streams_pattern
%%====================================================================

stream_path_test() ->
    ?assertEqual([streams, <<"ride">>, ?HEX],
        reckon_db_stream_path:stream_path(?USER)),
    ?assertEqual([streams, <<"$link">>, <<"high-value-orders">>],
        reckon_db_stream_path:stream_path(?SYS)).

versions_pattern_test() ->
    ?assertEqual([streams, <<"ride">>, ?HEX, ?KHEPRI_WILDCARD_STAR],
        reckon_db_stream_path:versions_pattern(?USER)).

type_streams_pattern_test() ->
    ?assertEqual([streams, <<"order">>, ?KHEPRI_WILDCARD_STAR],
        reckon_db_stream_path:type_streams_pattern(<<"order">>)).

type_of_test() ->
    ?assertEqual(<<"ride">>, reckon_db_stream_path:type_of(?USER)),
    ?assertEqual(<<"$link">>, reckon_db_stream_path:type_of(?SYS)).

%%====================================================================
%% stream_id_from_path/1 + round-trip (THE correctness property)
%%====================================================================

stream_id_from_path_user_test() ->
    %% From a 4-element event leaf
    ?assertEqual(?USER, reckon_db_stream_path:stream_id_from_path(
        [streams, <<"ride">>, ?HEX, <<"000000000003">>])),
    %% From a 3-element aggregate node (list_streams shape)
    ?assertEqual(?USER, reckon_db_stream_path:stream_id_from_path(
        [streams, <<"ride">>, ?HEX])).

stream_id_from_path_system_test() ->
    ?assertEqual(?SYS, reckon_db_stream_path:stream_id_from_path(
        [streams, <<"$link">>, <<"high-value-orders">>, <<"000000000000">>])).

stream_id_from_path_dcb_test() ->
    %% The 2-level DCB log reconstructs to the reserved id.
    ?assertEqual(<<"_dcb">>, reckon_db_stream_path:stream_id_from_path(
        [streams, <<"_dcb">>, <<"00000000000000000007">>])).

roundtrip_user_test() ->
    Id = reckon_gater_stream_id:new(<<"vehicle">>),
    Path = reckon_db_stream_path:event_path(Id, <<"000000000042">>),
    ?assertEqual(Id, reckon_db_stream_path:stream_id_from_path(Path)).

roundtrip_system_test() ->
    Ids = [<<"$link:hot-orders">>, <<"$link-sub:revenue">>,
           <<"$et:UserCreated">>, <<"$stats:host_01.example">>],
    [?assertEqual(Id, reckon_db_stream_path:stream_id_from_path(
        reckon_db_stream_path:event_path(Id, <<"000000000000">>)))
     || Id <- Ids].

roundtrip_property_test() ->
    %% For many generated user ids across varied prefixes, the path
    %% round-trips byte-for-byte.
    Prefixes = [<<"a">>, <<"order">>, <<"ride">>, <<"session">>,
                <<"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa">>],
    Ids = [reckon_gater_stream_id:new(P) || P <- Prefixes,
                                            _ <- lists:seq(1, 20)],
    [?assertEqual(Id, reckon_db_stream_path:stream_id_from_path(
        reckon_db_stream_path:event_path(Id, <<"000000000001">>)))
     || Id <- Ids].
