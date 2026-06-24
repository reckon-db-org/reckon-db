%%% @doc Unit tests for reckon_db_dcb:extract_payload_entries/2.
%%%
%%% Regression: evoq builds domain events whose `data' is a map with
%%% ATOM keys and binary values (e.g. #{lot_id => <<"L1">>}), while payload
%%% index declarations use BINARY keys (<<"lot_id">>). Extraction must match
%%% the declared binary key against atom-keyed maps, binary-keyed maps, and
%%% JSON-binary data alike — otherwise CCC payload indexes silently never
%%% populate for event-sourced consumers.
%%% @end
-module(reckon_db_dcb_payload_extract_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../../include/reckon_db.hrl").

-define(SINGLE, [{payload, <<"lot_id">>}, {payload, <<"plate">>}]).
-define(COMBO,  [{payload_hash, [<<"lot_id">>, <<"plate">>]}]).

ev(Data) ->
    #event{event_type = <<"vehicle_entered_lot">>,
           stream_id = <<"_dcb">>, version = 0, data = Data}.

%% Atom-keyed map (the evoq shape) — the regression case.
atom_keyed_single_test() ->
    E = ev(#{session_id => <<"s1">>, lot_id => <<"L1">>, plate => <<"P1">>}),
    Entries = reckon_db_dcb:extract_payload_entries(E, ?SINGLE),
    ?assert(lists:member({single, <<"lot_id">>, <<"L1">>}, Entries)),
    ?assert(lists:member({single, <<"plate">>, <<"P1">>}, Entries)).

%% Binary-keyed map (JSON-decoded shape) — must still work.
binary_keyed_single_test() ->
    E = ev(#{<<"lot_id">> => <<"L1">>, <<"plate">> => <<"P1">>}),
    Entries = reckon_db_dcb:extract_payload_entries(E, ?SINGLE),
    ?assert(lists:member({single, <<"lot_id">>, <<"L1">>}, Entries)),
    ?assert(lists:member({single, <<"plate">>, <<"P1">>}, Entries)).

%% JSON binary data — decoded to binary keys.
json_binary_single_test() ->
    E = ev(iolist_to_binary(json:encode(#{<<"lot_id">> => <<"L1">>}))),
    Entries = reckon_db_dcb:extract_payload_entries(E, ?SINGLE),
    ?assertEqual([{single, <<"lot_id">>, <<"L1">>}], Entries).

%% Composite hash: atom-keyed and binary-keyed maps must hash identically.
atom_and_binary_hash_match_test() ->
    Atom = reckon_db_dcb:extract_payload_entries(
             ev(#{lot_id => <<"L1">>, plate => <<"P1">>}), ?COMBO),
    Bin  = reckon_db_dcb:extract_payload_entries(
             ev(#{<<"lot_id">> => <<"L1">>, <<"plate">> => <<"P1">>}), ?COMBO),
    ?assertMatch([{combo, _}], Atom),
    ?assertEqual(Atom, Bin).

%% Missing field produces no entry; non-binary value is not indexed.
absent_and_non_binary_test() ->
    E = ev(#{lot_id => <<"L1">>, plate => 42}),
    Entries = reckon_db_dcb:extract_payload_entries(E, ?SINGLE),
    ?assertEqual([{single, <<"lot_id">>, <<"L1">>}], Entries).
