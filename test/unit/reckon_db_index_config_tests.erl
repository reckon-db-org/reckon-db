-module(reckon_db_index_config_tests).

-include_lib("eunit/include/eunit.hrl").
-include("reckon_db.hrl").

cfg(StoreId, Indexes) ->
    #store_config{store_id = StoreId, data_dir = "/tmp/x", indexes = Indexes}.

load_declared_clear_test() ->
    Store = idx_cfg_t1,
    ok = reckon_db_index_config:load(
        cfg(Store, [tags, event_type, {meta, <<"causation_id">>}])),
    ?assertEqual([event_type, tags, {meta, <<"causation_id">>}],
                 reckon_db_index_config:declared(Store)),
    ?assert(reckon_db_index_config:is_indexed(Store, tags)),
    ?assert(reckon_db_index_config:is_indexed(Store, event_type)),
    ?assert(reckon_db_index_config:is_indexed(Store, {meta, <<"causation_id">>})),
    ?assertNot(reckon_db_index_config:is_indexed(Store, {meta, <<"other">>})),
    ok = reckon_db_index_config:clear(Store),
    ?assertEqual([], reckon_db_index_config:declared(Store)).

default_empty_test() ->
    Store = idx_cfg_t2,
    ok = reckon_db_index_config:load(cfg(Store, [])),
    ?assertEqual([], reckon_db_index_config:declared(Store)),
    ?assertNot(reckon_db_index_config:is_indexed(Store, tags)),
    ok = reckon_db_index_config:clear(Store).

unknown_store_is_empty_test() ->
    ?assertEqual([], reckon_db_index_config:declared(never_loaded_store)).

normalizes_and_dedups_test() ->
    Store = idx_cfg_t3,
    %% Bogus declarations dropped; duplicates collapsed.
    ok = reckon_db_index_config:load(
        cfg(Store, [tags, tags, bogus, {meta, not_a_binary}, event_type])),
    ?assertEqual([event_type, tags], reckon_db_index_config:declared(Store)),
    ok = reckon_db_index_config:clear(Store).

payload_index_valid_test() ->
    Store = idx_cfg_t4,
    ok = reckon_db_index_config:load(cfg(Store, [
        tags,
        {payload, <<"account_id">>},
        {payload_hash, [<<"flight_id">>, <<"seat_no">>]}
    ])),
    Declared = reckon_db_index_config:declared(Store),
    ?assert(lists:member({payload, <<"account_id">>}, Declared)),
    ?assert(lists:member({payload_hash, [<<"flight_id">>, <<"seat_no">>]}, Declared)),
    ok = reckon_db_index_config:clear(Store).

declared_dcb_payload_returns_payload_decls_only_test() ->
    Store = idx_cfg_t5,
    ok = reckon_db_index_config:load(cfg(Store, [
        tags,
        event_type,
        {meta, <<"cid">>},
        {payload, <<"user_id">>},
        {payload_hash, [<<"a">>, <<"b">>]}
    ])),
    PayloadDecls = reckon_db_index_config:declared_dcb_payload(Store),
    ?assertEqual(2, length(PayloadDecls)),
    ?assert(lists:member({payload, <<"user_id">>}, PayloadDecls)),
    ?assert(lists:member({payload_hash, [<<"a">>, <<"b">>]}, PayloadDecls)),
    ok = reckon_db_index_config:clear(Store).

declared_dcb_payload_empty_when_none_declared_test() ->
    Store = idx_cfg_t6,
    ok = reckon_db_index_config:load(cfg(Store, [tags, event_type])),
    ?assertEqual([], reckon_db_index_config:declared_dcb_payload(Store)),
    ok = reckon_db_index_config:clear(Store).

invalid_payload_decls_are_dropped_test() ->
    Store = idx_cfg_t7,
    ok = reckon_db_index_config:load(cfg(Store, [
        {payload, not_a_binary},
        {payload_hash, []},
        {payload_hash, [not_a_binary]},
        {payload, <<"ok">>}
    ])),
    ?assertEqual([{payload, <<"ok">>}],
                 reckon_db_index_config:declared(Store)),
    ok = reckon_db_index_config:clear(Store).
