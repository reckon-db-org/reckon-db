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
