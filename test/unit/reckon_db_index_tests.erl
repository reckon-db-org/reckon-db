-module(reckon_db_index_tests).

-include_lib("eunit/include/eunit.hrl").
-include("reckon_db.hrl").

ev(StreamId, Version, EpochUs, EventType, Tags, Meta) ->
    #event{
        event_id = <<"id">>, event_type = EventType, stream_id = StreamId,
        version = Version, data = #{}, metadata = Meta, tags = Tags,
        timestamp = 0, epoch_us = EpochUs}.

%%====================================================================
%% order_key / event_ref
%%====================================================================

order_key_shape_test() ->
    E = ev(<<"ride-abc">>, 5, 1700000000000000, <<"e">>, [], #{}),
    %% pad(epoch,20) | stream | pad(version,12)
    ?assertEqual(
        <<"00001700000000000000|ride-abc|000000000005">>,
        reckon_db_index:order_key(E)).

order_key_sorts_by_epoch_then_stream_then_version_test() ->
    A = reckon_db_index:order_key(ev(<<"s-1">>, 0, 100, <<"e">>, [], #{})),
    B = reckon_db_index:order_key(ev(<<"s-1">>, 1, 100, <<"e">>, [], #{})),
    C = reckon_db_index:order_key(ev(<<"s-1">>, 0, 200, <<"e">>, [], #{})),
    ?assert(A < B),   %% same epoch+stream, later version
    ?assert(B < C).   %% later epoch dominates

event_ref_test() ->
    E = ev(<<"order-xyz">>, 7, 1, <<"e">>, [], #{}),
    ?assertEqual(#{stream_id => <<"order-xyz">>, version => 7},
                 reckon_db_index:event_ref(E)).

%%====================================================================
%% entries/2
%%====================================================================

entries_empty_when_no_declared_indexes_test() ->
    E = ev(<<"s-1">>, 0, 1, <<"placed">>, [<<"t">>], #{<<"k">> => <<"v">>}),
    ?assertEqual([], reckon_db_index:entries(E, [])).

entries_tags_one_per_tag_test() ->
    E = ev(<<"s-1">>, 0, 1, <<"e">>, [<<"a">>, <<"b">>], #{}),
    Entries = reckon_db_index:entries(E, [tags]),
    OK = reckon_db_index:order_key(E),
    Ref = reckon_db_index:event_ref(E),
    ?assertEqual(
        [{[idx, tag, <<"a">>, OK], Ref},
         {[idx, tag, <<"b">>, OK], Ref}],
        Entries).

entries_tags_skips_non_binary_and_undefined_test() ->
    ?assertEqual([], reckon_db_index:entries(
        ev(<<"s-1">>, 0, 1, <<"e">>, undefined, #{}), [tags])),
    ?assertEqual([], reckon_db_index:entries(
        ev(<<"s-1">>, 0, 1, <<"e">>, [foo, 42], #{}), [tags])).

entries_event_type_test() ->
    E = ev(<<"s-1">>, 0, 1, <<"order_placed_v1">>, [], #{}),
    OK = reckon_db_index:order_key(E),
    Ref = reckon_db_index:event_ref(E),
    ?assertEqual([{[idx, event_type, <<"order_placed_v1">>, OK], Ref}],
                 reckon_db_index:entries(E, [event_type])).

entries_meta_present_binary_value_test() ->
    E = ev(<<"s-1">>, 0, 1, <<"e">>, [],
           #{<<"causation_id">> => <<"evt-7">>, <<"other">> => <<"x">>}),
    OK = reckon_db_index:order_key(E),
    Ref = reckon_db_index:event_ref(E),
    ?assertEqual([{[idx, meta, <<"causation_id">>, <<"evt-7">>, OK], Ref}],
                 reckon_db_index:entries(E, [{meta, <<"causation_id">>}])).

entries_meta_absent_or_non_binary_skipped_test() ->
    %% key absent
    ?assertEqual([], reckon_db_index:entries(
        ev(<<"s-1">>, 0, 1, <<"e">>, [], #{<<"x">> => <<"y">>}),
        [{meta, <<"causation_id">>}])),
    %% key present but non-binary value
    ?assertEqual([], reckon_db_index:entries(
        ev(<<"s-1">>, 0, 1, <<"e">>, [], #{<<"causation_id">> => 12345}),
        [{meta, <<"causation_id">>}])).

entries_multiple_declared_indexes_compose_test() ->
    E = ev(<<"s-1">>, 0, 1, <<"placed">>, [<<"hot">>],
           #{<<"cid">> => <<"c1">>}),
    Entries = reckon_db_index:entries(
        E, [tags, event_type, {meta, <<"cid">>}]),
    %% one tag + one type + one meta = 3 entries
    ?assertEqual(3, length(Entries)).
