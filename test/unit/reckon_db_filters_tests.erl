%%% reckon_db_filters: the selector predicate both delivery paths apply, and
%%% the selectors a subscription refuses.
%%%
%%% matches/3 answered true for every event_pattern and event_payload
%%% selector, so a catch-up handed the subscriber the whole log, while the
%%% live trigger matched a map against an #event{} record and fired for
%%% nothing (reckon-db#8). by_tags had no tag condition on its trigger, so it
%%% delivered every event live (reckon-db#11). A selector a builder could not
%%% use raised after the subscription record was stored (reckon-db#9).
-module(reckon_db_filters_tests).
-include_lib("eunit/include/eunit.hrl").
-include("reckon_db.hrl").

event() ->
    #event{event_id = <<"e1">>, event_type = <<"order_placed_v1">>,
           stream_id = <<"order-0123456789abcdef0123456789abcdef">>, version = 0,
           data = #{<<"total">> => 10000, <<"currency">> => <<"EUR">>},
           metadata = #{correlation_id => <<"c1">>},
           tags = [<<"order">>, <<"customer:7">>]}.

%%% event_pattern: a map of #event{} fields, every one given must match

event_pattern_matches_on_the_named_field_test() ->
    ?assert(reckon_db_filters:matches(event_pattern, #{event_type => <<"order_placed_v1">>}, event())),
    ?assertNot(reckon_db_filters:matches(event_pattern, #{event_type => <<"user_registered_v1">>}, event())).

event_pattern_needs_every_field_to_match_test() ->
    ?assert(reckon_db_filters:matches(event_pattern,
        #{event_type => <<"order_placed_v1">>, version => 0}, event())),
    ?assertNot(reckon_db_filters:matches(event_pattern,
        #{event_type => <<"order_placed_v1">>, version => 1}, event())).

event_pattern_matches_a_map_field_by_the_keys_given_test() ->
    ?assert(reckon_db_filters:matches(event_pattern, #{data => #{<<"total">> => 10000}}, event())),
    ?assertNot(reckon_db_filters:matches(event_pattern, #{data => #{<<"total">> => 1}}, event())),
    ?assert(reckon_db_filters:matches(event_pattern,
        #{metadata => #{correlation_id => <<"c1">>}}, event())).

event_pattern_gater_spelling_is_the_same_predicate_test() ->
    ?assert(reckon_db_filters:matches(by_event_pattern, #{event_type => <<"order_placed_v1">>}, event())),
    ?assertNot(reckon_db_filters:matches(by_event_pattern, #{event_type => <<"x">>}, event())).

%%% event_payload: a map over the event's data, every key given has that value

event_payload_matches_every_key_given_test() ->
    ?assert(reckon_db_filters:matches(event_payload, #{<<"total">> => 10000}, event())),
    ?assert(reckon_db_filters:matches(by_event_payload,
        #{<<"total">> => 10000, <<"currency">> => <<"EUR">>}, event())).

event_payload_misses_on_a_value_or_a_key_test() ->
    ?assertNot(reckon_db_filters:matches(event_payload, #{<<"total">> => 9999}, event())),
    ?assertNot(reckon_db_filters:matches(event_payload, #{<<"absent">> => 1}, event())).

event_payload_misses_an_event_whose_data_is_not_a_map_test() ->
    ?assertNot(reckon_db_filters:matches(event_payload, #{<<"total">> => 1},
                                         (event())#event{data = <<"raw">>})).

%%% by_tags: the event carries every tag asked for

by_tags_matches_only_tagged_events_test() ->
    ?assert(reckon_db_filters:matches(by_tags, [<<"order">>], event())),
    ?assertNot(reckon_db_filters:matches(by_tags, [<<"invoice">>], event())),
    ?assertNot(reckon_db_filters:matches(by_tags, [<<"order">>], (event())#event{tags = undefined})).

%%% Selectors no subscription can use are refused by the builder

an_event_pattern_must_be_a_map_of_event_fields_test() ->
    ?assertMatch({error, {not_a_map, <<"user-*">>}}, reckon_db_filters:by_event_pattern(<<"user-*">>)),
    ?assertMatch({error, empty_selector}, reckon_db_filters:by_event_pattern(#{})),
    ?assertMatch({error, {unknown_event_fields, [<<"k">>]}},
                 reckon_db_filters:by_event_pattern(#{<<"k">> => 1})).

an_event_payload_must_be_a_non_empty_map_test() ->
    ?assertMatch({error, {not_a_map, <<"x">>}}, reckon_db_filters:by_event_payload(<<"x">>)),
    ?assertMatch({error, empty_selector}, reckon_db_filters:by_event_payload(#{})).

tags_must_be_a_non_empty_list_of_binaries_test() ->
    ?assertMatch({error, {not_a_tag_list, <<"order">>}}, reckon_db_filters:by_tags(<<"order">>)),
    ?assertMatch({error, empty_selector}, reckon_db_filters:by_tags([])),
    ?assertMatch({error, {not_a_tag_list, [order]}}, reckon_db_filters:by_tags([order])).

an_event_type_must_be_a_non_empty_binary_test() ->
    ?assertMatch({error, empty_selector}, reckon_db_filters:by_event_type(<<>>)),
    ?assertMatch({error, {not_a_binary, order}}, reckon_db_filters:by_event_type(order)).

usable_selectors_build_a_filter_test_() ->
    [?_assertNotMatch({error, _}, Built)
     || Built <- [reckon_db_filters:by_event_pattern(#{event_type => <<"t">>}),
                  reckon_db_filters:by_event_payload(#{<<"k">> => 1}),
                  reckon_db_filters:by_tags([<<"order">>]),
                  reckon_db_filters:by_event_type(<<"t">>)]].
