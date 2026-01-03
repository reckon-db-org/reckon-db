%% @doc Unit tests for schema registry module
%% @author R. Lefever

-module(reckon_db_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../../include/reckon_db.hrl").

%%====================================================================
%% Upcast Function Tests
%%====================================================================

%% These tests verify upcast logic without Khepri

upcast_v1_to_v2_test() ->
    %% Simulate V1 to V2 upcast - add missing field
    V1Data = #{name => <<"Order">>, amount => 100},
    UpcastFun = fun(Data) -> Data#{currency => <<"USD">>} end,

    V2Data = UpcastFun(V1Data),
    ?assertEqual(#{name => <<"Order">>, amount => 100, currency => <<"USD">>}, V2Data).

upcast_v2_to_v3_test() ->
    %% Simulate V2 to V3 upcast - rename field
    V2Data = #{name => <<"Order">>, amount => 100, currency => <<"USD">>},
    UpcastFun = fun(Data) ->
        Amount = maps:get(amount, Data),
        Currency = maps:get(currency, Data),
        Data#{
            total => #{value => Amount, currency => Currency}
        }
    end,

    V3Data = UpcastFun(V2Data),
    ?assertEqual(#{value => 100, currency => <<"USD">>}, maps:get(total, V3Data)).

upcast_chain_test() ->
    %% Test chaining multiple upcasts
    V1Data = #{name => <<"Test">>},

    V1ToV2 = fun(D) -> D#{added_in_v2 => true} end,
    V2ToV3 = fun(D) -> D#{added_in_v3 => <<"value">>} end,
    V3ToV4 = fun(D) -> D#{version => 4} end,

    V2Data = V1ToV2(V1Data),
    V3Data = V2ToV3(V2Data),
    V4Data = V3ToV4(V3Data),

    ?assertEqual(true, maps:get(added_in_v2, V4Data)),
    ?assertEqual(<<"value">>, maps:get(added_in_v3, V4Data)),
    ?assertEqual(4, maps:get(version, V4Data)).

%%====================================================================
%% Validator Function Tests
%%====================================================================

validator_passes_test() ->
    %% Simple validator that checks required fields
    Validator = fun(Data) ->
        case maps:is_key(name, Data) of
            true -> ok;
            false -> {error, {missing_field, name}}
        end
    end,

    ?assertEqual(ok, Validator(#{name => <<"Test">>})),
    ?assertEqual({error, {missing_field, name}}, Validator(#{})).

validator_with_type_check_test() ->
    %% Validator that checks field types
    Validator = fun(Data) ->
        Amount = maps:get(amount, Data, undefined),
        case is_integer(Amount) andalso Amount > 0 of
            true -> ok;
            false -> {error, {invalid_amount, Amount}}
        end
    end,

    ?assertEqual(ok, Validator(#{amount => 100})),
    ?assertEqual({error, {invalid_amount, -5}}, Validator(#{amount => -5})),
    ?assertEqual({error, {invalid_amount, <<"100">>}}, Validator(#{amount => <<"100">>})).

%%====================================================================
%% Schema Record Tests
%%====================================================================

schema_structure_test() ->
    Schema = #{
        event_type => <<"OrderPlaced">>,
        version => 3,
        upcast_from => #{
            1 => fun(D) -> D#{v2_field => default} end,
            2 => fun(D) -> D#{v3_field => true} end
        },
        description => <<"Schema for OrderPlaced events">>
    },

    ?assertEqual(<<"OrderPlaced">>, maps:get(event_type, Schema)),
    ?assertEqual(3, maps:get(version, Schema)),
    ?assert(is_map(maps:get(upcast_from, Schema))).

schema_upcast_from_has_functions_test() ->
    UpcastFuns = #{
        1 => fun(D) -> D#{new => true} end,
        2 => fun(D) -> D#{newer => true} end
    },

    %% Verify all values are functions
    lists:foreach(
        fun({_Version, Fun}) ->
            ?assert(is_function(Fun, 1))
        end,
        maps:to_list(UpcastFuns)
    ).

%%====================================================================
%% Event Version in Metadata Tests
%%====================================================================

event_schema_version_test() ->
    %% Event with schema version in metadata
    Event = #event{
        event_id = <<"evt-1">>,
        event_type = <<"OrderPlaced">>,
        stream_id = <<"orders-123">>,
        version = 0,
        data = #{name => <<"Test">>},
        metadata = #{schema_version => 2},
        timestamp = 1000,
        epoch_us = 1000000
    },

    ?assertEqual(2, maps:get(schema_version, Event#event.metadata)).

event_default_version_test() ->
    %% Event without schema version defaults to 1
    Event = #event{
        event_id = <<"evt-1">>,
        event_type = <<"OrderPlaced">>,
        stream_id = <<"orders-123">>,
        version = 0,
        data = #{name => <<"Test">>},
        metadata = #{},
        timestamp = 1000,
        epoch_us = 1000000
    },

    %% Convention: missing schema_version means version 1
    SchemaVersion = maps:get(schema_version, Event#event.metadata, 1),
    ?assertEqual(1, SchemaVersion).

%%====================================================================
%% Version Comparison Tests
%%====================================================================

needs_upcast_test() ->
    CurrentVersion = 3,
    EventVersion = 1,
    ?assert(EventVersion < CurrentVersion).

no_upcast_needed_test() ->
    CurrentVersion = 3,
    EventVersion = 3,
    ?assertNot(EventVersion < CurrentVersion).

%%====================================================================
%% Schema Info Tests
%%====================================================================

schema_info_structure_test() ->
    Info = #{
        event_type => <<"OrderPlaced">>,
        version => 2,
        registered_at => 1703000000000
    },

    ?assertEqual(<<"OrderPlaced">>, maps:get(event_type, Info)),
    ?assertEqual(2, maps:get(version, Info)),
    ?assert(is_integer(maps:get(registered_at, Info))).

%%====================================================================
%% Complex Upcast Scenarios
%%====================================================================

upcast_with_field_rename_test() ->
    %% V1: {user_name: "John"}
    %% V2: {username: "John"}
    UpcastFun = fun(Data) ->
        OldValue = maps:get(user_name, Data, undefined),
        maps:remove(user_name, Data#{username => OldValue})
    end,

    V1Data = #{user_name => <<"John">>},
    V2Data = UpcastFun(V1Data),

    ?assertNot(maps:is_key(user_name, V2Data)),
    ?assertEqual(<<"John">>, maps:get(username, V2Data)).

upcast_with_type_change_test() ->
    %% V1: {amount: 100}  (integer cents)
    %% V2: {amount: "1.00"}  (string dollars)
    UpcastFun = fun(Data) ->
        Cents = maps:get(amount, Data),
        Dollars = Cents / 100,
        DollarsStr = io_lib:format("~.2f", [Dollars]),
        Data#{amount => list_to_binary(DollarsStr)}
    end,

    V1Data = #{amount => 1050},
    V2Data = UpcastFun(V1Data),

    ?assertEqual(<<"10.50">>, maps:get(amount, V2Data)).

upcast_with_nested_structure_test() ->
    %% V1: {street: "123 Main", city: "NYC"}
    %% V2: {address: {street: "123 Main", city: "NYC"}}
    UpcastFun = fun(Data) ->
        Street = maps:get(street, Data),
        City = maps:get(city, Data),
        DataWithoutOld = maps:without([street, city], Data),
        DataWithoutOld#{address => #{street => Street, city => City}}
    end,

    V1Data = #{street => <<"123 Main">>, city => <<"NYC">>},
    V2Data = UpcastFun(V1Data),

    ?assertNot(maps:is_key(street, V2Data)),
    ?assertNot(maps:is_key(city, V2Data)),
    ?assertEqual(#{street => <<"123 Main">>, city => <<"NYC">>},
                 maps:get(address, V2Data)).

upcast_preserves_unknown_fields_test() ->
    %% Upcasting should preserve fields not mentioned
    UpcastFun = fun(Data) -> Data#{new_field => true} end,

    V1Data = #{known => <<"value">>, unknown => 123, extra => #{nested => true}},
    V2Data = UpcastFun(V1Data),

    ?assertEqual(<<"value">>, maps:get(known, V2Data)),
    ?assertEqual(123, maps:get(unknown, V2Data)),
    ?assertEqual(#{nested => true}, maps:get(extra, V2Data)),
    ?assertEqual(true, maps:get(new_field, V2Data)).

%%====================================================================
%% Backward Compatibility Tests
%%====================================================================

tolerant_reader_pattern_test() ->
    %% Consumer should accept both old and new format
    V1Data = #{name => <<"Test">>},
    V2Data = #{name => <<"Test">>, description => <<"A test item">>},

    %% Reader that handles both versions
    ReadData = fun(Data) ->
        Name = maps:get(name, Data),
        Description = maps:get(description, Data, <<"No description">>),
        #{name => Name, description => Description}
    end,

    Result1 = ReadData(V1Data),
    Result2 = ReadData(V2Data),

    ?assertEqual(<<"No description">>, maps:get(description, Result1)),
    ?assertEqual(<<"A test item">>, maps:get(description, Result2)).
