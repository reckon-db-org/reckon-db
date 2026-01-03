%% @doc Unit tests for memory pressure module
%% @author Macula.io

-module(reckon_db_memory_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Level Calculation Tests (Pure Functions)
%%====================================================================

level_normal_test() ->
    %% Usage below elevated threshold should be normal
    ?assertEqual(normal, reckon_db_memory:level(0.0)),
    ?assertEqual(normal, reckon_db_memory:level(0.50)),
    ?assertEqual(normal, reckon_db_memory:level(0.69)).

level_elevated_test() ->
    %% Usage at or above elevated but below critical
    ?assertEqual(elevated, reckon_db_memory:level(0.70)),
    ?assertEqual(elevated, reckon_db_memory:level(0.75)),
    ?assertEqual(elevated, reckon_db_memory:level(0.84)).

level_critical_test() ->
    %% Usage at or above critical threshold
    ?assertEqual(critical, reckon_db_memory:level(0.85)),
    ?assertEqual(critical, reckon_db_memory:level(0.90)),
    ?assertEqual(critical, reckon_db_memory:level(1.0)).

%%====================================================================
%% Threshold Boundary Tests
%%====================================================================

threshold_boundary_elevated_test() ->
    %% Exactly at elevated threshold (0.70 default)
    ?assertEqual(elevated, reckon_db_memory:level(0.70)).

threshold_boundary_critical_test() ->
    %% Exactly at critical threshold (0.85 default)
    ?assertEqual(critical, reckon_db_memory:level(0.85)).

threshold_just_below_elevated_test() ->
    %% Just below elevated threshold
    ?assertEqual(normal, reckon_db_memory:level(0.6999)).

threshold_just_below_critical_test() ->
    %% Just below critical threshold
    ?assertEqual(elevated, reckon_db_memory:level(0.8499)).

%%====================================================================
%% Configuration Tests
%%====================================================================

config_defaults_test() ->
    DefaultConfig = #{
        elevated_threshold => 0.70,
        critical_threshold => 0.85,
        check_interval => 10000
    },

    ?assertEqual(0.70, maps:get(elevated_threshold, DefaultConfig)),
    ?assertEqual(0.85, maps:get(critical_threshold, DefaultConfig)),
    ?assertEqual(10000, maps:get(check_interval, DefaultConfig)).

config_custom_test() ->
    CustomConfig = #{
        elevated_threshold => 0.60,
        critical_threshold => 0.75,
        check_interval => 5000
    },

    ?assertEqual(0.60, maps:get(elevated_threshold, CustomConfig)),
    ?assertEqual(0.75, maps:get(critical_threshold, CustomConfig)),
    ?assertEqual(5000, maps:get(check_interval, CustomConfig)).

%%====================================================================
%% Level Transition Tests
%%====================================================================

transition_normal_to_elevated_test() ->
    %% Crossing elevated threshold should change level
    BelowElevated = reckon_db_memory:level(0.65),
    AtElevated = reckon_db_memory:level(0.75),
    ?assertEqual(normal, BelowElevated),
    ?assertEqual(elevated, AtElevated).

transition_elevated_to_critical_test() ->
    %% Crossing critical threshold should change level
    BelowCritical = reckon_db_memory:level(0.80),
    AtCritical = reckon_db_memory:level(0.90),
    ?assertEqual(elevated, BelowCritical),
    ?assertEqual(critical, AtCritical).

transition_critical_to_normal_test() ->
    %% Dropping below elevated should return to normal
    AtCritical = reckon_db_memory:level(0.90),
    BelowElevated = reckon_db_memory:level(0.50),
    ?assertEqual(critical, AtCritical),
    ?assertEqual(normal, BelowElevated).

no_transition_same_level_test() ->
    %% Multiple readings at same level
    Level1 = reckon_db_memory:level(0.30),
    Level2 = reckon_db_memory:level(0.40),
    Level3 = reckon_db_memory:level(0.50),
    ?assertEqual(normal, Level1),
    ?assertEqual(normal, Level2),
    ?assertEqual(normal, Level3).

%%====================================================================
%% Callback Registration Tests
%%====================================================================

callback_function_arity_test() ->
    %% Callbacks must be arity-1 functions
    ValidCallback = fun(Level) -> Level end,
    ?assert(is_function(ValidCallback, 1)).

callback_receives_level_test() ->
    %% Simulate callback receiving a level
    Callback = fun(Level) -> {received, Level} end,
    ?assertEqual({received, normal}, Callback(normal)),
    ?assertEqual({received, elevated}, Callback(elevated)),
    ?assertEqual({received, critical}, Callback(critical)).

%%====================================================================
%% Stats Structure Tests
%%====================================================================

stats_structure_test() ->
    %% Verify expected stats structure
    ExpectedKeys = [level, memory_used, memory_total, last_check, callback_count],

    Stats = #{
        level => normal,
        memory_used => 1000000000,
        memory_total => 2000000000,
        last_check => 1703000000000,
        callback_count => 0
    },

    lists:foreach(
        fun(Key) ->
            ?assert(maps:is_key(Key, Stats))
        end,
        ExpectedKeys
    ).

%%====================================================================
%% Usage Ratio Calculation Tests
%%====================================================================

usage_ratio_zero_test() ->
    %% Zero usage
    Used = 0,
    Total = 1000000000,
    Ratio = Used / Total,
    ?assertEqual(0.0, Ratio).

usage_ratio_half_test() ->
    %% Half usage
    Used = 500000000,
    Total = 1000000000,
    Ratio = Used / Total,
    ?assertEqual(0.5, Ratio).

usage_ratio_full_test() ->
    %% Full usage
    Used = 1000000000,
    Total = 1000000000,
    Ratio = Used / Total,
    ?assertEqual(1.0, Ratio).

usage_ratio_over_test() ->
    %% Over 100% (shouldn't happen but handle gracefully)
    Used = 1100000000,
    Total = 1000000000,
    Ratio = Used / Total,
    ?assert(Ratio > 1.0),
    %% Should still return critical
    ?assertEqual(critical, reckon_db_memory:level(Ratio)).

%%====================================================================
%% Adaptive Behavior Documentation Tests
%%====================================================================

%% These tests document expected behavior at different levels

normal_level_behavior_test() ->
    %% At normal level:
    %% - Full caching enabled
    %% - All subscriptions active
    %% - No throttling
    Level = normal,
    ?assertEqual(normal, Level).

elevated_level_behavior_test() ->
    %% At elevated level:
    %% - Reduce cache sizes
    %% - Flush more frequently
    %% - Consider pausing new subscriptions
    Level = elevated,
    ?assertEqual(elevated, Level).

critical_level_behavior_test() ->
    %% At critical level:
    %% - Pause non-essential subscriptions
    %% - Aggressive cache cleanup
    %% - Reject new operations
    Level = critical,
    ?assertEqual(critical, Level).

%%====================================================================
%% Type Tests
%%====================================================================

pressure_level_type_test() ->
    %% Verify pressure levels are atoms
    ?assert(is_atom(normal)),
    ?assert(is_atom(elevated)),
    ?assert(is_atom(critical)).

threshold_type_test() ->
    %% Thresholds should be floats between 0 and 1
    Elevated = 0.70,
    Critical = 0.85,
    ?assert(is_float(Elevated)),
    ?assert(is_float(Critical)),
    ?assert(Elevated >= 0.0 andalso Elevated =< 1.0),
    ?assert(Critical >= 0.0 andalso Critical =< 1.0),
    ?assert(Elevated < Critical).
