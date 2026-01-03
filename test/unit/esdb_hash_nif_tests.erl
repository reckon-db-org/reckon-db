%% @doc Unit tests for esdb_hash_nif module.
%%
%% Tests both the pure Erlang implementations (always tested) and the NIF
%% implementations (tested only when available in Enterprise builds).
%%
%% @author R. Lefever

-module(esdb_hash_nif_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

small_data() -> <<"hello">>.
medium_data() -> crypto:strong_rand_bytes(100).
large_data() -> crypto:strong_rand_bytes(10000).

%%====================================================================
%% Pure Erlang Implementation Tests (Always Run)
%%====================================================================

erlang_xxhash64_consistent_test() ->
    Data = small_data(),
    Hash1 = esdb_hash_nif:erlang_xxhash64(Data),
    Hash2 = esdb_hash_nif:erlang_xxhash64(Data),
    ?assertEqual(Hash1, Hash2).

erlang_xxhash64_different_data_test() ->
    Hash1 = esdb_hash_nif:erlang_xxhash64(<<"hello">>),
    Hash2 = esdb_hash_nif:erlang_xxhash64(<<"world">>),
    ?assertNotEqual(Hash1, Hash2).

erlang_xxhash64_with_seed_test() ->
    Data = small_data(),
    Hash1 = esdb_hash_nif:erlang_xxhash64(Data, 0),
    Hash2 = esdb_hash_nif:erlang_xxhash64(Data, 42),
    ?assertNotEqual(Hash1, Hash2).

erlang_xxhash3_consistent_test() ->
    Data = medium_data(),
    Hash1 = esdb_hash_nif:erlang_xxhash3(Data),
    Hash2 = esdb_hash_nif:erlang_xxhash3(Data),
    ?assertEqual(Hash1, Hash2).

erlang_partition_hash_range_test() ->
    Data = small_data(),
    Partitions = 16,
    Partition = esdb_hash_nif:erlang_partition_hash(Data, Partitions),
    ?assert(Partition >= 0),
    ?assert(Partition < Partitions).

erlang_partition_hash_consistent_test() ->
    Data = medium_data(),
    Partitions = 8,
    P1 = esdb_hash_nif:erlang_partition_hash(Data, Partitions),
    P2 = esdb_hash_nif:erlang_partition_hash(Data, Partitions),
    ?assertEqual(P1, P2).

erlang_stream_partition_test() ->
    StoreId = <<"store1">>,
    StreamId = <<"stream-123">>,
    Partitions = 16,
    P = esdb_hash_nif:erlang_stream_partition(StoreId, StreamId, Partitions),
    ?assert(P >= 0),
    ?assert(P < Partitions).

erlang_partition_hash_batch_test() ->
    Items = [<<"a">>, <<"b">>, <<"c">>, <<"d">>],
    Partitions = 4,
    Results = esdb_hash_nif:erlang_partition_hash_batch(Items, Partitions),
    ?assertEqual(length(Items), length(Results)),
    lists:foreach(fun(P) ->
        ?assert(P >= 0),
        ?assert(P < Partitions)
    end, Results).

erlang_fnv1a_consistent_test() ->
    Data = small_data(),
    Hash1 = esdb_hash_nif:erlang_fnv1a(Data),
    Hash2 = esdb_hash_nif:erlang_fnv1a(Data),
    ?assertEqual(Hash1, Hash2).

erlang_fnv1a_known_value_test() ->
    %% FNV-1a of empty string should be offset basis
    EmptyHash = esdb_hash_nif:erlang_fnv1a(<<>>),
    ?assertEqual(14695981039346656037, EmptyHash).

erlang_fast_phash_range_test() ->
    Data = medium_data(),
    Range = 100,
    Hash = esdb_hash_nif:erlang_fast_phash(Data, Range),
    ?assert(Hash >= 0),
    ?assert(Hash < Range).

%%====================================================================
%% Public API Tests (Uses Dispatch)
%%====================================================================

api_xxhash64_test() ->
    Data = medium_data(),
    Hash = esdb_hash_nif:xxhash64(Data),
    ?assert(is_integer(Hash)),
    ?assert(Hash >= 0).

api_xxhash64_with_seed_test() ->
    Data = small_data(),
    Hash1 = esdb_hash_nif:xxhash64(Data, 0),
    Hash2 = esdb_hash_nif:xxhash64(Data, 12345),
    ?assert(is_integer(Hash1)),
    ?assert(is_integer(Hash2)).

api_xxhash3_test() ->
    Data = large_data(),
    Hash = esdb_hash_nif:xxhash3(Data),
    ?assert(is_integer(Hash)),
    ?assert(Hash >= 0).

api_partition_hash_test() ->
    Data = <<"stream-orders-12345">>,
    Partition = esdb_hash_nif:partition_hash(Data, 16),
    ?assert(Partition >= 0),
    ?assert(Partition < 16).

api_stream_partition_test() ->
    StoreId = <<"my_store">>,
    StreamId = <<"orders-customer-123">>,
    Partition = esdb_hash_nif:stream_partition(StoreId, StreamId, 8),
    ?assert(Partition >= 0),
    ?assert(Partition < 8).

api_partition_hash_batch_test() ->
    Items = [<<"item1">>, <<"item2">>, <<"item3">>],
    Results = esdb_hash_nif:partition_hash_batch(Items, 10),
    ?assertEqual(3, length(Results)),
    lists:foreach(fun(P) ->
        ?assert(P >= 0),
        ?assert(P < 10)
    end, Results).

api_fnv1a_test() ->
    Hash = esdb_hash_nif:fnv1a(<<"test">>),
    ?assert(is_integer(Hash)),
    ?assert(Hash >= 0).

api_fast_phash_test() ->
    Hash = esdb_hash_nif:fast_phash(<<"data">>, 1000),
    ?assert(Hash >= 0),
    ?assert(Hash < 1000).

%%====================================================================
%% Introspection Tests
%%====================================================================

implementation_returns_atom_test() ->
    Impl = esdb_hash_nif:implementation(),
    ?assert(Impl =:= nif orelse Impl =:= erlang).

is_nif_loaded_returns_boolean_test() ->
    Loaded = esdb_hash_nif:is_nif_loaded(),
    ?assert(is_boolean(Loaded)).

%%====================================================================
%% NIF Implementation Tests (Only When Available)
%%====================================================================

nif_tests_when_available_test_() ->
    case esdb_hash_nif:is_nif_loaded() of
        true ->
            [
                {"NIF xxhash64 works", fun nif_xxhash64_works/0},
                {"NIF xxhash3 works", fun nif_xxhash3_works/0},
                {"NIF partition_hash works", fun nif_partition_hash_works/0},
                {"NIF stream_partition works", fun nif_stream_partition_works/0},
                {"NIF batch works", fun nif_batch_works/0},
                {"NIF fnv1a works", fun nif_fnv1a_works/0}
            ];
        false ->
            []
    end.

nif_xxhash64_works() ->
    Data = medium_data(),
    Hash = esdb_hash_nif:nif_xxhash64(Data),
    ?assert(is_integer(Hash)),
    ?assert(Hash >= 0),
    %% Consistent
    ?assertEqual(Hash, esdb_hash_nif:nif_xxhash64(Data)).

nif_xxhash3_works() ->
    Data = large_data(),
    Hash = esdb_hash_nif:nif_xxhash3(Data),
    ?assert(is_integer(Hash)),
    ?assert(Hash >= 0).

nif_partition_hash_works() ->
    Data = <<"test-stream">>,
    P = esdb_hash_nif:nif_partition_hash(Data, 16),
    ?assert(P >= 0),
    ?assert(P < 16).

nif_stream_partition_works() ->
    StoreId = <<"store">>,
    StreamId = <<"stream">>,
    P = esdb_hash_nif:nif_stream_partition(StoreId, StreamId, 8),
    ?assert(P >= 0),
    ?assert(P < 8).

nif_batch_works() ->
    Items = [<<"a">>, <<"b">>, <<"c">>],
    Results = esdb_hash_nif:nif_partition_hash_batch(Items, 4),
    ?assertEqual(3, length(Results)).

nif_fnv1a_works() ->
    Hash = esdb_hash_nif:nif_fnv1a(<<"test">>),
    ?assert(is_integer(Hash)),
    ?assert(Hash >= 0).

%%====================================================================
%% Distribution Tests
%%====================================================================

distribution_test() ->
    %% Test that hashing distributes relatively evenly
    Partitions = 8,
    NumItems = 1000,
    Items = [crypto:strong_rand_bytes(20) || _ <- lists:seq(1, NumItems)],

    Results = esdb_hash_nif:partition_hash_batch(Items, Partitions),

    %% Count items per partition
    Counts = lists:foldl(fun(P, Acc) ->
        maps:update_with(P, fun(V) -> V + 1 end, 1, Acc)
    end, #{}, Results),

    %% Each partition should have roughly NumItems/Partitions items
    %% Allow for statistical variance (at least 50 per partition for 1000 items / 8 partitions)
    lists:foreach(fun(P) ->
        Count = maps:get(P, Counts, 0),
        ?assert(Count > 50, {partition, P, count, Count})
    end, lists:seq(0, Partitions - 1)).

%%====================================================================
%% Benchmark Tests (Only When NIF Available)
%%====================================================================

benchmark_test_() ->
    case esdb_hash_nif:is_nif_loaded() of
        true ->
            [{"Benchmark hashing", fun benchmark_hashing/0}];
        false ->
            []
    end.

benchmark_hashing() ->
    Data = medium_data(),
    Iterations = 10000,

    %% Benchmark Erlang phash2
    {TimeErl, _} = timer:tc(fun() ->
        [erlang:phash2(Data, 16) || _ <- lists:seq(1, Iterations)]
    end),

    %% Benchmark NIF xxhash3
    {TimeNif, _} = timer:tc(fun() ->
        [esdb_hash_nif:nif_partition_hash(Data, 16) || _ <- lists:seq(1, Iterations)]
    end),

    %% Log results
    logger:info("Hash benchmark (~p iterations): phash2=~pus, NIF xxhash3=~pus, Speedup=~.2fx",
               [Iterations, TimeErl, TimeNif, TimeErl / max(TimeNif, 1)]),

    %% NIF should be competitive (allow some overhead for NIF call)
    ?assert(TimeNif =< TimeErl * 2).
