%% @doc Unit tests for esdb_archive_nif module.
%%
%% Tests both the pure Erlang implementations (always tested) and the NIF
%% implementations (tested only when available in Enterprise builds).
%%
%% @author Macula.io

-module(esdb_archive_nif_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

%% Generate test data of various sizes
small_data() -> <<"Hello, World! This is a test.">>.
medium_data() -> crypto:strong_rand_bytes(1000).
large_data() -> crypto:strong_rand_bytes(100000).

%% Generate highly compressible data
compressible_data() ->
    %% Repeated pattern compresses well
    Pattern = <<"The quick brown fox jumps over the lazy dog. ">>,
    iolist_to_binary(lists:duplicate(100, Pattern)).

%%====================================================================
%% Pure Erlang Implementation Tests (Always Run)
%%====================================================================

erlang_zlib_roundtrip_test() ->
    Data = small_data(),
    {ok, Compressed} = esdb_archive_nif:erlang_compress_zlib(Data, 6),
    {ok, Decompressed} = esdb_archive_nif:erlang_decompress_zlib(Compressed),
    ?assertEqual(Data, Decompressed).

erlang_zlib_empty_test() ->
    Data = <<>>,
    {ok, Compressed} = esdb_archive_nif:erlang_compress_zlib(Data, 6),
    {ok, Decompressed} = esdb_archive_nif:erlang_decompress_zlib(Compressed),
    ?assertEqual(Data, Decompressed).

erlang_zlib_large_test() ->
    Data = large_data(),
    {ok, Compressed} = esdb_archive_nif:erlang_compress_zlib(Data, 6),
    {ok, Decompressed} = esdb_archive_nif:erlang_decompress_zlib(Compressed),
    ?assertEqual(Data, Decompressed).

erlang_zlib_levels_test() ->
    Data = compressible_data(),
    %% Test different compression levels
    lists:foreach(fun(Level) ->
        {ok, Compressed} = esdb_archive_nif:erlang_compress_zlib(Data, Level),
        {ok, Decompressed} = esdb_archive_nif:erlang_decompress_zlib(Compressed),
        ?assertEqual(Data, Decompressed)
    end, [0, 1, 5, 9]).

erlang_lz4_fallback_roundtrip_test() ->
    %% LZ4 fallback uses zlib internally
    Data = small_data(),
    {ok, Compressed} = esdb_archive_nif:erlang_compress_lz4(Data),
    {ok, Decompressed} = esdb_archive_nif:erlang_decompress_lz4(Compressed),
    ?assertEqual(Data, Decompressed).

erlang_zstd_fallback_roundtrip_test() ->
    %% Zstd fallback uses zlib internally
    Data = small_data(),
    {ok, Compressed} = esdb_archive_nif:erlang_compress_zstd(Data, 3),
    {ok, Decompressed} = esdb_archive_nif:erlang_decompress_zstd(Compressed),
    ?assertEqual(Data, Decompressed).

%%====================================================================
%% Public API Tests (Uses Dispatch)
%%====================================================================

api_compress_lz4_test() ->
    Data = medium_data(),
    {ok, Compressed} = esdb_archive_nif:compress(Data, lz4),
    ?assert(is_binary(Compressed)),
    {ok, Decompressed} = esdb_archive_nif:decompress(Compressed, lz4),
    ?assertEqual(Data, Decompressed).

api_compress_zstd_test() ->
    Data = medium_data(),
    {ok, Compressed} = esdb_archive_nif:compress(Data, zstd),
    ?assert(is_binary(Compressed)),
    {ok, Decompressed} = esdb_archive_nif:decompress(Compressed, zstd),
    ?assertEqual(Data, Decompressed).

api_compress_zlib_test() ->
    Data = medium_data(),
    {ok, Compressed} = esdb_archive_nif:compress(Data, zlib),
    ?assert(is_binary(Compressed)),
    {ok, Decompressed} = esdb_archive_nif:decompress(Compressed, zlib),
    ?assertEqual(Data, Decompressed).

api_compress_with_level_test() ->
    Data = compressible_data(),
    %% Zstd with level
    {ok, Compressed1} = esdb_archive_nif:compress(Data, zstd, 1),
    {ok, Compressed9} = esdb_archive_nif:compress(Data, zstd, 9),
    %% Higher level should produce smaller output (usually)
    ?assert(byte_size(Compressed9) =< byte_size(Compressed1) + 100).

api_compression_stats_test() ->
    Data = compressible_data(),
    {ok, Stats} = esdb_archive_nif:compression_stats(Data, zlib),
    ?assertEqual(byte_size(Data), maps:get(original_size, Stats)),
    ?assert(maps:get(compressed_size, Stats) < maps:get(original_size, Stats)),
    ?assert(maps:get(ratio, Stats) > 1.0).

%%====================================================================
%% Introspection Tests
%%====================================================================

implementation_returns_atom_test() ->
    Impl = esdb_archive_nif:implementation(),
    ?assert(Impl =:= nif orelse Impl =:= erlang).

is_nif_loaded_returns_boolean_test() ->
    Loaded = esdb_archive_nif:is_nif_loaded(),
    ?assert(is_boolean(Loaded)).

%%====================================================================
%% NIF Implementation Tests (Only When Available)
%%====================================================================

nif_tests_when_available_test_() ->
    case esdb_archive_nif:is_nif_loaded() of
        true ->
            [
                {"NIF LZ4 roundtrip", fun nif_lz4_roundtrip/0},
                {"NIF Zstd roundtrip", fun nif_zstd_roundtrip/0},
                {"NIF Zlib roundtrip", fun nif_zlib_roundtrip/0},
                {"NIF large data compression", fun nif_large_data/0},
                {"NIF compression stats", fun nif_compression_stats/0}
            ];
        false ->
            []
    end.

nif_lz4_roundtrip() ->
    Data = medium_data(),
    Compressed = esdb_archive_nif:nif_compress_lz4(Data),
    ?assert(is_binary(Compressed)),
    {ok, Decompressed} = esdb_archive_nif:nif_decompress_lz4(Compressed),
    ?assertEqual(Data, Decompressed).

nif_zstd_roundtrip() ->
    Data = medium_data(),
    Compressed = esdb_archive_nif:nif_compress_zstd(Data, 3),
    ?assert(is_binary(Compressed)),
    {ok, Decompressed} = esdb_archive_nif:nif_decompress_zstd(Compressed),
    ?assertEqual(Data, Decompressed).

nif_zlib_roundtrip() ->
    Data = medium_data(),
    Compressed = esdb_archive_nif:nif_compress_zlib(Data, 6),
    ?assert(is_binary(Compressed)),
    {ok, Decompressed} = esdb_archive_nif:nif_decompress_zlib(Compressed),
    ?assertEqual(Data, Decompressed).

nif_large_data() ->
    Data = large_data(),
    %% Test all algorithms with large data
    lists:foreach(fun(Algo) ->
        {ok, Compressed} = esdb_archive_nif:compress(Data, Algo),
        {ok, Decompressed} = esdb_archive_nif:decompress(Compressed, Algo),
        ?assertEqual(Data, Decompressed)
    end, [lz4, zstd, zlib]).

nif_compression_stats() ->
    Data = compressible_data(),
    {OrigSize, CompSize, Ratio} = esdb_archive_nif:nif_compression_stats(Data, lz4, 0),
    ?assertEqual(byte_size(Data), OrigSize),
    ?assert(CompSize < OrigSize),
    ?assert(Ratio > 1.0).

%%====================================================================
%% Equivalence Tests (Only When NIF Available)
%%====================================================================

equivalence_tests_test_() ->
    case esdb_archive_nif:is_nif_loaded() of
        true ->
            [
                {"Zlib equivalence", fun equivalence_zlib/0}
            ];
        false ->
            []
    end.

equivalence_zlib() ->
    Data = compressible_data(),

    %% Compress with both implementations
    {ok, ErlCompressed} = esdb_archive_nif:erlang_compress_zlib(Data, 6),
    NifCompressed = esdb_archive_nif:nif_compress_zlib(Data, 6),

    %% Decompress each with the other
    {ok, ErlDecomp} = esdb_archive_nif:erlang_decompress_zlib(NifCompressed),
    {ok, NifDecomp} = esdb_archive_nif:nif_decompress_zlib(ErlCompressed),

    %% Both should produce original data
    ?assertEqual(Data, ErlDecomp),
    ?assertEqual(Data, NifDecomp).

%%====================================================================
%% Benchmark Tests (Only When NIF Available)
%%====================================================================

benchmark_test_() ->
    case esdb_archive_nif:is_nif_loaded() of
        true ->
            [{"Benchmark compression", fun benchmark_compression/0}];
        false ->
            []
    end.

benchmark_compression() ->
    Data = large_data(),
    Iterations = 100,

    %% Benchmark Erlang zlib
    {TimeErl, _} = timer:tc(fun() ->
        [esdb_archive_nif:erlang_compress_zlib(Data, 6)
         || _ <- lists:seq(1, Iterations)]
    end),

    %% Benchmark NIF zlib
    {TimeNif, _} = timer:tc(fun() ->
        [esdb_archive_nif:nif_compress_zlib(Data, 6)
         || _ <- lists:seq(1, Iterations)]
    end),

    %% Log results
    logger:info("Zlib compress benchmark (~p iterations, ~p bytes): Erlang=~pus, NIF=~pus, Speedup=~.2fx",
               [Iterations, byte_size(Data), TimeErl, TimeNif, TimeErl / max(TimeNif, 1)]),

    %% NIF should be faster (or at least not much slower)
    ?assert(TimeNif =< TimeErl * 1.5).

%%====================================================================
%% Algorithm Comparison Tests
%%====================================================================

algorithm_comparison_test() ->
    Data = compressible_data(),

    %% Get stats for all algorithms
    {ok, LZ4Stats} = esdb_archive_nif:compression_stats(Data, lz4),
    {ok, ZstdStats} = esdb_archive_nif:compression_stats(Data, zstd),
    {ok, ZlibStats} = esdb_archive_nif:compression_stats(Data, zlib),

    %% All should compress the data
    ?assert(maps:get(ratio, LZ4Stats) > 1.0),
    ?assert(maps:get(ratio, ZstdStats) > 1.0),
    ?assert(maps:get(ratio, ZlibStats) > 1.0),

    %% Zstd typically has best ratio for compressible data
    %% (but this depends on data and settings, so we just verify they work)
    logger:info("Compression ratios - LZ4: ~.2f, Zstd: ~.2f, Zlib: ~.2f",
               [maps:get(ratio, LZ4Stats),
                maps:get(ratio, ZstdStats),
                maps:get(ratio, ZlibStats)]).
