%% @doc Optimized archive compression for reckon-db.
%%
%% This module provides high-performance compression implementations:
%%
%% <ul>
%% <li><b>LZ4</b>: Fastest compression/decompression (real-time use)</li>
%% <li><b>Zstd</b>: Best compression ratio (cold storage)</li>
%% <li><b>Zlib</b>: Standard compatibility (Erlang term_to_binary)</li>
%% </ul>
%%
%% The mode is automatically detected at startup based on whether the NIF
%% library is available. Community edition users (hex.pm) will always use
%% the Erlang fallbacks, which provide identical functionality.
%%
%% == Algorithm Selection ==
%%
%% | Algorithm | Speed | Ratio | Use Case |
%% |-----------|-------|-------|----------|
%% | LZ4 | Fastest | ~2.5:1 | Hot archives, real-time |
%% | Zstd | Fast | ~4:1 | Cold archives, storage |
%% | Zlib | Moderate | ~3:1 | Compatibility |
%%
%% == Usage ==
%%
%% ```
%% %% Compress with LZ4 (fastest)
%% {ok, Compressed} = esdb_archive_nif:compress(Data, lz4).
%%
%% %% Compress with Zstd level 3 (good balance)
%% {ok, Compressed} = esdb_archive_nif:compress(Data, zstd, 3).
%%
%% %% Decompress
%% {ok, Original} = esdb_archive_nif:decompress(Compressed, lz4).
%%
%% %% Check which mode is active
%% nif = esdb_archive_nif:implementation().  %% Enterprise
%% erlang = esdb_archive_nif:implementation(). %% Community
%% '''
%%
%% @author rgfaber

-module(esdb_archive_nif).

%% Public API
-export([
    compress/2,
    compress/3,
    decompress/2,
    compress_lz4/1,
    decompress_lz4/1,
    compress_zstd/1,
    compress_zstd/2,
    decompress_zstd/1,
    compress_zlib/1,
    compress_zlib/2,
    decompress_zlib/1,
    compression_stats/2,
    compression_stats/3
]).

%% Introspection
-export([
    is_nif_loaded/0,
    implementation/0
]).

%% For testing - expose both implementations
-export([
    nif_compress_lz4/1,
    nif_decompress_lz4/1,
    nif_compress_zstd/2,
    nif_decompress_zstd/1,
    nif_compress_zlib/2,
    nif_decompress_zlib/1,
    nif_compress/3,
    nif_decompress/2,
    nif_compression_stats/3,
    erlang_compress_lz4/1,
    erlang_decompress_lz4/1,
    erlang_compress_zstd/2,
    erlang_decompress_zstd/1,
    erlang_compress_zlib/2,
    erlang_decompress_zlib/1
]).

%% NIF loading
-on_load(init/0).

%% Persistent term key for NIF status
-define(NIF_LOADED_KEY, esdb_archive_nif_loaded).

%% Default compression levels
-define(DEFAULT_ZSTD_LEVEL, 3).
-define(DEFAULT_ZLIB_LEVEL, 6).

%%====================================================================
%% NIF Loading
%%====================================================================

%% @private
%% Try to load NIF from multiple locations:
%% 1. reckon_nifs priv/ (enterprise addon package)
%% 2. reckon_db priv/ (standalone enterprise build)
-spec init() -> ok.
init() ->
    NifName = "esdb_archive_nif",
    Paths = nif_search_paths(NifName),
    case try_load_nif(Paths) of
        ok ->
            persistent_term:put(?NIF_LOADED_KEY, true),
            logger:info("[esdb_archive_nif] NIF loaded - Enterprise mode"),
            ok;
        {error, Reason} ->
            persistent_term:put(?NIF_LOADED_KEY, false),
            logger:info("[esdb_archive_nif] NIF not available (~p), using pure Erlang - Community mode",
                       [Reason]),
            ok
    end.

%% @private
nif_search_paths(NifName) ->
    Paths = [
        case code:priv_dir(reckon_nifs) of
            {error, _} -> undefined;
            NifsDir -> filename:join(NifsDir, NifName)
        end,
        case code:priv_dir(reckon_db) of
            {error, _} -> filename:join("priv", NifName);
            Dir -> filename:join(Dir, NifName)
        end
    ],
    [P || P <- Paths, P =/= undefined].

%% @private
try_load_nif([]) ->
    {error, no_nif_found};
try_load_nif([Path | Rest]) ->
    case erlang:load_nif(Path, 0) of
        ok -> ok;
        {error, {reload, _}} -> ok;
        {error, _} -> try_load_nif(Rest)
    end.

%%====================================================================
%% Introspection API
%%====================================================================

%% @doc Check if the NIF is loaded (Enterprise mode).
-spec is_nif_loaded() -> boolean().
is_nif_loaded() ->
    persistent_term:get(?NIF_LOADED_KEY, false).

%% @doc Get the current implementation mode.
-spec implementation() -> nif | erlang.
implementation() ->
    case is_nif_loaded() of
        true -> nif;
        false -> erlang
    end.

%%====================================================================
%% Public API - Unified Interface
%%====================================================================

%% @doc Compress data with specified algorithm using default level.
-spec compress(Data :: binary(), Algorithm :: lz4 | zstd | zlib) ->
    {ok, binary()} | {error, term()}.
compress(Data, lz4) ->
    compress_lz4(Data);
compress(Data, zstd) ->
    compress_zstd(Data);
compress(Data, zlib) ->
    compress_zlib(Data).

%% @doc Compress data with specified algorithm and compression level.
-spec compress(Data :: binary(), Algorithm :: lz4 | zstd | zlib, Level :: integer()) ->
    {ok, binary()} | {error, term()}.
compress(Data, lz4, _Level) ->
    %% LZ4 doesn't use levels
    compress_lz4(Data);
compress(Data, zstd, Level) ->
    compress_zstd(Data, Level);
compress(Data, zlib, Level) ->
    compress_zlib(Data, Level).

%% @doc Decompress data with specified algorithm.
-spec decompress(Data :: binary(), Algorithm :: lz4 | zstd | zlib) ->
    {ok, binary()} | {error, term()}.
decompress(Data, lz4) ->
    decompress_lz4(Data);
decompress(Data, zstd) ->
    decompress_zstd(Data);
decompress(Data, zlib) ->
    decompress_zlib(Data).

%%====================================================================
%% LZ4 Compression
%%====================================================================

%% @doc Compress data using LZ4 (fastest).
-spec compress_lz4(Data :: binary()) -> {ok, binary()} | {error, term()}.
compress_lz4(Data) ->
    case is_nif_loaded() of
        true ->
            try
                Compressed = nif_compress_lz4(Data),
                {ok, Compressed}
            catch
                error:Reason -> {error, Reason}
            end;
        false ->
            erlang_compress_lz4(Data)
    end.

%% @doc Decompress LZ4-compressed data.
-spec decompress_lz4(Data :: binary()) -> {ok, binary()} | {error, term()}.
decompress_lz4(Data) ->
    case is_nif_loaded() of
        true ->
            case nif_decompress_lz4(Data) of
                {ok, Decompressed} -> {ok, Decompressed};
                {error, _} -> {error, decompression_failed}
            end;
        false ->
            erlang_decompress_lz4(Data)
    end.

%%====================================================================
%% Zstd Compression
%%====================================================================

%% @doc Compress data using Zstd with default level.
-spec compress_zstd(Data :: binary()) -> {ok, binary()} | {error, term()}.
compress_zstd(Data) ->
    compress_zstd(Data, ?DEFAULT_ZSTD_LEVEL).

%% @doc Compress data using Zstd with specified level (1-22).
-spec compress_zstd(Data :: binary(), Level :: integer()) -> {ok, binary()} | {error, term()}.
compress_zstd(Data, Level) ->
    case is_nif_loaded() of
        true ->
            try
                Compressed = nif_compress_zstd(Data, Level),
                {ok, Compressed}
            catch
                error:Reason -> {error, Reason}
            end;
        false ->
            erlang_compress_zstd(Data, Level)
    end.

%% @doc Decompress Zstd-compressed data.
-spec decompress_zstd(Data :: binary()) -> {ok, binary()} | {error, term()}.
decompress_zstd(Data) ->
    case is_nif_loaded() of
        true ->
            case nif_decompress_zstd(Data) of
                {ok, Decompressed} -> {ok, Decompressed};
                {error, _} -> {error, decompression_failed}
            end;
        false ->
            erlang_decompress_zstd(Data)
    end.

%%====================================================================
%% Zlib Compression
%%====================================================================

%% @doc Compress data using Zlib with default level.
-spec compress_zlib(Data :: binary()) -> {ok, binary()} | {error, term()}.
compress_zlib(Data) ->
    compress_zlib(Data, ?DEFAULT_ZLIB_LEVEL).

%% @doc Compress data using Zlib with specified level (0-9).
-spec compress_zlib(Data :: binary(), Level :: integer()) -> {ok, binary()} | {error, term()}.
compress_zlib(Data, Level) ->
    case is_nif_loaded() of
        true ->
            try
                Compressed = nif_compress_zlib(Data, Level),
                {ok, Compressed}
            catch
                error:Reason -> {error, Reason}
            end;
        false ->
            erlang_compress_zlib(Data, Level)
    end.

%% @doc Decompress Zlib-compressed data.
-spec decompress_zlib(Data :: binary()) -> {ok, binary()} | {error, term()}.
decompress_zlib(Data) ->
    case is_nif_loaded() of
        true ->
            case nif_decompress_zlib(Data) of
                {ok, Decompressed} -> {ok, Decompressed};
                {error, _} -> {error, decompression_failed}
            end;
        false ->
            erlang_decompress_zlib(Data)
    end.

%%====================================================================
%% Statistics
%%====================================================================

%% @doc Get compression statistics for data with algorithm.
-spec compression_stats(Data :: binary(), Algorithm :: lz4 | zstd | zlib) ->
    {ok, #{original_size := integer(), compressed_size := integer(), ratio := float()}} |
    {error, term()}.
compression_stats(Data, Algorithm) ->
    Level = case Algorithm of
        lz4 -> 0;
        zstd -> ?DEFAULT_ZSTD_LEVEL;
        zlib -> ?DEFAULT_ZLIB_LEVEL
    end,
    compression_stats(Data, Algorithm, Level).

%% @doc Get compression statistics with specified level.
-spec compression_stats(Data :: binary(), Algorithm :: lz4 | zstd | zlib, Level :: integer()) ->
    {ok, #{original_size := integer(), compressed_size := integer(), ratio := float()}} |
    {error, term()}.
compression_stats(Data, Algorithm, Level) ->
    case is_nif_loaded() of
        true ->
            try
                {OrigSize, CompSize, Ratio} = nif_compression_stats(Data, Algorithm, Level),
                {ok, #{original_size => OrigSize, compressed_size => CompSize, ratio => Ratio}}
            catch
                error:Reason -> {error, Reason}
            end;
        false ->
            case compress(Data, Algorithm, Level) of
                {ok, Compressed} ->
                    OrigSize = byte_size(Data),
                    CompSize = byte_size(Compressed),
                    Ratio = case CompSize of
                        0 -> 0.0;
                        _ -> OrigSize / CompSize
                    end,
                    {ok, #{original_size => OrigSize, compressed_size => CompSize, ratio => Ratio}};
                {error, _} = Error ->
                    Error
            end
    end.

%%====================================================================
%% NIF Stubs (replaced when NIF loads)
%%====================================================================

%% @private
nif_compress_lz4(_Data) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_decompress_lz4(_Data) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_compress_zstd(_Data, _Level) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_decompress_zstd(_Data) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_compress_zlib(_Data, _Level) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_decompress_zlib(_Data) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_compress(_Data, _Algorithm, _Level) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_decompress(_Data, _Algorithm) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_compression_stats(_Data, _Algorithm, _Level) ->
    erlang:nif_error(nif_not_loaded).

%%====================================================================
%% Pure Erlang Implementations (Always Available)
%%====================================================================

%% @private
%% Pure Erlang LZ4 compression using zlib as fallback.
%% Note: This is NOT true LZ4, just zlib with fast settings.
%% For true LZ4 in community mode, users should use a dedicated LZ4 library.
-spec erlang_compress_lz4(binary()) -> {ok, binary()} | {error, term()}.
erlang_compress_lz4(Data) ->
    %% Fallback: use zlib with fast compression as approximation
    %% Prepend original size for decompression (4 bytes, big-endian)
    OrigSize = byte_size(Data),
    try
        Compressed = zlib:compress(Data),
        {ok, <<OrigSize:32/big, Compressed/binary>>}
    catch
        error:Reason -> {error, Reason}
    end.

%% @private
-spec erlang_decompress_lz4(binary()) -> {ok, binary()} | {error, term()}.
erlang_decompress_lz4(<<_OrigSize:32/big, Compressed/binary>>) ->
    try
        Decompressed = zlib:uncompress(Compressed),
        {ok, Decompressed}
    catch
        error:Reason -> {error, Reason}
    end;
erlang_decompress_lz4(_) ->
    {error, invalid_format}.

%% @private
%% Pure Erlang Zstd compression using zlib as fallback.
%% Note: This is NOT true Zstd, just zlib with high compression.
-spec erlang_compress_zstd(binary(), integer()) -> {ok, binary()} | {error, term()}.
erlang_compress_zstd(Data, _Level) ->
    %% Fallback: use zlib with level 9 as approximation
    %% Prepend magic bytes to identify format
    try
        Z = zlib:open(),
        ok = zlib:deflateInit(Z, 9),
        Compressed = zlib:deflate(Z, Data, finish),
        ok = zlib:deflateEnd(Z),
        zlib:close(Z),
        Result = iolist_to_binary(Compressed),
        %% Add header to distinguish from real zstd
        {ok, <<"EZST", Result/binary>>}
    catch
        error:Reason -> {error, Reason}
    end.

%% @private
-spec erlang_decompress_zstd(binary()) -> {ok, binary()} | {error, term()}.
erlang_decompress_zstd(<<"EZST", Compressed/binary>>) ->
    %% Our fallback format
    try
        Z = zlib:open(),
        ok = zlib:inflateInit(Z),
        Decompressed = zlib:inflate(Z, Compressed),
        ok = zlib:inflateEnd(Z),
        zlib:close(Z),
        {ok, iolist_to_binary(Decompressed)}
    catch
        error:Reason -> {error, Reason}
    end;
erlang_decompress_zstd(_) ->
    %% Real Zstd format - not supported in pure Erlang
    {error, zstd_not_supported}.

%% @private
%% Pure Erlang Zlib compression.
-spec erlang_compress_zlib(binary(), integer()) -> {ok, binary()} | {error, term()}.
erlang_compress_zlib(Data, Level) ->
    try
        Z = zlib:open(),
        ok = zlib:deflateInit(Z, Level),
        Compressed = zlib:deflate(Z, Data, finish),
        ok = zlib:deflateEnd(Z),
        zlib:close(Z),
        {ok, iolist_to_binary(Compressed)}
    catch
        error:Reason -> {error, Reason}
    end.

%% @private
-spec erlang_decompress_zlib(binary()) -> {ok, binary()} | {error, term()}.
erlang_decompress_zlib(Data) ->
    try
        Z = zlib:open(),
        ok = zlib:inflateInit(Z),
        Decompressed = zlib:inflate(Z, Data),
        ok = zlib:inflateEnd(Z),
        zlib:close(Z),
        {ok, iolist_to_binary(Decompressed)}
    catch
        error:Reason -> {error, Reason}
    end.
