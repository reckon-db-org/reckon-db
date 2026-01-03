%% @doc Optimized hashing operations for reckon-db.
%%
%% This module provides high-performance hash implementations:
%%
%% <ul>
%% <li><b>xxHash64</b>: Extremely fast 64-bit hash</li>
%% <li><b>xxHash3</b>: Even faster, modern 64-bit hash with SIMD</li>
%% <li><b>Partition hash</b>: For consistent stream/subscription routing</li>
%% <li><b>FNV-1a</b>: Fast for small keys</li>
%% </ul>
%%
%% The mode is automatically detected at startup based on whether the NIF
%% library is available. Community edition users (hex.pm) will always use
%% the Erlang fallbacks, which provide identical functionality.
%%
%% == Usage ==
%%
%% ```
%% %% Fast hash for routing
%% Partition = esdb_hash_nif:partition_hash(StreamId, 16).
%%
%% %% Stream-specific routing
%% Partition = esdb_hash_nif:stream_partition(StoreId, StreamId, 16).
%%
%% %% Raw xxHash for checksums
%% Hash = esdb_hash_nif:xxhash64(Data).
%%
%% %% Check which mode is active
%% nif = esdb_hash_nif:implementation().  %% Enterprise
%% erlang = esdb_hash_nif:implementation(). %% Community
%% '''
%%
%% @author rgfaber

-module(esdb_hash_nif).

%% Public API
-export([
    xxhash64/1,
    xxhash64/2,
    xxhash3/1,
    partition_hash/2,
    stream_partition/3,
    partition_hash_batch/2,
    fnv1a/1,
    fast_phash/2
]).

%% Introspection
-export([
    is_nif_loaded/0,
    implementation/0
]).

%% For testing - expose both implementations
-export([
    nif_xxhash64/1,
    nif_xxhash64_seed/2,
    nif_xxhash3/1,
    nif_partition_hash/2,
    nif_stream_partition/3,
    nif_partition_hash_batch/2,
    nif_fnv1a/1,
    nif_fast_phash/2,
    erlang_xxhash64/1,
    erlang_xxhash64/2,
    erlang_xxhash3/1,
    erlang_partition_hash/2,
    erlang_stream_partition/3,
    erlang_partition_hash_batch/2,
    erlang_fnv1a/1,
    erlang_fast_phash/2
]).

%% NIF loading
-on_load(init/0).

%% Persistent term key for NIF status
-define(NIF_LOADED_KEY, esdb_hash_nif_loaded).

%% FNV-1a constants (64-bit)
-define(FNV_OFFSET_BASIS, 14695981039346656037).
-define(FNV_PRIME, 1099511628211).

%%====================================================================
%% NIF Loading
%%====================================================================

%% @private
%% Try to load NIF from multiple locations:
%% 1. reckon_nifs priv/ (enterprise addon package)
%% 2. reckon_db priv/ (standalone enterprise build)
-spec init() -> ok.
init() ->
    NifName = "esdb_hash_nif",
    Paths = nif_search_paths(NifName),
    case try_load_nif(Paths) of
        ok ->
            persistent_term:put(?NIF_LOADED_KEY, true),
            logger:info("[esdb_hash_nif] NIF loaded - Enterprise mode"),
            ok;
        {error, Reason} ->
            persistent_term:put(?NIF_LOADED_KEY, false),
            logger:info("[esdb_hash_nif] NIF not available (~p), using pure Erlang - Community mode",
                       [Reason]),
            ok
    end.

%% @private
nif_search_paths(NifName) ->
    Paths = [
        %% Try reckon_nifs first (enterprise addon)
        case code:priv_dir(reckon_nifs) of
            {error, _} -> undefined;
            NifsDir -> filename:join(NifsDir, NifName)
        end,
        %% Then try reckon_db priv/ (standalone build)
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
%% Public API
%%====================================================================

%% @doc Compute xxHash64 of binary data.
-spec xxhash64(Data :: binary()) -> non_neg_integer().
xxhash64(Data) ->
    case is_nif_loaded() of
        true -> nif_xxhash64(Data);
        false -> erlang_xxhash64(Data)
    end.

%% @doc Compute xxHash64 with a seed.
-spec xxhash64(Data :: binary(), Seed :: non_neg_integer()) -> non_neg_integer().
xxhash64(Data, Seed) ->
    case is_nif_loaded() of
        true -> nif_xxhash64_seed(Data, Seed);
        false -> erlang_xxhash64(Data, Seed)
    end.

%% @doc Compute xxHash3 (64-bit) of binary data.
%% xxHash3 is faster than xxHash64, especially for small inputs.
-spec xxhash3(Data :: binary()) -> non_neg_integer().
xxhash3(Data) ->
    case is_nif_loaded() of
        true -> nif_xxhash3(Data);
        false -> erlang_xxhash3(Data)
    end.

%% @doc Hash data and map to a partition number.
%% Used for consistent routing of streams/subscriptions to workers.
-spec partition_hash(Data :: binary(), Partitions :: pos_integer()) -> non_neg_integer().
partition_hash(Data, Partitions) when Partitions > 0 ->
    case is_nif_loaded() of
        true -> nif_partition_hash(Data, Partitions);
        false -> erlang_partition_hash(Data, Partitions)
    end.

%% @doc Hash {StoreId, StreamId} tuple for stream routing.
-spec stream_partition(StoreId :: binary(), StreamId :: binary(),
                       Partitions :: pos_integer()) -> non_neg_integer().
stream_partition(StoreId, StreamId, Partitions) when Partitions > 0 ->
    case is_nif_loaded() of
        true -> nif_stream_partition(StoreId, StreamId, Partitions);
        false -> erlang_stream_partition(StoreId, StreamId, Partitions)
    end.

%% @doc Hash multiple binaries and return their partition assignments.
-spec partition_hash_batch(Items :: [binary()], Partitions :: pos_integer()) -> [non_neg_integer()].
partition_hash_batch(Items, Partitions) when Partitions > 0 ->
    case is_nif_loaded() of
        true -> nif_partition_hash_batch(Items, Partitions);
        false -> erlang_partition_hash_batch(Items, Partitions)
    end.

%% @doc Compute FNV-1a hash of binary data.
%% Fast for small keys (under 32 bytes).
-spec fnv1a(Data :: binary()) -> non_neg_integer().
fnv1a(Data) ->
    case is_nif_loaded() of
        true -> nif_fnv1a(Data);
        false -> erlang_fnv1a(Data)
    end.

%% @doc Fast replacement for erlang:phash2/2.
-spec fast_phash(Data :: binary(), Range :: pos_integer()) -> non_neg_integer().
fast_phash(Data, Range) when Range > 0 ->
    case is_nif_loaded() of
        true -> nif_fast_phash(Data, Range);
        false -> erlang_fast_phash(Data, Range)
    end.

%%====================================================================
%% NIF Stubs (replaced when NIF loads)
%%====================================================================

%% @private
nif_xxhash64(_Data) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_xxhash64_seed(_Data, _Seed) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_xxhash3(_Data) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_partition_hash(_Data, _Partitions) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_stream_partition(_StoreId, _StreamId, _Partitions) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_partition_hash_batch(_Items, _Partitions) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_fnv1a(_Data) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_fast_phash(_Data, _Range) ->
    erlang:nif_error(nif_not_loaded).

%%====================================================================
%% Pure Erlang Implementations (Always Available)
%%====================================================================

%% @private
%% Pure Erlang xxHash64 approximation using phash2.
%% Note: This does NOT produce identical results to real xxHash64,
%% but provides consistent hashing for the same inputs.
-spec erlang_xxhash64(binary()) -> non_neg_integer().
erlang_xxhash64(Data) ->
    %% Use phash2 which gives 32-bit hash, extend to 64-bit
    Hash1 = erlang:phash2(Data, 16#FFFFFFFF),
    Hash2 = erlang:phash2({Data, Hash1}, 16#FFFFFFFF),
    (Hash1 bsl 32) bor Hash2.

%% @private
-spec erlang_xxhash64(binary(), non_neg_integer()) -> non_neg_integer().
erlang_xxhash64(Data, Seed) ->
    %% Incorporate seed into hash
    Hash1 = erlang:phash2({Data, Seed}, 16#FFFFFFFF),
    Hash2 = erlang:phash2({Data, Hash1, Seed}, 16#FFFFFFFF),
    (Hash1 bsl 32) bor Hash2.

%% @private
%% Pure Erlang xxHash3 approximation.
-spec erlang_xxhash3(binary()) -> non_neg_integer().
erlang_xxhash3(Data) ->
    %% Same as xxhash64 for fallback
    erlang_xxhash64(Data).

%% @private
-spec erlang_partition_hash(binary(), pos_integer()) -> non_neg_integer().
erlang_partition_hash(Data, Partitions) ->
    Hash = erlang:phash2(Data, 16#FFFFFFFF),
    Hash rem Partitions.

%% @private
-spec erlang_stream_partition(binary(), binary(), pos_integer()) -> non_neg_integer().
erlang_stream_partition(StoreId, StreamId, Partitions) ->
    %% Combine store and stream IDs
    Combined = <<StoreId/binary, 0, StreamId/binary>>,
    erlang:phash2(Combined, Partitions).

%% @private
-spec erlang_partition_hash_batch([binary()], pos_integer()) -> [non_neg_integer()].
erlang_partition_hash_batch(Items, Partitions) ->
    [erlang_partition_hash(Item, Partitions) || Item <- Items].

%% @private
%% FNV-1a hash implementation.
-spec erlang_fnv1a(binary()) -> non_neg_integer().
erlang_fnv1a(Data) ->
    fnv1a_loop(Data, ?FNV_OFFSET_BASIS).

fnv1a_loop(<<>>, Hash) ->
    Hash;
fnv1a_loop(<<Byte, Rest/binary>>, Hash) ->
    NewHash = ((Hash bxor Byte) * ?FNV_PRIME) band 16#FFFFFFFFFFFFFFFF,
    fnv1a_loop(Rest, NewHash).

%% @private
-spec erlang_fast_phash(binary(), pos_integer()) -> non_neg_integer().
erlang_fast_phash(Data, Range) ->
    erlang:phash2(Data, Range).
