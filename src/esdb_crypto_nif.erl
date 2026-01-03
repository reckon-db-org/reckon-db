%% @doc Optimized cryptographic operations for reckon-db.
%%
%% This module provides high-performance implementations of cryptographic
%% operations used throughout reckon-db. It supports two modes:
%%
%% <ul>
%% <li><b>Enterprise mode</b>: Uses Rust NIFs for maximum performance</li>
%% <li><b>Community mode</b>: Uses pure Erlang fallbacks (fully functional)</li>
%% </ul>
%%
%% The mode is automatically detected at startup based on whether the NIF
%% library is available. Community edition users (hex.pm) will always use
%% the Erlang fallbacks, which provide identical functionality.
%%
%% == Usage ==
%%
%% All functions work identically regardless of which implementation is active:
%%
%% ```
%% %% Verify Ed25519 signature
%% true = esdb_crypto_nif:verify_ed25519(Message, Signature, PublicKey).
%%
%% %% Generate token CID (SHA256 + base64)
%% CID = esdb_crypto_nif:hash_sha256_base64(Data).
%%
%% %% Check which mode is active
%% true = esdb_crypto_nif:is_nif_loaded().  %% Enterprise
%% false = esdb_crypto_nif:is_nif_loaded(). %% Community
%% '''
%%
%% @author rgfaber

-module(esdb_crypto_nif).

%% Public API
-export([
    verify_ed25519/3,
    hash_sha256/1,
    hash_sha256_base64/1,
    base64_encode_urlsafe/1,
    base64_decode_urlsafe/1,
    secure_compare/2
]).

%% Introspection
-export([
    is_nif_loaded/0,
    implementation/0
]).

%% For testing - expose both implementations
-export([
    nif_verify_ed25519/3,
    nif_hash_sha256/1,
    nif_hash_sha256_base64/1,
    nif_base64_encode_urlsafe/1,
    nif_base64_decode_urlsafe/1,
    nif_secure_compare/2,
    erlang_verify_ed25519/3,
    erlang_hash_sha256/1,
    erlang_hash_sha256_base64/1,
    erlang_base64_encode_urlsafe/1,
    erlang_base64_decode_urlsafe/1,
    erlang_secure_compare/2
]).

%% NIF loading
-on_load(init/0).

%% Persistent term key for NIF status
-define(NIF_LOADED_KEY, esdb_crypto_nif_loaded).

%%====================================================================
%% NIF Loading
%%====================================================================

%% @private
%% Try to load NIF from multiple locations:
%% 1. reckon_nifs priv/ (enterprise addon package)
%% 2. reckon_db priv/ (standalone enterprise build)
-spec init() -> ok.
init() ->
    NifName = "esdb_crypto_nif",
    Paths = nif_search_paths(NifName),
    case try_load_nif(Paths) of
        ok ->
            persistent_term:put(?NIF_LOADED_KEY, true),
            logger:info("[esdb_crypto_nif] NIF loaded - Enterprise mode"),
            ok;
        {error, Reason} ->
            persistent_term:put(?NIF_LOADED_KEY, false),
            logger:info("[esdb_crypto_nif] NIF not available (~p), using pure Erlang - Community mode",
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
%%
%% Returns `true' if running in Enterprise mode with NIF optimizations,
%% `false' if running in Community mode with pure Erlang.
-spec is_nif_loaded() -> boolean().
is_nif_loaded() ->
    persistent_term:get(?NIF_LOADED_KEY, false).

%% @doc Get the current implementation mode.
%%
%% Returns `nif' for Enterprise mode or `erlang' for Community mode.
-spec implementation() -> nif | erlang.
implementation() ->
    case is_nif_loaded() of
        true -> nif;
        false -> erlang
    end.

%%====================================================================
%% Public API - Automatic Dispatch
%%====================================================================

%% @doc Verify an Ed25519 signature.
%%
%% This function verifies that a signature was created by the private key
%% corresponding to the given public key.
%%
%% Arguments:
%% <ul>
%% <li>`Message' - The original message that was signed (binary)</li>
%% <li>`Signature' - The 64-byte Ed25519 signature (binary)</li>
%% <li>`PublicKey' - The 32-byte Ed25519 public key (binary)</li>
%% </ul>
%%
%% Returns `true' if valid, `false' otherwise.
-spec verify_ed25519(Message :: binary(), Signature :: binary(),
                     PublicKey :: binary()) -> boolean().
verify_ed25519(Message, Signature, PublicKey) ->
    case is_nif_loaded() of
        true -> nif_verify_ed25519(Message, Signature, PublicKey);
        false -> erlang_verify_ed25519(Message, Signature, PublicKey)
    end.

%% @doc Compute SHA-256 hash.
%%
%% Returns a 32-byte binary containing the SHA-256 hash of the input.
-spec hash_sha256(Data :: binary()) -> binary().
hash_sha256(Data) ->
    case is_nif_loaded() of
        true -> nif_hash_sha256(Data);
        false -> erlang_hash_sha256(Data)
    end.

%% @doc Compute SHA-256 hash and encode as URL-safe base64.
%%
%% This is optimized for token CID generation - combines hash + encode
%% in a single call to avoid intermediate allocations.
%%
%% Returns a URL-safe base64 string (no padding).
-spec hash_sha256_base64(Data :: binary()) -> binary().
hash_sha256_base64(Data) ->
    case is_nif_loaded() of
        true -> nif_hash_sha256_base64(Data);
        false -> erlang_hash_sha256_base64(Data)
    end.

%% @doc Encode binary as URL-safe base64 (no padding).
-spec base64_encode_urlsafe(Data :: binary()) -> binary().
base64_encode_urlsafe(Data) ->
    case is_nif_loaded() of
        true -> nif_base64_encode_urlsafe(Data);
        false -> erlang_base64_encode_urlsafe(Data)
    end.

%% @doc Decode URL-safe base64 string.
%%
%% Returns `{ok, Binary}' on success, `{error, invalid_base64}' on failure.
-spec base64_decode_urlsafe(Data :: binary()) ->
    {ok, binary()} | {error, invalid_base64}.
base64_decode_urlsafe(Data) ->
    case is_nif_loaded() of
        true ->
            case nif_base64_decode_urlsafe(Data) of
                {ok, Result} -> {ok, Result};
                {error, _} -> {error, invalid_base64}
            end;
        false -> erlang_base64_decode_urlsafe(Data)
    end.

%% @doc Constant-time comparison of two binaries.
%%
%% This is important for security - prevents timing attacks when comparing
%% signatures, hashes, or tokens. Always takes the same amount of time
%% regardless of where the difference is (if any).
%%
%% Returns `true' if equal, `false' otherwise.
-spec secure_compare(A :: binary(), B :: binary()) -> boolean().
secure_compare(A, B) ->
    case is_nif_loaded() of
        true -> nif_secure_compare(A, B);
        false -> erlang_secure_compare(A, B)
    end.

%%====================================================================
%% NIF Stubs (replaced when NIF loads)
%%====================================================================

%% @private
nif_verify_ed25519(_Message, _Signature, _PublicKey) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_hash_sha256(_Data) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_hash_sha256_base64(_Data) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_base64_encode_urlsafe(_Data) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_base64_decode_urlsafe(_Data) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_secure_compare(_A, _B) ->
    erlang:nif_error(nif_not_loaded).

%%====================================================================
%% Pure Erlang Implementations (Always Available)
%%====================================================================

%% @private
%% Pure Erlang Ed25519 verification using crypto module.
-spec erlang_verify_ed25519(binary(), binary(), binary()) -> boolean().
erlang_verify_ed25519(Message, Signature, PublicKey) ->
    try
        crypto:verify(eddsa, none, Message, Signature, [PublicKey, ed25519])
    catch
        error:_ -> false
    end.

%% @private
%% Pure Erlang SHA-256 hash.
-spec erlang_hash_sha256(binary()) -> binary().
erlang_hash_sha256(Data) ->
    crypto:hash(sha256, Data).

%% @private
%% Pure Erlang SHA-256 hash with base64 encoding.
-spec erlang_hash_sha256_base64(binary()) -> binary().
erlang_hash_sha256_base64(Data) ->
    Hash = crypto:hash(sha256, Data),
    base64:encode(Hash, #{mode => urlsafe, padding => false}).

%% @private
%% Pure Erlang URL-safe base64 encode.
-spec erlang_base64_encode_urlsafe(binary()) -> binary().
erlang_base64_encode_urlsafe(Data) ->
    base64:encode(Data, #{mode => urlsafe, padding => false}).

%% @private
%% Pure Erlang URL-safe base64 decode.
-spec erlang_base64_decode_urlsafe(binary()) ->
    {ok, binary()} | {error, invalid_base64}.
erlang_base64_decode_urlsafe(Data) ->
    try
        {ok, base64:decode(Data, #{mode => urlsafe, padding => false})}
    catch
        error:_ -> {error, invalid_base64}
    end.

%% @private
%% Pure Erlang constant-time comparison.
%% Uses XOR accumulation to ensure constant time regardless of input.
-spec erlang_secure_compare(binary(), binary()) -> boolean().
erlang_secure_compare(A, B) when byte_size(A) =/= byte_size(B) ->
    false;
erlang_secure_compare(A, B) ->
    erlang_secure_compare(A, B, 0).

erlang_secure_compare(<<X, RestA/binary>>, <<Y, RestB/binary>>, Acc) ->
    erlang_secure_compare(RestA, RestB, Acc bor (X bxor Y));
erlang_secure_compare(<<>>, <<>>, Acc) ->
    Acc =:= 0.
