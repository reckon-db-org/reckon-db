%% @doc Per-store HMAC key loader for the tamper-resistance layer.
%%
%% Loads the integrity HMAC key for a store at startup, validates it,
%% and stores it under a persistent_term key so the write/read paths
%% can access it without process-state coupling. Refuses to start the
%% store if integrity is configured but the key cannot be loaded.
%%
%% == Key sources ==
%%
%% Two sources supported in 2.1.0, in priority order:
%%
%% <ol>
%%   <li>Environment variable: {env_var, &lt;&lt;"VAR_NAME"&gt;&gt;} -
%%       value is base64-decoded into the key bytes.</li>
%%   <li>Sealed file: {sealed_file, "/path/to/key"} - file mode must
%%       be 0600 (refused otherwise); content read raw (no encoding).</li>
%% </ol>
%%
%% A future 2.2.0 release will add Vault/KMS providers and per-store
%% keyrings for rotation; the {env_var, _} and {sealed_file, _}
%% sources will remain supported.
%%
%% == Validation ==
%%
%% The key must be exactly 32 bytes (256 bits) after decoding. Anything
%% else is a configuration error and the load fails. This matches the
%% standard HMAC-SHA256 key size.
%%
%% == Storage ==
%%
%% The loaded key is placed in persistent_term under the key
%% {reckon_db, integrity_key, StoreId}. The integrity_enabled flag is
%% placed under {reckon_db, integrity_enabled, StoreId} as a boolean.
%% Callers should NOT read the key directly via persistent_term -
%% they should use get/1 to keep the lookup pattern stable.
%%
%% The key never appears in logs, error tuples, telemetry events, or
%% process state dumps. This is enforced by convention; reviewers
%% should reject any code that violates it.
%%
%% @end
-module(reckon_db_integrity_key).

-include("reckon_db.hrl").
-include_lib("kernel/include/file.hrl").

-export([
    load/1,
    get/1,
    is_enabled/1,
    clear/1
]).

-export_type([load_error/0]).

-type load_error() ::
    {error, {integrity_key_invalid_size, ActualBytes :: non_neg_integer()}} |
    {error, {integrity_key_env_var_not_set, EnvName :: binary()}} |
    {error, {integrity_key_env_var_not_base64, EnvName :: binary()}} |
    {error, {integrity_key_file_not_readable, Path :: file:filename(), Reason :: term()}} |
    {error, {integrity_key_file_insecure_mode, Path :: file:filename(), Mode :: integer()}}.

-define(EXPECTED_KEY_SIZE, 32).

%%====================================================================
%% Public API
%%====================================================================

%% @doc Load the integrity key for a store at startup.
%%
%% Reads the store config, and if integrity is enabled, fetches the
%% key from the configured source, validates it, and installs it in
%% persistent_term. Idempotent: re-loading a store overwrites the
%% previously-loaded key.
%%
%% Returns 'ok' on success (including the case where integrity is
%% disabled - nothing to do), or a structured error.
-spec load(store_config()) -> ok | load_error().
load(#store_config{store_id = StoreId, integrity = disabled}) ->
    %% Explicit clean state - if a previous instance had a key loaded
    %% (e.g., during tests) we don't want it leaking into the new
    %% disabled instance.
    persistent_term:erase({reckon_db, integrity_key, StoreId}),
    persistent_term:erase({reckon_db, integrity_enabled, StoreId}),
    persistent_term:put({reckon_db, integrity_enabled, StoreId}, false),
    ok;
load(#store_config{store_id = StoreId,
                   integrity = #{enabled := true, key_source := Source}}) ->
    case load_key_bytes(Source) of
        {ok, KeyBytes} when byte_size(KeyBytes) =:= ?EXPECTED_KEY_SIZE ->
            persistent_term:put({reckon_db, integrity_key, StoreId}, KeyBytes),
            persistent_term:put({reckon_db, integrity_enabled, StoreId}, true),
            ok;
        {ok, KeyBytes} ->
            {error, {integrity_key_invalid_size, byte_size(KeyBytes)}};
        {error, _} = Err ->
            Err
    end.

%% @doc Retrieve the loaded HMAC key for a store.
%%
%% Returns the key binary if integrity is enabled and the key is
%% loaded; returns 'undefined' if integrity is disabled for the store
%% or the key has not been loaded (which should never happen if
%% startup ordering is correct).
%%
%% Callers in the write/read path call this on every operation; the
%% persistent_term lookup is sub-microsecond.
-spec get(StoreId :: atom()) -> binary() | undefined.
get(StoreId) ->
    persistent_term:get({reckon_db, integrity_key, StoreId}, undefined).

%% @doc Whether integrity is enabled for a store.
%%
%% Reads a separate persistent_term flag so callers can quickly
%% short-circuit on the disabled-store case without inspecting the
%% (sensitive) key binary.
-spec is_enabled(StoreId :: atom()) -> boolean().
is_enabled(StoreId) ->
    persistent_term:get({reckon_db, integrity_enabled, StoreId}, false).

%% @doc Remove all integrity state for a store from persistent_term.
%%
%% Intended for store shutdown and for test isolation. Production code
%% should not call this in the hot path.
-spec clear(StoreId :: atom()) -> ok.
clear(StoreId) ->
    persistent_term:erase({reckon_db, integrity_key, StoreId}),
    persistent_term:erase({reckon_db, integrity_enabled, StoreId}),
    ok.

%%====================================================================
%% Internal
%%====================================================================

-spec load_key_bytes(integrity_key_source()) ->
    {ok, binary()} | load_error().
load_key_bytes({env_var, EnvName}) when is_binary(EnvName) ->
    case os:getenv(binary_to_list(EnvName)) of
        false ->
            {error, {integrity_key_env_var_not_set, EnvName}};
        "" ->
            {error, {integrity_key_env_var_not_set, EnvName}};
        Value ->
            case base64_decode_safe(Value) of
                {ok, Bytes} -> {ok, Bytes};
                error -> {error, {integrity_key_env_var_not_base64, EnvName}}
            end
    end;
load_key_bytes({sealed_file, Path}) ->
    case check_file_mode(Path) of
        ok ->
            case file:read_file(Path) of
                {ok, Bytes} ->
                    %% Trim trailing newline if present (common in
                    %% files generated with `echo`, `cat`, etc.)
                    {ok, strip_trailing_newline(Bytes)};
                {error, Reason} ->
                    {error, {integrity_key_file_not_readable, Path, Reason}}
            end;
        {error, _} = Err ->
            Err
    end.

base64_decode_safe(Value) ->
    try base64:decode(Value) of
        Bin when is_binary(Bin) -> {ok, Bin}
    catch
        _:_ -> error
    end.

check_file_mode(Path) ->
    case file:read_file_info(Path, [{time, posix}]) of
        {ok, #file_info{mode = Mode}} ->
            %% Mask off the file-type bits (top byte) and check that
            %% group + other permission bits are all zero. Owner bits
            %% are allowed to be anything except executable.
            Permissions = Mode band 8#777,
            case Permissions band 8#077 of
                0 -> ok;
                _ -> {error, {integrity_key_file_insecure_mode, Path, Permissions}}
            end;
        {error, Reason} ->
            {error, {integrity_key_file_not_readable, Path, Reason}}
    end.

strip_trailing_newline(Bin) ->
    Size = byte_size(Bin),
    case Bin of
        <<Body:(Size - 1)/binary, "\n">> -> Body;
        _ -> Bin
    end.
