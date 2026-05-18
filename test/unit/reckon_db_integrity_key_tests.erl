-module(reckon_db_integrity_key_tests).

-include_lib("eunit/include/eunit.hrl").
-include("reckon_db.hrl").

%%====================================================================
%% Disabled-integrity case
%%====================================================================

disabled_load_is_noop_test() ->
    StoreId = unique_store_id(),
    Cfg = base_cfg(StoreId, disabled),
    ?assertEqual(ok, reckon_db_integrity_key:load(Cfg)),
    ?assertNot(reckon_db_integrity_key:is_enabled(StoreId)),
    ?assertEqual(undefined, reckon_db_integrity_key:get(StoreId)),
    cleanup(StoreId).

disabled_load_clears_prior_state_test() ->
    %% If a store was previously loaded with integrity and then
    %% reconfigured to disabled, the old key must NOT leak into the
    %% new instance.
    StoreId = unique_store_id(),
    KeyBytes = <<"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA">>,
    EnvName = unique_env(),
    os:putenv(binary_to_list(EnvName), base64_encode(KeyBytes)),

    CfgEnabled = base_cfg(StoreId, enabled_env(EnvName)),
    ?assertEqual(ok, reckon_db_integrity_key:load(CfgEnabled)),
    ?assert(reckon_db_integrity_key:is_enabled(StoreId)),

    CfgDisabled = base_cfg(StoreId, disabled),
    ?assertEqual(ok, reckon_db_integrity_key:load(CfgDisabled)),
    ?assertNot(reckon_db_integrity_key:is_enabled(StoreId)),
    ?assertEqual(undefined, reckon_db_integrity_key:get(StoreId)),

    os:unsetenv(binary_to_list(EnvName)),
    cleanup(StoreId).

%%====================================================================
%% Env-var source
%%====================================================================

env_var_loads_valid_key_test() ->
    StoreId = unique_store_id(),
    KeyBytes = crypto:strong_rand_bytes(32),
    EnvName = unique_env(),
    os:putenv(binary_to_list(EnvName), base64_encode(KeyBytes)),

    Cfg = base_cfg(StoreId, enabled_env(EnvName)),
    ?assertEqual(ok, reckon_db_integrity_key:load(Cfg)),
    ?assert(reckon_db_integrity_key:is_enabled(StoreId)),
    ?assertEqual(KeyBytes, reckon_db_integrity_key:get(StoreId)),

    os:unsetenv(binary_to_list(EnvName)),
    cleanup(StoreId).

env_var_missing_is_an_error_test() ->
    StoreId = unique_store_id(),
    EnvName = unique_env(),
    %% Deliberately do NOT set EnvName.
    Cfg = base_cfg(StoreId, enabled_env(EnvName)),
    ?assertMatch({error, {integrity_key_env_var_not_set, _}},
                 reckon_db_integrity_key:load(Cfg)),
    ?assertNot(reckon_db_integrity_key:is_enabled(StoreId)),
    cleanup(StoreId).

env_var_empty_is_an_error_test() ->
    StoreId = unique_store_id(),
    EnvName = unique_env(),
    os:putenv(binary_to_list(EnvName), ""),

    Cfg = base_cfg(StoreId, enabled_env(EnvName)),
    ?assertMatch({error, {integrity_key_env_var_not_set, _}},
                 reckon_db_integrity_key:load(Cfg)),

    os:unsetenv(binary_to_list(EnvName)),
    cleanup(StoreId).

env_var_bad_base64_is_an_error_test() ->
    StoreId = unique_store_id(),
    EnvName = unique_env(),
    os:putenv(binary_to_list(EnvName), "not valid base64 !@#$%^"),

    Cfg = base_cfg(StoreId, enabled_env(EnvName)),
    ?assertMatch({error, {integrity_key_env_var_not_base64, _}},
                 reckon_db_integrity_key:load(Cfg)),

    os:unsetenv(binary_to_list(EnvName)),
    cleanup(StoreId).

env_var_wrong_size_is_an_error_test() ->
    StoreId = unique_store_id(),
    %% 16 bytes encoded, not 32
    TooShort = crypto:strong_rand_bytes(16),
    EnvName = unique_env(),
    os:putenv(binary_to_list(EnvName), base64_encode(TooShort)),

    Cfg = base_cfg(StoreId, enabled_env(EnvName)),
    ?assertMatch({error, {integrity_key_invalid_size, 16}},
                 reckon_db_integrity_key:load(Cfg)),

    os:unsetenv(binary_to_list(EnvName)),
    cleanup(StoreId).

%%====================================================================
%% Sealed-file source
%%====================================================================

sealed_file_loads_valid_key_test() ->
    StoreId = unique_store_id(),
    KeyBytes = crypto:strong_rand_bytes(32),
    Path = make_sealed_file(KeyBytes),

    Cfg = base_cfg(StoreId, enabled_file(Path)),
    ?assertEqual(ok, reckon_db_integrity_key:load(Cfg)),
    ?assertEqual(KeyBytes, reckon_db_integrity_key:get(StoreId)),

    file:delete(Path),
    cleanup(StoreId).

sealed_file_trims_trailing_newline_test() ->
    %% Files generated with `echo` or `cat` typically end in \n.
    %% The loader trims a single trailing newline before validating
    %% size.
    StoreId = unique_store_id(),
    KeyBytes = crypto:strong_rand_bytes(32),
    Path = make_sealed_file(<<KeyBytes/binary, "\n">>),

    Cfg = base_cfg(StoreId, enabled_file(Path)),
    ?assertEqual(ok, reckon_db_integrity_key:load(Cfg)),
    ?assertEqual(KeyBytes, reckon_db_integrity_key:get(StoreId)),

    file:delete(Path),
    cleanup(StoreId).

sealed_file_world_readable_is_refused_test() ->
    StoreId = unique_store_id(),
    KeyBytes = crypto:strong_rand_bytes(32),
    Path = tmp_path("insecure_key"),
    ok = file:write_file(Path, KeyBytes),
    ok = file:change_mode(Path, 8#644),  %% group+other readable

    Cfg = base_cfg(StoreId, enabled_file(Path)),
    ?assertMatch({error, {integrity_key_file_insecure_mode, _, _}},
                 reckon_db_integrity_key:load(Cfg)),

    file:delete(Path),
    cleanup(StoreId).

sealed_file_missing_is_an_error_test() ->
    StoreId = unique_store_id(),
    NoSuchPath = tmp_path("no_such_key_file_" ++ random_suffix()),

    Cfg = base_cfg(StoreId, enabled_file(NoSuchPath)),
    ?assertMatch({error, {integrity_key_file_not_readable, _, _}},
                 reckon_db_integrity_key:load(Cfg)),
    cleanup(StoreId).

sealed_file_wrong_size_is_an_error_test() ->
    StoreId = unique_store_id(),
    %% 20 bytes
    TooShort = crypto:strong_rand_bytes(20),
    Path = make_sealed_file(TooShort),

    Cfg = base_cfg(StoreId, enabled_file(Path)),
    ?assertMatch({error, {integrity_key_invalid_size, 20}},
                 reckon_db_integrity_key:load(Cfg)),

    file:delete(Path),
    cleanup(StoreId).

%%====================================================================
%% Helpers
%%====================================================================

base_cfg(StoreId, Integrity) ->
    #store_config{
        store_id = StoreId,
        data_dir = "/tmp/reckon_db_integrity_key_test_" ++
                   atom_to_list(StoreId),
        integrity = Integrity
    }.

enabled_env(EnvName) ->
    #{enabled => true, key_source => {env_var, EnvName}}.

enabled_file(Path) ->
    #{enabled => true, key_source => {sealed_file, Path}}.

unique_store_id() ->
    list_to_atom("reckon_db_integrity_key_test_" ++ random_suffix()).

unique_env() ->
    list_to_binary("RECKON_DB_TEST_INTEGRITY_KEY_" ++ random_suffix()).

random_suffix() ->
    integer_to_list(erlang:unique_integer([positive])).

base64_encode(Bin) ->
    binary_to_list(base64:encode(Bin)).

make_sealed_file(Content) ->
    Path = tmp_path("sealed_key_" ++ random_suffix()),
    ok = filelib:ensure_dir(Path),
    ok = file:write_file(Path, Content),
    ok = file:change_mode(Path, 8#600),
    Path.

tmp_path(Name) ->
    filename:join(["/tmp", "reckon_db_integrity_key_tests", Name]).

cleanup(StoreId) ->
    reckon_db_integrity_key:clear(StoreId),
    ok.
