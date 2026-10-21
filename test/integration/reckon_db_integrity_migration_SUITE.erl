%% @doc The one-time watermark migration, at store start.
%%
%% An earlier version wrote a stream's chain-start watermark as a plain
%% integer, and this version refuses one, because anybody who can write the
%% tree can write an integer. Refusing it without a migration means an
%% upgraded store stops serving: every append to those streams fails and
%% every read is a violation until somebody runs a command by hand. These
%% cases pin the migration down, and pin down what it will not do.
-module(reckon_db_integrity_migration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

-export([
    all/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    a_bare_watermark_is_sealed_at_store_start/1,
    a_stream_with_no_watermark_is_sealed_at_store_start/1,
    a_bare_integer_after_the_migration_is_refused/1,
    a_watermark_above_the_stream_is_not_sealed/1,
    a_watermark_over_a_chain_is_not_sealed/1,
    a_deleted_marker_does_not_reseal/1,
    a_deleted_watermark_is_not_minted_after_the_marker/1
]).

-define(KEY_ENV, "RECKON_DB_MIGRATION_TEST_KEY").

all() ->
    [
        a_bare_watermark_is_sealed_at_store_start,
        a_stream_with_no_watermark_is_sealed_at_store_start,
        a_bare_integer_after_the_migration_is_refused,
        a_watermark_above_the_stream_is_not_sealed,
        a_watermark_over_a_chain_is_not_sealed,
        a_deleted_marker_does_not_reseal,
        a_deleted_watermark_is_not_minted_after_the_marker
    ].

suite() ->
    [{timetrap, {seconds, 120}}].

init_per_suite(Config) ->
    case application:ensure_all_started(reckon_db) of
        {ok, _} -> ok;
        {error, {already_started, reckon_db}} -> ok
    end,
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_migration_" ++ atom_to_list(TestCase) ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom("migration_" ++ atom_to_list(TestCase) ++ "_" ++ Rand),
    os:putenv(?KEY_ENV, binary_to_list(base64:encode(crypto:strong_rand_bytes(32)))),
    [{data_dir, DataDir}, {store_id, StoreId} | Config].

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    _ = catch reckon_db_sup:stop_store(StoreId),
    os:unsetenv(?KEY_ENV),
    os:cmd("rm -rf " ++ proplists:get_value(data_dir, Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% @doc GIVEN a store written by an earlier version, whose stream carries a
%%          bare-integer watermark at the version integrity began
%%      WHEN it is started by this version, with integrity enabled
%%      THEN it serves reads and accepts appends with no hand step
%%
%% This is the whole point of the migration: the upgrade is an ordinary
%% restart, not a maintenance window with an operator command in it.
a_bare_watermark_is_sealed_at_store_start(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"legacy-with-watermark">>),

    ok = start_store(StoreId, DataDir, disabled),
    ok = write_legacy(StoreId, StreamId, 5),
    ok = put_bare_watermark(StoreId, StreamId, 5),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),

    {ok, Events} = reckon_db_streams:read(StoreId, StreamId, 0, 100, forward),
    ?assertEqual(5, length(Events)),
    ?assertMatch({ok, {sealed, 5}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),

    {ok, 5} = reckon_db_streams:append(StoreId, StreamId, 4,
                                       [#{event_type => <<"e">>,
                                          data => #{n => 6}}]),
    ok.

%% @doc GIVEN a store written by an earlier version, whose stream has events
%%          from before integrity and no watermark at all
%%      WHEN it is started by this version
%%      THEN the stream reads, sealed at the version its chain begins
a_stream_with_no_watermark_is_sealed_at_store_start(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"legacy-no-watermark">>),

    ok = start_store(StoreId, DataDir, disabled),
    ok = write_legacy(StoreId, StreamId, 3),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),

    {ok, Events} = reckon_db_streams:read(StoreId, StreamId, 0, 100, forward),
    ?assertEqual(3, length(Events)),
    ?assertMatch({ok, {sealed, 3}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)).

%% @doc GIVEN a store whose migration has run
%%      WHEN a bare integer is written afterwards
%%      THEN it is still refused, and nothing seals it
%%
%% The migration is once. After the marker, a bare integer is not a leftover
%% from an older version, it is a forgery, and the read path refuses it the
%% same way it refuses one on a store that was never migrated.
a_bare_integer_after_the_migration_is_refused(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"planted-after">>),

    ok = start_store(StoreId, DataDir, disabled),
    ok = write_legacy(StoreId, StreamId, 3),
    ok = stop_store(StoreId),
    ok = start_store(StoreId, DataDir, enabled),

    ok = clear_integrity_fields(StoreId, StreamId, 0),
    ok = clear_integrity_fields(StoreId, StreamId, 1),
    ok = clear_integrity_fields(StoreId, StreamId, 2),
    ok = rewrite_event(StoreId, StreamId, 1, #{tampered => 1}),
    ok = put_bare_watermark(StoreId, StreamId, 1000),

    ?assertMatch({error, {integrity_violation,
                          #{kind := missing_integrity,
                            context := #{detail := chain_start_unauthenticated}}}},
                 read_raw(StoreId, StreamId)),
    ?assertMatch({error, {chain_start_unauthenticated, 1000}},
                 reckon_db_streams:append(StoreId, StreamId, 2,
                                          [#{event_type => <<"e">>, data => #{}}])).

%% @doc GIVEN a bare watermark that sits above every event in the stream
%%      WHEN the migration runs
%%      THEN it is not sealed, and the stream stays refused
%%
%% A genuine watermark was written at the version integrity began, so it can
%% never be above the newest event. One that is, was put there by hand.
a_watermark_above_the_stream_is_not_sealed(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"watermark-above">>),

    ok = start_store(StoreId, DataDir, disabled),
    ok = write_legacy(StoreId, StreamId, 3),
    ok = put_bare_watermark(StoreId, StreamId, 1000),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),

    ?assertMatch({ok, {unsealed, 1000}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, #{kind := missing_integrity}}},
                 read_raw(StoreId, StreamId)).

%% @doc GIVEN events that carry a chain and a bare watermark that would
%%           excuse them
%%      WHEN the migration runs
%%      THEN it is not sealed
%%
%% A genuine legacy watermark never has a hash-bearing event below it: the
%% hash is the evidence that integrity had already begun. Sealing this one
%% would authenticate a stream that says two contradictory things about
%% itself.
a_watermark_over_a_chain_is_not_sealed(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"watermark-over-chain">>),

    ok = start_store(StoreId, DataDir, disabled),
    ok = write_legacy(StoreId, StreamId, 5),
    ok = stop_store(StoreId),
    ok = start_store(StoreId, DataDir, enabled),
    {ok, 9} = reckon_db_streams:append(StoreId, StreamId, 4,
                                        [#{event_type => <<"e">>,
                                           data => #{n => N}}
                                         || N <- lists:seq(6, 10)]),
    ok = forget_migration(StoreId),
    ok = put_bare_watermark(StoreId, StreamId, 8),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),

    ?assertMatch({ok, {unsealed, 8}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, #{kind := missing_integrity}}},
                 read_raw(StoreId, StreamId)).

%% @doc GIVEN a migrated store whose marker has been deleted
%%      WHEN a writer strips a stream, overwrites its watermark with a bare
%%           integer and restarts
%%      THEN nothing is sealed: a sealed watermark anywhere means the store
%%           has already been migrated
%%
%% Deleting the marker is not a way to make the migration run again. That
%% would be the ideal attack: strip a stream, delete the marker, and let the
%% next boot vouch for the result. The store's other sealed watermarks are
%% the evidence it was migrated, and they are what this checks.
%%
%% The residual, which the docs say out loud: a writer who overwrites EVERY
%% sealed watermark in the store as well has removed all of that evidence,
%% and no in-tree signal can tell that store from one that was never
%% migrated. That capability is exactly what the previous version gave them,
%% so nothing is widened; what is gone is the ability to do it with one write
%% to one stream.
a_deleted_marker_does_not_reseal(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"marker-deleted">>),
    OtherStream = stream(<<"marker-deleted-other">>),

    ok = start_store(StoreId, DataDir, disabled),
    ok = write_legacy(StoreId, StreamId, 3),
    ok = stop_store(StoreId),
    ok = start_store(StoreId, DataDir, enabled),

    %% A second stream that carries integrity events, so the store holds a
    %% sealed watermark the attack does not touch.
    {ok, 4} = reckon_db_streams:append(StoreId, OtherStream, ?NO_STREAM,
                                       [#{event_type => <<"e">>,
                                          data => #{n => N}}
                                        || N <- lists:seq(1, 5)]),
    {ok, _} = reckon_db_streams:append(StoreId, StreamId, 2,
                                       [#{event_type => <<"e">>,
                                          data => #{n => 4}}]),
    ok = forget_migration(StoreId),
    ok = clear_integrity_fields(StoreId, StreamId, 0),
    ok = clear_integrity_fields(StoreId, StreamId, 1),
    ok = clear_integrity_fields(StoreId, StreamId, 2),
    ok = clear_integrity_fields(StoreId, StreamId, 3),
    ok = put_bare_watermark(StoreId, StreamId, 4),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),

    ?assertMatch({ok, {sealed, 0}},
                 reckon_db_chain_watermark:lookup(StoreId, OtherStream)),
    ?assertMatch({ok, {unsealed, 4}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, #{kind := missing_integrity}}},
                 read_raw(StoreId, StreamId)).

%% @doc GIVEN a migrated store
%%      WHEN a stream's sealed watermark is DELETED and an append runs
%%      THEN the append does not mint a new one, and the stream stays refused
%%
%% The runtime rule that closes the same hole from the other side: only a new
%% stream at version 0 gets a watermark minted by an append. Without it, one
%% append would vouch for a chain start over events somebody had rewritten.
a_deleted_watermark_is_not_minted_after_the_marker(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"deleted-after-marker">>),

    ok = start_store(StoreId, DataDir, enabled),
    {ok, 2} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM,
                                       [#{event_type => <<"e">>,
                                          data => #{n => N}}
                                        || N <- lists:seq(1, 3)]),
    ok = clear_integrity_fields(StoreId, StreamId, 0),
    ok = clear_integrity_fields(StoreId, StreamId, 1),
    ok = clear_integrity_fields(StoreId, StreamId, 2),
    ok = rewrite_event(StoreId, StreamId, 2, #{tampered => 1}),
    ok = khepri:delete(StoreId, watermark_path(StreamId)),

    ?assertMatch({error, {chain_start_missing, _}},
                 reckon_db_streams:append(StoreId, StreamId, 2,
                                          [#{event_type => <<"e">>, data => #{}}])),
    ?assertMatch({ok, undefined},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, #{kind := missing_integrity}}},
                 read_raw(StoreId, StreamId)).

%%====================================================================
%% Helpers
%%====================================================================

store(Config) ->
    {proplists:get_value(store_id, Config), proplists:get_value(data_dir, Config)}.

stream(Name) ->
    reckon_db_test_helpers:sid(Name).

start_store(StoreId, DataDir, Integrity) ->
    Config = #store_config{store_id = StoreId,
                           data_dir = DataDir,
                           mode = single,
                           writer_pool_size = 1,
                           reader_pool_size = 1,
                           gateway_pool_size = 1,
                           integrity = integrity_config(Integrity),
                           options = #{}},
    case reckon_db_sup:start_store(Config) of
        {ok, _} -> ok;
        {error, Reason} -> ct:fail("store ~p did not start: ~p", [StoreId, Reason])
    end.

integrity_config(disabled) -> disabled;
integrity_config(enabled) ->
    #{enabled => true, key_source => {env_var, list_to_binary(?KEY_ENV)}}.

stop_store(StoreId) ->
    _ = catch reckon_db_sup:stop_store(StoreId),
    ok.

%% Events written while integrity is off: no chain, no MAC, no watermark.
write_legacy(StoreId, StreamId, N) ->
    {ok, _} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"e">>, data => #{n => I}} || I <- lists:seq(1, N)]),
    ok.

put_bare_watermark(StoreId, StreamId, Version) ->
    khepri:put(StoreId, watermark_path(StreamId), Version).

forget_migration(StoreId) ->
    khepri:delete(StoreId, ?INTEGRITY_MIGRATION_PATH).

watermark_path(StreamId) ->
    ?INTEGRITY_CHAIN_START_PATH ++ [StreamId].

%% Reads with the store's own verification, which is what a consumer sees.
read_raw(StoreId, StreamId) ->
    reckon_db_streams:read(StoreId, StreamId, 0, 100, forward).

clear_integrity_fields(StoreId, StreamId, Version) ->
    Path = event_path(StreamId, Version),
    {ok, Event} = khepri:get(StoreId, Path),
    ok = khepri:put(StoreId, Path, Event#event{prev_event_hash = undefined,
                                               mac = undefined}).

rewrite_event(StoreId, StreamId, Version, Data) ->
    Path = event_path(StreamId, Version),
    {ok, Event} = khepri:get(StoreId, Path),
    ok = khepri:put(StoreId, Path, Event#event{data = Data}).

event_path(StreamId, Version) ->
    reckon_db_stream_path:event_path(StreamId, pad_version(Version)).

pad_version(Version) ->
    list_to_binary(io_lib:format("~*..0B", [?VERSION_PADDING, Version])).
