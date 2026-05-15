%% @doc Common Test suite for snapshot tamper-resistance.
%%
%% Covers the snapshot save and load surfaces:
%%
%%   - save: anchor_hash + mac populated when integrity is enabled;
%%     refused if no event exists at the snapshot version;
%%     pass-through for integrity-disabled stores.
%%
%%   - load: snapshot verifies against the current chain hash at
%%     the snapshot version; tampered snapshot data caught;
%%     tampered stream events caught (anchor mismatch); legacy
%%     snapshots pass through; integrity-disabled stores skip
%%     verification entirely.
%%
%% @end
-module(reckon_db_integrity_snapshots_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

-export([
    all/0,
    groups/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Save path
-export([
    save_with_integrity_populates_fields/1,
    save_on_disabled_store_is_legacy/1,
    save_refused_when_event_absent/1,
    save_refused_when_event_is_legacy/1
]).

%% Load path
-export([
    load_intact_snapshot_succeeds/1,
    tampered_snapshot_state_is_caught/1,
    tampered_snapshot_metadata_is_caught/1,
    tampered_snapshot_anchor_is_caught/1,
    tampered_snapshot_mac_is_caught/1,
    tampered_stream_event_breaks_snapshot/1,
    legacy_snapshot_passes_through_on_load/1,
    integrity_disabled_store_skips_load_verification/1
]).

%%====================================================================
%% CT boilerplate
%%====================================================================

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        {group, save_path},
        {group, load_path}
    ].

groups() ->
    [
        {save_path, [sequence], [
            save_with_integrity_populates_fields,
            save_on_disabled_store_is_legacy,
            save_refused_when_event_absent,
            save_refused_when_event_is_legacy
        ]},
        {load_path, [sequence], [
            load_intact_snapshot_succeeds,
            tampered_snapshot_state_is_caught,
            tampered_snapshot_metadata_is_caught,
            tampered_snapshot_anchor_is_caught,
            tampered_snapshot_mac_is_caught,
            tampered_stream_event_breaks_snapshot,
            legacy_snapshot_passes_through_on_load,
            integrity_disabled_store_skips_load_verification
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    RaDataDir = "/tmp/reckon_db_integrity_snapshots_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),

    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),

    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    RaDataDir = proplists:get_value(ra_data_dir, Config),
    os:cmd("rm -rf " ++ RaDataDir),
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_integrity_snapshots_" ++
              atom_to_list(TestCase) ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom(
        "integrity_snap_test_" ++ atom_to_list(TestCase) ++ "_" ++ Rand),

    {ok, _} = khepri:start(DataDir, StoreId),
    khepri:put(StoreId, [streams], #{}),
    khepri:put(StoreId, [snapshots], #{}),
    khepri:put(StoreId, [metadata], #{}),

    [{data_dir, DataDir}, {store_id, StoreId} | Config].

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    DataDir = proplists:get_value(data_dir, Config),
    catch khepri:stop(StoreId),
    reckon_db_integrity_key:clear(StoreId),
    os:cmd("rm -rf " ++ DataDir),
    ok.

%%====================================================================
%% Save path
%%====================================================================

%% Saving a snapshot on an integrity-enabled store should populate
%% anchor_hash + mac (32 bytes / {1, 32-byte} respectively).
save_with_integrity_populates_fields(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"snap-save-1">>,
    write_n_events(StoreId, StreamId, 3),

    ok = reckon_db_snapshots:save(StoreId, StreamId, 2, #{count => 3}),

    {ok, Snap} = reckon_db_snapshots:load(StoreId, StreamId),
    ?assert(is_binary(Snap#snapshot.anchor_hash)),
    ?assertEqual(32, byte_size(Snap#snapshot.anchor_hash)),
    ?assertMatch({1, _}, Snap#snapshot.mac),
    {1, MacBytes} = Snap#snapshot.mac,
    ?assertEqual(32, byte_size(MacBytes)),
    ok.

%% On an integrity-disabled store, snapshots are written with no
%% integrity fields (legacy shape).
save_on_disabled_store_is_legacy(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    %% Integrity not loaded — store is in default disabled state.
    StreamId = <<"snap-disabled">>,
    {ok, _} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"e">>, data => #{n => I}} || I <- [1, 2, 3]]),

    ok = reckon_db_snapshots:save(StoreId, StreamId, 2, #{count => 3}),
    {ok, Snap} = reckon_db_snapshots:load(StoreId, StreamId),

    ?assertEqual(undefined, Snap#snapshot.anchor_hash),
    ?assertEqual(undefined, Snap#snapshot.mac),
    ?assert(reckon_gater_integrity:is_legacy_snapshot(Snap)),
    ok.

%% If integrity is enabled and the caller tries to snapshot a
%% version for which no event exists, the save is refused — a
%% snapshot whose anchor cannot be established is worse than no
%% snapshot at all.
save_refused_when_event_absent(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"snap-no-event">>,
    %% Write only events 0, 1, 2.
    write_n_events(StoreId, StreamId, 3),
    %% Try to snapshot at version 99 — no event there.
    Result = reckon_db_snapshots:save(StoreId, StreamId, 99, #{}),
    ?assertMatch({error, {snapshot_anchor_unavailable, _}}, Result),
    ok.

%% If integrity is enabled but the underlying event at the
%% snapshot version is a legacy event (no prev_event_hash, e.g.
%% from before integrity was enabled), the save is refused. The
%% operator would need to wait for an integrity-bearing event to
%% land before taking the snapshot, OR snapshot a later version.
save_refused_when_event_is_legacy(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"snap-legacy-event">>,
    %% Write a legacy event (integrity not yet enabled).
    {ok, _} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"e">>, data => #{n => 1}}]),
    %% Now enable integrity (no new appends yet, so no watermark).
    install_random_key(StoreId),

    Result = reckon_db_snapshots:save(StoreId, StreamId, 0, #{}),
    ?assertMatch({error, {snapshot_anchor_unavailable,
                          #{reason := event_is_legacy}}},
                 Result),
    ok.

%%====================================================================
%% Load path — happy + tamper
%%====================================================================

%% Save + load roundtrip on an intact stream.
load_intact_snapshot_succeeds(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"snap-load-ok">>,
    write_n_events(StoreId, StreamId, 5),

    ok = reckon_db_snapshots:save(StoreId, StreamId, 4, #{state => intact}),
    {ok, Snap} = reckon_db_snapshots:load(StoreId, StreamId),
    ?assertEqual(4, Snap#snapshot.version),
    ?assertEqual(#{state => intact}, Snap#snapshot.data),
    ok.

%% Tampering with the snapshot's data field breaks the MAC.
tampered_snapshot_state_is_caught(Config) ->
    expect_load_violation(Config, <<"snap-t-state">>,
        fun(S) -> S#snapshot{data = #{forged => true}} end,
        snapshot_mac_mismatch).

%% Tampering with the snapshot's metadata also breaks the MAC.
tampered_snapshot_metadata_is_caught(Config) ->
    expect_load_violation(Config, <<"snap-t-meta">>,
        fun(S) -> S#snapshot{metadata = #{forged => true}} end,
        snapshot_mac_mismatch).

%% Tampering with the anchor_hash directly is caught as an anchor
%% mismatch — we compute the actual chain hash from the underlying
%% event at load time and compare. (The MAC would also catch it,
%% but the anchor check runs first and surfaces a more specific
%% failure kind.)
tampered_snapshot_anchor_is_caught(Config) ->
    expect_load_violation(Config, <<"snap-t-anchor">>,
        fun(S) -> S#snapshot{anchor_hash = <<99:256>>} end,
        snapshot_anchor_mismatch).

%% Tampering with the snapshot's MAC directly. The anchor check
%% may or may not pass (depending on whether the attacker also
%% mutated the anchor); if it does pass, the MAC check catches it.
tampered_snapshot_mac_is_caught(Config) ->
    expect_load_violation(Config, <<"snap-t-mac">>,
        fun(#snapshot{mac = {KeyId, _}} = S) ->
            S#snapshot{mac = {KeyId, <<0:256>>}}
        end,
        snapshot_mac_mismatch).

%% Tampering with the underlying stream event AFTER the snapshot
%% was taken breaks the anchor: the snapshot's recorded anchor
%% no longer matches the chain hash computed from the (now
%% tampered) event. This is the key property the anchor provides
%% over the MAC alone.
tampered_stream_event_breaks_snapshot(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"snap-t-stream">>,
    write_n_events(StoreId, StreamId, 5),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 4, #{state => intact}),

    %% Tamper the underlying event at version 4 (the snapshot's
    %% target). Use the same MAC re-signing trick as the read suite
    %% so that the per-event MAC still verifies — proving that the
    %% anchor is the load-bearing check, not the event MAC.
    Key = reckon_db_integrity_key:get(StoreId),
    PadV = pad_version_for_event(4),
    {ok, Event} = khepri:get(StoreId, [streams, StreamId, PadV]),
    Tampered = sign_event_with_key(
        Event#event{data = #{n => 99999}}, Key, Event#event.prev_event_hash),
    ok = khepri:put(StoreId, [streams, StreamId, PadV], Tampered),

    %% The snapshot's anchor still refers to the ORIGINAL event 4's
    %% chain hash. Now that event 4 has been mutated, the recomputed
    %% chain hash differs.
    Result = reckon_db_snapshots:load(StoreId, StreamId),
    ?assertMatch({error, {integrity_violation,
                          #{kind := snapshot_anchor_mismatch}}}, Result),
    ok.

%% A legacy snapshot (no integrity fields) on an integrity-enabled
%% store passes through. The caller may choose to discard it; we
%% don't refuse to return it. This matches the event-side
%% skip_legacy default.
legacy_snapshot_passes_through_on_load(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"snap-legacy">>,
    %% Write everything as legacy first.
    {ok, _} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"e">>, data => #{n => I}} || I <- [1, 2, 3]]),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 2, #{state => legacy}),

    %% Now enable integrity (no new events written).
    install_random_key(StoreId),

    %% Load returns the legacy snapshot untouched.
    {ok, Snap} = reckon_db_snapshots:load(StoreId, StreamId),
    ?assertEqual(undefined, Snap#snapshot.anchor_hash),
    ?assertEqual(undefined, Snap#snapshot.mac),
    ?assertEqual(#{state => legacy}, Snap#snapshot.data),
    ok.

%% A store with integrity disabled never verifies snapshots, even
%% if they happen to carry integrity fields somehow.
integrity_disabled_store_skips_load_verification(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"snap-disabled-load">>,
    %% Write events, save snapshot — all legacy.
    {ok, _} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"e">>, data => #{n => I}} || I <- [1, 2, 3]]),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 2, #{state => x}),

    {ok, Snap} = reckon_db_snapshots:load(StoreId, StreamId),
    ?assertEqual(#{state => x}, Snap#snapshot.data),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

setup_integrity_store(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    Key = crypto:strong_rand_bytes(32),
    persistent_term:put({reckon_db, integrity_key, StoreId}, Key),
    persistent_term:put({reckon_db, integrity_enabled, StoreId}, true),
    {StoreId, Key}.

install_random_key(StoreId) ->
    Key = crypto:strong_rand_bytes(32),
    persistent_term:put({reckon_db, integrity_key, StoreId}, Key),
    persistent_term:put({reckon_db, integrity_enabled, StoreId}, true),
    Key.

write_n_events(StoreId, StreamId, N) ->
    {ok, _} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"e">>, data => #{n => I}}
         || I <- lists:seq(1, N)]),
    ok.

%% Tamper a snapshot at the latest version, then attempt load and
%% confirm we get the expected violation kind.
expect_load_violation(Config, StreamId, TamperFun, ExpectedKind) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    write_n_events(StoreId, StreamId, 5),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 4, #{state => intact}),

    %% Reach into the snapshot store and rewrite the saved snapshot
    %% with TamperFun applied.
    {ok, Original} = reckon_db_snapshots:load(StoreId, StreamId),
    Tampered = TamperFun(Original),
    ok = put_snapshot_directly(StoreId, StreamId, 4, Tampered),

    Result = reckon_db_snapshots:load(StoreId, StreamId),
    ?assertMatch({error, {integrity_violation, #{kind := ExpectedKind}}},
                 Result),
    ok.

%% Directly overwrite a snapshot at a known version via khepri,
%% bypassing the API (which would re-compute integrity fields).
%% This mirrors what a filesystem-level attacker would do.
put_snapshot_directly(StoreId, StreamId, Version, Snapshot) ->
    PadV = pad_snapshot_version(Version),
    Path = [snapshots, StreamId, PadV],
    ok = khepri:put(StoreId, Path, Snapshot).

%% Snapshots pad version to 10 digits (per reckon_db_snapshots_store).
%% Events pad to 12 (?VERSION_PADDING). Don't conflate them.
pad_snapshot_version(Version) ->
    VersionStr = integer_to_list(Version),
    Padding = 10 - length(VersionStr),
    list_to_binary(lists:duplicate(Padding, $0) ++ VersionStr).

%% Sign an event under a specific key (for the stream-tampering test).
sign_event_with_key(Event, Key, PrevHash) ->
    %% Set prev_event_hash and recompute MAC. The chain check inside
    %% verify_event will still pass (we kept prev_event_hash), and
    %% the per-event MAC will pass (signed under the same key). What
    %% won't match: chain_hash(Event, PrevHash) — the value the
    %% snapshot anchor was computed from. That is exactly what this
    %% test exercises.
    Event1 = Event#event{prev_event_hash = PrevHash, mac = undefined,
                         signature = undefined},
    Stripped = Event1,
    Bytes = iolist_to_binary(
        reckon_gater_canonical:encode_for_mac(event, Stripped)),
    MacBytes = crypto:mac(hmac, sha256, Key, Bytes),
    Event1#event{mac = {1, MacBytes}}.

pad_version_for_event(Version) ->
    VersionStr = integer_to_list(Version),
    Padding = ?VERSION_PADDING - length(VersionStr),
    list_to_binary(lists:duplicate(Padding, $0) ++ VersionStr).
