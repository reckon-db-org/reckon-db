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
    a_restored_old_life_cannot_be_read_through_its_snapshot/1,
    a_legacy_shaped_snapshot_on_a_sealed_stream_is_refused/1,
    a_snapshot_under_a_broken_seal_is_refused/1,
    a_snapshot_list_with_no_anchor_is_refused/1,
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
            a_restored_old_life_cannot_be_read_through_its_snapshot,
            a_legacy_shaped_snapshot_on_a_sealed_stream_is_refused,
            a_snapshot_under_a_broken_seal_is_refused,
            a_snapshot_list_with_no_anchor_is_refused,
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
    %% The node-local life anchor is not optional: an integrity-enabled
    %% read path refuses when it is absent, so every suite that enables
    %% integrity runs one, as the store does.
    {ok, _} = reckon_db_life_anchor:start_link(StoreId, DataDir),
    khepri:put(StoreId, [streams], #{}),
    khepri:put(StoreId, [snapshots], #{}),
    khepri:put(StoreId, [metadata], #{}),

    [{data_dir, DataDir}, {store_id, StoreId} | Config].

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    DataDir = proplists:get_value(data_dir, Config),
    reckon_db_life_anchor:stop(StoreId),
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
    StreamId = reckon_db_test_helpers:sid(<<"snap-save-1">>),
    write_n_events(StoreId, StreamId, 3),

    ok = reckon_db_snapshots:save(StoreId, StreamId, 2, #{count => 3}),

    {ok, Snap} = reckon_db_snapshots:load(StoreId, StreamId),
    ?assert(is_binary(Snap#snapshot.anchor_hash)),
    ?assertEqual(32, byte_size(Snap#snapshot.anchor_hash)),
    ?assertMatch({v2, 1, _}, Snap#snapshot.mac),
    {v2, 1, MacBytes} = Snap#snapshot.mac,
    ?assertEqual(32, byte_size(MacBytes)),
    ok.

%% On an integrity-disabled store, snapshots are written with no
%% integrity fields (legacy shape).
save_on_disabled_store_is_legacy(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    %% Integrity not loaded — store is in default disabled state.
    StreamId = reckon_db_test_helpers:sid(<<"snap-disabled">>),
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
    StreamId = reckon_db_test_helpers:sid(<<"snap-no-event">>),
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
    StreamId = reckon_db_test_helpers:sid(<<"snap-legacy-event">>),
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
    StreamId = reckon_db_test_helpers:sid(<<"snap-load-ok">>),
    write_n_events(StoreId, StreamId, 5),

    ok = reckon_db_snapshots:save(StoreId, StreamId, 4, #{state => intact}),
    {ok, Snap} = reckon_db_snapshots:load(StoreId, StreamId),
    ?assertEqual(4, Snap#snapshot.version),
    ?assertEqual(#{state => intact}, Snap#snapshot.data),
    ok.

%% Tampering with the snapshot's data field breaks the MAC.
tampered_snapshot_state_is_caught(Config) ->
    expect_load_violation(Config, reckon_db_test_helpers:sid(<<"snap-t-state">>),
        fun(S) -> S#snapshot{data = #{forged => true}} end,
        snapshot_mac_mismatch).

%% Tampering with the snapshot's metadata also breaks the MAC.
tampered_snapshot_metadata_is_caught(Config) ->
    expect_load_violation(Config, reckon_db_test_helpers:sid(<<"snap-t-meta">>),
        fun(S) -> S#snapshot{metadata = #{forged => true}} end,
        snapshot_mac_mismatch).

%% Tampering with the anchor_hash directly is caught as an anchor
%% mismatch — we compute the actual chain hash from the underlying
%% event at load time and compare. (The MAC would also catch it,
%% but the anchor check runs first and surfaces a more specific
%% failure kind.)
tampered_snapshot_anchor_is_caught(Config) ->
    expect_load_violation(Config, reckon_db_test_helpers:sid(<<"snap-t-anchor">>),
        fun(S) -> S#snapshot{anchor_hash = <<99:256>>} end,
        snapshot_anchor_mismatch).

%% Tampering with the snapshot's MAC directly. The anchor check
%% may or may not pass (depending on whether the attacker also
%% mutated the anchor); if it does pass, the MAC check catches it.
tampered_snapshot_mac_is_caught(Config) ->
    expect_load_violation(Config, reckon_db_test_helpers:sid(<<"snap-t-mac">>),
        fun(#snapshot{mac = {v2, KeyId, _}} = S) ->
            S#snapshot{mac = {v2, KeyId, <<0:256>>}}
        end,
        snapshot_mac_mismatch).

%% Tampering with the underlying stream event AFTER the snapshot
%% was taken breaks the anchor: the snapshot's recorded anchor
%% no longer matches the chain hash computed from the (now
%% tampered) event. This is the key property the anchor provides
%% over the MAC alone.
tampered_stream_event_breaks_snapshot(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"snap-t-stream">>),
    write_n_events(StoreId, StreamId, 5),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 4, #{state => intact}),

    %% Tamper the underlying event at version 4 (the snapshot's
    %% target). Use the same MAC re-signing trick as the read suite
    %% so that the per-event MAC still verifies — proving that the
    %% anchor is the load-bearing check, not the event MAC.
    Key = reckon_db_integrity_key:get(StoreId),
    PadV = pad_version_for_event(4),
    {ok, Event} = khepri:get(StoreId, reckon_db_stream_path:event_path(StreamId, PadV)),
    Tampered = sign_event_with_key(
        Event#event{data = #{n => 99999}}, Key, Event#event.prev_event_hash),
    ok = khepri:put(StoreId, reckon_db_stream_path:event_path(StreamId, PadV), Tampered),

    %% The snapshot's anchor still refers to the ORIGINAL event 4's
    %% chain hash. Now that event 4 has been mutated, the recomputed
    %% chain hash differs.
    Result = reckon_db_snapshots:load(StoreId, StreamId),
    ?assertMatch({error, {integrity_violation,
                          #{kind := snapshot_anchor_mismatch}}}, Result),
    ok.

%% Terra T21-2. The snapshot load path was a targeted read that skipped the
%% node-local anchor. A snapshot's MAC binds the life its stream carries, so an
%% attacker who restores an older life's seal, life token and events AND the
%% snapshot taken under it holds a set that verifies against itself: the stream
%% read refused, and `load' served the old life's state.
a_restored_old_life_cannot_be_read_through_its_snapshot(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"snap-old-life">>),
    write_n_events(StoreId, StreamId, 5),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 4, #{state => old}),
    {ok, OldSnapshot} = reckon_db_snapshots:load(StoreId, StreamId),
    {ok, OldLife} = reckon_db_chain_watermark:life(StoreId, StreamId),
    {ok, OldSeal} = khepri:get(StoreId,
                               reckon_db_chain_watermark:path(StreamId)),
    {ok, Events} = reckon_db_streams:read(StoreId, StreamId, 0, 100, forward),
    OldEvents = [{E#event.version, E} || E <- Events],

    %% Delete, then re-create: the stream now carries generation 2.
    ok = reckon_db_streams:delete(StoreId, StreamId),
    write_n_events(StoreId, StreamId, 2),

    %% Every part of the older life goes back: seal, life token, events, and
    %% the snapshot that was taken under it.
    ok = khepri:put(StoreId, reckon_db_chain_watermark:path(StreamId), OldSeal),
    ok = khepri:put(StoreId, reckon_db_chain_watermark:life_path(StreamId),
                    OldLife),
    ok = khepri:put(StoreId, reckon_db_stream_path:life_gen_path(StreamId), 1),
    [ok = khepri:put(StoreId,
                     reckon_db_stream_path:event_path(StreamId,
                                                      pad_version_for_event(V)),
                     E) || {V, E} <- OldEvents],
    ok = put_snapshot_directly(StoreId, StreamId, 4, OldSnapshot),

    ?assertMatch({error, {integrity_violation, _}},
                 reckon_db_streams:read(StoreId, StreamId, 0, 3, forward)),
    ?assertMatch({error, {integrity_violation,
                          #{context := #{detail := life_gen_rolled_back}}}},
                 reckon_db_snapshots:load(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, _}},
                 reckon_db_snapshots:load_at(StoreId, StreamId, 4)),
    %% ... and the list is not a way around either check (reckon-db#22). The
    %% anchor's answer is about the STREAM, so it refuses the whole list rather
    %% than answering "no snapshots" (Jupiter J23-3).
    ?assertMatch({error, {integrity_violation,
                          #{context := #{detail := life_gen_rolled_back}}}},
                 reckon_db_snapshots:list(StoreId, StreamId)),
    ok.

%% Terra T22-2. A snapshot with no anchor and no MAC is legacy data - but not
%% on a stream that HAS a seal: a tree writer who replaces the record with that
%% shape and a forged payload was served by both load paths, because the
%% "legacy" answer skipped the MAC check AND the life anchor.
a_legacy_shaped_snapshot_on_a_sealed_stream_is_refused(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"snap-legacy-shaped">>),
    write_n_events(StoreId, StreamId, 5),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 4, #{balance => 10}),
    {ok, Honest} = reckon_db_snapshots:load(StoreId, StreamId),

    Forged = Honest#snapshot{data = #{balance => 1000000},
                             anchor_hash = undefined, mac = undefined},
    ok = put_snapshot_directly(StoreId, StreamId, 4, Forged),

    ?assertMatch({error, {integrity_violation, _}},
                 reckon_db_snapshots:load(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, _}},
                 reckon_db_snapshots:load_at(StoreId, StreamId, 4)),
    ?assertEqual({ok, []}, reckon_db_snapshots:list(StoreId, StreamId)),
    ok.

%% Jupiter J23-1 / Terra T23-1. The anchor answers for EVERY snapshot on an
%% integrity store, legacy-shaped or not. "Must carry a MAC" used to be decided
%% by binding/2, which answers undefined for a seal that was removed or whose
%% MAC was flipped - so a tree writer who broke the seal turned a forged
%% legacy-shaped snapshot into "legacy data" and the anchor was skipped with it,
%% while the anchor still held the stream's life. A seal a broken tree cannot
%% vouch for is a violation, not a legacy stream.
a_snapshot_under_a_broken_seal_is_refused(Config) ->
    [{_, _} = broken_seal_case(Config, Label, Break)
     || {Label, Break} <- [{<<"snap-seal-mac-flipped">>, fun flip_seal_mac/2},
                           {<<"snap-seal-removed">>, fun remove_seal/2}]],
    ok.

%% @private The control (intact seal) is the test above; here the seal is
%% broken before the forged record is read.
broken_seal_case(Config, Label, Break) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(Label),
    write_n_events(StoreId, StreamId, 5),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 4, #{balance => 10}),
    {ok, Honest} = reckon_db_snapshots:load(StoreId, StreamId),

    Forged = Honest#snapshot{data = #{balance => 1000000},
                             anchor_hash = undefined, mac = undefined},
    ok = put_snapshot_directly(StoreId, StreamId, 4, Forged),
    %% Control: with the seal intact the forgery is refused.
    ?assertMatch({error, {integrity_violation, _}},
                 reckon_db_snapshots:load(StoreId, StreamId)),

    ok = Break(StoreId, StreamId),

    ?assertMatch({error, {integrity_violation, _}},
                 reckon_db_snapshots:load(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, _}},
                 reckon_db_snapshots:load_at(StoreId, StreamId, 4)),
    ?assertMatch({error, {integrity_violation, _}},
                 reckon_db_snapshots:list(StoreId, StreamId)),
    {StoreId, StreamId}.

%% @private A tree writer's cheapest move: take the seal node away.
remove_seal(StoreId, StreamId) ->
    khepri:delete(StoreId, reckon_db_chain_watermark:path(StreamId)).

%% @private
flip_seal_mac(StoreId, StreamId) ->
    Path = reckon_db_chain_watermark:path(StreamId),
    {ok, #{mac := <<First, Rest/binary>>} = Seal} = khepri:get(StoreId, Path),
    ok = khepri:put(StoreId, Path,
                    Seal#{mac => <<(First bxor 16#ff), Rest/binary>>}).

%% Jupiter J23-3. The anchor's answer is about the STREAM, so an anchor that
%% cannot answer refuses the whole list. It used to answer {ok, []} on the same
%% store where load refused: a caller (the gateway's list_snapshots) was told
%% the stream has no snapshots when it had one.
a_snapshot_list_with_no_anchor_is_refused(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"snap-list-no-anchor">>),
    write_n_events(StoreId, StreamId, 5),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 4, #{state => kept}),

    reckon_db_life_anchor:stop(StoreId),

    ?assertMatch({error, {integrity_violation,
                          #{context := #{detail := life_anchor_unavailable}}}},
                 reckon_db_snapshots:load(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation,
                          #{context := #{detail := life_anchor_unavailable}}}},
                 reckon_db_snapshots:list(StoreId, StreamId)),
    ok.

%% A legacy snapshot (no integrity fields) on an integrity-enabled
%% store passes through. The caller may choose to discard it; we
%% don't refuse to return it. This matches the event-side
%% skip_legacy default.
legacy_snapshot_passes_through_on_load(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"snap-legacy">>),
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
    StreamId = reckon_db_test_helpers:sid(<<"snap-disabled-load">>),
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
