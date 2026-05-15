%% @doc Common Test suite for the tamper-resistance read path.
%%
%% Covers what a hostile or accidentally-corrupted on-disk state
%% should look like to the reader. For each attack vector we plant a
%% mutation directly via khepri:put (bypassing the public API), then
%% verify the read surfaces an integrity_violation.
%%
%% Also exercises the verify mode matrix (skip_legacy, strict,
%% skip_all) so the configuration knobs are tested as well as the
%% detection itself.
%%
%% @end
-module(reckon_db_integrity_reads_SUITE).

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

%% Happy path
-export([
    intact_chain_reads_clean/1,
    read_single_event_at_chain_start/1,
    read_from_middle_of_chain/1
]).

%% Per-field tamper detection
-export([
    tampered_data_is_caught/1,
    tampered_metadata_is_caught/1,
    tampered_event_type_is_caught/1,
    tampered_tags_is_caught/1,
    tampered_timestamp_is_caught/1,
    tampered_mac_is_caught/1,
    tampered_prev_event_hash_is_caught/1,
    cleared_integrity_fields_treated_as_legacy/1
]).

%% Chain-structural tamper detection
-export([
    deleted_middle_event_breaks_chain/1,
    inserted_forged_event_breaks_chain/1,
    swapped_two_adjacent_events_caught/1
]).

%% Verify mode matrix
-export([
    skip_legacy_passes_legacy_events_through/1,
    strict_rejects_legacy_events/1,
    skip_all_returns_tampered_events/1
]).

%% Boundary + mixed-stream cases
-export([
    legacy_and_integrity_events_in_same_stream/1,
    backward_read_bypasses_verification/1,
    integrity_disabled_store_bypasses_verification/1
]).

%%====================================================================
%% CT boilerplate
%%====================================================================

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        {group, happy_path},
        {group, per_field_tampering},
        {group, structural_tampering},
        {group, verify_modes},
        {group, boundary_cases}
    ].

groups() ->
    [
        {happy_path, [sequence], [
            intact_chain_reads_clean,
            read_single_event_at_chain_start,
            read_from_middle_of_chain
        ]},
        {per_field_tampering, [sequence], [
            tampered_data_is_caught,
            tampered_metadata_is_caught,
            tampered_event_type_is_caught,
            tampered_tags_is_caught,
            tampered_timestamp_is_caught,
            tampered_mac_is_caught,
            tampered_prev_event_hash_is_caught,
            cleared_integrity_fields_treated_as_legacy
        ]},
        {structural_tampering, [sequence], [
            deleted_middle_event_breaks_chain,
            inserted_forged_event_breaks_chain,
            swapped_two_adjacent_events_caught
        ]},
        {verify_modes, [sequence], [
            skip_legacy_passes_legacy_events_through,
            strict_rejects_legacy_events,
            skip_all_returns_tampered_events
        ]},
        {boundary_cases, [sequence], [
            legacy_and_integrity_events_in_same_stream,
            backward_read_bypasses_verification,
            integrity_disabled_store_bypasses_verification
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    RaDataDir = "/tmp/reckon_db_integrity_reads_test_ra",
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
    DataDir = "/tmp/reckon_db_integrity_reads_" ++
              atom_to_list(TestCase) ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom(
        "integrity_reads_test_" ++ atom_to_list(TestCase) ++ "_" ++ Rand),

    {ok, _} = khepri:start(DataDir, StoreId),
    khepri:put(StoreId, [streams], #{}),
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
%% Happy path
%%====================================================================

%% Intact chain of N events reads cleanly with no errors.
intact_chain_reads_clean(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"clean-chain">>,
    write_n_events(StoreId, StreamId, 10),

    {ok, Events} = reckon_db_streams:read(StoreId, StreamId, 0, 100, forward),
    ?assertEqual(10, length(Events)),
    [?assert(is_binary(E#event.prev_event_hash)) || E <- Events],
    [?assertMatch({1, _}, E#event.mac) || E <- Events],
    ok.

%% Reading exactly one event at version 0 (the chain start) should
%% succeed and that event's prev_event_hash should equal genesis.
read_single_event_at_chain_start(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"single">>,
    write_n_events(StoreId, StreamId, 1),

    {ok, [Event]} = reckon_db_streams:read(StoreId, StreamId, 0, 1, forward),
    ?assertEqual(reckon_gater_integrity:genesis_prev_hash(),
                 Event#event.prev_event_hash),
    ok.

%% Reading from version > 0 still succeeds when the predecessor is
%% present and intact. This exercises the resolve_read_initial_tip
%% codepath that reads the predecessor from storage.
read_from_middle_of_chain(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"middle">>,
    write_n_events(StoreId, StreamId, 5),

    {ok, Events} = reckon_db_streams:read(StoreId, StreamId, 2, 3, forward),
    ?assertEqual(3, length(Events)),
    [?assertEqual(V, E#event.version) ||
        {V, E} <- lists:zip([2, 3, 4], Events)],
    ok.

%%====================================================================
%% Per-field tamper detection
%%====================================================================

tampered_data_is_caught(Config) ->
    expect_mac_mismatch(Config, <<"t-data">>,
        fun(E) -> E#event{data = #{tampered => true}} end).

tampered_metadata_is_caught(Config) ->
    expect_mac_mismatch(Config, <<"t-meta">>,
        fun(E) -> E#event{metadata = #{forged => true}} end).

tampered_event_type_is_caught(Config) ->
    expect_mac_mismatch(Config, <<"t-type">>,
        fun(E) -> E#event{event_type = <<"forged_type">>} end).

tampered_tags_is_caught(Config) ->
    expect_mac_mismatch(Config, <<"t-tags">>,
        fun(E) -> E#event{tags = [<<"forged">>]} end).

tampered_timestamp_is_caught(Config) ->
    expect_mac_mismatch(Config, <<"t-ts">>,
        fun(E) -> E#event{timestamp = 99999999999} end).

tampered_mac_is_caught(Config) ->
    expect_mac_mismatch(Config, <<"t-mac">>,
        fun(#event{mac = {KeyId, _}} = E) ->
            E#event{mac = {KeyId, <<0:256>>}}
        end).

%% Tampering with prev_event_hash breaks the chain check (not the MAC
%% check, since prev_event_hash is INSIDE the MAC computation - so
%% mutating it should actually break BOTH checks). The chain check
%% runs first, so we expect chain_mismatch as the failure kind.
tampered_prev_event_hash_is_caught(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"t-prev">>,
    write_n_events(StoreId, StreamId, 3),

    %% Tamper event at version 1: change its prev_event_hash to junk.
    tamper_event(StoreId, StreamId, 1,
        fun(E) -> E#event{prev_event_hash = <<7:256>>} end),

    Result = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),
    ?assertMatch({error, {integrity_violation,
                  #{kind := chain_mismatch, version := 1}}},
                 Result).

%% Clearing all integrity fields makes an event LOOK legacy. With
%% skip_legacy mode this should NOT error (the event is in the
%% integrity regime per its version vs watermark, but the legacy
%% predicate kicks in on the field shape).
%%
%% Actually we want strict detection here - the watermark says this
%% version MUST be integrity-bearing, so missing fields should be
%% caught.
cleared_integrity_fields_treated_as_legacy(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"t-cleared">>,
    write_n_events(StoreId, StreamId, 3),

    tamper_event(StoreId, StreamId, 1,
        fun(E) -> E#event{prev_event_hash = undefined,
                          mac = undefined,
                          signature = undefined}
        end),

    %% Under skip_legacy this is technically classified as a legacy
    %% event by version vs watermark. The is_legacy_event/2 predicate
    %% uses version < ChainStart - and version 1 >= ChainStart (0), so
    %% it's NOT legacy by predicate, and the verifier WILL be invoked,
    %% which then surfaces missing_integrity.
    Result = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),
    ?assertMatch({error, {integrity_violation,
                  #{kind := missing_integrity, version := 1}}},
                 Result).

%%====================================================================
%% Structural tamper detection
%%====================================================================

%% Delete the event at version 1. The read returns events [0, 2, 3, ...]
%% — event 2's prev_event_hash points at chain_hash(event 1) but the
%% running tip after event 0 is chain_hash(event 0). The chain check
%% on event 2 fails.
deleted_middle_event_breaks_chain(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"t-deleted">>,
    write_n_events(StoreId, StreamId, 5),

    %% Delete event at version 1 directly via khepri.
    PaddedV1 = pad_version(1, ?VERSION_PADDING),
    Path = [streams, StreamId, PaddedV1],
    ok = khepri:delete(StoreId, Path),

    Result = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),
    %% Either the read returns the surviving events with a chain
    %% mismatch on the next one we touch (version 2), or - if
    %% the storage layer returns events with a gap - it would
    %% still surface as chain_mismatch.
    ?assertMatch({error, {integrity_violation, #{kind := chain_mismatch}}}, Result).

%% Insert a fully-forged event at version 1 (the attacker overwrites
%% a legitimate event with one whose MAC was computed under a
%% different key). The MAC check catches it.
inserted_forged_event_breaks_chain(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"t-inserted">>,
    write_n_events(StoreId, StreamId, 5),

    %% Build a fake event using a DIFFERENT key — the MAC won't match.
    AttackerKey = crypto:strong_rand_bytes(32),
    {ok, [_E0, RealE1 | _]} = reckon_db_streams:read(
        StoreId, StreamId, 0, 10, forward),
    %% Build a tampered version of RealE1 signed under the attacker key.
    BadE1 = sign_with_key(
        RealE1#event{data = #{forged => true}}, AttackerKey),

    PaddedV1 = pad_version(1, ?VERSION_PADDING),
    Path = [streams, StreamId, PaddedV1],
    ok = khepri:put(StoreId, Path, BadE1),

    Result = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),
    ?assertMatch({error, {integrity_violation, #{kind := mac_mismatch, version := 1}}},
                 Result).

%% Swap events at versions 1 and 2. Each individual event's MAC
%% still verifies (they are real events, written by the legitimate
%% writer), but the CHAIN breaks - event 2's prev_event_hash points
%% at chain_hash(event 1), but after swap the running tip after
%% reading event-now-at-position-1 (which is the original event 2)
%% does not match what the original event 1 (now at position 2)
%% expected.
%%
%% Plus: the events have stream_id stored in the path - which we
%% are NOT changing here - and version stored on the record. The
%% version on the moved events does not match the path, but the
%% reader uses path-derived versions. Let me just verify that the
%% chain breaks.
swapped_two_adjacent_events_caught(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"t-swapped">>,
    write_n_events(StoreId, StreamId, 5),

    {ok, [_E0, E1, E2 | _]} = reckon_db_streams:read(
        StoreId, StreamId, 0, 10, forward),

    PaddedV1 = pad_version(1, ?VERSION_PADDING),
    PaddedV2 = pad_version(2, ?VERSION_PADDING),
    %% Put E2 at version 1's slot and vice versa.
    ok = khepri:put(StoreId, [streams, StreamId, PaddedV1], E2),
    ok = khepri:put(StoreId, [streams, StreamId, PaddedV2], E1),

    Result = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),
    %% Either chain_mismatch (the structural check) or mac_mismatch
    %% (because the swapped event records carry a `version` field
    %% that no longer matches their path position, and MAC was
    %% computed over the original version).
    ?assertMatch({error, {integrity_violation, #{}}}, Result),
    {error, {integrity_violation, ViolationMap}} = Result,
    Kind = maps:get(kind, ViolationMap),
    ?assert(lists:member(Kind, [chain_mismatch, mac_mismatch]),
            Kind).

%%====================================================================
%% Verify mode matrix
%%====================================================================

%% A legacy event in the legacy region (version < watermark) passes
%% through unchanged in skip_legacy mode.
skip_legacy_passes_legacy_events_through(Config) ->
    {StoreId, _Key} = setup_integrity_store_with_legacy_prefix(Config, 3),
    StreamId = <<"mixed-skip-legacy">>,
    write_legacy_events(StoreId, StreamId, 3),
    %% Now enable integrity (set watermark to 3) and write 2 more.
    {ok, _} = reckon_db_chain_watermark:set_if_absent(StoreId, StreamId, 3),
    write_n_events_starting_at(StoreId, StreamId, 3, 2),

    {ok, Events} = reckon_db_streams:read(
        StoreId, StreamId, 0, 10, forward,
        #{verify => skip_legacy}),
    ?assertEqual(5, length(Events)),
    %% First 3 are legacy (no integrity fields), last 2 have them.
    LegacyEvents = lists:sublist(Events, 3),
    IntegrityEvents = lists:nthtail(3, Events),
    [?assertEqual(undefined, E#event.mac) || E <- LegacyEvents],
    [?assertMatch({1, _}, E#event.mac) || E <- IntegrityEvents],
    ok.

%% Strict mode refuses to return legacy events when integrity is
%% enabled on the store. The scenario simulates: legacy data exists
%% from before integrity was enabled; an operator then enables
%% integrity but configures strict reads. Any read that touches the
%% legacy region must error out.
strict_rejects_legacy_events(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"strict-test">>,
    %% First: integrity disabled, write some legacy events.
    write_legacy_events(StoreId, StreamId, 3),
    %% Now enable integrity on the store. No watermark gets set
    %% because no integrity-bearing append has happened yet; that
    %% means lookup returns undefined, which the integrity-enabled
    %% read path treats as "the entire stream is legacy."
    Key = crypto:strong_rand_bytes(32),
    persistent_term:put({reckon_db, integrity_key, StoreId}, Key),
    persistent_term:put({reckon_db, integrity_enabled, StoreId}, true),

    Result = reckon_db_streams:read(
        StoreId, StreamId, 0, 10, forward,
        #{verify => strict}),
    ?assertMatch({error, {integrity_violation, #{kind := missing_integrity}}}, Result).

%% skip_all is an escape hatch: even tampered events come back.
%% Documented as dangerous; tested only to confirm the mode behaves
%% as advertised.
skip_all_returns_tampered_events(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"skip-all">>,
    write_n_events(StoreId, StreamId, 3),
    tamper_event(StoreId, StreamId, 1,
        fun(E) -> E#event{data = #{forged => true}} end),

    %% Strict-default would fail; skip_all returns regardless.
    {ok, Events} = reckon_db_streams:read(
        StoreId, StreamId, 0, 10, forward,
        #{verify => skip_all}),
    ?assertEqual(3, length(Events)),
    ok.

%%====================================================================
%% Boundary cases
%%====================================================================

%% A stream that contains BOTH legacy and integrity events (because
%% integrity was enabled mid-life) reads cleanly under skip_legacy.
legacy_and_integrity_events_in_same_stream(Config) ->
    {StoreId, _Key} = setup_integrity_store_with_legacy_prefix(Config, 2),
    StreamId = <<"mixed">>,
    write_legacy_events(StoreId, StreamId, 2),
    {ok, _} = reckon_db_chain_watermark:set_if_absent(StoreId, StreamId, 2),
    write_n_events_starting_at(StoreId, StreamId, 2, 3),

    {ok, Events} = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),
    ?assertEqual(5, length(Events)),
    %% Verify boundary: event at watermark version has genesis prev_hash.
    ChainStartEvent = lists:nth(3, Events), %% version 2
    ?assertEqual(reckon_gater_integrity:genesis_prev_hash(),
                 ChainStartEvent#event.prev_event_hash),
    ok.

%% Backward reads bypass verification in 2.1.0 (documented gap).
%% A tampered event in a backward read returns successfully.
backward_read_bypasses_verification(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = <<"backward">>,
    write_n_events(StoreId, StreamId, 3),
    tamper_event(StoreId, StreamId, 1,
        fun(E) -> E#event{data = #{forged => true}} end),

    %% Forward read catches it...
    ?assertMatch({error, {integrity_violation, _}},
        reckon_db_streams:read(StoreId, StreamId, 0, 10, forward)),
    %% ...but backward read does not (known gap).
    {ok, _} = reckon_db_streams:read(StoreId, StreamId, 2, 3, backward),
    ok.

%% A store with integrity disabled bypasses verification entirely,
%% regardless of opts.
integrity_disabled_store_bypasses_verification(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    %% Do NOT load any integrity key — store is in default disabled state.
    StreamId = <<"disabled">>,

    {ok, 2} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [
            #{event_type => <<"e">>, data => #{n => 1}},
            #{event_type => <<"e">>, data => #{n => 2}},
            #{event_type => <<"e">>, data => #{n => 3}}
        ]),

    {ok, Events} = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),
    ?assertEqual(3, length(Events)),
    [?assertEqual(undefined, E#event.mac) || E <- Events],
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

%% Build a store where the first LegacyPrefix events were written
%% BEFORE integrity was enabled. We achieve this in the test by
%% installing the key only AFTER the first events have been written,
%% so the writer treats them as legacy.
setup_integrity_store_with_legacy_prefix(Config, _LegacyPrefix) ->
    StoreId = proplists:get_value(store_id, Config),
    %% Integrity is NOT yet enabled. Caller will write legacy events
    %% first via write_legacy_events/3, then we install the key.
    {StoreId, undefined}.

write_legacy_events(StoreId, StreamId, N) ->
    %% Integrity disabled mode: append goes through normal path with
    %% no integrity fields. After this we install the key for
    %% subsequent appends.
    {ok, _} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"e">>, data => #{n => I}} || I <- lists:seq(1, N)]),
    ok.

write_n_events(StoreId, StreamId, N) ->
    write_n_events_starting_at(StoreId, StreamId, 0, N).

write_n_events_starting_at(StoreId, StreamId, StartVersion, N) ->
    %% Install the key right before writing, so all events from this
    %% point on are integrity-bearing.
    case persistent_term:get({reckon_db, integrity_enabled, StoreId}, false) of
        false ->
            Key = crypto:strong_rand_bytes(32),
            persistent_term:put({reckon_db, integrity_key, StoreId}, Key),
            persistent_term:put({reckon_db, integrity_enabled, StoreId}, true);
        true ->
            ok
    end,
    ExpectedVersion = case StartVersion of
        0 -> ?NO_STREAM;
        V -> V - 1
    end,
    {ok, _} = reckon_db_streams:append(
        StoreId, StreamId, ExpectedVersion,
        [#{event_type => <<"e">>, data => #{n => I}}
         || I <- lists:seq(StartVersion + 1, StartVersion + N)]),
    ok.

tamper_event(StoreId, StreamId, Version, Fun) ->
    PaddedVersion = pad_version(Version, ?VERSION_PADDING),
    Path = [streams, StreamId, PaddedVersion],
    {ok, Event} = khepri:get(StoreId, Path),
    ok = khepri:put(StoreId, Path, Fun(Event)),
    ok.

sign_with_key(Event, Key) ->
    %% Compute MAC under the attacker key, attach to event. The
    %% prev_event_hash and other fields remain whatever they were.
    Stripped = Event#event{mac = undefined, signature = undefined},
    Bytes = iolist_to_binary(
        reckon_gater_canonical:encode_for_mac(event, Stripped)),
    MacBytes = crypto:mac(hmac, sha256, Key, Bytes),
    Event#event{mac = {1, MacBytes}}.

%% Pad a version integer to a fixed-width binary - mirrors the
%% private helper inside reckon_db_streams.
pad_version(Version, Width) ->
    VersionStr = integer_to_list(Version),
    Padding = Width - length(VersionStr),
    PaddedStr = lists:duplicate(Padding, $0) ++ VersionStr,
    list_to_binary(PaddedStr).

expect_mac_mismatch(Config, StreamId, TamperFun) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    write_n_events(StoreId, StreamId, 3),
    tamper_event(StoreId, StreamId, 1, TamperFun),

    Result = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),
    ?assertMatch({error, {integrity_violation,
                  #{kind := mac_mismatch, version := 1}}},
                 Result).
