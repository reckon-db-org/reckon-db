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
    swapped_two_adjacent_events_caught/1,
    a_replayed_life_with_a_stripped_tail_refuses_a_targeted_read/1,
    a_replayed_watermark_on_a_mid_chain_event_refuses_its_legacy_region/1,
    a_replayed_watermark_past_the_newest_event_refuses_its_legacy_region/1
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
    backward_read_catches_tampering/1,
    backward_read_returns_events_in_descending_order/1,
    a_scavenged_stream_is_read_from_the_oldest_event_that_is_there/1,
    a_hand_deleted_prefix_is_read_as_a_violation/1,
    a_gap_just_before_the_read_start_is_a_violation/1,
    a_deleted_chain_start_does_not_turn_events_into_legacy/1,
    a_forged_chain_start_does_not_excuse_a_missing_predecessor/1,
    a_cleared_event_above_a_forged_watermark_is_refused/1,
    a_rewritten_event_under_a_sealed_watermark_is_caught/1,
    an_unsealed_watermark_never_excuses_a_hashless_stream/1,
    an_append_does_not_seal_a_forged_watermark/1,
    an_append_refuses_a_watermark_ahead_of_the_stream/1,
    a_deleted_watermark_is_not_minted_by_an_append/1,
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
            swapped_two_adjacent_events_caught,
            a_replayed_life_with_a_stripped_tail_refuses_a_targeted_read,
            a_replayed_watermark_on_a_mid_chain_event_refuses_its_legacy_region,
            a_replayed_watermark_past_the_newest_event_refuses_its_legacy_region
        ]},
        {verify_modes, [sequence], [
            skip_legacy_passes_legacy_events_through,
            strict_rejects_legacy_events,
            skip_all_returns_tampered_events
        ]},
        {boundary_cases, [sequence], [
            legacy_and_integrity_events_in_same_stream,
            backward_read_catches_tampering,
            backward_read_returns_events_in_descending_order,
            a_scavenged_stream_is_read_from_the_oldest_event_that_is_there,
            a_hand_deleted_prefix_is_read_as_a_violation,
            a_gap_just_before_the_read_start_is_a_violation,
            a_deleted_chain_start_does_not_turn_events_into_legacy,
            a_forged_chain_start_does_not_excuse_a_missing_predecessor,
            a_cleared_event_above_a_forged_watermark_is_refused,
            a_rewritten_event_under_a_sealed_watermark_is_caught,
            an_unsealed_watermark_never_excuses_a_hashless_stream,
            an_append_does_not_seal_a_forged_watermark,
            an_append_refuses_a_watermark_ahead_of_the_stream,
            a_deleted_watermark_is_not_minted_by_an_append,
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
    StreamId = reckon_db_test_helpers:sid(<<"clean-chain">>),
    write_n_events(StoreId, StreamId, 10),

    {ok, Events} = reckon_db_streams:read(StoreId, StreamId, 0, 100, forward),
    ?assertEqual(10, length(Events)),
    [?assert(is_binary(E#event.prev_event_hash)) || E <- Events],
    [?assertMatch({v2, 1, _}, E#event.mac) || E <- Events],
    ok.

%% Reading exactly one event at version 0 (the chain start) should
%% succeed and that event's prev_event_hash should equal genesis.
read_single_event_at_chain_start(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"single">>),
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
    StreamId = reckon_db_test_helpers:sid(<<"middle">>),
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
    expect_mac_mismatch(Config, reckon_db_test_helpers:sid(<<"t-data">>),
        fun(E) -> E#event{data = #{tampered => true}} end).

tampered_metadata_is_caught(Config) ->
    expect_mac_mismatch(Config, reckon_db_test_helpers:sid(<<"t-meta">>),
        fun(E) -> E#event{metadata = #{forged => true}} end).

tampered_event_type_is_caught(Config) ->
    expect_mac_mismatch(Config, reckon_db_test_helpers:sid(<<"t-type">>),
        fun(E) -> E#event{event_type = <<"forged_type">>} end).

tampered_tags_is_caught(Config) ->
    expect_mac_mismatch(Config, reckon_db_test_helpers:sid(<<"t-tags">>),
        fun(E) -> E#event{tags = [<<"forged">>]} end).

tampered_timestamp_is_caught(Config) ->
    expect_mac_mismatch(Config, reckon_db_test_helpers:sid(<<"t-ts">>),
        fun(E) -> E#event{timestamp = 99999999999} end).

tampered_mac_is_caught(Config) ->
    expect_mac_mismatch(Config, reckon_db_test_helpers:sid(<<"t-mac">>),
        fun(#event{mac = {v2, KeyId, _}} = E) ->
            E#event{mac = {v2, KeyId, <<0:256>>}}
        end).

%% Tampering with prev_event_hash breaks the chain check (not the MAC
%% check, since prev_event_hash is INSIDE the MAC computation - so
%% mutating it should actually break BOTH checks). The chain check
%% runs first, so we expect chain_mismatch as the failure kind.
tampered_prev_event_hash_is_caught(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"t-prev">>),
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
    StreamId = reckon_db_test_helpers:sid(<<"t-cleared">>),
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
    StreamId = reckon_db_test_helpers:sid(<<"t-deleted">>),
    write_n_events(StoreId, StreamId, 5),

    %% Delete event at version 1 directly via khepri.
    PaddedV1 = pad_version(1, ?VERSION_PADDING),
    Path = reckon_db_stream_path:event_path(StreamId, PaddedV1),
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
    StreamId = reckon_db_test_helpers:sid(<<"t-inserted">>),
    write_n_events(StoreId, StreamId, 5),

    %% Build a fake event using a DIFFERENT key — the MAC won't match.
    AttackerKey = crypto:strong_rand_bytes(32),
    {ok, [_E0, RealE1 | _]} = reckon_db_streams:read(
        StoreId, StreamId, 0, 10, forward),
    %% Build a tampered version of RealE1 signed under the attacker key.
    BadE1 = sign_with_key(
        RealE1#event{data = #{forged => true}}, AttackerKey),

    PaddedV1 = pad_version(1, ?VERSION_PADDING),
    Path = reckon_db_stream_path:event_path(StreamId, PaddedV1),
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
    StreamId = reckon_db_test_helpers:sid(<<"t-swapped">>),
    write_n_events(StoreId, StreamId, 5),

    {ok, [_E0, E1, E2 | _]} = reckon_db_streams:read(
        StoreId, StreamId, 0, 10, forward),

    PaddedV1 = pad_version(1, ?VERSION_PADDING),
    PaddedV2 = pad_version(2, ?VERSION_PADDING),
    %% Put E2 at version 1's slot and vice versa.
    ok = khepri:put(StoreId, reckon_db_stream_path:event_path(StreamId, PaddedV1), E2),
    ok = khepri:put(StoreId, reckon_db_stream_path:event_path(StreamId, PaddedV2), E1),

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
    StreamId = reckon_db_test_helpers:sid(<<"mixed-skip-legacy">>),
    write_legacy_events(StoreId, StreamId, 3),
    %% Now enable integrity and write 2 more. The key goes in first, then the
    %% migration gives the pre-integrity prefix its watermark: an append never
    %% mints one for an existing stream.
    ok = enable_integrity(StoreId),
    ok = reckon_db_chain_watermark:migrate(StoreId),
    write_n_events_starting_at(StoreId, StreamId, 3, 2),

    {ok, Events} = reckon_db_streams:read(
        StoreId, StreamId, 0, 10, forward,
        #{verify => skip_legacy}),
    ?assertEqual(5, length(Events)),
    %% First 3 are legacy (no integrity fields), last 2 have them.
    LegacyEvents = lists:sublist(Events, 3),
    IntegrityEvents = lists:nthtail(3, Events),
    [?assertEqual(undefined, E#event.mac) || E <- LegacyEvents],
    [?assertMatch({v2, 1, _}, E#event.mac) || E <- IntegrityEvents],
    ok.

%% Strict mode refuses to return legacy events when integrity is
%% enabled on the store. The scenario simulates: legacy data exists
%% from before integrity was enabled; an operator then enables
%% integrity but configures strict reads. Any read that touches the
%% legacy region must error out.
strict_rejects_legacy_events(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"strict-test">>),
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
    StreamId = reckon_db_test_helpers:sid(<<"skip-all">>),
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
    StreamId = reckon_db_test_helpers:sid(<<"mixed">>),
    write_legacy_events(StoreId, StreamId, 2),
    ok = enable_integrity(StoreId),
    ok = reckon_db_chain_watermark:migrate(StoreId),
    write_n_events_starting_at(StoreId, StreamId, 2, 3),

    {ok, Events} = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),
    ?assertEqual(5, length(Events)),
    %% Verify boundary: event at watermark version has genesis prev_hash.
    ChainStartEvent = lists:nth(3, Events), %% version 2
    ?assertEqual(reckon_gater_integrity:genesis_prev_hash(),
                 ChainStartEvent#event.prev_event_hash),
    ok.

%% Backward reads verify the same chain as forward reads — the
%% ordering of the returned list is the only behavioural difference.
%% A tampered event encountered during a backward read surfaces as
%% an integrity_violation, just like a forward read would.
backward_read_catches_tampering(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"backward-tamper">>),
    write_n_events(StoreId, StreamId, 3),
    tamper_event(StoreId, StreamId, 1,
        fun(E) -> E#event{data = #{forged => true}} end),

    ?assertMatch({error, {integrity_violation, _}},
        reckon_db_streams:read(StoreId, StreamId, 0, 10, forward)),
    ?assertMatch({error, {integrity_violation, _}},
        reckon_db_streams:read(StoreId, StreamId, 2, 3, backward)),
    ok.

%% An intact backward read returns the events highest-version-first,
%% as the caller requested — verification works in either direction
%% without reshaping the result-ordering contract.
backward_read_returns_events_in_descending_order(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"backward-clean">>),
    write_n_events(StoreId, StreamId, 5),

    {ok, Events} = reckon_db_streams:read(StoreId, StreamId, 4, 3, backward),
    ?assertEqual(3, length(Events)),
    %% Events come back as [v4, v3, v2] — backward order preserved.
    [?assertEqual(V, E#event.version)
     || {V, E} <- lists:zip([4, 3, 2], Events)],
    %% Each event still has its integrity fields populated.
    [?assert(is_binary(E#event.prev_event_hash)) || E <- Events],
    ok.

%% @doc GIVEN an integrity store whose stream is scavenged from the front
%%      WHEN the surviving events are read forward, backward, and folded
%%      THEN every one of them is returned and verified
%%
%% The first surviving event's prev_event_hash names an event the scavenge
%% deleted, so the chain cannot be walked from the start: the read anchors on
%% the boundary the scavenge recorded (the oldest surviving version, with the
%% chain hash of the last removed event, MAC'd under the integrity key) and
%% walks from there. Before this case's fix the forward read and the fold
%% answered {error, {integrity_violation, version 10}} and the backward read
%% exited with a function_clause in handle_integrity, a crash in a gateway
%% worker (the reckon-db#4 class), over a stream whose events were all intact.
a_scavenged_stream_is_read_from_the_oldest_event_that_is_there(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"integrity-scavenged">>),
    write_n_events(StoreId, StreamId, 30),

    {ok, #{deleted_versions := {0, 9}}} =
        reckon_db_scavenge:scavenge(StoreId, StreamId,
            #{before_version => 10, require_snapshot => false}),

    Survivors = lists:seq(10, 29),

    {ok, Forward} = reckon_db_streams:read_all(StoreId, StreamId, 100, forward),
    ?assertEqual(Survivors, [E#event.version || E <- Forward]),

    {ok, Backward} = reckon_db_streams:read_all(StoreId, StreamId, 100, backward),
    ?assertEqual(lists:reverse(Survivors), [E#event.version || E <- Backward]),

    Collect = fun(Events, Acc) -> Acc ++ [E#event.version || E <- Events] end,
    {ok, Folded} = reckon_db_streams:fold_event_batches(
        StoreId, StreamId, 7, forward, Collect, []),
    ?assertEqual(Survivors, Folded).

%% @doc GIVEN an integrity store whose prefix was deleted by hand, with no
%%      scavenge to record a boundary
%%      WHEN the survivors are read forward and backward
%%      THEN both refuse: the gap is not one the store cut
%%
%% This is the shape a scavenge leaves in the tree, and the reason the
%% scavenge record exists. Anchoring on whatever the survivor carries would
%% make a deletion indistinguishable from a scavenge, and a reader who can
%% verify a truncated stream verifies nothing.
a_hand_deleted_prefix_is_read_as_a_violation(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"integrity-hand-deleted">>),
    write_n_events(StoreId, StreamId, 30),

    %% No scavenge: the events are removed straight from the tree.
    lists:foreach(fun(V) -> khepri:delete(StoreId, event_path(StreamId, V)) end,
                  lists:seq(0, 9)),

    ?assertMatch({error, {integrity_violation, #{kind := missing_predecessor}}},
                 reckon_db_streams:read_all(StoreId, StreamId, 100, forward)),
    ?assertMatch({error, {integrity_violation, #{kind := missing_predecessor}}},
                 reckon_db_streams:read_all(StoreId, StreamId, 100, backward)),
    Collect = fun(Events, Acc) -> Acc ++ [E#event.version || E <- Events] end,
    ?assertMatch({error, {integrity_violation, #{kind := missing_predecessor}}},
                 reckon_db_streams:fold_event_batches(
                   StoreId, StreamId, 7, forward, Collect, [])).

%% @doc GIVEN an intact integrity stream
%%      WHEN the single event just before a read's start is deleted by hand
%%      THEN a read from that start refuses, in both directions
%%
%% A gap in the middle has to stay a violation: a read that starts above it
%% would otherwise verify the survivors against a predecessor it never had.
a_gap_just_before_the_read_start_is_a_violation(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"integrity-gap">>),
    write_n_events(StoreId, StreamId, 30),

    ok = khepri:delete(StoreId, event_path(StreamId, 14)),

    ?assertMatch({error, {integrity_violation, #{kind := missing_predecessor}}},
                 reckon_db_streams:read(StoreId, StreamId, 15, 5, forward)),
    ?assertMatch({error, {integrity_violation, #{kind := missing_predecessor}}},
                 reckon_db_streams:read(StoreId, StreamId, 19, 5, backward)).

%% @doc GIVEN an integrity stream
%%      WHEN the chain-start watermark is deleted
%%      THEN the read refuses instead of handing the events back as legacy
%%
%% The watermark is a plain tree node: whoever can delete an event can delete
%% it, and under skip_legacy everything below it comes back unverified. A
%% stream whose events carry a chain but whose watermark cannot be believed
%% has no starting point the store can vouch for, so it is refused rather than
%% read as if the chain began nowhere.
a_deleted_chain_start_does_not_turn_events_into_legacy(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"integrity-watermark-deleted">>),
    write_n_events(StoreId, StreamId, 30),

    ok = khepri:delete(StoreId, watermark_path(StreamId)),

    ?assertMatch({error, {integrity_violation,
                          #{kind := missing_integrity,
                            context := #{detail := chain_start_unauthenticated}}}},
                 reckon_db_streams:read_all(StoreId, StreamId, 100, forward)),
    ?assertMatch({error, {integrity_violation,
                          #{kind := missing_integrity}}},
                 reckon_db_streams:read_all(StoreId, StreamId, 100, backward)).

%% @doc GIVEN an integrity stream
%%      WHEN events are deleted and the chain-start watermark is moved above
%%           the gap in the same shape
%%      THEN the read still refuses
%%
%% A bare integer is a node anybody can write, so moving it above a tampered
%% prefix is the whole attack: with it believed, the gap is just a legacy
%% prefix. It is not believed.
a_forged_chain_start_does_not_excuse_a_missing_predecessor(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"integrity-watermark-forged">>),
    write_n_events(StoreId, StreamId, 30),

    lists:foreach(fun(V) -> khepri:delete(StoreId, event_path(StreamId, V)) end,
                  lists:seq(0, 9)),
    ok = khepri:put(StoreId, watermark_path(StreamId), 1000),

    ?assertMatch({error, {integrity_violation, #{kind := _}}},
                 reckon_db_streams:read_all(StoreId, StreamId, 100, forward)),
    ?assertMatch({error, {integrity_violation, #{kind := _}}},
                 reckon_db_streams:read_all(StoreId, StreamId, 100, backward)).

%% @doc GIVEN an integrity stream
%%      WHEN one event's payload is rewritten and its integrity fields are
%%           cleared, and the watermark is moved above it
%%      THEN the read refuses instead of calling the event legacy
a_cleared_event_above_a_forged_watermark_is_refused(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"integrity-watermark-cleared">>),
    write_n_events(StoreId, StreamId, 5),

    ok = clear_integrity_fields(StoreId, StreamId, 2),
    ok = khepri:put(StoreId, watermark_path(StreamId), 1000),

    ?assertMatch({error, {integrity_violation, #{kind := _}}},
                 reckon_db_streams:read_all(StoreId, StreamId, 100, forward)).

%% @doc GIVEN a sealed watermark and an intact stream
%%      WHEN one event's payload is rewritten in place
%%      THEN the MAC catches it
%%
%% The watermark being believable is not the same as the data being right.
a_rewritten_event_under_a_sealed_watermark_is_caught(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"integrity-sealed">>),
    write_n_events(StoreId, StreamId, 5),

    ok = rewrite_event(StoreId, StreamId, 2, #{tampered => 1}),

    ?assertMatch({error, {integrity_violation, #{kind := mac_mismatch}}},
                 reckon_db_streams:read_all(StoreId, StreamId, 100, forward)).

%% @doc GIVEN a stream whose integrity fields have ALL been cleared
%%      WHEN the watermark is replaced with a bare integer above them
%%      THEN the read refuses, even though the batch carries no chain at all
%%
%% The attack the per-event rule alone does not stop: strip every event, so
%% nothing in the batch is chain-bearing, and write a large bare number. If
%% the bare number were believed only when the batch has no chain, that is
%% exactly the case it is believed in, and the tampered stream reads back
%% clean. An unsealed watermark is never believed.
an_unsealed_watermark_never_excuses_a_hashless_stream(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"integrity-unsealed-stripped">>),
    write_n_events(StoreId, StreamId, 5),

    lists:foreach(fun(V) -> ok = clear_integrity_fields(StoreId, StreamId, V) end,
                  lists:seq(0, 4)),
    ok = rewrite_event(StoreId, StreamId, 2, #{tampered => 1}),
    ok = khepri:put(StoreId, watermark_path(StreamId), 1000),

    ?assertMatch({error, {integrity_violation,
                          #{kind := missing_integrity,
                            context := #{detail := chain_start_unauthenticated}}}},
                 reckon_db_streams:read_all(StoreId, StreamId, 100, forward)),
    ?assertMatch({error, {integrity_violation,
                          #{kind := missing_integrity}}},
                 reckon_db_streams:read_all(StoreId, StreamId, 100, backward)).

%% @doc GIVEN a bare-integer watermark written over a stripped stream
%%      WHEN an ordinary append runs
%%      THEN it is refused and the watermark is NOT sealed
%%
%% An append must never turn whatever bare integer it finds into the point
%% where the store vouches the chain begins. Only seal_all/1 seals. Before
%% this, the append sealed the forged value outside the transaction, then
%% died on the version check, leaving a store that returned the tampered
%% stream unverified for good.
an_append_does_not_seal_a_forged_watermark(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"integrity-unsealed-append">>),
    write_n_events(StoreId, StreamId, 5),

    lists:foreach(fun(V) -> ok = clear_integrity_fields(StoreId, StreamId, V) end,
                  lists:seq(0, 4)),
    ok = rewrite_event(StoreId, StreamId, 2, #{tampered => 1}),
    ok = khepri:put(StoreId, watermark_path(StreamId), 1000),

    ?assertMatch({error, {chain_start_unauthenticated, 1000}},
                 reckon_db_streams:append(
                     StoreId, StreamId, 4,
                     [#{event_type => <<"e">>, data => #{n => 9}}])),
    ?assertMatch({ok, {unsealed, 1000}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, #{kind := missing_integrity}}},
                 reckon_db_streams:read_all(StoreId, StreamId, 100, forward)).

%% @doc GIVEN a sealed watermark that sits above the stream's newest version
%%      WHEN an append runs
%%      THEN it answers an error instead of crashing
%%
%% A watermark moved past the stream is not a reason for the gateway worker
%% to die: the caller gets a reason it can report, and nothing is written.
an_append_refuses_a_watermark_ahead_of_the_stream(Config) ->
    {StoreId, Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"integrity-watermark-ahead">>),

    %% A sealed watermark above the stream cannot be produced through the
    %% API any more (sealing refuses one, and the migration refuses one), so
    %% it is written the only way it can now arise: with the key.
    Life = crypto:strong_rand_bytes(16),
    ok = khepri:put(StoreId, life_path(StreamId), Life),
    ok = khepri:put(StoreId, watermark_path(StreamId),
                    sealed_watermark(Key, StoreId, StreamId, Life, 1000)),
    ?assertMatch({ok, {sealed, 1000}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),

    ?assertMatch({error, {chain_start_ahead_of_append,
                          #{next_version := 0, chain_start := 1000}}},
                 reckon_db_streams:append(
                     StoreId, StreamId, ?NO_STREAM,
                     [#{event_type => <<"e">>, data => #{n => 9}}])),
    ?assertNot(reckon_db_streams:exists(StoreId, StreamId)).

%% @doc GIVEN a stream whose integrity fields were all cleared and whose
%%           watermark was DELETED
%%      WHEN one ordinary append runs
%%      THEN it is refused, and the stream stays unreadable
%%
%% The third way to get a chain start the store never chose: delete the
%% watermark instead of forging it. An append used to mint a sealed one at
%% its own next version for a stream that already held events, and the read
%% path then believed it, so one append vouched for a chain start over a
%% stream that had been rewritten underneath. Only a new stream, at version
%% 0, gets a watermark minted at runtime.
a_deleted_watermark_is_not_minted_by_an_append(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"integrity-watermark-deleted-append">>),
    write_n_events(StoreId, StreamId, 5),

    lists:foreach(fun(V) -> ok = clear_integrity_fields(StoreId, StreamId, V) end,
                  lists:seq(0, 4)),
    ok = rewrite_event(StoreId, StreamId, 4, #{tampered => 1}),
    ok = khepri:delete(StoreId, watermark_path(StreamId)),

    ?assertMatch({error, {chain_start_missing, _}},
                 reckon_db_streams:append(
                     StoreId, StreamId, 4,
                     [#{event_type => <<"e">>, data => #{n => 99}}])),
    ?assertMatch({ok, undefined},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, #{kind := missing_integrity}}},
                 reckon_db_streams:read_all(StoreId, StreamId, 100, forward)).

%% @private The authenticated watermark shape, MAC'd under the store's key.
sealed_watermark(Key, StoreId, StreamId, Life, Version) ->
    Mac = crypto:mac(hmac, sha256, Key,
                     term_to_binary({reckon_db_chain_watermark, seal, StoreId,
                                     StreamId, Life, Version, Version},
                                    [deterministic])),
    #{version => Version, v2_from => Version, life => Life, mac => Mac}.

%% @private
life_path(StreamId) ->
    ?INTEGRITY_STREAM_LIFE_PATH ++ [StreamId].

%% @private
watermark_path(StreamId) ->
    ?INTEGRITY_CHAIN_START_PATH ++ [StreamId].

%% @private Rewrite an event's payload in place, leaving its chain fields as
%% they are: the MAC no longer covers the data.
rewrite_event(StoreId, StreamId, Version, Data) ->
    Path = event_path(StreamId, Version),
    {ok, Event} = khepri:get(StoreId, Path),
    khepri:put(StoreId, Path, Event#event{data = Data}).

%% @private Remove an event's integrity fields, keeping its payload.
clear_integrity_fields(StoreId, StreamId, Version) ->
    Path = event_path(StreamId, Version),
    {ok, Event} = khepri:get(StoreId, Path),
    khepri:put(StoreId, Path, Event#event{prev_event_hash = undefined,
                                          mac = undefined}).

%% @private
event_path(StreamId, Version) ->
    reckon_db_stream_path:event_path(StreamId, pad_version(Version, ?VERSION_PADDING)).

%% A store with integrity disabled bypasses verification entirely,
%% regardless of opts.
integrity_disabled_store_bypasses_verification(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    %% Do NOT load any integrity key — store is in default disabled state.
    StreamId = reckon_db_test_helpers:sid(<<"disabled">>),

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

%% @doc GIVEN a stream whose earlier life's sealed record and life token were
%%           restored, and whose newest event was then stripped and rewritten
%%      WHEN a read STARTS at that stripped event
%%      THEN it is refused: a chain-bearing event is never followed by a
%%           chain-less one, and the page start never saw its neighbour
%%
%% Jupiter r15 JP3 tail variant. The full read refused at the surviving
%% MAC'd event that no longer verifies; the targeted read passed, because
%% verification only ever looked at the events it returned.
a_replayed_life_with_a_stripped_tail_refuses_a_targeted_read(Config) ->
    {StoreId, Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"replayed-life-tail">>),
    write_n_events(StoreId, StreamId, 3),

    OldLife = crypto:strong_rand_bytes(16),
    OldRecord = sealed_watermark(Key, StoreId, StreamId, OldLife, 5),
    ok = khepri:put(StoreId, life_path(StreamId), OldLife),
    ok = khepri:put(StoreId, watermark_path(StreamId), OldRecord),
    ok = clear_integrity_fields(StoreId, StreamId, 2),
    ok = rewrite_event(StoreId, StreamId, 2, #{tampered => 1}),

    ?assertMatch({error, {integrity_violation, _}},
                 reckon_db_streams:read_all(StoreId, StreamId, 100, forward)),
    ?assertMatch({error, {integrity_violation,
                          #{context := #{detail := chain_break}}}},
                 reckon_db_streams:read(StoreId, StreamId, 2, 1, forward)).

%% @doc GIVEN an earlier life's sealed record replayed at a version a new-life
%%           event still sits at, with a run of new-life events stripped
%%      WHEN a read starts at the top of that run
%%      THEN it refuses: the event AT the replayed limit does not carry the
%%           genesis predecessor, so nothing the store wrote explains the
%%           region below it
%%
%% Terra/Jupiter r15, item 2(a). A replayed limit always lands on a mid-chain
%% event of the new life (its predecessor is a chain hash, not genesis) or on
%% nothing at all; only the honest chain start carries genesis.
a_replayed_watermark_on_a_mid_chain_event_refuses_its_legacy_region(Config) ->
    {StoreId, Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"replayed-mid-chain">>),
    write_n_events(StoreId, StreamId, 7),

    OldLife = crypto:strong_rand_bytes(16),
    OldRecord = sealed_watermark(Key, StoreId, StreamId, OldLife, 6),
    ok = khepri:put(StoreId, life_path(StreamId), OldLife),
    ok = khepri:put(StoreId, watermark_path(StreamId), OldRecord),
    ok = clear_integrity_fields(StoreId, StreamId, 4),
    ok = clear_integrity_fields(StoreId, StreamId, 5),
    ok = rewrite_event(StoreId, StreamId, 4, #{tampered => 1}),

    ?assertMatch({error, {integrity_violation,
                          #{context := #{detail := legacy_region_unexplained}}}},
                 reckon_db_streams:read(StoreId, StreamId, 4, 1, forward)).

%% @doc GIVEN an earlier life's sealed record replayed ABOVE every new-life
%%           event, with a run of them stripped
%%      WHEN a read starts at the top of that run
%%      THEN it refuses: a chain-bearing newest event under a limit that has
%%           no event at it is the same replay, over a shorter life
%%
%% Terra/Jupiter r15, item 2(b). An honest chain-less stream is sealed at
%% newest + 1, which also has no event at the limit - and it stays legal,
%% because its newest event carries no chain.
a_replayed_watermark_past_the_newest_event_refuses_its_legacy_region(Config) ->
    {StoreId, Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"replayed-past-newest">>),
    write_n_events(StoreId, StreamId, 5),

    OldLife = crypto:strong_rand_bytes(16),
    OldRecord = sealed_watermark(Key, StoreId, StreamId, OldLife, 9),
    ok = khepri:put(StoreId, life_path(StreamId), OldLife),
    ok = khepri:put(StoreId, watermark_path(StreamId), OldRecord),
    ok = clear_integrity_fields(StoreId, StreamId, 1),
    ok = clear_integrity_fields(StoreId, StreamId, 2),
    ok = rewrite_event(StoreId, StreamId, 1, #{tampered => 1}),

    ?assertMatch({error, {integrity_violation,
                          #{context := #{detail := legacy_region_unexplained}}}},
                 reckon_db_streams:read(StoreId, StreamId, 1, 1, forward)).

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

%% Enable integrity on an already-populated store: the key has to be in
%% place before the watermark is written, because the watermark is sealed
%% under it. A watermark written without a key is a bare integer and is not
%% believed by anything.
enable_integrity(StoreId) ->
    Key = crypto:strong_rand_bytes(32),
    persistent_term:put({reckon_db, integrity_key, StoreId}, Key),
    persistent_term:put({reckon_db, integrity_enabled, StoreId}, true),
    ok.

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
    Path = reckon_db_stream_path:event_path(StreamId, PaddedVersion),
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
