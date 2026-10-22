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
    a_bare_watermark_is_sealed_at_the_proven_chain_start/1,
    a_watermark_moved_onto_a_stripped_chain_start_is_not_sealed/1,
    a_scavenged_chain_start_is_sealed_at_the_boundary/1,
    a_deleted_marker_does_not_reseal/1,
    a_deleted_watermark_is_not_minted_after_the_marker/1,
    a_deleted_watermark_is_evidence_and_refuses_chainless_streams/1,
    the_operator_act_seals_only_the_streams_it_names/1,
    a_node_local_anchor_stops_a_second_migration/1,
    the_migration_writes_no_in_progress_record/1,
    deleting_a_stream_deletes_its_watermark/1,
    a_sealed_watermark_does_not_survive_delete_and_recreate/1,
    a_junk_watermark_refuses_only_its_own_stream/1,
    a_junk_marker_does_not_count_as_migrated/1,
    a_snapshot_authenticated_cut_is_sealed/1,
    a_cut_does_not_cover_events_that_still_survive/1,
    a_cut_with_a_watermark_above_the_survivors_is_refused/1,
    an_honest_old_line_upgrade_seals_its_legacy_streams/1,
    a_junk_watermark_is_refused_by_name/1,
    a_bare_watermark_below_a_genesis_start_is_evidence/1,
    a_forged_prefix_below_a_sealed_cut_is_refused/1,
    a_lost_anchor_log_is_refilled_from_the_tree_at_start/1
]).

-define(KEY_ENV, "RECKON_DB_MIGRATION_TEST_KEY").

all() ->
    [
        a_bare_watermark_is_sealed_at_store_start,
        a_stream_with_no_watermark_is_sealed_at_store_start,
        a_bare_integer_after_the_migration_is_refused,
        a_watermark_above_the_stream_is_not_sealed,
        a_bare_watermark_is_sealed_at_the_proven_chain_start,
        a_watermark_moved_onto_a_stripped_chain_start_is_not_sealed,
        a_scavenged_chain_start_is_sealed_at_the_boundary,
        a_deleted_marker_does_not_reseal,
        a_deleted_watermark_is_not_minted_after_the_marker,
        a_deleted_watermark_is_evidence_and_refuses_chainless_streams,
        the_operator_act_seals_only_the_streams_it_names,
        a_node_local_anchor_stops_a_second_migration,
        the_migration_writes_no_in_progress_record,
        deleting_a_stream_deletes_its_watermark,
        a_sealed_watermark_does_not_survive_delete_and_recreate,
        a_junk_watermark_refuses_only_its_own_stream,
        a_junk_marker_does_not_count_as_migrated,
        a_snapshot_authenticated_cut_is_sealed,
        a_cut_does_not_cover_events_that_still_survive,
        a_cut_with_a_watermark_above_the_survivors_is_refused,
        an_honest_old_line_upgrade_seals_its_legacy_streams,
        a_junk_watermark_is_refused_by_name,
        a_bare_watermark_below_a_genesis_start_is_evidence,
        a_forged_prefix_below_a_sealed_cut_is_refused,
        a_lost_anchor_log_is_refilled_from_the_tree_at_start
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

%% @doc GIVEN events that carry a chain, a bare watermark sitting away from
%%           where that chain begins, and the genesis event still verifying
%%      WHEN the migration runs
%%      THEN it is sealed at the PROVEN chain start, not at the value on the
%%           node
%%
%% A bare integer is not evidence. What is evidence is the event the store
%% alone can write at a chain start: a genesis predecessor whose MAC
%% verifies. Sealing at the node's value would let a writer move the claim
%% wherever they liked.
a_bare_watermark_is_sealed_at_the_proven_chain_start(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"watermark-elsewhere">>),

    ok = start_store(StoreId, DataDir, disabled),
    ok = write_legacy(StoreId, StreamId, 5),
    ok = stop_store(StoreId),
    ok = start_store(StoreId, DataDir, enabled),
    {ok, 9} = append_from(StoreId, StreamId, 4, 5, 9),
    ok = forget_migration(StoreId),
    ok = forget_anchor(DataDir),
    ok = put_bare_watermark(StoreId, StreamId, 8),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, {sealed, 5}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertEqual(10, length(events(StoreId, StreamId))).

%% @doc GIVEN a chain start whose first events were STRIPPED and a bare
%%           watermark moved onto the new lowest chain-bearing event
%%      WHEN the migration runs
%%      THEN it seals nothing for that stream: the event that would prove a
%%           chain start no longer verifies, so the stream is refused
%%
%% This is the attack the proven-chain-start rule exists for: strip the chain
%% off the first three chain-bearing events, move the bare watermark up three,
%% and the new "chain start" is a version nobody can account for.
a_watermark_moved_onto_a_stripped_chain_start_is_not_sealed(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"watermark-over-chain">>),

    ok = start_store(StoreId, DataDir, disabled),
    ok = write_legacy(StoreId, StreamId, 5),
    ok = stop_store(StoreId),
    ok = start_store(StoreId, DataDir, enabled),
    {ok, 9} = append_from(StoreId, StreamId, 4, 5, 9),
    ok = clear_integrity_fields(StoreId, StreamId, 5),
    ok = clear_integrity_fields(StoreId, StreamId, 6),
    ok = clear_integrity_fields(StoreId, StreamId, 7),
    ok = forget_migration(StoreId),
    ok = forget_anchor(DataDir),
    ok = put_bare_watermark(StoreId, StreamId, 8),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, {unsealed, 8}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, #{kind := missing_integrity}}},
                 read_raw(StoreId, StreamId)).

%% @doc GIVEN a stream whose chain start was removed by a real scavenge, so
%%           the genesis event no longer exists
%%      WHEN the migration runs
%%      THEN it is sealed at the verified scavenge boundary, which is where
%%           the surviving chain begins
%%
%% Only a scavenge can write that boundary, and it is MAC'd, so it is the one
%% thing that tells a scavenged chain start from a stripped one. Without it
%% the stream stays refused.
a_scavenged_chain_start_is_sealed_at_the_boundary(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"scavenged-chain-start">>),

    ok = start_store(StoreId, DataDir, enabled),
    {ok, 9} = append_events(StoreId, StreamId, 10),
    {ok, _} = reckon_db_scavenge:scavenge(
                StoreId, StreamId,
                #{before_version => 5, require_snapshot => false}),
    ok = forget_migration(StoreId),
    ok = forget_anchor(DataDir),
    ok = delete_watermark(StoreId, StreamId),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, {sealed, 5}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertEqual(5, length(events(StoreId, StreamId))).

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

%% @doc GIVEN a store whose marker, node-local anchor and every watermark
%%          have been deleted: one chain-bearing stream lost its watermark,
%%          one chain-less stream lost its watermark too
%%      WHEN it is started by this version
%%      THEN the chain-bearing stream's watermark is rebuilt from its own
%%           genesis event, and the chain-less stream is NOT sealed: the
%%           deletion is evidence, and a stream the store cannot account for
%%           is refused rather than vouched for
%%
%% Sealing the chain-less stream at its newest + 1 would be exactly the
%% attack: strip a stream, delete the watermarks, and let the migration vouch
%% for the result. The intact stream's missing watermark is the evidence that
%% makes every other missing watermark evidence too.
a_deleted_watermark_is_evidence_and_refuses_chainless_streams(Config) ->
    {StoreId, _DataDir, Chainless, Chain} = evidence_store(Config),

    ?assertMatch({ok, {sealed, 0}},
                 reckon_db_chain_watermark:lookup(StoreId, Chain)),
    ?assertEqual(3, length(events(StoreId, Chain))),

    ?assertMatch({ok, undefined},
                 reckon_db_chain_watermark:lookup(StoreId, Chainless)),
    ?assertMatch({error, {integrity_violation, #{kind := missing_integrity}}},
                 read_raw(StoreId, Chainless)).

%% @doc GIVEN a migrated store whose node-local anchor log is gone
%%      WHEN it starts
%%      THEN the life pass records every stream's sealed generation at once,
%%           before any read could adopt whatever the tree holds
%%
%% The marker is on the tree and reached this node whole; the anchor is per
%% node and did not. Without the pass, every stream would be a first sight, and
%% an older life restored before the first read of one would be adopted (r21
%% review, item 7 / design item 8).
a_lost_anchor_log_is_refilled_from_the_tree_at_start(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"life-pass">>),
    ok = start_store(StoreId, DataDir, enabled),
    {ok, _} = append_events(StoreId, StreamId, 3),
    {ok, Life} = reckon_db_chain_watermark:life(StoreId, StreamId),
    ?assertEqual({1, Life}, await_entry(StoreId, StreamId)),

    ok = stop_store(StoreId),
    %% The LIVES log, not the migration's own anchor file: this is the node's
    %% record of stream generations, and it is the one that is missing.
    ok = file:delete(reckon_db_life_anchor:log_path(DataDir)),
    ok = start_store(StoreId, DataDir, enabled),

    %% Nobody has read the stream since the restart: the entry is the pass's.
    ?assertEqual({1, Life}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    ok = stop_store(StoreId).

%% @private The anchor writer coalesces; wait for the table to say it.
await_entry(StoreId, StreamId) ->
    await_entry(StoreId, StreamId, 600).

await_entry(_StoreId, _StreamId, 0) ->
    ct:fail("the anchor recorded nothing");
await_entry(StoreId, StreamId, Attempts) ->
    case reckon_db_life_anchor:entry(StoreId, StreamId) of
        undefined -> timer:sleep(5), await_entry(StoreId, StreamId, Attempts - 1);
        Entry -> Entry
    end.

%% @doc GIVEN a store where the migration refused a chain-less stream
%%      WHEN the operator names that stream in accept_missing
%%      THEN it is sealed, the marker stops naming it, and naming a stream
%%           the migration never refused is an error
%%
%% The act is per stream on purpose: a store-wide "yes" would vouch for data
%% nobody looked at, and a store showing tampering evidence is an incident,
%% not a reason to trust the rest of it.
the_operator_act_seals_only_the_streams_it_names(Config) ->
    {StoreId, _DataDir, Chainless, _Chain} = evidence_store(Config),
    Unnamed = stream(<<"operator-unnamed">>),

    ?assertMatch({ok, undefined},
                 reckon_db_chain_watermark:lookup(StoreId, Chainless)),
    ?assertMatch({error, {not_refused, [Unnamed]}},
                 reckon_db_chain_watermark:migrate(
                   StoreId, #{accept_missing => [Unnamed]})),

    ok = reckon_db_chain_watermark:migrate(
           StoreId, #{accept_missing => [Chainless],
                      operator => <<"migration test">>}),
    ?assertMatch({ok, {sealed, 3}},
                 reckon_db_chain_watermark:lookup(StoreId, Chainless)),
    ?assertEqual(3, length(events(StoreId, Chainless))),

    %% Nothing left to accept: the marker does not name it any more.
    ?assertMatch({error, {not_refused, [Chainless]}},
                 reckon_db_chain_watermark:migrate(
                   StoreId, #{accept_missing => [Chainless]})).

%% @doc GIVEN a migrated store with its node-local anchor on disk
%%      WHEN the marker and the watermarks are deleted and a bare watermark
%%           is planted
%%      THEN the store reports the violation and seals nothing, because the
%%           anchor proves this node migrated the store already
%%
%% Tree access alone cannot remove a file beside the data directory. What it
%% still buys an attacker is named in the threat model: a wiped data
%% directory, or a node that never held the store, has no anchor.
a_node_local_anchor_stops_a_second_migration(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"anchor">>),

    ok = start_store(StoreId, DataDir, disabled),
    ok = write_legacy(StoreId, StreamId, 3),
    ok = stop_store(StoreId),
    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, {sealed, 3}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assert(filelib:is_regular(anchor_file(DataDir))),

    ok = forget_migration(StoreId),
    ok = delete_watermark(StoreId, StreamId),
    ok = put_bare_watermark(StoreId, StreamId, 3),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, {unsealed, 3}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, #{kind := missing_integrity}}},
                 read_raw(StoreId, StreamId)).

%% @doc GIVEN a store the migration had to refuse a stream on
%%      WHEN the migration has finished
%%      THEN the seals and the done marker went in one transaction, so there
%%           is no in-progress record for a writer to replay, and the marker
%%           names what was left behind
the_migration_writes_no_in_progress_record(Config) ->
    {StoreId, _DataDir, Chainless, _Chain} = evidence_store(Config),

    ?assertEqual(absent, presence(StoreId,
                                  ?INTEGRITY_MIGRATION_PATH ++ [in_progress])),
    ?assertMatch({ok, #{refused := [_]}},
                 khepri:get(StoreId, ?INTEGRITY_MIGRATION_PATH)),
    ?assertMatch({ok, undefined},
                 reckon_db_chain_watermark:lookup(StoreId, Chainless)).

%% @doc GIVEN a stream with a sealed watermark
%%      WHEN the stream is deleted and a new stream is created under the same
%%           id
%%      THEN the watermark went with it, in the same transaction
%%
%% A watermark left behind gives the new stream a chain start it never had:
%% its earliest events would read as pre-integrity legacy, unverified.
deleting_a_stream_deletes_its_watermark(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"deleted-stream">>),

    ok = start_store(StoreId, DataDir, enabled),
    {ok, 2} = append_events(StoreId, StreamId, 3),
    ?assertMatch({ok, {sealed, 0}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),

    ok = reckon_db_streams:delete(StoreId, StreamId),
    ?assertMatch({ok, undefined},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)).

%%====================================================================
%% Helpers
%%====================================================================

%% @private A store left in the shape an attacker who deleted every
%% watermark leaves: one chain-bearing stream (intact) with no watermark, and
%% one chain-less stream with no watermark and no marker. The anchor goes too,
%% so the migration runs rather than reporting the violation the anchor would
%% raise.
evidence_store(Config) ->
    {StoreId, DataDir} = store(Config),
    Chainless = stream(<<"evidence-chainless">>),
    Chain = stream(<<"evidence-chain">>),
    ok = start_store(StoreId, DataDir, disabled),
    ok = write_legacy(StoreId, Chainless, 3),
    ok = stop_store(StoreId),
    ok = start_store(StoreId, DataDir, enabled),
    {ok, 2} = append_events(StoreId, Chain, 3),
    ok = forget_migration(StoreId),
    ok = forget_anchor(DataDir),
    ok = delete_watermark(StoreId, Chainless),
    ok = delete_watermark(StoreId, Chain),
    ok = stop_store(StoreId),
    ok = start_store(StoreId, DataDir, enabled),
    {StoreId, DataDir, Chainless, Chain}.

%% @private Append events for versions First..Last, with `Expected' the
%% version the caller believes the stream is at now (append/4's guard).
append_from(StoreId, StreamId, Expected, First, Last) ->
    reckon_db_streams:append(StoreId, StreamId, Expected,
                             [#{event_type => <<"e">>, data => #{n => N}}
                              || N <- lists:seq(First, Last)]).

%% @private
append_events(StoreId, StreamId, N) ->
    reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM,
                             [#{event_type => <<"e">>, data => #{n => I}}
                              || I <- lists:seq(1, N)]).

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

delete_watermark(StoreId, StreamId) ->
    _ = khepri:delete(StoreId, watermark_path(StreamId)),
    ok.

anchor_file(DataDir) ->
    filename:join(DataDir, "integrity_migration.done").

forget_anchor(DataDir) ->
    _ = file:delete(anchor_file(DataDir)),
    ok.

presence(StoreId, Path) ->
    case khepri:get(StoreId, Path) of
        {ok, _} -> present;
        _ -> absent
    end.

events(StoreId, StreamId) ->
    {ok, Events} = read_raw(StoreId, StreamId),
    Events.

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

%% @doc GIVEN a sealed watermark and its life token saved from a stream's
%%           first life
%%      WHEN the stream is deleted, re-created, and both are put back
%%      THEN the stream refuses: the new life's events were MAC'd under a
%%           life the restored token contradicts
%%
%% The life token is a tree node, so it replays with the seal. What refuses
%% the replay is the v2 event MAC, which covers the life: a surviving MAC'd
%% event of the new life cannot verify under the restored token.
a_sealed_watermark_does_not_survive_delete_and_recreate(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"life-replay">>),

    ok = start_store(StoreId, DataDir, enabled),
    {ok, 2} = append_events(StoreId, StreamId, 3),
    {ok, OldRecord} = current_watermark(StoreId, StreamId),
    {ok, OldLife} = reckon_db_chain_watermark:life(StoreId, StreamId),

    ok = reckon_db_streams:delete(StoreId, StreamId),
    {ok, 2} = append_events(StoreId, StreamId, 3),

    %% The replay: the old life token and the old sealed record, together.
    ok = khepri:put(StoreId, life_path(StreamId), OldLife),
    ok = khepri:put(StoreId, watermark_path(StreamId), OldRecord),

    ?assertMatch({error, {integrity_violation, _}},
                 read_raw(StoreId, StreamId)).

%% @doc GIVEN one stream whose watermark is a map with a junk MAC
%%      WHEN the store starts
%%      THEN that stream is refused and every other stream is sealed and
%%           readable; the migration is not aborted
%%
%% The compare-and-swap is against the raw tree value, so a value of an
%% unexpected shape refuses its own stream instead of aborting the store's
%% whole migration.
a_junk_watermark_refuses_only_its_own_stream(Config) ->
    {StoreId, DataDir} = store(Config),
    Forged = stream(<<"junk-watermark">>),
    Innocent = stream(<<"junk-innocent">>),

    ok = start_store(StoreId, DataDir, disabled),
    ok = write_legacy(StoreId, Forged, 3),
    ok = write_legacy(StoreId, Innocent, 3),
    ok = khepri:put(StoreId, watermark_path(Forged),
                    #{version => 3, mac => <<0:256>>}),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, {sealed, 3}},
                 reckon_db_chain_watermark:lookup(StoreId, Innocent)),
    ?assertEqual(3, length(events(StoreId, Innocent))),
    ?assertMatch({ok, {unsealed, _}},
                 reckon_db_chain_watermark:lookup(StoreId, Forged)),
    ?assertMatch({error, {integrity_violation, _}},
                 read_raw(StoreId, Forged)),
    ?assertMatch({ok, _}, khepri:get(StoreId, ?INTEGRITY_MIGRATION_PATH)).

%% @doc GIVEN a store whose marker path holds a value that does not verify
%%      WHEN it starts with integrity enabled
%%      THEN nothing is sealed, the anchor is not written, and the store is
%%           not called migrated
%%
%% Presence alone is not evidence of a migration: only a marker that
%% verifies under the store's key is.
a_junk_marker_does_not_count_as_migrated(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"junk-marker">>),

    ok = start_store(StoreId, DataDir, disabled),
    ok = write_legacy(StoreId, StreamId, 3),
    ok = khepri:put(StoreId, ?INTEGRITY_MIGRATION_PATH, #{junk => true}),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, undefined},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, _}},
                 read_raw(StoreId, StreamId)),
    ?assertNot(filelib:is_regular(anchor_file(DataDir))).

%% @doc GIVEN a stream whose prefix was removed by a scavenge the store
%%           recorded a snapshot for, so the genesis event is gone, left in
%%           the shape the 5.11 line leaves: no watermark and no life token
%%      WHEN the store starts
%%      THEN the stream is sealed at the cut, not read as deletion evidence,
%%           and the boundary the seal mints verifies under the life the
%%           migration writes - so the stream it just sealed still reads
%%
%% A snapshot whose anchor_hash is the hash the surviving event carries, at
%% one below its version, is proof the store wrote. Without it the stream is
%% refused (see the evidence case above).
%%
%% The life token is deleted on purpose (Jupiter r16 JR6): with one present
%% the migration reuses it, and a boundary minted under either the old or the
%% new life would verify, hiding the mistake. On a 5.11-shaped stream the plan
%% mints a NEW life in the same transaction as the seal and the boundary, and
%% a boundary that names the old (absent) one stops verifying the instant the
%% seal lands - the stream refuses every read, silently.
a_snapshot_authenticated_cut_is_sealed(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"snapshot-cut">>),

    ok = start_store(StoreId, DataDir, enabled),
    {ok, 9} = append_events(StoreId, StreamId, 10),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 4, #{state => s}),
    %% The survivors, the snapshot and the chain as 5.11 left them: scheme-1
    %% MACs over the whole stream, no life token, no watermark. Rewriting the
    %% MACs leaves the chain hashes alone, so the snapshot still anchors the
    %% event at 5 exactly as it did before.
    ok = chain_scheme_one(StoreId, StreamId, 0, 10),
    ok = delete_events(StoreId, StreamId, 0, 4),
    ok = make_old_line_snapshot(StoreId, StreamId),
    ok = forget_migration(StoreId),
    ok = forget_anchor(DataDir),
    ok = delete_watermark(StoreId, StreamId),
    ok = khepri:delete(StoreId, life_path(StreamId)),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, {sealed, 5}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertEqual(5, length(events(StoreId, StreamId))),
    %% The boundary the migration minted beside the seal must VERIFY, or the
    %% stream it just sealed refuses every read: the migration writes the new
    %% life token in the same transaction, so the boundary has to name the new
    %% life and not the one the stream happened to hold before (Jupiter r16
    %% JR6 - the half that is easy to get wrong, and silent when wrong).
    {ok, Boundary} = reckon_db_scavenge_boundary:lookup(StoreId, StreamId),
    ?assert(reckon_db_scavenge_boundary:verify(StoreId, StreamId, Boundary)),
    {ok, Served} = reckon_db_streams:read(StoreId, StreamId, 5, 10, forward),
    ?assertEqual(5, length(Served)).

%% @doc GIVEN a chain whose first five events were STRIPPED but still sit in
%%           the tree, a snapshot taken at 4, and the watermark deleted
%%      WHEN the store starts
%%      THEN nothing is sealed: a cut may only be anchored at the lowest
%%           SURVIVING event, and five events survive below this one
%%
%% Jupiter r15 JR1 and JR1b. Anchoring the cut at the lowest CHAIN-BEARING
%% event instead let a snapshot at 4 vouch for events 0..4 that were still
%% there and no longer carried a chain: the stream was sealed at 5 and a
%% rewritten event 2 read back as ok.
a_cut_does_not_cover_events_that_still_survive(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"cut-over-survivors">>),

    ok = start_store(StoreId, DataDir, enabled),
    {ok, 9} = append_events(StoreId, StreamId, 10),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 4, #{state => s}),
    lists:foreach(fun(V) -> ok = clear_integrity_fields(StoreId, StreamId, V) end,
                  lists:seq(0, 4)),
    ok = forget_migration(StoreId),
    ok = forget_anchor(DataDir),
    ok = delete_watermark(StoreId, StreamId),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, undefined},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, _}}, read_raw(StoreId, StreamId)).

%% @doc GIVEN a genuine scavenged gap whose snapshot cut is sound, and a bare
%%           watermark that claims a version ABOVE the first survivor
%%      WHEN the store starts
%%      THEN the stream is refused rather than sealed at the cut, because an
%%           honest old-line watermark is the original chain start and can
%%           never sit above it
%%
%% Terra r15, probe_cut_with_w_above_survivor. T3's cut path carries this
%% final condition for the watermark that is still there.
a_cut_with_a_watermark_above_the_survivors_is_refused(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"cut-watermark-above">>),

    ok = start_store(StoreId, DataDir, enabled),
    {ok, 9} = append_events(StoreId, StreamId, 10),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 4, #{state => s}),
    ok = delete_events(StoreId, StreamId, 0, 4),
    ok = forget_migration(StoreId),
    ok = forget_anchor(DataDir),
    ok = put_bare_watermark(StoreId, StreamId, 7),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, {unsealed, 7}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertMatch({error, {integrity_violation, _}}, read_raw(StoreId, StreamId)).

%% @doc GIVEN an honest store as the 5.11 line left it: a chain stream whose
%%           bare watermark sits at its genesis-proven chain start, beside a
%%           stream that predates integrity and has no watermark at all
%%      WHEN the store starts with integrity enabled
%%      THEN both are sealed and both serve
%%
%% Jupiter r15 JR2 and Terra's probe_honest_old_line. Before this release
%% nothing was ever sealed, so counting an unsealed watermark as deletion
%% evidence refused every legacy stream in an honestly upgraded store: the
%% upgrade is supposed to be an ordinary restart.
an_honest_old_line_upgrade_seals_its_legacy_streams(Config) ->
    {StoreId, DataDir} = store(Config),
    ChainId = stream(<<"old-line-chain">>),
    LegacyId = stream(<<"old-line-legacy">>),
    WateredId = stream(<<"old-line-watered">>),

    ok = start_store(StoreId, DataDir, enabled),
    {ok, 4} = append_events(StoreId, ChainId, 5),
    ok = write_legacy(StoreId, LegacyId, 3),
    ok = write_legacy(StoreId, WateredId, 3),
    %% Put the store back into the shape the 5.11 line left: a chain with
    %% store-blind MACs and its original bare watermark, and streams that
    %% predate integrity entirely, one of them with a bare watermark too.
    ok = make_old_line_chain(StoreId, ChainId, 5),
    ok = make_pre_integrity(StoreId, LegacyId, 3, undefined),
    ok = make_pre_integrity(StoreId, WateredId, 3, 3),
    ok = forget_migration(StoreId),
    ok = forget_anchor(DataDir),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, {sealed, 0}},
                 reckon_db_chain_watermark:lookup(StoreId, ChainId)),
    ?assertMatch({ok, {sealed, 3}},
                 reckon_db_chain_watermark:lookup(StoreId, LegacyId)),
    ?assertMatch({ok, {sealed, 3}},
                 reckon_db_chain_watermark:lookup(StoreId, WateredId)),
    ?assertEqual(5, length(events(StoreId, ChainId))),
    ?assertEqual(3, length(events(StoreId, LegacyId))),
    ?assertEqual(3, length(events(StoreId, WateredId))).

%% @doc GIVEN an old-line stream whose bare watermark sits BELOW a
%%           genesis-proven chain start, and a chain-less stream with no
%%           watermark
%%      WHEN the store starts and the migration scans
%%      THEN the chain-bearing stream is sealed from its own proof, and the
%%           chain-less stream is NOT sealed: the value below the start is
%%           evidence, because the old line cannot write one
%%
%% Terra r16, observation. `setup_integrity/3' mints the watermark at
%% newest + 1 and stamps the first event at that same version, so on the
%% genesis path the old line's bare value EQUALS the start. Honouring a
%% smaller number lets a writer who deleted a watermark suppress the store's
%% deletion evidence by putting a lower one in its place, and the streams that
%% evidence exists to protect are sealed over. On the cut path a scavenge
%% leaves the watermark where it was, so below the start stays honest there.
a_bare_watermark_below_a_genesis_start_is_evidence(Config) ->
    {StoreId, DataDir} = store(Config),
    ChainId = stream(<<"below-start-chain">>),
    ChainlessId = stream(<<"below-start-chainless">>),

    ok = start_store(StoreId, DataDir, enabled),
    {ok, 5} = append_events(StoreId, ChainId, 6),
    ok = write_legacy(StoreId, ChainlessId, 3),
    ok = make_old_line_chain_from(StoreId, ChainId, 3, 6, 2),
    ok = make_pre_integrity(StoreId, ChainlessId, 3, undefined),
    ok = forget_migration(StoreId),
    ok = forget_anchor(DataDir),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, {sealed, 3}},
                 reckon_db_chain_watermark:lookup(StoreId, ChainId)),
    ?assertMatch({ok, undefined},
                 reckon_db_chain_watermark:lookup(StoreId, ChainlessId)),
    ?assertMatch({error, {integrity_violation, #{kind := missing_integrity}}},
                 read_raw(StoreId, ChainlessId)).

%% @doc GIVEN a stream the migration sealed at a cut (a boundary at 5, the
%%           prefix removed by a scavenge), with chain-less events PLANTED at
%%           2..4 afterwards
%%      WHEN a read covers the planted range
%%      THEN it refuses: a boundary records a prefix that was REMOVED, so
%%           nothing honest survives below it and no region there is honoured
%%
%% Terra r18 T18-1. is_legacy/2 only asked whether an event has no chain and
%% sits below the seal, so planted records below a sealed cut were served as
%% pre-integrity data. A full read refused at the seal; a read that started
%% inside the planted range never looked there.
a_forged_prefix_below_a_sealed_cut_is_refused(Config) ->
    {StoreId, DataDir} = store(Config),
    StreamId = stream(<<"forged-below-cut">>),

    ok = start_store(StoreId, DataDir, enabled),
    {ok, 9} = append_events(StoreId, StreamId, 10),
    ok = reckon_db_snapshots:save(StoreId, StreamId, 4, #{state => s}),
    ok = chain_scheme_one(StoreId, StreamId, 0, 10),
    ok = delete_events(StoreId, StreamId, 0, 4),
    ok = make_old_line_snapshot(StoreId, StreamId),
    ok = forget_migration(StoreId),
    ok = forget_anchor(DataDir),
    ok = delete_watermark(StoreId, StreamId),
    ok = khepri:delete(StoreId, life_path(StreamId)),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, {sealed, 5}},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    {ok, #{} = _Boundary} =
        reckon_db_scavenge_boundary:lookup(StoreId, StreamId),

    lists:foreach(fun(V) -> ok = plant_chainless_event(StoreId, StreamId, V) end,
                  lists:seq(2, 4)),
    ?assertMatch({error, {integrity_violation, _}},
                 reckon_db_streams:read(StoreId, StreamId, 2, 3, forward)).

%% @doc GIVEN a junk MAC map at the watermark path
%%      WHEN the store starts
%%      THEN that stream alone is refused, by a reason that names the junk
%%           watermark for what it is, whichever shape the stream has
%%
%% J2 and Terra r15 probe_junk_map_chain_bearing: a chain-bearing stream was
%% silently sealed over it, and a chain-less one was refused as
%% {above_newest, Map}, which names the wrong thing.
a_junk_watermark_is_refused_by_name(Config) ->
    {StoreId, DataDir} = store(Config),
    ChainId = stream(<<"junk-map-chain">>),
    LegacyId = stream(<<"junk-map-legacy">>),

    ok = start_store(StoreId, DataDir, enabled),
    {ok, 4} = append_events(StoreId, ChainId, 5),
    ok = write_legacy(StoreId, LegacyId, 3),
    Junk = #{version => 0, v2_from => 0, life => <<"nope">>, mac => <<"junk">>},
    ok = khepri:put(StoreId, watermark_path(ChainId), Junk),
    ok = khepri:put(StoreId, watermark_path(LegacyId), Junk),
    ok = forget_migration(StoreId),
    ok = forget_anchor(DataDir),
    ok = stop_store(StoreId),

    ok = start_store(StoreId, DataDir, enabled),
    ?assertMatch({ok, {unsealed, #{mac := <<"junk">>}}},
                 reckon_db_chain_watermark:lookup(StoreId, ChainId)),
    ?assertMatch({ok, {unsealed, #{mac := <<"junk">>}}},
                 reckon_db_chain_watermark:lookup(StoreId, LegacyId)),
    Refused = refused_streams(StoreId),
    ?assert(lists:member(ChainId, Refused)),
    ?assert(lists:member(LegacyId, Refused)).

%% @private Turn a stream into the shape the 5.11 line left for a chain: every
%% event stamped with the store-blind (scheme 1) MAC over the same payload, the
%% predecessor chain re-stamped from the genesis value, the sealed watermark
%% replaced by the original bare integer, and no life token. This release
%% cannot produce a scheme-1 event any more, so the test rewrites one by hand.
make_old_line_chain(StoreId, StreamId, N) ->
    Key = reckon_db_integrity_key:get(StoreId),
    ?assert(is_binary(Key)),
    _Tip = lists:foldl(
             fun(Version, PrevTip) ->
                     {ok, Event0} = khepri:get(StoreId,
                                               event_path(StreamId, Version)),
                     Bare = Event0#event{prev_event_hash = PrevTip,
                                         mac = undefined, signature = undefined},
                     Mac = reckon_gater_integrity:compute_event_mac(Bare, Key),
                     Event = Bare#event{mac = Mac},
                     ok = khepri:put(StoreId, event_path(StreamId, Version), Event),
                     reckon_gater_integrity:compute_chain_hash(Event, PrevTip)
             end, reckon_gater_integrity:genesis_prev_hash(),
             lists:seq(0, N - 1)),
    ok = khepri:delete(StoreId, life_path(StreamId)),
    ok = khepri:put(StoreId, watermark_path(StreamId), 0),
    ok.

%% @private Plant an event with no chain fields at `Version', which is what a
%% tree writer can do: a forged "pre-integrity" record.
plant_chainless_event(StoreId, StreamId, Version) ->
    Event = #event{event_id = integer_to_binary(Version),
                   event_type = <<"forged">>,
                   stream_id = StreamId,
                   version = Version,
                   data = #{forged => 1},
                   metadata = #{}},
    khepri:put(StoreId, event_path(StreamId, Version), Event).

%% @private Rewrite a stream's newest snapshot as the 5.11 line wrote it: a
%% scheme-1 MAC over the snapshot and the key alone, with neither the store
%% nor a life token bound to it. That is the shape a store holding no life
%% token at all has, and the only one whose MAC still verifies for a stream
%% whose life the migration is about to mint.
make_old_line_snapshot(StoreId, StreamId) ->
    Key = reckon_db_integrity_key:get(StoreId),
    ?assert(is_binary(Key)),
    Snapshot = reckon_db_snapshots_store:get_latest(StoreId, StreamId),
    Mac = reckon_gater_integrity:compute_snapshot_mac(Snapshot, Key),
    reckon_db_snapshots_store:put(StoreId, Snapshot#snapshot{mac = Mac}).

%% @private Give versions `Start'..`N'-1 a scheme-1 chain that begins at the
%% genesis predecessor: the chain and the MACs the 5.11 line wrote, bound to
%% the key alone.
chain_scheme_one(StoreId, StreamId, Start, N) ->
    Key = reckon_db_integrity_key:get(StoreId),
    ?assert(is_binary(Key)),
    _Tip = lists:foldl(
             fun(Version, PrevTip) ->
                     {ok, Event0} = khepri:get(StoreId,
                                               event_path(StreamId, Version)),
                     Bare = Event0#event{prev_event_hash = PrevTip,
                                         mac = undefined, signature = undefined},
                     Mac = reckon_gater_integrity:compute_event_mac(Bare, Key),
                     Event = Bare#event{mac = Mac},
                     ok = khepri:put(StoreId, event_path(StreamId, Version), Event),
                     reckon_gater_integrity:compute_chain_hash(Event, PrevTip)
             end, reckon_gater_integrity:genesis_prev_hash(),
             lists:seq(Start, N - 1)),
    ok.

%% @private A stream whose events below `Start' predate integrity and whose
%% events from `Start' carry a scheme-1 chain beginning at the genesis
%% predecessor - the shape the 5.11 line leaves when integrity was enabled
%% part-way through a stream's life.
make_old_line_chain_from(StoreId, StreamId, Start, N, Watermark) ->
    ok = chain_scheme_one(StoreId, StreamId, Start, N),
    lists:foreach(fun(V) -> ok = clear_integrity_fields(StoreId, StreamId, V) end,
                  lists:seq(0, Start - 1)),
    ok = khepri:delete(StoreId, life_path(StreamId)),
    ok = khepri:put(StoreId, watermark_path(StreamId), Watermark).

%% @private A stream as it looked before integrity existed: no chain fields on
%% any event, no life token, and either no watermark at all or the old bare
%% integer.
make_pre_integrity(StoreId, StreamId, N, Watermark) ->
    lists:foreach(fun(V) -> ok = clear_integrity_fields(StoreId, StreamId, V) end,
                  lists:seq(0, N - 1)),
    ok = khepri:delete(StoreId, life_path(StreamId)),
    case Watermark of
        undefined -> ok = khepri:delete(StoreId, watermark_path(StreamId));
        Version -> ok = khepri:put(StoreId, watermark_path(StreamId), Version)
    end.

%% @private
refused_streams(StoreId) ->
    {ok, #{refused := Refused}} = khepri:get(StoreId, ?INTEGRITY_MIGRATION_PATH),
    Refused.

%% @private
current_watermark(StoreId, StreamId) ->
    khepri:get(StoreId, watermark_path(StreamId)).

%% @private
delete_events(StoreId, StreamId, From, To) ->
    lists:foreach(
      fun(V) ->
              _ = khepri:delete(StoreId,
                                reckon_db_stream_path:event_path(
                                  StreamId, pad_version(V)))
      end, lists:seq(From, To)),
    ok.

%% @private
life_path(StreamId) ->
    ?INTEGRITY_STREAM_LIFE_PATH ++ [StreamId].
