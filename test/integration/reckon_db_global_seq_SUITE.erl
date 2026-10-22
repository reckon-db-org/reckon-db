%% @doc The stable global sequence (reckon-db#7).
%%
%% A seq is assigned inside the append transaction, is never reused, and is
%% what a consumer checkpoints on. These cases pin the properties the design
%% claims: contiguity per append, continuity across appends, a scavenged
%% event leaving a hole that is skipped rather than an offset that shifts,
%% and a backfilled store numbering its survivors so a pre-upgrade
%% checkpoint keeps its meaning.
-module(reckon_db_global_seq_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([a_new_store_numbers_from_zero/1,
         a_second_append_continues_after_the_first/1,
         a_scavenged_event_is_a_hole_not_a_shift/1,
         a_backfilled_store_numbers_survivors_in_reader_order/1,
         an_unbackfilled_store_with_events_refuses_an_append/1,
         a_read_on_an_unbackfilled_store_is_refused/1,
         the_read_carries_the_seq_a_consumer_checkpoints_on/1,
         a_catchup_subscription_delivers_the_seq/1,
         the_head_authenticates_the_seq_log/1,
         a_rewritten_mapping_is_detected/1,
         a_deleted_mapping_is_detected/1,
         a_mapping_above_the_head_is_detected/1,
         an_append_refuses_a_log_whose_head_is_gone/1,
         a_store_numbered_without_integrity_is_sealed_at_start/1,
         concurrent_appends_keep_one_verified_head/1]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [a_new_store_numbers_from_zero,
     a_second_append_continues_after_the_first,
     a_scavenged_event_is_a_hole_not_a_shift,
     a_backfilled_store_numbers_survivors_in_reader_order,
     an_unbackfilled_store_with_events_refuses_an_append,
     a_read_on_an_unbackfilled_store_is_refused,
     the_read_carries_the_seq_a_consumer_checkpoints_on,
     a_catchup_subscription_delivers_the_seq,
     the_head_authenticates_the_seq_log,
     a_rewritten_mapping_is_detected,
     a_deleted_mapping_is_detected,
     a_mapping_above_the_head_is_detected,
     an_append_refuses_a_log_whose_head_is_gone,
     a_store_numbered_without_integrity_is_sealed_at_start,
     concurrent_appends_keep_one_verified_head].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    RaDataDir = "/tmp/reckon_db_global_seq_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    Config.

end_per_suite(_Config) ->
    ok.

%%====================================================================
%% CT boilerplate
%%====================================================================

init_per_testcase(TestCase, Config) ->
    StoreId = store_name(TestCase),
    DataDir = test_data_dir(TestCase),
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    {ok, _} = khepri:start(DataDir, StoreId),
    ok = khepri:put(StoreId, [streams], #{}),
    ok = khepri:put(StoreId, [metadata], #{}),
    [{store_id, StoreId}, {data_dir, DataDir} | Config].

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    _ = catch khepri:stop(StoreId),
    os:cmd("rm -rf " ++ proplists:get_value(data_dir, Config)),
    ok.

store_name(TestCase) ->
    list_to_atom("global_seq_" ++ atom_to_list(TestCase) ++ "_" ++
                 integer_to_list(erlang:unique_integer([positive]))).

test_data_dir(TestCase) ->
    "/tmp/reckon_db_global_seq_" ++ atom_to_list(TestCase) ++ "_" ++
        integer_to_list(erlang:unique_integer([positive])).

%%====================================================================
%% Cases
%%====================================================================

%% A store with nothing in it starts at 0 and numbers each event of the
%% append in order.
a_new_store_numbers_from_zero(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"seq-new">>),
    {ok, 1} = append(StoreId, StreamId, ?NO_STREAM, [1, 2]),
    ?assertEqual({ok, 0}, reckon_db_global_seq:seq_for(StoreId, StreamId, 0)),
    ?assertEqual({ok, 1}, reckon_db_global_seq:seq_for(StoreId, StreamId, 1)),
    ?assertEqual({ok, 1}, reckon_db_global_seq:last_assigned(StoreId)).

a_second_append_continues_after_the_first(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"seq-continue">>),
    {ok, 0} = append(StoreId, StreamId, ?NO_STREAM, [1]),
    {ok, 1} = append(StoreId, StreamId, 0, [2]),
    ?assertEqual({ok, 0}, reckon_db_global_seq:seq_for(StoreId, StreamId, 0)),
    ?assertEqual({ok, 1}, reckon_db_global_seq:seq_for(StoreId, StreamId, 1)),
    ?assertEqual({ok, 1}, reckon_db_global_seq:last_assigned(StoreId)).

%% Scavenge deletes event nodes and renumbers nothing. The seq of a
%% surviving event must not move, and the hole is skipped, not counted.
a_scavenged_event_is_a_hole_not_a_shift(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"seq-scavenge">>),
    {ok, 3} = append(StoreId, StreamId, ?NO_STREAM, [1, 2, 3, 4]),
    %% Exactly what a scavenge of the first two versions does.
    ok = khepri:delete(StoreId, event_path(StreamId, 0)),
    ok = khepri:delete(StoreId, event_path(StreamId, 1)),
    {ok, Page} = reckon_db_global_seq:read_from(StoreId, none, 10),
    ?assertEqual([2, 3], [Seq || {Seq, _Event} <- Page]),
    %% A checkpoint is inclusive, so it never skips: from seq 2 the reader
    %% answers the survivor at 2 and the one after it.
    {ok, FromCheckpoint} = reckon_db_global_seq:read_from(StoreId, 2, 10),
    ?assertEqual([2, 3], [Seq || {Seq, _Event} <- FromCheckpoint]).

%% A store whose events predate the sequence: the backfill numbers the
%% survivors in the order the old reader produced, so a pre-upgrade
%% checkpoint (an offset) still names the same event afterwards.
a_backfilled_store_numbers_survivors_in_reader_order(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"seq-backfill">>),
    {ok, 2} = append(StoreId, StreamId, ?NO_STREAM, [1, 2, 3]),
    %% Rewind the store to its pre-upgrade shape: no seqs, no marker.
    ok = khepri:delete(StoreId, [metadata, global_seq, backfilled]),
    ok = delete_seq_refs(StoreId, StreamId, [0, 1, 2]),
    ?assertEqual(none, reckon_db_global_seq:last_assigned(StoreId)),
    %% The old order is what the backfill numbers.
    {ok, Ordered} = reckon_db_streams:all_events_in_global_order(StoreId),
    ok = reckon_db_global_seq:backfill(StoreId),
    ?assert(reckon_db_global_seq:is_backfilled(StoreId)),
    {ok, Page} = reckon_db_global_seq:read_from(StoreId, none, 10),
    ?assertEqual([E#event.version || E <- Ordered], [E#event.version || {_Seq, E} <- Page]),
    ?assertEqual([0, 1, 2], [Seq || {Seq, _E} <- Page]).

%% A store with events and no backfill is refused: numbering new events
%% from 0 beside unnumbered survivors would give two events one seq.
an_unbackfilled_store_with_events_refuses_an_append(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"seq-refuse">>),
    {ok, 0} = append(StoreId, StreamId, ?NO_STREAM, [1]),
    ok = khepri:delete(StoreId, [metadata, global_seq, backfilled]),
    ok = delete_seq_refs(StoreId, StreamId, [0]),
    ?assertMatch({error, _}, append_any(StoreId, sid(<<"seq-refuse-2">>), [1])),
    ok.

a_read_on_an_unbackfilled_store_is_refused(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    ?assertEqual({error, {store_not_migrated, StoreId}},
                 reckon_db_global_seq:read_from(StoreId, none, 10)).


%%====================================================================
%% The read path and the authenticated head
%%====================================================================

%% What a consumer checkpoints on: the seq travels with the event, on both
%% read paths, so a projection saves the same number whichever delivered it.
the_read_carries_the_seq_a_consumer_checkpoints_on(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"seq-position">>),
    {ok, 2} = append(StoreId, StreamId, ?NO_STREAM, [1, 2, 3]),
    {ok, Events} = reckon_db_streams:read_all_global(StoreId, 0, 10),
    ?assertEqual([0, 1, 2], [position(E) || E <- Events]),
    %% The stored event is unchanged: the seq is delivery metadata, not a
    %% field of the record.
    {ok, [Stored]} = reckon_db_streams:read(StoreId, StreamId, 0, 1, forward),
    ?assertEqual(undefined, maps:get(global_position, Stored#event.metadata, undefined)),
    %% A cursor is inclusive and names events, not offsets: reading from the
    %% second event's seq answers that event and everything after it.
    {ok, FromOne} = reckon_db_streams:read_all_global(StoreId, 1, 10),
    ?assertEqual([1, 2], [position(E) || E <- FromOne]).

%% The catch-up subscription hands the subscriber the same number, as
%% metadata on each delivered event.
a_catchup_subscription_delivers_the_seq(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"seq-catchup">>),
    {ok, 2} = append(StoreId, StreamId, ?NO_STREAM, [1, 2, 3]),
    {ok, _} = reckon_db_subscriptions:subscribe(
                StoreId, stream, StreamId, <<"seq_catchup_sub">>,
                #{subscriber => self()}),
    Delivered = collect_events(3, []),
    ?assertEqual([0, 1, 2], [position(E) || E <- Delivered]).

%% A store with integrity signs the seq log: the head covers every mapping
%% the store wrote, and verifying recomputes it from the tree.
the_head_authenticates_the_seq_log(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    with_integrity(StoreId),
    StreamId = sid(<<"seq-head">>),
    {ok, 2} = append(StoreId, StreamId, ?NO_STREAM, [1, 2, 3]),
    {ok, Head} = reckon_db_global_seq:head(StoreId),
    ?assertEqual(0, maps:get(from, Head)),
    ?assertEqual(2, maps:get(last, Head)),
    ?assertEqual(ok, reckon_db_global_seq:verify(StoreId)).

%% Pointing a seq at another event is what a rewritten resume point looks
%% like; the chain no longer lands where the head says it does.
a_rewritten_mapping_is_detected(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    with_integrity(StoreId),
    StreamId = sid(<<"seq-rewrite">>),
    {ok, 2} = append(StoreId, StreamId, ?NO_STREAM, [1, 2, 3]),
    ok = khepri:put(StoreId, reckon_db_global_seq:path(1),
                    #{stream_id => StreamId, version => 2}),
    ?assertEqual({error, {ref_moved, 2}},
                 reckon_db_global_seq:verify(StoreId)).

%% Deleting a mapping out of the middle of the log leaves a resume point
%% that names nothing.
a_deleted_mapping_is_detected(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    with_integrity(StoreId),
    StreamId = sid(<<"seq-delete">>),
    {ok, 2} = append(StoreId, StreamId, ?NO_STREAM, [1, 2, 3]),
    ok = khepri:delete(StoreId, reckon_db_global_seq:path(1)),
    ?assertEqual({error, {ref_missing, 1}},
                 reckon_db_global_seq:verify(StoreId)).

%% A mapping above the head is a seq this store never authenticated, which
%% is exactly the tail an attacker appends to move a resume point forward.
a_mapping_above_the_head_is_detected(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    with_integrity(StoreId),
    StreamId = sid(<<"seq-above">>),
    {ok, 2} = append(StoreId, StreamId, ?NO_STREAM, [1, 2, 3]),
    %% A mapping added without moving the counter: a seq the head does not
    %% cover.
    {ok, ok} = khepri:transaction(
           StoreId,
           fun() ->
                   ok = khepri_tx:put(reckon_db_global_seq:path(3),
                                      #{stream_id => StreamId, version => 3}),
                   ok
           end),
    ?assertEqual({error, {ref_beyond_head, 3}},
                 reckon_db_global_seq:verify(StoreId)),
    %% The same mapping AND a counter moved to match it: the counter and the
    %% head are written in one command, so a pair that disagrees is a pair
    %% one of which was rewritten.
    {ok, ok} = khepri:transaction(
           StoreId,
           fun() ->
                   ok = khepri_tx:put([metadata, global_seq, last_assigned], 3),
                   ok
           end),
    ?assertEqual({error, {head_counter_mismatch, 2, 3}},
                 reckon_db_global_seq:verify(StoreId)).

%% Deleting the head over a log that has seqs is refused rather than
%% silently re-chained: extending a head the store cannot vouch for would
%% carry the tampering forward.
an_append_refuses_a_log_whose_head_is_gone(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    with_integrity(StoreId),
    StreamId = sid(<<"seq-nohead">>),
    {ok, 0} = append(StoreId, StreamId, ?NO_STREAM, [1]),
    ok = khepri:delete(StoreId, [metadata, integrity, global_seq_head]),
    ?assertMatch({error, _}, append_any(StoreId, sid(<<"seq-nohead-2">>), [1])),
    ?assertEqual({error, {head_absent, StoreId}},
                 reckon_db_global_seq:verify(StoreId)).

%% A store numbered while integrity was off gets its head sealed at start,
%% over the seqs it already holds: switching integrity on is not an
%% operator step.
a_store_numbered_without_integrity_is_sealed_at_start(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"seq-seal">>),
    {ok, 3} = append(StoreId, StreamId, ?NO_STREAM, [1, 2, 3, 4]),
    ?assertEqual(none, reckon_db_global_seq:head(StoreId)),
    with_integrity(StoreId),
    %% What a store start does with integrity on: the migration seals each
    %% existing stream's chain start, then the sequence head is sealed over
    %% the seqs already written. Neither is an operator step.
    {ok, _Sealed} = reckon_db_chain_watermark:migrate(StoreId, #{}),
    ok = reckon_db_global_seq:ensure_head(StoreId),
    {ok, Head} = reckon_db_global_seq:head(StoreId),
    ?assertEqual(0, maps:get(from, Head)),
    ?assertEqual(3, maps:get(last, Head)),
    ?assertEqual(ok, reckon_db_global_seq:verify(StoreId)),
    %% And the next append extends the sealed head rather than starting over.
    {ok, 4} = append(StoreId, StreamId, 3, [5]),
    {ok, Extended} = reckon_db_global_seq:head(StoreId),
    ?assertEqual(0, maps:get(from, Extended)),
    ?assertEqual(4, maps:get(last, Extended)),
    ?assertEqual(ok, reckon_db_global_seq:verify(StoreId)).


%% The head is prepared outside the append transaction, so two appends that
%% race take each other's seqs and one of them must start again. What must
%% hold either way: every append lands, every event has its own seq, and the
%% head still covers the log afterwards.
concurrent_appends_keep_one_verified_head(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    with_integrity(StoreId),
    Writers = 8,
    Each = 5,
    Parent = self(),
    Pids = [spawn_link(fun() ->
                          Stream = sid(<<"seq-race-", (integer_to_binary(N))/binary>>),
                          Results = [append(StoreId, Stream, Expected, [M])
                                     || {M, Expected} <-
                                            lists:zip(lists:seq(1, Each),
                                                      [?NO_STREAM | lists:seq(0, Each - 2)])],
                          Parent ! {done, self(), Results}
                  end) || N <- lists:seq(1, Writers)],
    Results = [receive {done, Pid, R} -> R after 20000 -> ct:fail(append_timeout) end
               || Pid <- Pids],
    ?assertEqual([], [E || R <- Results, E <- R, element(1, E) =/= ok]),
    {ok, All} = reckon_db_global_seq:read_from(StoreId, none, 100000),
    Seqs = [Seq || {Seq, _Event} <- All],
    ?assertEqual(lists:seq(0, Writers * Each - 1), Seqs),
    ?assertEqual(ok, reckon_db_global_seq:verify(StoreId)).

%%====================================================================
%% Helpers
%%====================================================================

sid(Name) ->
    reckon_db_test_helpers:sid(Name).

append(StoreId, StreamId, ExpectedVersion, Versions) ->
    Events = [#{event_type => <<"seq_test_happened">>,
                data => #{n => N}} || N <- Versions],
    reckon_db_streams:append(StoreId, StreamId, ExpectedVersion, Events).

append_any(StoreId, StreamId, Ns) ->
    append(StoreId, StreamId, ?NO_STREAM, Ns).

event_path(StreamId, Version) ->
    reckon_db_stream_path:event_path(
      StreamId, reckon_db_stream_path:version_key(Version)).

%% Put the store back in the shape an earlier version wrote: no seq, no
%% refs, no marker. Deletes the nodes this test's appends created.
delete_seq_refs(StoreId, StreamId, Versions) ->
    lists:foreach(
      fun(Version) ->
              {ok, Seq} = reckon_db_global_seq:seq_for(StoreId, StreamId, Version),
              ok = khepri:delete(StoreId,
                                 reckon_db_global_seq:ref_path(StreamId, Version)),
              ok = khepri:delete(StoreId, reckon_db_global_seq:path(Seq))
      end, Versions),
    ok = khepri:delete(StoreId, [metadata, global_seq, last_assigned]),
    ok.

%% Install an integrity key for this store, as reckon_db_integrity_key:load/1
%% would after reading it from its source.
with_integrity(StoreId) ->
    Key = crypto:strong_rand_bytes(32),
    persistent_term:put({reckon_db, integrity_key, StoreId}, Key),
    persistent_term:put({reckon_db, integrity_enabled, StoreId}, true),
    ok.

%% The seq the store delivered this event at.
position(#event{metadata = Metadata}) ->
    maps:get(global_position, Metadata).

%% Catch-up delivers in batches; collect until Count events have arrived.
collect_events(Count, Acc) when length(Acc) >= Count ->
    lists:reverse(Acc);
collect_events(Count, Acc) ->
    receive
        {events, Events} -> collect_events(Count, lists:reverse(Events) ++ Acc)
    after 5000 ->
            ct:fail({expected_events, Count, got, length(Acc)})
    end.
