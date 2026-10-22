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
         a_read_on_an_unbackfilled_store_is_refused/1]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [a_new_store_numbers_from_zero,
     a_second_append_continues_after_the_first,
     a_scavenged_event_is_a_hole_not_a_shift,
     a_backfilled_store_numbers_survivors_in_reader_order,
     an_unbackfilled_store_with_events_refuses_an_append,
     a_read_on_an_unbackfilled_store_is_refused].

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
    ?assertEqual({error, global_seq_not_backfilled},
                 append_any(StoreId, sid(<<"seq-refuse-2">>), [1])),
    ok.

a_read_on_an_unbackfilled_store_is_refused(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    ?assertEqual({error, global_seq_not_backfilled},
                 reckon_db_global_seq:read_from(StoreId, none, 10)).

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
