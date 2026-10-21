%% @doc Concurrent appends to one stream must not lose events.
%%
%% The version an append writes from is read before the write transaction -
%% the event records and their index entries are built outside it, because
%% HMAC stamping and crypto cannot run inside a Ra transaction - so two
%% appends that read the same version build the same version numbers and one
%% silently overwrites the other. Every call answers `{ok, Version}': nothing
%% fails, nothing logs, the events are simply gone.
%%
%% The version is therefore verified again INSIDE the transaction, which
%% aborts if the node the batch would occupy is taken or if the node it was
%% built from is gone (reckon_db_streams:append_guard/3), and the append
%% re-reads and retries. These cases are the proof: before it, four writers
%% appending 25 events each to one stream left 25 of the 100 in the store.
%% The five cases before the last five are the race and contention shapes:
%% four writers in ten batches of five, four writers appending singly, two
%% appends that both name version 1, and ten and twenty writers on one stream.
%% The last five are the edges: a scavenged stream in every prefix shape (the
%% count is short of the tip, so the append is refused rather than writing
%% over a live event), a scavenge that deletes every event, which is the one
%% shape that is not a refusal, an append of no events, which writes nothing,
%% a stream deleted between the read and the write, which leaves no
%% predecessor to verify, and the favour the retry's re-read asks for, which a
%% follower must not answer from a stale snapshot. All ten are listed in
%% all/0; eight are red on 66edb40, the empty append and the full scavenge
%% being the two this release does not change.
-module(reckon_db_concurrent_append_SUITE).

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
    concurrent_batch_appends_keep_every_event/1,
    concurrent_single_appends_keep_every_event/1,
    an_explicit_expected_version_still_loses_to_a_concurrent_append/1,
    ten_writers_on_one_stream_keep_every_event/1,
    twenty_writers_on_one_stream_keep_every_event/1,
    a_scavenged_stream_refuses_every_prefix_shape/1,
    a_fully_scavenged_stream_restarts_at_version_zero/1,
    an_append_of_no_events_answers_the_version_it_read/1,
    a_stream_deleted_between_the_read_and_the_write_is_refused/1,
    a_retry_reads_the_version_consistently/1
]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [concurrent_batch_appends_keep_every_event,
     concurrent_single_appends_keep_every_event,
     an_explicit_expected_version_still_loses_to_a_concurrent_append,
     ten_writers_on_one_stream_keep_every_event,
     twenty_writers_on_one_stream_keep_every_event,
     a_scavenged_stream_refuses_every_prefix_shape,
     a_fully_scavenged_stream_restarts_at_version_zero,
     an_append_of_no_events_answers_the_version_it_read,
     a_stream_deleted_between_the_read_and_the_write_is_refused,
     a_retry_reads_the_version_consistently].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    %% No pg scope here on purpose: the append path does not use one, and
    %% starting it by hand leaves a process that is not owned by the reckon_db
    %% application, so every later suite's ensure_all_started(reckon_db) dies
    %% with {already_started, _}.
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    TCStr = atom_to_list(TestCase),
    DataDir = "/tmp/reckon_db_concurrent_append_" ++ TCStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom("concurrent_append_" ++ TCStr ++ "_" ++ Rand),
    {ok, _} = khepri:start(DataDir, StoreId),
    ok = khepri:put(StoreId, [streams], #{}),
    ok = khepri:put(StoreId, [subscriptions], #{}),
    ok = khepri:put(StoreId, [procs], #{}),
    [{data_dir, DataDir}, {store_id, StoreId} | Config].

end_per_testcase(_TestCase, Config) ->
    khepri:stop(proplists:get_value(store_id, Config)),
    os:cmd("rm -rf " ++ proplists:get_value(data_dir, Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% Four writers, ten batches of five events each, all starting together.
concurrent_batch_appends_keep_every_event(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"concurrent-batches">>),
    Results = race(StoreId, StreamId, 4, 10, fun() -> events(5) end),
    assert_every_event_landed(StoreId, StreamId, Results, 40, 200).

%% The same with one event per append: a much smaller window per append, so
%% this one needs the retry to be genuine rather than lucky.
concurrent_single_appends_keep_every_event(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"concurrent-singles">>),
    Results = race(StoreId, StreamId, 4, 25, fun() -> events(1) end),
    assert_every_event_landed(StoreId, StreamId, Results, 100, 100).

%% An append that names its expected version must still be refused when
%% another writer got there first: the guard inside the transaction is what
%% makes `wrong_expected_version' true rather than a race that writes anyway.
an_explicit_expected_version_still_loses_to_a_concurrent_append(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"concurrent-expected">>),
    %% Two events, versions 0 and 1: the stream's version is 1.
    {ok, 1} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(2)),

    %% Both believe they are appending at version 1, i.e. writing version 2.
    Self = self(),
    _ = [spawn_link(fun() ->
                            Self ! {result, reckon_db_streams:append(
                                               StoreId, StreamId, 1, events(1))}
                    end) || _ <- [1, 2]],
    Results = [receive {result, R} -> R after 10000 -> timeout end || _ <- [1, 2]],

    ct:pal("results: ~p", [Results]),
    ?assertEqual(1, length([ok || {ok, _} <- Results])),
    ?assertMatch([{error, {wrong_expected_version, 1, 2}}],
                 [E || {error, _} = E <- Results]),

    {ok, EventList} = reckon_db_streams:read(StoreId, StreamId, 0, 100, forward),
    ?assertEqual(3, length(EventList)),
    ?assertEqual([0, 1, 2], [E#event.version || E <- EventList]),
    ok.

%% Appends reach one stream from the store's gateway workers, and
%% gateway_pool_size defaults to 1 (macula-realm runs 2), so ten and twenty
%% concurrent writers on one stream is already more contention than a
%% deployment produces. Every append must land, which means no writer may
%% lose the race sixty times in a row.
ten_writers_on_one_stream_keep_every_event(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"concurrent-ten">>),
    Results = race(StoreId, StreamId, 10, 100, fun() -> events(1) end),
    assert_every_event_landed(StoreId, StreamId, Results, 1000, 1000).

twenty_writers_on_one_stream_keep_every_event(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"concurrent-twenty">>),
    Results = race(StoreId, StreamId, 20, 100, fun() -> events(1) end),
    assert_every_event_landed(StoreId, StreamId, Results, 2000, 2000).

%% A scavenged stream keeps its version numbers: deleting old versions does
%% not renumber the rest, so the live count is below the newest event. Every
%% prefix shape must be refused rather than written over - the short prefix
%% because the node the batch would take is still there, a half or more
%% because the node it counts as the predecessor is gone. A batch short
%% enough to fit below the survivors must be refused too.
a_scavenged_stream_refuses_every_prefix_shape(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    %% {last scavenged version, events per append}
    [a_scavenged_stream_refuses(StoreId, Last, Batch)
     || {Last, Batch} <- [{0, 1}, {4, 1}, {7, 1}, {7, 2}, {8, 1}]],
    ok.

%% A scavenge that deletes every event is the one shape that is not a
%% refusal: nothing is left to verify. The stream is gone as far as the count
%% can tell, so get_version answers -1 and the next append lands at version 0
%% again - one event answers {ok, 0}, a batch of three {ok, 2}. Nothing is
%% overwritten; a reader or subscriber that keys on the version simply sees
%% 0, 1, ... a second time.
a_fully_scavenged_stream_restarts_at_version_zero(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    One = reckon_db_test_helpers:sid(<<"scavenged-all-one">>),
    {ok, 9} = reckon_db_streams:append(StoreId, One, ?NO_STREAM, events(10)),
    {ok, #{deleted_versions := {0, 9}}} =
        reckon_db_scavenge:scavenge(StoreId, One,
                                    #{before_version => 10,
                                      require_snapshot => false}),
    ?assertEqual(?NO_STREAM, reckon_db_streams:get_version(StoreId, One)),
    ?assertEqual({ok, 0},
                 reckon_db_streams:append(StoreId, One, ?ANY_VERSION,
                                          events(1))),

    Batch = reckon_db_test_helpers:sid(<<"scavenged-all-batch">>),
    {ok, 9} = reckon_db_streams:append(StoreId, Batch, ?NO_STREAM, events(10)),
    {ok, #{deleted_versions := {0, 9}}} =
        reckon_db_scavenge:scavenge(StoreId, Batch,
                                    #{before_version => 10,
                                      require_snapshot => false}),
    ?assertEqual({ok, 2},
                 reckon_db_streams:append(StoreId, Batch, ?ANY_VERSION,
                                          events(3))),
    %% Nothing is overwritten: the batch is versions 0, 1 and 2, and a reader
    %% that keys on the version sees them again.
    {ok, Restored} = reckon_db_streams:read(StoreId, Batch, 0, 100, forward),
    ?assertEqual([0, 1, 2], [E#event.version || E <- Restored]),
    ok.

%% An append of no events writes nothing and answers the version it read, as
%% it did before the version was verified inside the write transaction: there
%% is no transaction to verify in.
an_append_of_no_events_answers_the_version_it_read(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"concurrent-empty">>),

    ?assertEqual({ok, ?NO_STREAM},
                 reckon_db_streams:append(StoreId, StreamId, ?ANY_VERSION, [])),
    ?assertEqual(?NO_STREAM, reckon_db_streams:get_version(StoreId, StreamId)),

    {ok, 1} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(2)),
    ?assertEqual({ok, 1},
                 reckon_db_streams:append(StoreId, StreamId, ?ANY_VERSION, [])),
    ?assertEqual({ok, 1},
                 reckon_db_streams:append(StoreId, StreamId, 1, [])),
    ?assertEqual({error, {wrong_expected_version, 0, 1}},
                 reckon_db_streams:append(StoreId, StreamId, 0, [])),

    {ok, Stored} = reckon_db_streams:read(StoreId, StreamId, 0, 100, forward),
    ?assertEqual([0, 1], [E#event.version || E <- Stored]),
    ok.

%% A stream deleted between the read and the write leaves the transaction
%% nothing to verify: the predecessor node the batch was built from is gone,
%% so the append is refused at once, with what the transaction's own read of
%% that node answered as Reason, and is not retried. The stream is not
%% re-created at the version that was read, either - a delete is not an
%% empty stream.
a_stream_deleted_between_the_read_and_the_write_is_refused(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"deleted-under-the-append">>),
    {ok, 4} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(5)),

    meck:new(khepri, [passthrough, no_link]),
    try
        meck:expect(khepri, count,
                    fun(S, P, Opts) ->
                            Result = meck:passthrough([S, P, Opts]),
                            %% The stream goes AFTER the count, so the append
                            %% that started from it has lost its predecessor.
                            ok = reckon_db_streams:delete(StoreId, StreamId),
                            Result
                    end),
        ?assertMatch({error, {version_unverifiable, 4,
                              {khepri, node_not_found, _}}},
                     reckon_db_streams:append(StoreId, StreamId, ?ANY_VERSION,
                                              events(1)))
    after
        meck:unload(khepri)
    end,
    ?assertEqual(?NO_STREAM, reckon_db_streams:get_version(StoreId, StreamId)),
    ok.

%% A lost race must not be judged from a follower's stale snapshot.
%%
%% After a lost race the append reads the version again and reads "the
%% version did not move" as the scavenged-prefix shape, which is refused
%% without retrying. A follower's re-read can answer the same count the first
%% read did while the stream has in fact moved on the leader, so a healthy
%% stream on a multi-host cluster would be refused with version_unverifiable.
%% The retry therefore reads consistently; the first read of an append does
%% not pay for that. This case forces the race deterministically - the stream
%% is advanced between the read and the write - and records the favour each
%% read asked for.
a_retry_reads_the_version_consistently(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"retry-consistency">>),
    {ok, 0} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(1)),

    Self = self(),
    meck:new(khepri, [passthrough, no_link]),
    try
        meck:expect(khepri, count,
                    fun(S, P, Opts) ->
                            Result = meck:passthrough([S, P, Opts]),
                            %% The stream moves AFTER the count, so the append
                            %% that started from it has lost the race.
                            move_stream_once(S, StreamId),
                            Self ! {version_read, Opts},
                            Result
                    end),
        ?assertEqual({ok, 2}, reckon_db_streams:append(
                                StoreId, StreamId, ?ANY_VERSION, events(1)))
    after
        meck:unload(khepri)
    end,

    Favours = drain_version_reads([]),
    ct:pal("version reads, in order: ~p", [Favours]),
    %% The first read of the append is the cheap local one, and the retry is
    %% the one that asked the cluster.
    ?assertEqual(#{}, hd(Favours)),
    ?assert(lists:member(#{favor => consistency}, Favours)),

    {ok, Stored} = reckon_db_streams:read(StoreId, StreamId, 0, 100, forward),
    ?assertEqual([0, 1, 2], [E#event.version || E <- Stored]),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% @private Advance the stream on the first call only: the nested append
%% reads the version too, and that read must not advance it again.
move_stream_once(StoreId, StreamId) ->
    case get(stream_moved) of
        undefined ->
            put(stream_moved, true),
            {ok, _} = reckon_db_streams:append(StoreId, StreamId, ?ANY_VERSION,
                                               events(1)),
            ok;
        _ ->
            ok
    end.

%% @private
drain_version_reads(Acc) ->
    receive
        {version_read, Opts} -> drain_version_reads([Opts | Acc])
    after 0 ->
            lists:reverse(Acc)
    end.

%% Scavenge versions 0..Last of a ten-event stream, then try to append. The
%% surviving events must be untouched and the append refused with the
%% version it could not verify: no retry clears a scavenged stream, so this
%% is the non-retried answer, not a version_conflict.
a_scavenged_stream_refuses(StoreId, Last, Batch) ->
    Label = "scavenged-" ++ integer_to_list(Last) ++ "-" ++ integer_to_list(Batch),
    StreamId = reckon_db_test_helpers:sid(list_to_binary(Label)),
    {ok, 9} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(10)),
    {ok, [TipBefore]} = reckon_db_streams:read(StoreId, StreamId, 9, 1, forward),

    {ok, #{deleted_versions := {0, Last}}} =
        reckon_db_scavenge:scavenge(StoreId, StreamId,
                                    #{before_version => Last + 1,
                                      require_snapshot => false}),

    Result = reckon_db_streams:append(StoreId, StreamId, ?ANY_VERSION,
                                      events(Batch)),
    ct:pal("scavenged 0..~p, append of ~p: ~p", [Last, Batch, Result]),
    ?assertMatch({error, {version_unverifiable, _, _}}, Result),

    {ok, [TipAfter]} = reckon_db_streams:read(StoreId, StreamId, 9, 1, forward),
    ?assertEqual(TipBefore, TipAfter),
    ok.

%% Start Writers processes that all wait for one go message, each appending
%% Batches times. Returns every append result.
race(StoreId, StreamId, Writers, Batches, EventFun) ->
    Self = self(),
    Gate = make_ref(),
    Pids = [spawn_link(fun() ->
                               receive {go, Gate} -> Self ! {results, self(),
                                                            loop(StoreId, StreamId,
                                                                 Batches, EventFun)}
                               end
                       end) || _ <- lists:seq(1, Writers)],
    [Pid ! {go, Gate} || Pid <- Pids],
    lists:append([receive {results, Pid, Results} -> Results
                  after 30000 -> ct:fail("writer ~p did not finish", [Pid])
                  end || Pid <- Pids]).

loop(_StoreId, _StreamId, 0, _EventFun) ->
    [];
loop(StoreId, StreamId, N, EventFun) ->
    Result = reckon_db_streams:append(StoreId, StreamId, ?ANY_VERSION, EventFun()),
    [Result | loop(StoreId, StreamId, N - 1, EventFun)].

%% Every append answered ok, and every event is in the stream: no version
%% was written twice, and no gap was left behind.
assert_every_event_landed(StoreId, StreamId, Results, Appends, Events) ->
    ?assertEqual(Appends, length(Results)),
    ?assertEqual([], [E || {error, _} = E <- Results]),

    ?assertEqual(Events - 1, reckon_db_streams:get_version(StoreId, StreamId)),
    {ok, Stored} = reckon_db_streams:read(StoreId, StreamId, 0, Events + 10, forward),
    ?assertEqual(Events, length(Stored)),
    ?assertEqual(lists:seq(0, Events - 1),
                 [E#event.version || E <- Stored]).

events(Count) ->
    [#{event_type => <<"concurrent_append_probe">>,
       data => #{<<"n">> => N}}
     || N <- lists:seq(1, Count)].
