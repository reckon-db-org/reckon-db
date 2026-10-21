%% @doc Concurrent appends to one stream must not lose events.
%%
%% The version an append writes from is read before the write transaction -
%% the event records and their index entries are built outside it, because
%% HMAC stamping and crypto cannot run inside a Ra transaction - so two
%% appends that read the same version build the same version numbers and one
%% silently overwrites the other. Every call answers `{ok, Version}': nothing
%% fails, nothing logs, the events are simply gone.
%%
%% The version is therefore verified again INSIDE the transaction
%% (reckon_db_streams:append_guard/5): it aborts if the stream's counter
%% moved, or if the version node the batch would occupy already holds an
%% event, and the append re-reads and retries. These cases are the proof:
%% before it, four writers appending 25 events each to one stream left 25 of
%% the 100 in the store.
%%
%% The five cases after this one are the race and contention shapes: four
%% writers in ten batches of five, four writers appending singly, an append
%% naming an explicit version against a concurrent one, and ten and twenty
%% writers on one stream. The last four of these are the edges: a scavenged
%% stream, whose append must land after the newest event rather than over it,
%% an append of no events, which writes nothing and answers the version it
%% read, and the favour the retry's re-read asks for, which a follower must
%% not answer from a stale snapshot. The last four are the delete: a stream
%% removed between the read and the write, with and without a counter, under
%% ANY and under an explicit version.
%%
%% All fourteen are listed in all/0: the first six are the races, then the
%% two scavenged-stream appends, the empty append and the favour the retry
%% asks for, and the last four are the delete - a stream removed between the
%% read and the write, with and without a counter, under ANY and under an
%% explicit version. The races are red on 66edb40; the empty append passes
%% there too, and the four deleted-stream cases are new in this round.
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
    a_scavenged_stream_appends_after_its_tip/1,
    a_fully_scavenged_stream_keeps_its_counter/1,
    a_fully_scavenged_counter_less_stream_keeps_its_version/1,
    an_append_of_no_events_answers_the_version_it_read/1,
    a_retry_reads_the_version_consistently/1,
    a_deleted_stream_with_a_counter_is_re_created_at_zero_by_any/1,
    a_deleted_stream_with_a_counter_refuses_an_explicit_version/1,
    a_deleted_counter_less_stream_is_refused_under_any/1,
    a_deleted_counter_less_stream_refuses_an_explicit_version/1
]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [concurrent_batch_appends_keep_every_event,
     concurrent_single_appends_keep_every_event,
     an_explicit_expected_version_still_loses_to_a_concurrent_append,
     ten_writers_on_one_stream_keep_every_event,
     twenty_writers_on_one_stream_keep_every_event,
     a_scavenged_stream_appends_after_its_tip,
     a_fully_scavenged_stream_keeps_its_counter,
     a_fully_scavenged_counter_less_stream_keeps_its_version,
     an_append_of_no_events_answers_the_version_it_read,
     a_retry_reads_the_version_consistently,
     a_deleted_stream_with_a_counter_is_re_created_at_zero_by_any,
     a_deleted_stream_with_a_counter_refuses_an_explicit_version,
     a_deleted_counter_less_stream_is_refused_under_any,
     a_deleted_counter_less_stream_refuses_an_explicit_version].

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

%% The default pool is ten writers on one store, so ten and twenty writers on
%% one stream is the contention a deployment actually produces. Every append
%% must land, which means no writer may lose the race fifty times in a row.
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
%% not renumber the rest. The per-stream counter is what makes the version
%% exact anyway, so an append after a scavenge of any prefix lands after the
%% newest event - never in the hole, and never over a survivor. 5.11.13 had
%% to refuse these appends because its version was a count; this is the
%% release that makes them work.
a_scavenged_stream_appends_after_its_tip(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    %% {last scavenged version, events per append}
    [a_scavenged_stream_appends(StoreId, Last, Batch)
     || {Last, Batch} <- [{0, 1}, {4, 1}, {7, 1}, {7, 2}, {8, 1}]],
    ok.

%% A scavenge that deletes every event is where the counter shows what it is
%% for. The tree is empty, so a count-derived version would say -1 and the
%% next append would land at 0 again over a stream that has had 10 events. The
%% counter is outside the stream's subtree, survives the scavenge, and keeps
%% the version exact: the next append lands at 10.
a_fully_scavenged_stream_keeps_its_counter(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"scavenged-all">>),
    {ok, 9} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(10)),
    {ok, #{deleted_versions := {0, 9}}} =
        reckon_db_scavenge:scavenge(StoreId, StreamId,
                                    #{before_version => 10,
                                      require_snapshot => false}),
    ?assertEqual(9, reckon_db_streams:get_version(StoreId, StreamId)),
    ?assertEqual({ok, 10}, reckon_db_streams:append(
                             StoreId, StreamId, ?ANY_VERSION, events(1))),
    ?assertEqual(10, reckon_db_streams:get_version(StoreId, StreamId)),
    {ok, Appended} = reckon_db_streams:read(StoreId, StreamId, 10, 1, forward),
    ?assertEqual([10], [E#event.version || E <- Appended]),
    ok.

%% The counter-less sibling of the case above: a stream a 5.11.x store wrote
%% has no counter, so its version nodes are the only memory it has of how far
%% it got. A scavenge that deletes every one of them without leaving a number
%% behind empties that memory, and the next append starts at 0 and reuses
%% versions the stream already used.
a_fully_scavenged_counter_less_stream_keeps_its_version(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"scavenged-all-no-counter">>),
    {ok, 9} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(10)),
    drop_the_counter(StoreId, StreamId),

    {ok, #{deleted_versions := {0, 9}}} =
        reckon_db_scavenge:scavenge(StoreId, StreamId,
                                    #{before_version => 10,
                                      require_snapshot => false}),
    ?assertEqual([], stored_versions(StoreId, StreamId)),
    ?assertEqual(9, reckon_db_streams:get_version(StoreId, StreamId)),
    ?assertEqual({ok, 10}, reckon_db_streams:append(
                             StoreId, StreamId, ?ANY_VERSION, events(1))),
    ?assertEqual(10, reckon_db_streams:get_version(StoreId, StreamId)),
    {ok, Appended} = reckon_db_streams:read(StoreId, StreamId, 10, 1, forward),
    ?assertEqual([10], [E#event.version || E <- Appended]),
    ok.

%% An append of no events writes nothing and answers the version it read, as
%% it did before the version was verified inside the write transaction. It
%% used to reach the transaction and crash the calling process - a gateway
%% worker, through the pool.
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

%%====================================================================
%% Helpers
%%====================================================================
%% A lost race must not be judged from a follower's stale snapshot.
%%
%% After a lost race the append reads the version again, and on a follower
%% that read can answer the same version the first one did while the stream
%% has in fact moved on the leader - so the retry loses the race again, sixty
%% times, and a healthy multi-host stream is answered version_conflict. The
%% retry therefore reads consistently; the first read of an append does not
%% pay for that. This case forces the race deterministically - the stream is
%% advanced between the read and the write - and records the favour each read
%% of the counter asked for.
a_retry_reads_the_version_consistently(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"retry-consistency">>),
    {ok, 0} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(1)),
    CounterPath = reckon_db_stream_path:version_counter_path(StreamId),

    Self = self(),
    meck:new(khepri, [passthrough, no_link]),
    try
        meck:expect(khepri, get,
                    fun(S, P, Opts) ->
                            Result = meck:passthrough([S, P, Opts]),
                            case P =:= CounterPath of
                                true ->
                                    %% The stream moves AFTER the read, so the
                                    %% append that started from it has lost the
                                    %% race. Only the counter path is touched:
                                    %% every other get passes through silently.
                                    move_stream_once(S, StreamId),
                                    Self ! {version_read, Opts};
                                false ->
                                    ok
                            end,
                            Result
                    end),
        ?assertEqual({ok, 2}, reckon_db_streams:append(
                                StoreId, StreamId, ?ANY_VERSION, events(1)))
    after
        meck:unload(khepri)
    end,

    Favours = drain_version_reads([]),
    ct:pal("counter reads, in order: ~p", [Favours]),
    ?assertMatch([#{} | _], Favours),
    ?assert(lists:member(#{favor => consistency}, Favours)),

    {ok, Stored} = reckon_db_streams:read(StoreId, StreamId, 0, 100, forward),
    ?assertEqual([0, 1, 2], [E#event.version || E <- Stored]),
    ok.


%% A stream deleted between the read and the write, while a counter exists:
%% the transaction finds the counter gone, aborts, and the retry re-reads a
%% stream that is no longer there. ANY_VERSION then writes at 0 - a fresh
%% stream under the same id - and never at the version the deleted one had
%% reached.
a_deleted_stream_with_a_counter_is_re_created_at_zero_by_any(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"deleted-with-counter-any">>),
    {ok, 4} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(5)),
    delete_after_the_counter_read(StoreId, StreamId,
                                  fun() ->
                                          ?assertEqual(
                                             {ok, 0},
                                             reckon_db_streams:append(
                                               StoreId, StreamId, ?ANY_VERSION,
                                               events(1)))
                                  end),
    ?assertEqual([0], stored_versions(StoreId, StreamId)),
    ?assertEqual(0, reckon_db_streams:get_version(StoreId, StreamId)),
    ok.

%% The same delete under an explicit version: the retry reads a stream that is
%% not there, so the version the caller named cannot hold, and the answer is
%% the wrong-expected-version pair, not a write.
a_deleted_stream_with_a_counter_refuses_an_explicit_version(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"deleted-with-counter-explicit">>),
    {ok, 4} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(5)),
    delete_after_the_counter_read(StoreId, StreamId,
                                  fun() ->
                                          ?assertEqual(
                                             {error,
                                              {wrong_expected_version, 4, -1}},
                                             reckon_db_streams:append(
                                               StoreId, StreamId, 4, events(1)))
                                  end),
    ?assertEqual([], stored_versions(StoreId, StreamId)),
    ok.

%% A stream with no counter - one a 5.11.x store wrote - deleted between the
%% read and the write. There is no counter to compare, so the version node the
%% batch was built from is the evidence: it is gone, and the append is refused
%% with what the transaction's own read of that node answered. ANY_VERSION is
%% not an answer to it: a delete is not an empty stream, and writing there
%% re-creates it at N+1.
a_deleted_counter_less_stream_is_refused_under_any(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"deleted-no-counter-any">>),
    {ok, 4} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(5)),
    drop_the_counter(StoreId, StreamId),
    delete_after_the_tree_read(StoreId, StreamId,
                               fun() ->
                                       ?assertMatch(
                                          {error, {version_unverifiable, 4, _}},
                                          reckon_db_streams:append(
                                            StoreId, StreamId, ?ANY_VERSION,
                                            events(1)))
                               end),
    ?assertEqual([], stored_versions(StoreId, StreamId)),
    ok.

%% The same stream, the same delete, an explicit version: refused the same way.
%% The version the caller named is the one the batch was built from, so the
%% caller is told that the store could not verify it rather than that it was
%% wrong.
a_deleted_counter_less_stream_refuses_an_explicit_version(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"deleted-no-counter-explicit">>),
    {ok, 4} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(5)),
    drop_the_counter(StoreId, StreamId),
    delete_after_the_tree_read(StoreId, StreamId,
                               fun() ->
                                       ?assertMatch(
                                          {error, {version_unverifiable, 4, _}},
                                          reckon_db_streams:append(
                                            StoreId, StreamId, 4, events(1)))
                               end),
    ?assertEqual([], stored_versions(StoreId, StreamId)),
    ok.

%% @private Make the stream look like one a 5.11.x store wrote: no counter.
drop_the_counter(StoreId, StreamId) ->
    ok = khepri:delete(StoreId,
                       reckon_db_stream_path:version_counter_path(StreamId)).

%% @private Run Fun with the stream deleted the moment its counter is read:
%% the append has its version and the stream is gone before the transaction
%% opens.
delete_after_the_counter_read(StoreId, StreamId, Fun) ->
    CounterPath = reckon_db_stream_path:version_counter_path(StreamId),
    hooked(StoreId, StreamId, khepri, get,
           fun(_S, P, _Opts) -> P =:= CounterPath end, Fun).

%% @private The same, for a stream with no counter: the version comes from the
%% tree, so that read is where the delete has to land.
delete_after_the_tree_read(StoreId, StreamId, Fun) ->
    hooked(StoreId, StreamId, khepri_adv, get_many,
           fun(_S, _Pattern, _Query) -> true end, Fun).

%% @private Run Fun with one module function patched: on the first call that
%% Matches, the stream is deleted after the real call has answered.
hooked(StoreId, StreamId, Module, Function, Matches, Fun) ->
    meck:new(Module, [passthrough, no_link]),
    try
        meck:expect(Module, Function,
                    fun(A, B, C) ->
                            Result = meck:passthrough([A, B, C]),
                            case Matches(A, B, C) andalso get(stream_deleted) =:= undefined of
                                true ->
                                    put(stream_deleted, true),
                                    ok = reckon_db_streams:delete(StoreId, StreamId);
                                false ->
                                    ok
                            end,
                            Result
                    end),
        Fun()
    after
        meck:unload(Module)
    end.

%% @private The versions the stream actually holds, in order.
stored_versions(StoreId, StreamId) ->
    case reckon_db_streams:read(StoreId, StreamId, 0, 1000, forward) of
        {ok, Events} -> [E#event.version || E <- Events];
        {error, _} -> []
    end.

%% @private Advance the stream on the first call only: the nested append reads
%% the counter too, and that read must not advance it again.
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

%% Scavenge versions 0..Last of a ten-event stream, then append. The
%% surviving events must be untouched, the append must land at version 10
%% (the tip is 9 whatever the count says), and the stream's version must
%% follow it.
a_scavenged_stream_appends(StoreId, Last, Batch) ->
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
    ?assertEqual({ok, 9 + Batch}, Result),
    ?assertEqual(9 + Batch, reckon_db_streams:get_version(StoreId, StreamId)),

    {ok, [TipAfter]} = reckon_db_streams:read(StoreId, StreamId, 9, 1, forward),
    ?assertEqual(TipBefore, TipAfter),
    {ok, Appended} = reckon_db_streams:read(StoreId, StreamId, 10, Batch, forward),
    ?assertEqual(lists:seq(10, 9 + Batch), [E#event.version || E <- Appended]),
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
