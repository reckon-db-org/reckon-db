%% @doc A stream's latest version is the stream's version counter, not the
%% number of version nodes it happens to have.
%%
%% Scavenging deletes old version nodes and deliberately does NOT renumber
%% the rest — a version is a name, not a position. Deriving the version
%% from `khepri:count' therefore became wrong the moment a stream was
%% scavenged: the count fell, the next append reused versions that still
%% held live events, wrote over them, and the trigger never fired for the
%% overwritten paths (they are `update's, the filters watch `create's), so
%% a live subscriber lost those events too and nothing raised.
%%
%% These cases pin the invariant from both ends: the tree keeps every
%% event that was not scavenged, and appends continue at the version after
%% the newest one.
-module(reckon_db_scavenge_version_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

%% CT callbacks
-export([
    all/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    append_after_scavenge_keeps_versions_and_delivery/1,
    a_scavenged_stream_never_reuses_a_version/1,
    dry_run_scavenge_leaves_versions_alone/1,
    stream_without_a_counter_reports_its_newest_version/1,
    a_counter_below_the_tree_is_not_the_newest_version/1,
    read_all_answers_from_the_end_it_is_asked_from/1,
    read_all_folds_from_the_live_end_after_a_scavenge/1,
    read_all_follows_the_stream_through_a_mid_fold_scavenge/1,
    deleting_a_stream_resets_its_versions/1
]).

suite() ->
    [{timetrap, {minutes, 2}}].

all() ->
    [dry_run_scavenge_leaves_versions_alone,
     a_scavenged_stream_never_reuses_a_version,
     append_after_scavenge_keeps_versions_and_delivery,
     stream_without_a_counter_reports_its_newest_version,
     a_counter_below_the_tree_is_not_the_newest_version,
     read_all_answers_from_the_end_it_is_asked_from,
     read_all_folds_from_the_live_end_after_a_scavenge,
     read_all_follows_the_stream_through_a_mid_fold_scavenge,
     deleting_a_stream_resets_its_versions].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(telemetry),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    PgScope = reckon_db_test_helpers:start_pg_scope(),
    [{pg_scope, PgScope} | Config].

end_per_suite(Config) ->
    reckon_db_test_helpers:stop_pg_scope(proplists:get_value(pg_scope, Config, already_running)),
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    TCStr = atom_to_list(TestCase),
    DataDir = "/tmp/reckon_db_scavenge_version_" ++ TCStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    StoreId = list_to_atom("scavenge_version_" ++ TCStr ++ "_" ++ Rand),
    case khepri:start(DataDir, StoreId) of
        {ok, _} ->
            ok = khepri:put(StoreId, [streams], #{}),
            ok = khepri:put(StoreId, [subscriptions], #{}),
            ok = khepri:put(StoreId, [procs], #{}),
            [{data_dir, DataDir}, {store_id, StoreId} | Config];
        {error, Reason} ->
            ct:fail("Failed to start Khepri: ~p", [Reason])
    end.

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    khepri:stop(StoreId),
    os:cmd("rm -rf " ++ proplists:get_value(data_dir, Config)),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc No subscription and no counter API: the tree alone. This is the case
%% that is red on the released code for the right reason - the append after a
%% scavenge reuses versions that still hold events, so the read-back is short
%% and the events that were there are gone.
a_scavenged_stream_never_reuses_a_version(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"scavver-tree">>),
    Before = typed(<<"before_">>, 10),
    {ok, 9} = reckon_db_streams:append(StoreId, StreamId, -2, [event(T) || T <- Before]),

    {ok, #{deleted_count := 2, deleted_versions := {0, 1}}} =
        reckon_db_scavenge:scavenge(StoreId, StreamId,
            #{before_version => 2, require_snapshot => false}),

    %% Eight events left, at versions 2..9. The next one is 10.
    ?assertEqual(9, reckon_db_streams:get_version(StoreId, StreamId)),
    After = typed(<<"after_">>, 5),
    {ok, 14} = reckon_db_streams:append(StoreId, StreamId, -2, [event(T) || T <- After]),

    {ok, Stored} = reckon_db_streams:read(StoreId, StreamId, 0, 100, forward),
    Expected = lists:zip(lists:nthtail(2, Before) ++ After, lists:seq(2, 14)),
    ?assertEqual(Expected, [{E#event.event_type, E#event.version} || E <- Stored]).

%% @doc GIVEN a stream whose first two versions have been scavenged
%%      WHEN events are appended
%%      THEN they take the versions after the newest surviving event
%%      AND the surviving events are still the ones that were there
%%      AND a live subscriber receives the new events
append_after_scavenge_keeps_versions_and_delivery(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"scavver-001">>),
    SubName = <<"scavver_sub">>,

    {ok, SubKey} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, SubName, #{subscriber => self()}),
    EmitterPid = start_emitter(StoreId, SubKey),

    try
        Before = typed(<<"before_">>, 5),
        {ok, 4} = reckon_db_streams:append(StoreId, StreamId, -2, [event(T) || T <- Before]),
        receive_all(Before),

        {ok, #{deleted_count := 2, deleted_versions := {0, 1}}} =
            reckon_db_scavenge:scavenge(StoreId, StreamId,
                #{before_version => 2, require_snapshot => false}),

        %% Version 4 was the newest; the next event is 5, whatever the
        %% two deleted nodes did to the count.
        ?assertEqual(4, reckon_db_streams:get_version(StoreId, StreamId)),
        After = typed(<<"after_">>, 5),
        {ok, 9} = reckon_db_streams:append(StoreId, StreamId, -2, [event(T) || T <- After]),

        %% The append continued where the stream actually was: versions
        %% 5..9, and versions 2..4 still hold the events they held before.
        {ok, Stored} = reckon_db_streams:read(StoreId, StreamId, 0, 100, forward),
        Expected = lists:zip(lists:nthtail(2, Before) ++ After, lists:seq(2, 9)),
        ?assertEqual(Expected, [{E#event.event_type, E#event.version} || E <- Stored]),

        %% The overwritten versions were updates at existing paths, which
        %% the `create'-filtered trigger never reports: the events were
        %% lost silently, both from the store and from the subscriber.
        receive_all(After)
    after
        gen_server:stop(EmitterPid),
        reckon_db_subscriptions:unsubscribe(StoreId, SubKey)
    end,
    ok.

%% @doc GIVEN a stream with events
%%      WHEN a dry-run scavenge reports what it would delete
%%      THEN nothing is deleted and the version does not move
%%      (the control: this case was already green before the fix)
dry_run_scavenge_leaves_versions_alone(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"scavver-002">>),
    Types = typed(<<"before_">>, 5),
    {ok, 4} = reckon_db_streams:append(StoreId, StreamId, -2, [event(T) || T <- Types]),

    {ok, #{deleted_count := 2, deleted_versions := {0, 1}}} =
        reckon_db_scavenge:scavenge(StoreId, StreamId,
            #{before_version => 2, dry_run => true, require_snapshot => false}),

    ?assertEqual(4, reckon_db_streams:get_version(StoreId, StreamId)),
    {ok, Stored} = reckon_db_streams:read(StoreId, StreamId, 0, 100, forward),
    ?assertEqual(Types, [E#event.event_type || E <- Stored]).

%% @doc GIVEN a stream written before version counters existed (its
%%      counter node is absent), already scavenged
%%      WHEN its version is read and events are appended
%%      THEN the newest surviving version is reported and the append
%%      continues after it, writing the counter from then on
stream_without_a_counter_reports_its_newest_version(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"scavver-003">>),
    Types = typed(<<"before_">>, 5),
    {ok, 4} = reckon_db_streams:append(StoreId, StreamId, -2, [event(T) || T <- Types]),
    {ok, _} = reckon_db_scavenge:scavenge(StoreId, StreamId,
        #{before_version => 2, require_snapshot => false}),

    %% A 5.11.x store: events present, no counter node anywhere.
    CounterPath = reckon_db_stream_path:version_counter_path(StreamId),
    ok = khepri:delete(StoreId, CounterPath),
    ?assertMatch({error, _}, khepri:get(StoreId, CounterPath)),

    ?assertEqual(4, reckon_db_streams:get_version(StoreId, StreamId)),
    {ok, 5} = reckon_db_streams:append(StoreId, StreamId, -2, [event(<<"next">>)]),
    ?assertEqual({ok, 5}, khepri:get(StoreId, CounterPath)),
    ?assertEqual(5, reckon_db_streams:get_version(StoreId, StreamId)).

%% @doc GIVEN a stream whose counter is below the newest event
%%      WHEN the newest version is asked for, and a keep_versions scavenge runs
%%      THEN the tree answers, not the counter; the scavenge keeps the newest
%%           events rather than measuring from a version that is not the
%%           newest; it raises the counter to what it kept; and the events it
%%           kept are readable from either end
%%
%% The shape is a 5.11.x writer: that release kept no counter, so it could
%% append past one. Counter 9 over a tree holding 0..15 answered 9, and
%% keep_versions 3 then deleted {0,6} instead of {0,12}.
%%
%% The read at the end is the point of the case. A scavenge leaves a counter
%% below the newest event and a tree that starts at 13, and a positional read
%% from 0 answers {ok, []} over events the store holds - data hidden from
%% readers. Each of the three assertions at the end fails without its own fix.
a_counter_below_the_tree_is_not_the_newest_version(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"scavver-005">>),
    {ok, 15} = reckon_db_streams:append(StoreId, StreamId, -2,
                                        [event(T) || T <- typed(<<"before_">>, 16)]),
    ?assertEqual(15, reckon_db_streams:get_version(StoreId, StreamId)),

    %% The counter sits below the tree it was written with: 9, where the tree
    %% holds 0..15.
    CounterPath = reckon_db_stream_path:version_counter_path(StreamId),
    ok = khepri:put(StoreId, CounterPath, 9),
    ?assertEqual({ok, 15}, reckon_db_streams:newest_event_version(StoreId, StreamId)),

    {ok, Result} = reckon_db_scavenge:scavenge(StoreId, StreamId,
        #{before_version => 100, keep_versions => 3, require_snapshot => false}),

    %% The newest three are what is left: measured from 9 the scavenge stops
    %% at 6 and leaves {7,15} behind, thirteen events where three belong.
    ?assertMatch(#{deleted_versions := {0, 12}}, Result),
    ?assertEqual(3, reckon_db_streams:live_event_count(StoreId, StreamId)),
    ?assertEqual({ok, 15}, reckon_db_streams:newest_event_version(StoreId, StreamId)),

    %% The counter is raised to what the stream is, not left at the 9 that
    %% says the stream never got past version 9.
    ?assertEqual({ok, 15}, khepri:get(StoreId, CounterPath)),
    ?assertEqual(15, reckon_db_streams:get_version(StoreId, StreamId)),

    %% A batch smaller than the thirteen versions that were deleted: the
    %% positional window over 0..2 holds no event at all.
    ?assertEqual({ok, [13, 14, 15]},
                 versions(reckon_db_streams:read_all(StoreId, StreamId, 3, forward))),
    ?assertEqual({ok, [15, 14, 13]},
                 versions(reckon_db_streams:read_all(StoreId, StreamId, 3, backward))),
    ?assertEqual({ok, [13, 14, 15]},
                 versions(reckon_db_streams:read(StoreId, StreamId, 13, 3, forward))).

%% @doc GIVEN a stream longer than one batch, with no scavenge
%%      WHEN it is read a batch at a time, forwards and backwards
%%      THEN each call answers a full batch from the end it asks from, and
%%           the fold visits every event exactly once
%%
%% The backward half is the one that was wrong for every stream, scavenged or
%% not: read_all/4 passed a start version of 0, and a backward window from 0
%% is version 0 alone.
read_all_answers_from_the_end_it_is_asked_from(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"scavver-006">>),
    {ok, 9} = reckon_db_streams:append(StoreId, StreamId, -2,
                                       [event(T) || T <- typed(<<"plain_">>, 10)]),

    ?assertEqual({ok, [0, 1, 2, 3]},
                 versions(reckon_db_streams:read_all(StoreId, StreamId, 4, forward))),
    ?assertEqual({ok, [9, 8, 7, 6]},
                 versions(reckon_db_streams:read_all(StoreId, StreamId, 4, backward))),

    ?assertEqual({ok, lists:seq(0, 9)}, versions(fold_all(StoreId, StreamId, 4, forward))),
    ?assertEqual({ok, lists:seq(9, 0, -1)},
                 versions(fold_all(StoreId, StreamId, 4, backward))).

%% @doc GIVEN a scavenged stream whose live events are all above the batch
%%      WHEN the whole stream is folded a batch at a time
%%      THEN the fold starts at the live end and visits every surviving event
%%
%% The cursor a fold carries is a version, and the versions a scavenge removed
%% are still versions: starting from 0 with a batch of 3 stops at {ok, []} and
%% never reaches version 13.
read_all_folds_from_the_live_end_after_a_scavenge(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"scavver-007">>),
    {ok, 15} = reckon_db_streams:append(StoreId, StreamId, -2,
                                        [event(T) || T <- typed(<<"folded_">>, 16)]),
    {ok, #{deleted_versions := {0, 12}}} =
        reckon_db_scavenge:scavenge(StoreId, StreamId,
            #{before_version => 13, require_snapshot => false}),

    ?assertEqual({ok, [13, 14, 15]}, versions(fold_all(StoreId, StreamId, 3, forward))),
    ?assertEqual({ok, [15, 14, 13]}, versions(fold_all(StoreId, StreamId, 3, backward))).

%% @doc GIVEN a stream longer than two batches
%%      WHEN a scavenge deletes a prefix of it between the first and the
%%           second batch
%%      THEN the fold carries on past the short batch and visits every event
%%           that is still there
%%
%% A short batch is not the end of the stream. Before this case's fix the fold
%% read 0..49, the scavenge removed 0..74, the next window answered 75..99 -
%% twenty-five events where fifty were asked for - and the fold stopped there,
%% dropping 100..149 with nothing said. A link backfill and an event-type
%% summary both ride on this fold, and both would have answered a partial
%% result that looked complete.
read_all_follows_the_stream_through_a_mid_fold_scavenge(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"scavver-008">>),
    {ok, 149} = reckon_db_streams:append(StoreId, StreamId, -2,
                                         [event(T) || T <- typed(<<"walked_">>, 150)]),

    Step = fun(Events, {Seen, Scavenged}) ->
                   {Seen ++ [E#event.version || E <- Events],
                    scavenge_once(StoreId, StreamId, Scavenged)}
           end,
    {ok, {Seen, true}} =
        reckon_db_streams:fold_event_batches(StoreId, StreamId, 50, forward,
                                             Step, {[], false}),

    %% 50..74 were deleted before the fold reached them; everything else is
    %% visited, in order, once.
    ?assertEqual(lists:seq(0, 49) ++ lists:seq(75, 149), Seen).

%% @doc GIVEN a stream and then no stream
%%      WHEN it is deleted and appended to again under the same id
%%      THEN it starts at version 0, not after the deleted stream's last
deleting_a_stream_resets_its_versions(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"scavver-004">>),
    {ok, 9} = reckon_db_streams:append(StoreId, StreamId, -2,
                                       [event(T) || T <- typed(<<"before_">>, 10)]),
    ?assertEqual(9, reckon_db_streams:get_version(StoreId, StreamId)),

    ok = reckon_db_streams:delete(StoreId, StreamId),
    ?assertEqual(?NO_STREAM, reckon_db_streams:get_version(StoreId, StreamId)),

    {ok, 0} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, [event(<<"again">>)]),
    ?assertEqual(0, reckon_db_streams:get_version(StoreId, StreamId)).

%%====================================================================
%% Helpers
%%====================================================================

event(Type) ->
    #{event_type => Type, data => #{<<"k">> => Type}}.

%% The versions a read answered, so a case asserts the numbers it means rather
%% than a list of records.
versions({ok, Events}) when is_list(Events) ->
    {ok, [E#event.version || E <- Events]};
versions(Other) ->
    Other.

fold_all(StoreId, StreamId, BatchSize, Direction) ->
    Collect = fun(Events, Acc) -> Acc ++ Events end,
    reckon_db_streams:fold_event_batches(StoreId, StreamId, BatchSize, Direction,
                                         Collect, []).

%% A scavenge runs once, on the fold's first batch: deleting a prefix that
%% reaches past the batch the fold already has is the hole a fold has to see.
scavenge_once(_StoreId, _StreamId, true) ->
    true;
scavenge_once(StoreId, StreamId, false) ->
    {ok, _} = reckon_db_scavenge:scavenge(StoreId, StreamId,
        #{before_version => 75, require_snapshot => false}),
    true.

typed(Prefix, N) ->
    [<<Prefix/binary, (integer_to_binary(I))/binary>> || I <- lists:seq(1, N)].

start_emitter(StoreId, SubKey) ->
    EmitterName = reckon_db_emitter_group:emitter_name(StoreId, SubKey),
    {ok, EmitterPid} = reckon_db_emitter:start_link(StoreId, SubKey, self(), EmitterName),
    EmitterPid.

receive_all(Types) ->
    receive_all(Types, 5000).

receive_all([], _Timeout) ->
    ok;
receive_all(Types, Timeout) ->
    receive
        {events, Events} when is_list(Events) ->
            Seen = [E#event.event_type || E <- Events],
            receive_all(Types -- Seen, Timeout)
    after Timeout ->
        ct:fail("Did not receive ~p within ~b ms", [Types, Timeout])
    end.
