%% @doc A stream 5.11.x wrote to last: the counter did not move, the tree did.
%%
%% 5.11.x knows nothing about the per-stream counter this release keeps, so
%% after a rollback the counter can disagree with the tree in either
%% direction. The shapes that matter are the ones where it sits ABOVE the
%% tree, because both the append and the scavenge trust it:
%%
%%   - the stream was deleted and re-created under the same id: counter 4
%%     over events 0..2;
%%   - it was re-created and then scavenged from the bottom: counter 4 over
%%     events 1..2;
%%   - it was scavenged from the top: counter 9 over events 13..15.
%%
%% Every one of them is built here by moving the counter node by hand after
%% this release's own appends, which is exactly the state a rollback leaves;
%% the cross-version round trip that produces the same state is
%% reckon_db_upgrade_store_SUITE's fixtures.
-module(reckon_db_stale_counter_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([a_scavenge_keeps_the_newest_events_of_a_stale_counter_stream/1,
         an_any_append_lands_above_the_survivors_of_a_scavenge/1,
         an_any_append_on_a_re_created_stream_lands_at_its_tip/1,
         a_re_created_stream_scavenged_from_the_bottom_is_repaired/1]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [a_scavenge_keeps_the_newest_events_of_a_stale_counter_stream,
     an_any_append_lands_above_the_survivors_of_a_scavenge,
     an_any_append_on_a_re_created_stream_lands_at_its_tip,
     a_re_created_stream_scavenged_from_the_bottom_is_repaired].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    TCStr = atom_to_list(TestCase),
    DataDir = "/tmp/reckon_db_stale_counter_" ++ TCStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom("stale_counter_" ++ TCStr ++ "_" ++ Rand),
    {ok, _} = khepri:start(DataDir, StoreId),
    ok = khepri:put(StoreId, [streams], #{}),
    ok = khepri:put(StoreId, [subscriptions], #{}),
    ok = khepri:put(StoreId, [procs], #{}),
    [{data_dir, DataDir}, {store_id, StoreId} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = khepri:stop(proplists:get_value(store_id, Config)),
    os:cmd("rm -rf " ++ proplists:get_value(data_dir, Config)),
    ok.

%% A scavenge measures `keep_versions' against the version it thinks the
%% stream has. With the counter above a re-created tree that version does
%% not exist, so the protection covers nothing and the dry run offers to
%% delete the whole stream - the newest events included.
a_scavenge_keeps_the_newest_events_of_a_stale_counter_stream(Config) ->
    Store = store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"re-created-then-scavenged">>),
    ok = re_created_under_a_stale_counter(Store, StreamId),
    ?assertEqual(4, reckon_db_streams:get_version(Store, StreamId)),

    Options = #{before_version => 3, keep_versions => 1,
                require_snapshot => false},

    {ok, DryRun} = reckon_db_scavenge:dry_run(Store, StreamId, Options),
    ?assertEqual(2, maps:get(deleted_count, DryRun)),
    ?assertEqual({0, 1}, maps:get(deleted_versions, DryRun)),

    {ok, Result} = reckon_db_scavenge:scavenge(Store, StreamId, Options),
    ?assertEqual(2, maps:get(deleted_count, Result)),
    ?assertEqual([2], stored_versions(Store, StreamId)),
    ok.

%% 5.11.x appended past the counter and then scavenged from the bottom. The
%% counter still says 9 while the oldest live event is 13, so an append that
%% trusts it writes 10, 11 and 12 BELOW events that were written before it -
%% a forward read then hands a projection newer events ahead of older ones.
an_any_append_lands_above_the_survivors_of_a_scavenge(Config) ->
    Store = store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"scavenged-above-the-counter">>),
    ok = scavenged_above_a_stale_counter(Store, StreamId),
    ?assertEqual(9, reckon_db_streams:get_version(Store, StreamId)),

    ?assertEqual({ok, 16}, reckon_db_streams:append(
                            Store, StreamId, ?ANY_VERSION, [event()])),
    ?assertEqual([13, 14, 15, 16], stored_versions(Store, StreamId)),
    ?assertEqual(16, reckon_db_streams:get_version(Store, StreamId)),
    ok.

%% A stream 5.11.x deleted and re-created: counter 4, events 0..2. An append
%% that trusts the counter lands at 5 and leaves versions 3 and 4 empty for
%% the rest of the stream's life - and a caller that names the version the
%% tree offers (2) is refused on every attempt.
an_any_append_on_a_re_created_stream_lands_at_its_tip(Config) ->
    Store = store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"re-created-any-first">>),
    ok = re_created_under_a_stale_counter(Store, StreamId),

    ?assertEqual({ok, 3}, reckon_db_streams:append(
                           Store, StreamId, ?ANY_VERSION, [event()])),
    ?assertEqual([0, 1, 2, 3], stored_versions(Store, StreamId)),
    ?assertEqual(3, reckon_db_streams:get_version(Store, StreamId)),

    %% And the hole the counter used to leave is not there: an explicit
    %% append at the tree's tip is accepted.
    ?assertEqual({ok, 4}, reckon_db_streams:append(Store, StreamId, 3, [event()])),
    ?assertEqual([0, 1, 2, 3, 4], stored_versions(Store, StreamId)),
    ok.

%% The same re-created stream, then scavenged from the bottom by 5.11.x:
%% counter 4 over events 1..2. The tree is not contiguous from 0, so a
%% repair that insists on a whole re-created stream leaves the counter at 4,
%% refuses the explicit tip 2 for good, and lets the next ANY append land at
%% 5 over a hole at 3.
a_re_created_stream_scavenged_from_the_bottom_is_repaired(Config) ->
    Store = store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"re-created-then-scavenged-low">>),
    ok = re_created_under_a_stale_counter(Store, StreamId),
    ok = khepri:delete(Store, event_path(StreamId, 0)),
    ?assertEqual([1, 2], stored_versions(Store, StreamId)),
    ?assertEqual(4, reckon_db_streams:get_version(Store, StreamId)),

    ?assertEqual({ok, 3}, reckon_db_streams:append(Store, StreamId, 2, [event()])),
    ?assertEqual([1, 2, 3], stored_versions(Store, StreamId)),
    ?assertEqual(3, reckon_db_streams:get_version(Store, StreamId)),
    ok.

%%====================================================================
%% Shapes
%%====================================================================

%% @private Counter 4 over events 0..2: what 5.11.x deleting a stream and
%% then appending to it again under the same id leaves behind.
re_created_under_a_stale_counter(Store, StreamId) ->
    {ok, 4} = reckon_db_streams:append(Store, StreamId, ?NO_STREAM, events(5)),
    ok = reckon_db_streams:delete(Store, StreamId),
    {ok, 2} = reckon_db_streams:append(Store, StreamId, ?NO_STREAM, events(3)),
    ok = khepri:put(Store, counter_path(StreamId), 4),
    ?assertEqual([0, 1, 2], stored_versions(Store, StreamId)),
    ok.

%% @private Counter 9 over events 13..15: 5.11.x appended past the counter,
%% then scavenged 0..12 the only way it can - by deleting them.
scavenged_above_a_stale_counter(Store, StreamId) ->
    {ok, 15} = reckon_db_streams:append(Store, StreamId, ?NO_STREAM, events(16)),
    [ok = khepri:delete(Store, event_path(StreamId, V)) || V <- lists:seq(0, 12)],
    ok = khepri:put(Store, counter_path(StreamId), 9),
    ?assertEqual([13, 14, 15], stored_versions(Store, StreamId)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

store(Config) ->
    proplists:get_value(store_id, Config).

%% @private The versions the stream actually holds, in order.
stored_versions(Store, StreamId) ->
    case reckon_db_streams:read(Store, StreamId, 0, 1000, forward) of
        {ok, Events} -> [E#event.version || E <- Events];
        {error, _} -> []
    end.

%% @private The version node's path, with the fixed-width name the stream
%% store uses.
event_path(StreamId, Version) ->
    reckon_db_stream_path:event_path(StreamId, pad(Version)).

counter_path(StreamId) ->
    reckon_db_stream_path:version_counter_path(StreamId).

pad(Version) ->
    iolist_to_binary(io_lib:format("~*..0w", [?VERSION_PADDING, Version])).

event() ->
    #{event_type => <<"stale_counter_event">>, data => #{<<"n">> => 1}}.

events(Count) ->
    [#{event_type => <<"stale_counter_event">>, data => #{<<"n">> => N}}
     || N <- lists:seq(1, Count)].
