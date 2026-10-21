%% @doc A link links every event once, across the boundary between its
%% backfill and its live delivery.
%%
%% The link stream is appended with `-2' - no expected version, no dedupe - so
%% "once" is not something anything downstream can repair. It comes from the
%% fence: taken per source stream INSIDE start/2, after the subscription
%% exists and before the backfill reads anything. Everything at or below the
%% fence is the backfill's; everything above it is the live path's.
%%
%% These cases inject the append that straddles the boundary from inside the
%% backfill itself, so the race is deterministic: the filter runs while the
%% backfill is reading, appends more events to the source stream, and those
%% events are above the fence by construction. They are delivered here the way
%% the emitter delivers them - a `{events, [...]}' message to the worker -
%% because a bare Khepri store has no supervision tree to start an emitter
%% pool, and the message is the whole of the contract between the two.
-module(reckon_db_links_backfill_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([the_append_during_a_backfill_is_linked_exactly_once/1,
         a_redelivered_live_event_is_not_linked_twice/1]).

suite() -> [{timetrap, {minutes, 2}}].

all() ->
    [the_append_during_a_backfill_is_linked_exactly_once,
     a_redelivered_live_event_is_not_linked_twice].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(telemetry),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    PgScope = reckon_db_test_helpers:start_pg_scope(),
    [{pg_scope, PgScope} | Config].

end_per_suite(Config) ->
    reckon_db_test_helpers:stop_pg_scope(proplists:get_value(pg_scope, Config)),
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_links_" ++ atom_to_list(TestCase) ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    StoreId = list_to_atom("links_" ++ atom_to_list(TestCase) ++ "_" ++ Rand),
    {ok, _} = khepri:start(DataDir, StoreId),
    ok = khepri:put(StoreId, [streams], #{}),
    ok = khepri:put(StoreId, [subscriptions], #{}),
    ok = khepri:put(StoreId, [procs], #{}),
    ok = khepri:put(StoreId, [links], #{}),
    [{data_dir, DataDir}, {store_id, StoreId} | Config].

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    _ = reckon_db_links:stop(StoreId, <<"orders">>),
    khepri:stop(StoreId),
    os:cmd("rm -rf " ++ proplists:get_value(data_dir, Config)),
    ok.

%% @doc GIVEN a link whose backfill is running
%%      WHEN five events are appended to the source stream while it reads
%%      THEN the link stream holds every source event exactly once: the ones
%%           below the fence from the backfill, the ones above it live
the_append_during_a_backfill_is_linked_exactly_once(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"orders">>),
    LinkName = <<"orders">>,

    {ok, 9} = append_events(StoreId, StreamId, 10),

    %% The filter is the hook: it appends five more events the first time the
    %% backfill asks it about an event, which is after the fence was taken.
    Appender = self(),
    Filter = fun(_Event) ->
                     Appender ! {appended, append_during_backfill(StoreId, StreamId)},
                     true
             end,
    ok = reckon_db_links:create(StoreId, #{name => LinkName,
                                           source => #{type => stream,
                                                       stream_id => StreamId},
                                           filter => Filter,
                                           backfill => true}),
    ok = reckon_db_links:start(StoreId, LinkName),

    receive
        {appended, {ok, 14}} -> ok
    after 10000 ->
        ct:fail("the filter never ran during the backfill")
    end,

    %% The fence was 9 when the link started: the backfill stops there, and
    %% the five appended during it are above it.
    ?assertEqual(lists:seq(0, 9), link_versions(StoreId, LinkName)),

    %% They arrive the way the emitter delivers them.
    Live = source_events(StoreId, StreamId, lists:seq(10, 14)),
    Worker = whereis(reckon_db_naming:link_worker_name(StoreId, LinkName)),
    Worker ! {events, Live},
    ok = wait_for_versions(StoreId, LinkName, lists:seq(0, 14)),
    ?assertEqual(lists:seq(0, 14), link_versions(StoreId, LinkName)).

%% @doc GIVEN a link that has linked a live event
%%      WHEN the same event is delivered again
%%      THEN it is not appended a second time
%%
%% The emitter may re-deliver after a restart, and the fence is what makes
%% that harmless: an event at or below its stream's fence is already in the
%% link stream.
a_redelivered_live_event_is_not_linked_twice(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"orders">>),
    LinkName = <<"orders">>,

    {ok, 4} = append_events(StoreId, StreamId, 5),
    ok = reckon_db_links:create(StoreId, #{name => LinkName,
                                           source => #{type => stream,
                                                       stream_id => StreamId},
                                           backfill => true}),
    ok = reckon_db_links:start(StoreId, LinkName),
    ?assertEqual(lists:seq(0, 4), link_versions(StoreId, LinkName)),

    %% A new source event, delivered twice.
    {ok, 5} = append_events(StoreId, StreamId, 5, 1),
    [Live] = source_events(StoreId, StreamId, [5]),
    Worker = whereis(reckon_db_naming:link_worker_name(StoreId, LinkName)),
    Worker ! {events, [Live]},
    ok = wait_for_versions(StoreId, LinkName, lists:seq(0, 5)),
    Worker ! {events, [Live]},
    timer:sleep(200),
    ?assertEqual(lists:seq(0, 5), link_versions(StoreId, LinkName)).

%%====================================================================
%% Helpers
%%====================================================================

append_events(StoreId, StreamId, Count) ->
    append_events(StoreId, StreamId, 0, Count).

append_events(StoreId, StreamId, Start, Count) ->
    Events = [#{event_type => <<"order_placed_v1">>,
                data => #{n => N}} || N <- lists:seq(Start, Start + Count - 1)],
    reckon_db_streams:append(StoreId, StreamId, -2, Events).

append_during_backfill(StoreId, StreamId) ->
    case get(during_backfill) of
        undefined ->
            put(during_backfill, done),
            append_events(StoreId, StreamId, 10, 5);
        done ->
            {ok, 14}
    end.

%% The source version of every event in the link stream, so a duplicate or a
%% hole shows up as a list that is not the one expected.
link_versions(StoreId, LinkName) ->
    LinkStreamId = reckon_db_links:link_stream_id(LinkName),
    case reckon_db_streams:read_all(StoreId, LinkStreamId, 1000, forward) of
        {ok, Events} ->
            [maps:get(source_version, E#event.metadata) || E <- Events];
        {error, _} ->
            []
    end.

source_events(StoreId, StreamId, Versions) ->
    lists:filtermap(
      fun(Version) ->
              case reckon_db_streams:read(StoreId, StreamId, Version, 1, forward) of
                  {ok, [Event]} -> {true, Event};
                  _ -> false
              end
      end, Versions).

wait_for_versions(StoreId, LinkName, Expected) ->
    wait_for_versions(StoreId, LinkName, Expected, 50).

wait_for_versions(StoreId, LinkName, Expected, 0) ->
    ct:fail("link stream never reached ~p; it holds ~p",
            [Expected, link_versions(StoreId, LinkName)]);
wait_for_versions(StoreId, LinkName, Expected, Tries) ->
    case link_versions(StoreId, LinkName) of
        Expected -> ok;
        _ -> timer:sleep(100), wait_for_versions(StoreId, LinkName, Expected, Tries - 1)
    end.
