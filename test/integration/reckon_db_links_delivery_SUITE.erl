%% @doc A link, running on a real store: delivery, restarts and fences.
%%
%% `reckon_db_links_backfill_SUITE' drives the fence arithmetic with the
%% delivery message posted by hand, which is the only way to make the race
%% deterministic. That says nothing about whether a link on a real store ever
%% receives anything, so these cases run the other half: the store's own
%% supervision tree, its trigger, its emitter pool and its subscriber, with
%% every event appended through the public API and nothing posted by hand.
%%
%% What is asserted throughout is the same invariant: each source event ends
%% up in the link stream exactly once, across a start, a stop, a crash and a
%% restart. The link stream is appended with `-2', so nothing downstream can
%% repair a duplicate or a hole.
-module(reckon_db_links_delivery_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([an_append_after_start_is_linked/1,
         a_pattern_link_links_matching_streams_only/1,
         stop_then_start_restarts_the_link/1,
         a_restarted_link_links_each_event_once/1,
         a_killed_worker_resumes_from_its_fence/1,
         a_failed_append_does_not_advance_the_fence/1,
         a_failing_link_does_not_take_the_store_with_it/1,
         a_wildcard_link_does_not_link_its_own_output/1]).

suite() -> [{timetrap, {minutes, 2}}].

all() ->
    [an_append_after_start_is_linked,
     a_pattern_link_links_matching_streams_only,
     stop_then_start_restarts_the_link,
     a_restarted_link_links_each_event_once,
     a_killed_worker_resumes_from_its_fence,
     a_failed_append_does_not_advance_the_fence,
     a_failing_link_does_not_take_the_store_with_it,
     a_wildcard_link_does_not_link_its_own_output].

%%====================================================================
%% CT callbacks
%%====================================================================

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    RaDataDir = "/tmp/reckon_db_links_delivery_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    %% The store's own supervision tree - emitter pool and links supervisor -
    %% comes with the application.
    case application:ensure_all_started(reckon_db) of
        {ok, _} -> ok;
        {error, {already_started, reckon_db}} -> ok
    end,
    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ proplists:get_value(ra_data_dir, Config)),
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_links_delivery_" ++ atom_to_list(TestCase) ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom("links_delivery_" ++ atom_to_list(TestCase) ++ "_" ++ Rand),
    StoreConfig = #store_config{
        store_id = StoreId,
        data_dir = DataDir,
        mode = single,
        writer_pool_size = 1,
        reader_pool_size = 1,
        gateway_pool_size = 1,
        options = #{}
    },
    case reckon_db_sup:start_store(StoreConfig) of
        {ok, _} -> [{data_dir, DataDir}, {store_id, StoreId} | Config];
        {error, Reason} -> ct:fail("Failed to start store: ~p", [Reason])
    end.

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    _ = catch reckon_db_sup:stop_store(StoreId),
    os:cmd("rm -rf " ++ proplists:get_value(data_dir, Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% @doc GIVEN a link that has started on a store
%%      WHEN an event is appended to its source, through the public API
%%      THEN the link stream holds it, with nothing posted to the worker
%%
%% This is the real delivery path end to end: append, trigger, emitter pool,
%% subscriber. Nothing here touches the worker's mailbox directly.
an_append_after_start_is_linked(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"orders-live">>),
    ok = create_link(StoreId, <<"orders">>, #{type => stream, stream_id => StreamId}),
    ok = reckon_db_links:start(StoreId, <<"orders">>),

    {ok, 2} = append(StoreId, StreamId, 3),
    ok = wait_for_versions(StoreId, <<"orders">>, lists:seq(0, 2)).

%% @doc GIVEN a link whose source is a stream pattern
%%      WHEN events are appended to streams that match, and to one that does
%%      THEN only the matching streams' events are linked
a_pattern_link_links_matching_streams_only(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    First = reckon_db_test_helpers:generate_stream_id(),
    Second = reckon_db_test_helpers:generate_stream_id(),
    Other = reckon_db_test_helpers:sid(<<"shipments">>),
    ok = create_link(StoreId, <<"orders">>,
                     #{type => stream_pattern, pattern => <<"test-*">>}),
    ok = reckon_db_links:start(StoreId, <<"orders">>),

    {ok, 0} = append(StoreId, First, 1),
    {ok, 0} = append(StoreId, Second, 1),
    {ok, 0} = append(StoreId, Other, 1),

    ok = wait_for_count(StoreId, <<"orders">>, 2),
    ?assertEqual(lists:sort([First, Second]),
                 lists:sort(source_streams_linked(StoreId, <<"orders">>))).

%% @doc GIVEN a running link
%%      WHEN it is stopped and started again
%%      THEN the second start succeeds and links what is appended after it
%%
%% The child spec is `transient' with the link's name for an id, so a worker
%% that was merely stopped stays in the supervisor and the next start is
%% refused with already_present.
stop_then_start_restarts_the_link(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"orders-restart">>),
    ok = create_link(StoreId, <<"orders">>, #{type => stream, stream_id => StreamId}),
    ok = reckon_db_links:start(StoreId, <<"orders">>),
    {ok, 0} = append(StoreId, StreamId, 1),
    ok = wait_for_versions(StoreId, <<"orders">>, [0]),

    ok = reckon_db_links:stop(StoreId, <<"orders">>),
    ok = reckon_db_links:start(StoreId, <<"orders">>),

    {ok, 1} = append(StoreId, StreamId, 1),
    ok = wait_for_versions(StoreId, <<"orders">>, [0, 1]).

%% @doc GIVEN a link that has linked some of its source
%%      WHEN it is stopped, the source grows, and it is started again
%%      THEN every source event is in the link stream exactly once
%%
%% The fence is what makes this true: the second run resumes at the version
%% the first one recorded, instead of appending the whole link stream again.
a_restarted_link_links_each_event_once(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"orders-resume">>),
    ok = create_link(StoreId, <<"orders">>, #{type => stream, stream_id => StreamId}),
    ok = reckon_db_links:start(StoreId, <<"orders">>),
    {ok, 2} = append(StoreId, StreamId, 3),
    ok = wait_for_versions(StoreId, <<"orders">>, [0, 1, 2]),

    ok = reckon_db_links:stop(StoreId, <<"orders">>),
    {ok, 4} = append(StoreId, StreamId, 2),

    ok = reckon_db_links:start(StoreId, <<"orders">>),
    ok = wait_for_versions(StoreId, <<"orders">>, lists:seq(0, 4)).

%% @doc GIVEN a link worker that dies without stopping cleanly
%%      WHEN events are appended while it is down
%%      THEN the replacement links them, and each event is linked once
a_killed_worker_resumes_from_its_fence(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"orders-killed">>),
    ok = create_link(StoreId, <<"orders">>, #{type => stream, stream_id => StreamId}),
    ok = reckon_db_links:start(StoreId, <<"orders">>),
    {ok, 1} = append(StoreId, StreamId, 2),
    ok = wait_for_versions(StoreId, <<"orders">>, [0, 1]),

    Worker = reckon_db_naming:link_worker_name(StoreId, <<"orders">>),
    exit(whereis(Worker), kill),

    {ok, 2} = append(StoreId, StreamId, 1),
    ok = wait_for_versions(StoreId, <<"orders">>, [0, 1, 2]).

%% @doc GIVEN a link whose append to the link stream fails
%%      WHEN the next source event arrives
%%      THEN the link stops with status error, the fence does not move past
%%           the event, and once appends work again starting the link links it
%%
%% A failed append that still advanced the fence would lose the event for
%% good: the backfill resumes above it and the live path skips it.
a_failed_append_does_not_advance_the_fence(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = reckon_db_test_helpers:sid(<<"orders-failing">>),
    Name = <<"orders">>,
    ok = create_link(StoreId, Name, #{type => stream, stream_id => StreamId}),
    ok = reckon_db_links:start(StoreId, Name),
    {ok, 1} = append(StoreId, StreamId, 2),
    ok = wait_for_versions(StoreId, Name, [0, 1]),

    ok = meck:new(reckon_db_links, [passthrough, no_link]),
    ok = meck:expect(reckon_db_links, append_to_link,
                     fun(_S, _N, _E) -> {error, injected} end),

    {ok, 2} = append(StoreId, StreamId, 1),
    ok = wait_for_status(StoreId, Name, error),

    ?assertEqual(1, fence(StoreId, Name, StreamId)),
    ?assertEqual([0, 1], link_versions(StoreId, Name)),

    ok = meck:unload(reckon_db_links),
    ok = reckon_db_links:start(StoreId, Name),
    ok = wait_for_versions(StoreId, Name, [0, 1, 2]).

%% @doc GIVEN two running links on one store
%%      WHEN one of them cannot append to its own link stream
%%      THEN it stops with status error, the store's core is untouched, and
%%           the other link keeps delivering
%%
%% A link is derived data. Its failure used to restart the links supervisor
%% past its intensity, and that supervisor sat inside the core's
%% one_for_all group, so a broken link took persistence, notification and
%% the gateway with it, and every other running link with them.
a_failing_link_does_not_take_the_store_with_it(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    SourceA = reckon_db_test_helpers:sid(<<"orders-broken">>),
    SourceB = reckon_db_test_helpers:sid(<<"orders-healthy">>),
    A = <<"broken">>,
    B = <<"healthy">>,
    ok = create_link(StoreId, A, #{type => stream, stream_id => SourceA}),
    ok = create_link(StoreId, B, #{type => stream, stream_id => SourceB}),
    ok = reckon_db_links:start(StoreId, A),
    ok = reckon_db_links:start(StoreId, B),
    {ok, 0} = append(StoreId, SourceA, 1),
    {ok, 0} = append(StoreId, SourceB, 1),
    ok = wait_for_versions(StoreId, A, [0]),
    ok = wait_for_versions(StoreId, B, [0]),

    Core = core_pids(StoreId),

    ok = meck:new(reckon_db_links, [passthrough, no_link]),
    ok = meck:expect(reckon_db_links, append_to_link,
                     fun(_S, Name, _E) when Name =:= A -> {error, injected};
                        (S, Name, Events) -> meck:passthrough([S, Name, Events])
                     end),

    {ok, 1} = append(StoreId, SourceA, 1),
    ok = wait_for_status(StoreId, A, error),

    {ok, 1} = append(StoreId, SourceB, 1),
    ok = wait_for_versions(StoreId, B, [0, 1]),

    ?assertEqual(Core, core_pids(StoreId)),
    ?assertEqual([0], link_versions(StoreId, A)),

    ok = meck:unload(reckon_db_links),
    ok = reckon_db_links:start(StoreId, A),
    ok = wait_for_versions(StoreId, A, [0, 1]).

%% @doc GIVEN a link whose source is the wildcard pattern
%%      WHEN one event is appended to a stream
%%      THEN the link stream holds exactly that event
%%
%% A link stream is named `$link:<name>', which the wildcard matches. Once the
%% live path really delivers, a wildcard link links its own output, the new
%% event matches again, and the store fills up with copies of itself. A
%% wildcard source never covers a link stream, its own or anybody else's.
a_wildcard_link_does_not_link_its_own_output(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    Name = <<"wildcard">>,
    Source = reckon_db_test_helpers:sid(<<"orders-wildcard">>),
    ok = create_link(StoreId, Name,
                     #{type => stream_pattern, pattern => <<"*">>}),
    ok = reckon_db_links:start(StoreId, Name),

    {ok, 0} = append(StoreId, Source, 1),
    ok = wait_for_versions(StoreId, Name, [0]),
    timer:sleep(500),
    ?assertEqual([0], link_versions(StoreId, Name)).

%%====================================================================
%% Helpers
%%====================================================================

create_link(StoreId, Name, Source) ->
    ok = reckon_db_links:create(StoreId, #{name => Name, source => Source,
                                           backfill => true}),
    ok.

append(StoreId, StreamId, Count) ->
    Events = [#{event_type => <<"order_placed_v1">>, data => #{n => N}}
              || N <- lists:seq(1, Count)],
    reckon_db_streams:append(StoreId, StreamId, -2, Events).

%% The source version of every event in the link stream, so a duplicate or a
%% hole shows up as a list that is not the one expected.
link_versions(StoreId, Name) ->
    LinkStreamId = reckon_db_links:link_stream_id(Name),
    case reckon_db_streams:read_all(StoreId, LinkStreamId, 1000, forward) of
        {ok, Events} -> [maps:get(source_version, E#event.metadata) || E <- Events];
        {error, _} -> []
    end.

source_streams_linked(StoreId, Name) ->
    LinkStreamId = reckon_db_links:link_stream_id(Name),
    case reckon_db_streams:read_all(StoreId, LinkStreamId, 1000, forward) of
        {ok, Events} -> [maps:get(source_stream, E#event.metadata) || E <- Events];
        {error, _} -> []
    end.

%% The pids that make the store a store. If a failing link restarts any of
%% them, this list changes.
core_pids(StoreId) ->
    [whereis(reckon_db_naming:core_sup_name(StoreId)),
     whereis(reckon_db_naming:persistence_sup_name(StoreId)),
     whereis(reckon_db_naming:notification_sup_name(StoreId)),
     whereis(reckon_db_naming:gateway_sup_name(StoreId)),
     whereis(reckon_db_naming:links_sup_name(StoreId))].

wait_for_status(StoreId, Name, Expected) ->
    Status = fun() -> case reckon_db_links:info(StoreId, Name) of
                          {ok, #{status := S}} -> S;
                          _ -> unknown
                      end
              end,
    wait_for(Status, Expected, Name, StoreId).

fence(StoreId, Name, StreamId) ->
    case khepri:get(StoreId, [links, StoreId, Name, fences, StreamId]) of
        {ok, Version} when is_integer(Version) -> Version;
        _ -> undefined
    end.

wait_for_versions(StoreId, Name, Expected) ->
    wait_for(fun() -> link_versions(StoreId, Name) end, Expected, Name, StoreId).

wait_for_count(StoreId, Name, Expected) ->
    wait_for(fun() -> length(source_streams_linked(StoreId, Name)) end,
             Expected, Name, StoreId).

wait_for(Fun, Expected, Name, StoreId) ->
    wait_for(Fun, Expected, Name, StoreId, 100).

wait_for(_Fun, _Expected, Name, _StoreId, 0) ->
    ct:fail("link ~p never reached ~p", [Name, _Expected]);
wait_for(Fun, Expected, Name, StoreId, Tries) ->
    case Fun() of
        Expected -> ok;
        _ -> timer:sleep(100), wait_for(Fun, Expected, Name, StoreId, Tries - 1)
    end.
