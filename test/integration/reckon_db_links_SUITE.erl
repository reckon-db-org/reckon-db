%% @doc A started link follows its source (reckon-db#24).
%%
%% Each case runs against a full store (reckon_db_sup:start_store/1), so
%% the leader, the emitters and the link consumers run as they do in a
%% release. A link stream is read back with reckon_db_streams:read/5 and
%% compared by the source position each derived event carries in its
%% metadata (source_stream, source_version): that pair says which source
%% event it came from, so a missing event and a duplicate both show.
%%
%% @author rgfaber

-module(reckon_db_links_SUITE).

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
    stream_source_follows_live_events/1,
    stream_pattern_source_follows_matching_streams/1,
    all_source_follows_every_stream_but_links/1,
    filter_and_transform_apply_to_live_events/1,
    backfill_then_live_writes_each_event_once/1,
    without_backfill_only_later_events_arrive/1,
    create_refuses_a_source_it_cannot_follow/1,
    start_answers_the_subscribe_error/1,
    stop_stops_delivery/1,
    a_crashed_consumer_loses_and_repeats_nothing/1,
    a_running_link_resumes_after_the_store_restarts/1
]).

-define(WAIT_MS, 5000).

suite() ->
    [{timetrap, {seconds, 60}}].

all() ->
    [
        stream_source_follows_live_events,
        stream_pattern_source_follows_matching_streams,
        all_source_follows_every_stream_but_links,
        filter_and_transform_apply_to_live_events,
        backfill_then_live_writes_each_event_once,
        without_backfill_only_later_events_arrive,
        create_refuses_a_source_it_cannot_follow,
        start_answers_the_subscribe_error,
        stop_stops_delivery,
        a_crashed_consumer_loses_and_repeats_nothing,
        a_running_link_resumes_after_the_store_restarts
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    RaDataDir = "/tmp/reckon_db_links_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),
    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),
    {ok, _} = application:ensure_all_started(reckon_db),
    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ proplists:get_value(ra_data_dir, Config)),
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    TCStr = atom_to_list(TestCase),
    DataDir = "/tmp/reckon_db_links_" ++ TCStr ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreConfig = #store_config{
        store_id = list_to_atom("links_" ++ Rand),
        data_dir = DataDir,
        mode = single,
        writer_pool_size = 1,
        reader_pool_size = 1,
        gateway_pool_size = 1,
        options = #{}
    },
    {ok, _} = reckon_db_sup:start_store(StoreConfig),
    ok = wait_for_leader(StoreConfig#store_config.store_id),
    [{store_config, StoreConfig}, {store_id, StoreConfig#store_config.store_id},
     {data_dir, DataDir} | Config].

end_per_testcase(_TestCase, Config) ->
    _ = reckon_db_sup:stop_store(?config(store_id, Config)),
    os:cmd("rm -rf " ++ ?config(data_dir, Config)),
    ok.

%%====================================================================
%% Source types
%%====================================================================

%% A stream source: a live event on the source stream reaches the link
%% stream, an event on another stream does not.
stream_source_follows_live_events(Config) ->
    StoreId = ?config(store_id, Config),
    Source = sid(<<"linksource">>),
    Other = sid(<<"linkother">>),
    ok = create_and_start(StoreId, <<"one">>, #{type => stream, stream_id => Source}),
    append(StoreId, Other, 1),
    append(StoreId, Source, 2),
    ok = link_holds(StoreId, <<"one">>, [{Source, 0}, {Source, 1}]).

%% A stream_pattern source: the wildcard is applied to whole stream ids,
%% live, which used to crash start/2 (function_clause in
%% reckon_db_filters:by_event_pattern/1).
stream_pattern_source_follows_matching_streams(Config) ->
    StoreId = ?config(store_id, Config),
    A = sid(<<"linkorder-a">>),
    B = sid(<<"linkorder-b">>),
    Other = sid(<<"linkinvoice">>),
    ok = create_and_start(StoreId, <<"orders">>,
                          #{type => stream_pattern, pattern => <<"linkorder*">>}),
    append(StoreId, A, 1),
    append(StoreId, Other, 1),
    append(StoreId, B, 1),
    ok = link_holds(StoreId, <<"orders">>, [{A, 0}, {B, 0}]).

%% An all source: every user stream, never a link stream. Two all links
%% side by side would otherwise feed each other without end.
all_source_follows_every_stream_but_links(Config) ->
    StoreId = ?config(store_id, Config),
    A = sid(<<"linkalpha">>),
    B = sid(<<"linkbeta">>),
    ok = create_and_start(StoreId, <<"every">>, #{type => all}),
    ok = create_and_start(StoreId, <<"every-too">>, #{type => all}),
    append(StoreId, A, 1),
    append(StoreId, B, 1),
    ok = link_holds(StoreId, <<"every">>, [{A, 0}, {B, 0}]),
    ok = link_holds(StoreId, <<"every-too">>, [{A, 0}, {B, 0}]).

filter_and_transform_apply_to_live_events(Config) ->
    StoreId = ?config(store_id, Config),
    Source = sid(<<"linkfiltered">>),
    ok = reckon_db_links:create(StoreId, #{
        name => <<"big">>,
        source => #{type => stream, stream_id => Source},
        filter => fun(E) -> maps:get(n, E#event.data) >= 1 end,
        transform => fun(E) -> E#event{data = maps:put(seen, 1, E#event.data)} end
    }),
    ok = reckon_db_links:start(StoreId, <<"big">>),
    append(StoreId, Source, 3),
    ok = link_holds(StoreId, <<"big">>, [{Source, 1}, {Source, 2}]),
    {ok, Linked} = read_link(StoreId, <<"big">>),
    ?assertEqual([1, 1], [maps:get(seen, E#event.data) || E <- Linked]).

%%====================================================================
%% Backfill and catch-up
%%====================================================================

%% Backfill writes what was there; live picks up from where backfill
%% ended, so every source event is in the link stream once.
backfill_then_live_writes_each_event_once(Config) ->
    StoreId = ?config(store_id, Config),
    Source = sid(<<"linkbackfill">>),
    append(StoreId, Source, 3),
    ok = reckon_db_links:create(StoreId, #{
        name => <<"history">>,
        source => #{type => stream, stream_id => Source},
        backfill => true
    }),
    ok = reckon_db_links:start(StoreId, <<"history">>),
    append(StoreId, Source, 2),
    ok = link_holds(StoreId, <<"history">>, [{Source, V} || V <- lists:seq(0, 4)]).

without_backfill_only_later_events_arrive(Config) ->
    StoreId = ?config(store_id, Config),
    Source = sid(<<"linknobackfill">>),
    append(StoreId, Source, 2),
    ok = create_and_start(StoreId, <<"later">>, #{type => stream, stream_id => Source}),
    append(StoreId, Source, 1),
    ok = link_holds(StoreId, <<"later">>, [{Source, 2}]).

%%====================================================================
%% Errors
%%====================================================================

create_refuses_a_source_it_cannot_follow(Config) ->
    StoreId = ?config(store_id, Config),
    Bad = [#{type => stream}, #{type => stream, stream_id => 42},
           #{type => stream_pattern}, #{type => stream_pattern, pattern => <<>>},
           #{type => every_stream}, not_a_map],
    lists:foreach(
      fun(Source) ->
          ?assertEqual({error, {invalid_source, Source}},
                       reckon_db_links:create(StoreId, #{name => <<"bad">>, source => Source})),
          ?assertEqual({error, not_found}, reckon_db_links:get(StoreId, <<"bad">>))
      end, Bad).

%% start/2 used to ignore the subscribe result and answer ok.
start_answers_the_subscribe_error(Config) ->
    StoreId = ?config(store_id, Config),
    ok = reckon_db_links:create(StoreId, #{name => <<"refused">>,
                                           source => #{type => all}}),
    ok = meck:new(reckon_db_subscriptions, [passthrough]),
    try
        ok = meck:expect(reckon_db_subscriptions, subscribe,
                         fun(_, _, _, _, _) -> {error, refused_for_test} end),
        ?assertEqual({error, refused_for_test}, reckon_db_links:start(StoreId, <<"refused">>)),
        {ok, Info} = reckon_db_links:get(StoreId, <<"refused">>),
        ?assertEqual(stopped, maps:get(status, Info))
    after
        meck:unload(reckon_db_subscriptions)
    end.

%%====================================================================
%% Lifecycle
%%====================================================================

%% stop/2 used to call unsubscribe/2 with the name where it takes the key.
stop_stops_delivery(Config) ->
    StoreId = ?config(store_id, Config),
    Source = sid(<<"linkstop">>),
    ok = create_and_start(StoreId, <<"stopped">>, #{type => stream, stream_id => Source}),
    append(StoreId, Source, 1),
    ok = link_holds(StoreId, <<"stopped">>, [{Source, 0}]),
    ok = reckon_db_links:stop(StoreId, <<"stopped">>),
    {ok, Subs} = reckon_db_subscriptions:list(StoreId),
    ?assertEqual([], [S || #subscription{subscription_name = <<"$link-sub:", _/binary>>} = S <- Subs]),
    append(StoreId, Source, 1),
    timer:sleep(1000),
    ?assertEqual([{Source, 0}], positions(read_link(StoreId, <<"stopped">>))).

%% The consumer is killed and an event appended before its supervisor
%% has restarted it: the event still arrives, once, and nothing before it
%% is written again.
a_crashed_consumer_loses_and_repeats_nothing(Config) ->
    StoreId = ?config(store_id, Config),
    Source = sid(<<"linkcrash">>),
    ok = create_and_start(StoreId, <<"crashy">>, #{type => stream, stream_id => Source}),
    append(StoreId, Source, 2),
    ok = link_holds(StoreId, <<"crashy">>, [{Source, 0}, {Source, 1}]),
    Consumer = consumer(StoreId, <<"crashy">>),
    exit(Consumer, kill),
    append(StoreId, Source, 1),
    ok = link_holds(StoreId, <<"crashy">>, [{Source, V} || V <- lists:seq(0, 2)]),
    append(StoreId, Source, 1),
    ok = link_holds(StoreId, <<"crashy">>, [{Source, V} || V <- lists:seq(0, 3)]).

%% A link that was running is running again after the store restarts:
%% nothing written before the restart is written again, and events after
%% it arrive.
a_running_link_resumes_after_the_store_restarts(Config) ->
    StoreId = ?config(store_id, Config),
    Source = sid(<<"linkrestart">>),
    ok = create_and_start(StoreId, <<"durable">>, #{type => stream, stream_id => Source}),
    append(StoreId, Source, 2),
    ok = link_holds(StoreId, <<"durable">>, [{Source, 0}, {Source, 1}]),
    ok = reckon_db_sup:stop_store(StoreId),
    {ok, _} = reckon_db_sup:start_store(?config(store_config, Config)),
    ok = wait_for_leader(StoreId),
    ok = reckon_db_test_helpers:wait_for(
           fun() -> is_pid(consumer(StoreId, <<"durable">>)) end, ?WAIT_MS),
    append(StoreId, Source, 1),
    ok = link_holds(StoreId, <<"durable">>, [{Source, V} || V <- lists:seq(0, 2)]),
    {ok, Info} = reckon_db_links:get(StoreId, <<"durable">>),
    ?assertEqual(running, maps:get(status, Info)).

%%====================================================================
%% Helpers
%%====================================================================

create_and_start(StoreId, Name, Source) ->
    ok = reckon_db_links:create(StoreId, #{name => Name, source => Source}),
    reckon_db_links:start(StoreId, Name).

append(StoreId, StreamId, Count) ->
    Events = [#{event_type => <<"link_tested_v1">>, data => #{n => N}}
              || N <- lists:seq(0, Count - 1)],
    {ok, _} = reckon_db_streams:append(StoreId, StreamId, -2, Events),
    ok.

read_link(StoreId, Name) ->
    case reckon_db_streams:read(StoreId, <<"$link:", Name/binary>>, 0, 1000, forward) of
        {ok, Events} -> {ok, Events};
        {error, _} -> {ok, []}
    end.

positions({ok, Events}) ->
    [{maps:get(source_stream, M), maps:get(source_version, M)}
     || #event{metadata = M} <- Events].

%% The link stream holds exactly Expected, in that order, within the wait;
%% then it still does a moment later, so a late duplicate shows too.
link_holds(StoreId, Name, Expected) ->
    Holds = fun() -> positions(read_link(StoreId, Name)) =:= Expected end,
    case reckon_db_test_helpers:wait_for(Holds, ?WAIT_MS) of
        ok ->
            timer:sleep(300),
            ?assertEqual(Expected, positions(read_link(StoreId, Name))),
            ok;
        timeout ->
            ct:fail("link ~s holds ~p, expected ~p",
                    [Name, positions(read_link(StoreId, Name)), Expected])
    end.

consumer(StoreId, Name) ->
    global:whereis_name(reckon_db_link_consumer:name(StoreId, Name)).

sid(Label) ->
    reckon_db_test_helpers:sid(Label).

wait_for_leader(StoreId) ->
    reckon_db_test_helpers:wait_for(fun() -> reckon_db_leader:is_active(StoreId) end, 10000).
