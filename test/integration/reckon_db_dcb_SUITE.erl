%%% @doc Integration tests for reckon_db_dcb:append_if_no_tag_matches/4.
%%%
%%% Runs a real Khepri store (per-store ra_system pattern, matching
%%% reckon_db_store production setup). Each test gets a fresh data dir.
%%%
%%% Smoke-level coverage for P3.1c. P3.2 expands with concurrency,
%%% property-based tests, and large-scale benchmarks.
%%% @end
-module(reckon_db_dcb_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("../../include/reckon_db.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    single_event_no_tags/1,
    single_event_with_tags/1,
    conflict_aborts/1,
    non_conflict_appends_after_cutoff/1,
    multi_event_batch_assigns_contiguous_seqs/1,
    counter_advances_across_appends/1,
    empty_events_rejected/1,
    tag_index_entries_written/1,
    dcb_events_use_streams_path/1
]).

all() ->
    [single_event_no_tags,
     single_event_with_tags,
     conflict_aborts,
     non_conflict_appends_after_cutoff,
     multi_event_batch_assigns_contiguous_seqs,
     counter_advances_across_appends,
     empty_events_rejected,
     tag_index_entries_written,
     dcb_events_use_streams_path].

%%====================================================================
%% Setup / Teardown
%%====================================================================

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    [{base_data_dir, "/tmp/reckon_db_dcb_SUITE"} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    %% Fresh per-test data dirs + ra_system + Khepri store.
    Rand = integer_to_list(erlang:unique_integer([positive])),
    Base = proplists:get_value(base_data_dir, Config),
    RaDataDir = Base ++ "_ra_" ++ atom_to_list(Case) ++ "_" ++ Rand,
    StoreDataDir = Base ++ "_store_" ++ atom_to_list(Case) ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ RaDataDir),
    os:cmd("rm -rf " ++ StoreDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    ok = filelib:ensure_dir(filename:join(StoreDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),
    {ok, _} = ra:start([{data_dir, RaDataDir}]),
    StoreId = list_to_atom("dcb_test_" ++ Rand),
    RaSystemName = list_to_atom("ra_" ++ atom_to_list(StoreId)),
    RaSystemConfig = (ra_system:default_config())#{
        name => RaSystemName,
        data_dir => StoreDataDir,
        wal_data_dir => StoreDataDir,
        names => ra_system:derive_names(RaSystemName)
    },
    case ra_system:start(RaSystemConfig) of
        {ok, _}                       -> ok;
        {error, {already_started, _}} -> ok
    end,
    {ok, _} = application:ensure_all_started(khepri),
    {ok, _} = khepri:start(RaSystemName, StoreId, 5000),
    [{store_id, StoreId},
     {ra_data_dir, RaDataDir},
     {store_data_dir, StoreDataDir} | Config].

end_per_testcase(_Case, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    catch khepri:stop(StoreId),
    os:cmd("rm -rf " ++ proplists:get_value(ra_data_dir, Config)),
    os:cmd("rm -rf " ++ proplists:get_value(store_data_dir, Config)),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

single_event_no_tags(Config) ->
    StoreId = ?config(store_id, Config),
    Event = #{event_type => <<"thing_happened_v1">>,
              data => #{value => 42}},
    Result = reckon_db_dcb:append_if_no_tag_matches(
               StoreId, {any_of, [<<"never-existed">>]}, 0, [Event]),
    ct:pal("result: ~p", [Result]),
    ?assertMatch({ok, 0}, Result),
    %% Counter advanced to 0.
    ?assertEqual({ok, 0}, khepri:get(StoreId, ?DCB_SEQ_COUNTER_PATH)),
    %% Event stored at /streams/_dcb/<seq_key(0)>.
    {ok, Stored} = khepri:get(StoreId,
                              reckon_db_dcb_paths:event_path(0)),
    ?assertMatch(#event{event_type = <<"thing_happened_v1">>,
                        stream_id = ?DCB_STREAM,
                        version = 0},
                 Stored),
    ok.

single_event_with_tags(Config) ->
    StoreId = ?config(store_id, Config),
    Event = #{event_type => <<"capability_announced_v1">>,
              data => #{mri => <<"cap-1">>},
              tags => [<<"agent:alice">>, <<"cap:weather">>]},
    {ok, 0} = reckon_db_dcb:append_if_no_tag_matches(
                StoreId, {any_of, [<<"never">>]}, 0, [Event]),
    %% Tag index entries present for both tags.
    {ok, MapA} = khepri:get_many(StoreId,
        reckon_db_dcb_paths:by_tag_pattern(<<"agent:alice">>)),
    {ok, MapC} = khepri:get_many(StoreId,
        reckon_db_dcb_paths:by_tag_pattern(<<"cap:weather">>)),
    ?assertEqual(1, maps:size(MapA)),
    ?assertEqual(1, maps:size(MapC)),
    ok.

conflict_aborts(Config) ->
    StoreId = ?config(store_id, Config),
    %% Seed: append an event tagged "x". That gives seq=0.
    Seed = #{event_type => <<"e1">>, data => #{}, tags => [<<"x">>]},
    {ok, 0} = reckon_db_dcb:append_if_no_tag_matches(
                StoreId, {any_of, [<<"never">>]}, 0, [Seed]),
    %% Now try to append with cutoff=0 (anything > 0 is a conflict).
    %% Wait — there's a subtle invariant here. The cutoff is "what I
    %% saw last." If I saw seq=0 and want to check that no NEW matching
    %% event has appeared, cutoff should be 0 → only seqs > 0 count.
    %% Seq 0 itself does NOT trigger conflict (cutoff is exclusive).
    %% So this append should SUCCEED.
    Second = #{event_type => <<"e2">>, data => #{}, tags => [<<"x">>]},
    {ok, 1} = reckon_db_dcb:append_if_no_tag_matches(
                StoreId, {any_of, [<<"x">>]}, 0, [Second]),
    %% NOW try to append with cutoff=0 again — seq=1 exists (>0) → conflict.
    Third = #{event_type => <<"e3">>, data => #{}, tags => [<<"x">>]},
    Result = reckon_db_dcb:append_if_no_tag_matches(
               StoreId, {any_of, [<<"x">>]}, 0, [Third]),
    ?assertEqual({error, {context_changed, 1}}, Result),
    %% Counter should still be 1 (third event NOT written).
    ?assertEqual({ok, 1}, khepri:get(StoreId, ?DCB_SEQ_COUNTER_PATH)),
    ok.

non_conflict_appends_after_cutoff(Config) ->
    StoreId = ?config(store_id, Config),
    Seed = #{event_type => <<"e1">>, data => #{}, tags => [<<"y">>]},
    {ok, 0} = reckon_db_dcb:append_if_no_tag_matches(
                StoreId, {any_of, [<<"never">>]}, 0, [Seed]),
    %% Read counter = 0. Use that as cutoff for next append.
    %% Filter matches tag "y" which has seq=0. cutoff=0 means
    %% "anything strictly greater than 0" → 0 itself is filtered out → no conflict.
    Next = #{event_type => <<"e2">>, data => #{}, tags => [<<"y">>]},
    {ok, 1} = reckon_db_dcb:append_if_no_tag_matches(
                StoreId, {any_of, [<<"y">>]}, 0, [Next]),
    ok.

multi_event_batch_assigns_contiguous_seqs(Config) ->
    StoreId = ?config(store_id, Config),
    Events = [#{event_type => <<"batch">>, data => #{n => N}}
              || N <- lists:seq(1, 5)],
    {ok, LastSeq} = reckon_db_dcb:append_if_no_tag_matches(
                      StoreId, {any_of, [<<"never">>]}, 0, Events),
    ?assertEqual(4, LastSeq),  %% seq 0..4
    %% All 5 events stored.
    [begin
         {ok, Ev} = khepri:get(StoreId, reckon_db_dcb_paths:event_path(N)),
         ?assertEqual(N, Ev#event.version)
     end
     || N <- lists:seq(0, 4)],
    ?assertEqual({ok, 4}, khepri:get(StoreId, ?DCB_SEQ_COUNTER_PATH)),
    ok.

counter_advances_across_appends(Config) ->
    StoreId = ?config(store_id, Config),
    E = fun(N) -> #{event_type => <<"e">>, data => #{n => N}} end,
    {ok, 0} = reckon_db_dcb:append_if_no_tag_matches(
                StoreId, {any_of, [<<"never">>]}, 0, [E(1)]),
    {ok, 2} = reckon_db_dcb:append_if_no_tag_matches(
                StoreId, {any_of, [<<"never">>]}, 0, [E(2), E(3)]),
    {ok, 3} = reckon_db_dcb:append_if_no_tag_matches(
                StoreId, {any_of, [<<"never">>]}, 0, [E(4)]),
    ?assertEqual({ok, 3}, khepri:get(StoreId, ?DCB_SEQ_COUNTER_PATH)),
    ok.

empty_events_rejected(Config) ->
    StoreId = ?config(store_id, Config),
    ?assertEqual({error, no_events},
        reckon_db_dcb:append_if_no_tag_matches(
            StoreId, {any_of, [<<"x">>]}, 0, [])),
    ok.

tag_index_entries_written(Config) ->
    StoreId = ?config(store_id, Config),
    Event = #{event_type => <<"e">>, data => #{},
              tags => [<<"t1">>, <<"t2">>, <<"t3">>]},
    {ok, 0} = reckon_db_dcb:append_if_no_tag_matches(
                StoreId, {any_of, [<<"never">>]}, 0, [Event]),
    %% One entry per tag.
    lists:foreach(
      fun(Tag) ->
          {ok, Map} = khepri:get_many(
                        StoreId, reckon_db_dcb_paths:by_tag_pattern(Tag)),
          ?assertEqual(1, maps:size(Map))
      end,
      [<<"t1">>, <<"t2">>, <<"t3">>]),
    ok.

dcb_events_use_streams_path(Config) ->
    %% Validates the design choice: DCB events live under ?STREAMS_PATH,
    %% so they're discoverable via the same path scheme as regular streams.
    %% (P3.2 will test discoverability via the actual read APIs.)
    StoreId = ?config(store_id, Config),
    Event = #{event_type => <<"e">>, data => #{}},
    {ok, 0} = reckon_db_dcb:append_if_no_tag_matches(
                StoreId, {any_of, [<<"never">>]}, 0, [Event]),
    Path = reckon_db_dcb_paths:event_path(0),
    %% The path starts with `streams`, not `events`.
    ?assertMatch([streams, ?DCB_STREAM, _], Path),
    %% Khepri returns it.
    {ok, _} = khepri:get(StoreId, Path),
    ok.
