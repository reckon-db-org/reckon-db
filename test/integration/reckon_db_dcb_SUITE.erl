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
    dcb_events_use_streams_path/1,
    integrity_enabled_rejected/1,
    facade_routes_to_dcb_module/1,
    cutoff_minus_one_for_empty_initial_state/1,
    concurrent_uniqueness_only_one_wins/1,
    dcb_events_visible_via_read_by_tags/1
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
     dcb_events_use_streams_path,
     integrity_enabled_rejected,
     facade_routes_to_dcb_module,
     cutoff_minus_one_for_empty_initial_state,
     concurrent_uniqueness_only_one_wins,
     dcb_events_visible_via_read_by_tags
     %% dcb_events_deliver_to_tag_subscription: deferred. Requires the
     %% full subscription delivery pipeline (emitter pool wiring per
     %% reckon_db_subscription_delivery_SUITE). DCB events use
     %% khepri_tx:put on the same paths as regular events, so Khepri
     %% triggers + tag-filter routing should match identically.
     %% Will be verified end-to-end as part of P3.4 (gateway dispatch)
     %% or sooner in a focused follow-up.
    ].

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
    %% Per-store ra_system pattern, matches reckon_db_store production
    %% setup. Sufficient for everything except subscription-delivery
    %% tests, which require manual emitter wiring (see the
    %% reckon_db_subscription_delivery_SUITE pattern). Phase 2 interop
    %% test deferred to P3.2 follow-up.
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

integrity_enabled_rejected(Config) ->
    %% v1 fail-closed safety check. DCB v1 ships without HMAC chain;
    %% on integrity-enabled stores it would create a silent tamper-
    %% detection gap. Refuse with an explicit error instead.
    StoreId = ?config(store_id, Config),
    %% Simulate integrity-on by setting the persistent_term flag directly
    %% (the integrity_key module's standard mechanism).
    persistent_term:put({reckon_db, integrity_enabled, StoreId}, true),
    try
        Event = #{event_type => <<"e">>, data => #{}},
        ?assertEqual(
            {error, integrity_not_supported_in_dcb_v1},
            reckon_db_dcb:append_if_no_tag_matches(
                StoreId, {any_of, [<<"never">>]}, 0, [Event])),
        %% Nothing written: counter still absent.
        ?assertMatch({error, _}, khepri:get(StoreId, ?DCB_SEQ_COUNTER_PATH))
    after
        persistent_term:erase({reckon_db, integrity_enabled, StoreId})
    end.

facade_routes_to_dcb_module(Config) ->
    %% reckon_db_streams:append_if_no_tag_matches/4 must delegate to
    %% reckon_db_dcb:append_if_no_tag_matches/4. Same return, same side
    %% effects.
    StoreId = ?config(store_id, Config),
    Event = #{event_type => <<"facade_test">>, data => #{},
              tags => [<<"facade">>]},
    Result = reckon_db_streams:append_if_no_tag_matches(
               StoreId, {any_of, [<<"never">>]}, 0, [Event]),
    ?assertMatch({ok, 0}, Result),
    %% Verify the event landed at the expected DCB path.
    {ok, #event{event_type = <<"facade_test">>}} =
        khepri:get(StoreId, reckon_db_dcb_paths:event_path(0)),
    ok.

%%====================================================================
%% P3.2 — cutoff semantics, concurrent contention, Phase 1+2 interop
%%====================================================================

cutoff_minus_one_for_empty_initial_state(Config) ->
    %% The canonical uniqueness idiom: cutoff = -1 means "I saw nothing
    %% yet". The first writer succeeds. The second writer (also passing
    %% cutoff = -1 because they too saw nothing at read time) gets
    %% context_changed because the first one's event is now there.
    StoreId = ?config(store_id, Config),
    Event1 = #{event_type => <<"email_registered">>,
               data => #{email => <<"alice@example.com">>},
               tags => [<<"email:alice@example.com">>]},
    {ok, 0} = reckon_db_dcb:append_if_no_tag_matches(
                StoreId,
                {any_of, [<<"email:alice@example.com">>]},
                -1,
                [Event1]),
    Event2 = #{event_type => <<"email_registered">>,
               data => #{email => <<"alice@example.com">>},
               tags => [<<"email:alice@example.com">>]},
    ?assertEqual({error, {context_changed, 0}},
        reckon_db_dcb:append_if_no_tag_matches(
            StoreId,
            {any_of, [<<"email:alice@example.com">>]},
            -1,
            [Event2])),
    %% Counter still at 0 — second write rejected.
    ?assertEqual({ok, 0}, khepri:get(StoreId, ?DCB_SEQ_COUNTER_PATH)),
    ok.

concurrent_uniqueness_only_one_wins(Config) ->
    %% 50 processes race to register the same email. Exactly one
    %% succeeds; the rest see context_changed. The store ends up with
    %% exactly one event for that tag.
    StoreId = ?config(store_id, Config),
    Tag = <<"email:race@example.com">>,
    NumWorkers = 50,
    Self = self(),
    %% Spawn workers that block on a barrier message, then race.
    Workers = [spawn_link(fun() -> uniqueness_worker(Self, StoreId, Tag, N) end)
               || N <- lists:seq(1, NumWorkers)],
    %% Release all workers at once. (Each barrier-receive accepts {go}.)
    [W ! go || W <- Workers],
    Results = collect_worker_results(NumWorkers, []),
    Successes = [R || {ok, _} = R <- Results],
    Conflicts = [R || {error, {context_changed, _}} = R <- Results],
    ct:pal("successes=~p conflicts=~p", [length(Successes), length(Conflicts)]),
    ?assertEqual(1, length(Successes)),
    ?assertEqual(NumWorkers - 1, length(Conflicts)),
    %% Exactly one event under the tag.
    {ok, TagMap} = khepri:get_many(
                     StoreId, reckon_db_dcb_paths:by_tag_pattern(Tag)),
    ?assertEqual(1, maps:size(TagMap)),
    %% Counter at 0 (one event committed).
    ?assertEqual({ok, 0}, khepri:get(StoreId, ?DCB_SEQ_COUNTER_PATH)),
    ok.

dcb_events_visible_via_read_by_tags(Config) ->
    %% Phase 1 interop. DCB events live under ?STREAMS_PATH, so the
    %% existing reckon_db_streams:read_by_tags must see them.
    StoreId = ?config(store_id, Config),
    Event = #{event_type => <<"announced">>,
              data => #{capability => <<"weather">>},
              tags => [<<"agent:alice">>, <<"cap:weather">>]},
    {ok, 0} = reckon_db_dcb:append_if_no_tag_matches(
                StoreId, {any_of, [<<"never">>]}, -1, [Event]),
    {ok, [Found]} = reckon_db_streams:read_by_tags(
                      StoreId, [<<"agent:alice">>], any, 10),
    ?assertEqual(<<"announced">>, Found#event.event_type),
    ?assertEqual(?DCB_STREAM, Found#event.stream_id),
    %% And the cross-tag filter sees it too.
    {ok, [_]} = reckon_db_streams:read_by_tags(
                  StoreId, [<<"cap:weather">>], any, 10),
    %% All-of (intersection) sees it when both tags match.
    {ok, [_]} = reckon_db_streams:read_by_tags(
                  StoreId, [<<"agent:alice">>, <<"cap:weather">>], all, 10),
    ok.

%%====================================================================
%% Helpers for the concurrent test
%%====================================================================

uniqueness_worker(Parent, StoreId, Tag, N) ->
    receive go -> ok end,
    Event = #{event_type => <<"email_registered">>,
              data => #{n => N},
              tags => [Tag]},
    Result = reckon_db_dcb:append_if_no_tag_matches(
               StoreId, {any_of, [Tag]}, -1, [Event]),
    Parent ! {worker_result, N, Result}.

collect_worker_results(0, Acc) ->
    lists:reverse(Acc);
collect_worker_results(N, Acc) ->
    receive
        {worker_result, _N, R} -> collect_worker_results(N - 1, [R | Acc])
    after 30000 ->
        ct:fail({worker_timeout, remaining, N})
    end.
