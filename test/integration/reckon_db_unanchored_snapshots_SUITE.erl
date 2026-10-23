%%% @doc Snapshots on system streams that hold no events, on a store with
%%% integrity on: sealed with a store-bound MAC and no chain anchor
%%% (reckon-db#25; the threat model is in that issue).
%%%
%%% A projection checkpoint lives in a snapshot on a stream with no events.
%%% An integrity store seals every snapshot with an anchor into its stream's
%%% event chain, so it refused every checkpoint. Now a "$" stream with no
%%% events is sealed with a scheme-2 MAC (store id, life <<"$unanchored">>)
%%% and nothing else. Each case below is one row of the issue's table; the
%%% tree writes are what attacker A1 (writes the tree, holds no key) can do.
%%% @end
-module(reckon_db_unanchored_snapshots_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("../../include/reckon_db.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([a_checkpoint_on_a_system_stream_is_sealed_and_loads/1,
         a_raised_checkpoint_is_refused/1,
         a_checkpoint_copied_from_another_stream_is_refused/1,
         a_checkpoint_sealed_for_another_store_is_refused/1,
         an_unsealed_system_snapshot_fails_closed/1,
         a_scheme_1_mac_on_a_system_stream_is_refused/1,
         a_system_stream_with_events_keeps_its_anchor/1,
         an_unanchored_seal_on_a_stream_with_events_is_refused/1,
         a_user_stream_snapshot_is_unchanged/1,
         a_store_without_integrity_is_unchanged/1]).

-define(STREAM, <<"$evoq-checkpoint:order_summary">>).
-define(OTHER, <<"$evoq-checkpoint:invoice_totals">>).

all() ->
    [a_checkpoint_on_a_system_stream_is_sealed_and_loads,
     a_raised_checkpoint_is_refused,
     a_checkpoint_copied_from_another_stream_is_refused,
     a_checkpoint_sealed_for_another_store_is_refused,
     an_unsealed_system_snapshot_fails_closed,
     a_scheme_1_mac_on_a_system_stream_is_refused,
     a_system_stream_with_events_keeps_its_anchor,
     an_unanchored_seal_on_a_stream_with_events_is_refused,
     a_user_stream_snapshot_is_unchanged,
     a_store_without_integrity_is_unchanged].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    [{base_data_dir, "/tmp/reckon_db_unanchored_snapshots_SUITE"} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    %% Per-store ra_system pattern, matches reckon_db_store production
    %% setup.
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
    StoreId = list_to_atom("unanchored_" ++ Rand),
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
    enable_integrity_unless(Case, StoreId),
    [{store_id, StoreId},
     {ra_data_dir, RaDataDir},
     {store_data_dir, StoreDataDir} | Config].

end_per_testcase(_Case, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    persistent_term:erase({reckon_db, integrity_enabled, StoreId}),
    persistent_term:erase({reckon_db, integrity_key, StoreId}),
    catch khepri:stop(StoreId),
    os:cmd("rm -rf " ++ proplists:get_value(ra_data_dir, Config)),
    os:cmd("rm -rf " ++ proplists:get_value(store_data_dir, Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

a_checkpoint_on_a_system_stream_is_sealed_and_loads(Config) ->
    StoreId = ?config(store_id, Config),
    ?assertEqual(ok, save(StoreId, ?STREAM, 41)),
    {ok, Snap} = reckon_db_snapshots:load(StoreId, ?STREAM),
    ?assertMatch(#snapshot{version = 41, data = #{checkpoint := 41},
                           anchor_hash = undefined, mac = {v2, _, _}}, Snap).

%% A1 edits the stored checkpoint forward: the MAC covers version and data.
a_raised_checkpoint_is_refused(Config) ->
    StoreId = ?config(store_id, Config),
    ok = save(StoreId, ?STREAM, 41),
    Stored = reckon_db_snapshots_store:get(StoreId, ?STREAM, 41),
    ok = reckon_db_snapshots_store:put(StoreId, Stored#snapshot{version = 900,
                                                               data = #{checkpoint => 900}}),
    ?assertMatch({error, {integrity_violation, _}}, reckon_db_snapshots:load(StoreId, ?STREAM)).

%% A1 copies another projection's genuine checkpoint into this stream.
a_checkpoint_copied_from_another_stream_is_refused(Config) ->
    StoreId = ?config(store_id, Config),
    ok = save(StoreId, ?OTHER, 900),
    Other = reckon_db_snapshots_store:get(StoreId, ?OTHER, 900),
    ok = reckon_db_snapshots_store:put(StoreId, Other#snapshot{stream_id = ?STREAM}),
    ?assertMatch({error, {integrity_violation, _}}, reckon_db_snapshots:load(StoreId, ?STREAM)).

%% A1 copies a checkpoint sealed in another store under the same key.
a_checkpoint_sealed_for_another_store_is_refused(Config) ->
    StoreId = ?config(store_id, Config),
    Key = reckon_db_integrity_key:get(StoreId),
    Snap0 = snapshot(?STREAM, 900),
    Mac = reckon_gater_integrity:compute_snapshot_mac(
            Snap0, Key, #{store_id => another_store, life => <<"$unanchored">>}),
    ok = reckon_db_snapshots_store:put(StoreId, Snap0#snapshot{mac = Mac}),
    ?assertMatch({error, {integrity_violation, _}}, reckon_db_snapshots:load(StoreId, ?STREAM)).

%% A1 plants a bare checkpoint, no anchor and no MAC: a bare value is not
%% believed on a system stream.
an_unsealed_system_snapshot_fails_closed(Config) ->
    StoreId = ?config(store_id, Config),
    ok = reckon_db_snapshots_store:put(StoreId, snapshot(?STREAM, 900)),
    ?assertEqual({error, {integrity_violation, unsealed_system_snapshot}},
                 reckon_db_snapshots:load(StoreId, ?STREAM)).

%% A store-blind scheme-1 MAC is never what this rule writes.
a_scheme_1_mac_on_a_system_stream_is_refused(Config) ->
    StoreId = ?config(store_id, Config),
    Key = reckon_db_integrity_key:get(StoreId),
    Snap0 = snapshot(?STREAM, 900),
    Mac = reckon_gater_integrity:compute_snapshot_mac(Snap0, Key),
    ok = reckon_db_snapshots_store:put(StoreId, Snap0#snapshot{mac = Mac}),
    ?assertMatch({error, {integrity_violation, _}}, reckon_db_snapshots:load(StoreId, ?STREAM)).

%% Rule 4: a system stream that holds events is anchored as before, so a
%% save at a version with no event is refused, and one at an event loads.
a_system_stream_with_events_keeps_its_anchor(Config) ->
    StoreId = ?config(store_id, Config),
    {ok, _} = reckon_db_streams:append(StoreId, ?STREAM, -1,
                                       [#{event_type => <<"noted_v1">>, data => #{n => 1}}]),
    ?assertMatch({error, _}, save(StoreId, ?STREAM, 41)),
    ?assertEqual(ok, save(StoreId, ?STREAM, 0)),
    {ok, Snap} = reckon_db_snapshots:load(StoreId, ?STREAM),
    ?assertNotEqual(undefined, Snap#snapshot.anchor_hash).

%% A1 adds events under a checkpoint stream after it was sealed unanchored:
%% the stream decides, and an unanchored seal on it is refused.
an_unanchored_seal_on_a_stream_with_events_is_refused(Config) ->
    StoreId = ?config(store_id, Config),
    ok = save(StoreId, ?STREAM, 41),
    {ok, _} = reckon_db_streams:append(StoreId, ?STREAM, -1,
                                       [#{event_type => <<"noted_v1">>, data => #{n => 1}}]),
    ?assertMatch({error, {integrity_violation, _}}, reckon_db_snapshots:load(StoreId, ?STREAM)).

%% A user stream's snapshot is anchored exactly as before.
a_user_stream_snapshot_is_unchanged(Config) ->
    StoreId = ?config(store_id, Config),
    Stream = <<"order-018f6a7b8c9d4e5f60718293a4b5c6d7">>,
    ?assertMatch({error, _}, save(StoreId, Stream, 3)).

%% No integrity: saved and loaded as before, no seal.
a_store_without_integrity_is_unchanged(Config) ->
    StoreId = ?config(store_id, Config),
    ?assertEqual(ok, save(StoreId, ?STREAM, 41)),
    ?assertMatch({ok, #snapshot{version = 41, mac = undefined}},
                 reckon_db_snapshots:load(StoreId, ?STREAM)).

%%====================================================================
%% Helpers
%%====================================================================

save(StoreId, Stream, N) ->
    reckon_db_snapshots:save(StoreId, Stream, N, #{checkpoint => N}, #{}).

snapshot(Stream, N) ->
    #snapshot{stream_id = Stream, version = N, data = #{checkpoint => N},
              metadata = #{}, timestamp = 1}.

enable_integrity_unless(a_store_without_integrity_is_unchanged, _StoreId) ->
    ok;
enable_integrity_unless(_Case, StoreId) ->
    persistent_term:put({reckon_db, integrity_enabled, StoreId}, true),
    persistent_term:put({reckon_db, integrity_key, StoreId}, crypto:strong_rand_bytes(32)).
