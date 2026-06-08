%% @doc Common Test suite for the tamper-resistance write path.
%%
%% Verifies that with integrity enabled on a store, appended events
%% carry prev_event_hash + mac populated correctly, the chain is
%% continuous across appends, and disabled stores produce no
%% integrity fields.
%%
%% @end
-module(reckon_db_integrity_writes_SUITE).

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
    disabled_store_writes_legacy_events/1,
    enabled_store_writes_integrity_fields/1,
    chain_continues_across_appends/1,
    watermark_is_recorded_on_first_append/1,
    different_keys_produce_different_macs/1
]).

%%====================================================================
%% CT boilerplate
%%====================================================================

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        disabled_store_writes_legacy_events,
        enabled_store_writes_integrity_fields,
        chain_continues_across_appends,
        watermark_is_recorded_on_first_append,
        different_keys_produce_different_macs
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    RaDataDir = "/tmp/reckon_db_integrity_writes_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),

    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),

    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    RaDataDir = proplists:get_value(ra_data_dir, Config),
    os:cmd("rm -rf " ++ RaDataDir),
    ok.

init_per_testcase(TestCase, Config) ->
    %% Each test gets a fresh Khepri store and a unique HMAC key.
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_integrity_writes_" ++
              atom_to_list(TestCase) ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom("integrity_test_" ++ atom_to_list(TestCase) ++ "_" ++ Rand),

    {ok, _} = khepri:start(DataDir, StoreId),
    khepri:put(StoreId, [streams], #{}),
    khepri:put(StoreId, [metadata], #{}),

    [
        {data_dir, DataDir},
        {store_id, StoreId}
        | Config
    ].

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    DataDir = proplists:get_value(data_dir, Config),
    catch khepri:stop(StoreId),
    reckon_db_integrity_key:clear(StoreId),
    os:cmd("rm -rf " ++ DataDir),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% A store with integrity disabled (the default) writes events with
%% no prev_event_hash and no mac — same shape as pre-2.1.
disabled_store_writes_legacy_events(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    %% Explicitly load with disabled to mirror real store startup.
    ok = reckon_db_integrity_key:load(
        #store_config{store_id = StoreId, data_dir = "/tmp",
                      integrity = disabled}),

    StreamId = reckon_db_test_helpers:sid(<<"stream-disabled">>),
    {ok, 0} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"x_happened">>, data => #{n => 1}}]),

    {ok, [Event]} = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),
    ?assertEqual(undefined, Event#event.prev_event_hash),
    ?assertEqual(undefined, Event#event.mac),
    ?assertEqual(undefined, Event#event.signature),
    ok.

%% With integrity enabled, the first event in a fresh stream:
%%   - has prev_event_hash = genesis (32 zero bytes)
%%   - has mac populated as {KeyId, MacBytes}
%%   - the MAC verifies under the loaded key
%%   - the chain_start watermark is set to 0
enabled_store_writes_integrity_fields(Config) ->
    {StoreId, Key} = setup_integrity_store(Config),

    StreamId = reckon_db_test_helpers:sid(<<"stream-enabled-0">>),
    {ok, 0} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"x_happened">>, data => #{n => 1}}]),

    {ok, [Event]} = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),

    %% prev_event_hash should equal genesis for the first event.
    Genesis = reckon_gater_integrity:genesis_prev_hash(),
    ?assertEqual(Genesis, Event#event.prev_event_hash),

    %% MAC populated as {1, <<32 bytes>>}.
    ?assertMatch({1, _}, Event#event.mac),
    {1, MacBytes} = Event#event.mac,
    ?assertEqual(32, byte_size(MacBytes)),

    %% Verifier accepts the event.
    ?assertEqual(ok,
        reckon_gater_integrity:verify_event(Event, Genesis, Key)),

    %% Watermark recorded.
    {ok, 0} = reckon_db_chain_watermark:lookup(StoreId, StreamId),
    ok.

%% Across N appends, each event's prev_event_hash must equal the
%% chain hash of its predecessor. The verifier walks the whole chain.
chain_continues_across_appends(Config) ->
    {StoreId, Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"stream-chain-test">>),

    %% Append 5 events in two batches (2 then 3) to exercise both
    %% intra-batch chaining and across-batch chaining.
    {ok, 1} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [
            #{event_type => <<"e1">>, data => #{n => 1}},
            #{event_type => <<"e2">>, data => #{n => 2}}
        ]),
    {ok, 4} = reckon_db_streams:append(
        StoreId, StreamId, 1,
        [
            #{event_type => <<"e3">>, data => #{n => 3}},
            #{event_type => <<"e4">>, data => #{n => 4}},
            #{event_type => <<"e5">>, data => #{n => 5}}
        ]),

    %% Read all back and verify the chain end-to-end.
    {ok, Events} = reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),
    ?assertEqual(5, length(Events)),

    Genesis = reckon_gater_integrity:genesis_prev_hash(),
    walk_chain_and_verify(Events, Genesis, Key),
    ok.

%% First append on each stream creates the watermark. The watermark
%% value equals the version of the first integrity-bearing event,
%% which on a fresh stream is 0.
watermark_is_recorded_on_first_append(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),

    StreamA = reckon_db_test_helpers:sid(<<"stream-A">>),
    StreamB = reckon_db_test_helpers:sid(<<"stream-B">>),

    %% Before any append, watermarks are absent.
    ?assertEqual({ok, undefined},
        reckon_db_chain_watermark:lookup(StoreId, StreamA)),
    ?assertEqual({ok, undefined},
        reckon_db_chain_watermark:lookup(StoreId, StreamB)),

    {ok, 0} = reckon_db_streams:append(
        StoreId, StreamA, ?NO_STREAM,
        [#{event_type => <<"e">>, data => #{}}]),

    ?assertEqual({ok, 0},
        reckon_db_chain_watermark:lookup(StoreId, StreamA)),
    %% B still has no watermark.
    ?assertEqual({ok, undefined},
        reckon_db_chain_watermark:lookup(StoreId, StreamB)),

    %% Subsequent appends on A do NOT change the watermark.
    {ok, 1} = reckon_db_streams:append(
        StoreId, StreamA, 0,
        [#{event_type => <<"e">>, data => #{}}]),
    ?assertEqual({ok, 0},
        reckon_db_chain_watermark:lookup(StoreId, StreamA)),
    ok.

%% Two stores with different keys, same event payload, produce
%% different MACs. Validates that the loaded key is actually used
%% rather than a hard-coded constant.
different_keys_produce_different_macs(Config) ->
    StoreId = proplists:get_value(store_id, Config),

    Key1 = crypto:strong_rand_bytes(32),
    Key2 = crypto:strong_rand_bytes(32),

    StreamId = reckon_db_test_helpers:sid(<<"stream-mac-vs">>),
    EventPayload = #{event_type => <<"e">>, data => #{value => 42}},

    %% Append with Key1
    load_key_directly(StoreId, Key1),
    {ok, 0} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, [EventPayload]),
    {ok, [#event{mac = {_, Mac1}}]} =
        reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),

    %% Reload with Key2 and append to a different stream so we can compare.
    Stream2 = reckon_db_test_helpers:sid(<<"stream-mac-vs-2">>),
    load_key_directly(StoreId, Key2),
    {ok, 0} = reckon_db_streams:append(StoreId, Stream2, ?NO_STREAM, [EventPayload]),
    {ok, [#event{mac = {_, Mac2}}]} =
        reckon_db_streams:read(StoreId, Stream2, 0, 10, forward),

    ?assertNotEqual(Mac1, Mac2),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

setup_integrity_store(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    Key = crypto:strong_rand_bytes(32),
    load_key_directly(StoreId, Key),
    {StoreId, Key}.

%% Bypass the env-var/sealed-file path and install a key directly in
%% persistent_term. Equivalent to what reckon_db_integrity_key:load/1
%% does after a successful load, but skips the file/env plumbing for
%% test speed.
load_key_directly(StoreId, Key) when is_binary(Key), byte_size(Key) =:= 32 ->
    persistent_term:put({reckon_db, integrity_key, StoreId}, Key),
    persistent_term:put({reckon_db, integrity_enabled, StoreId}, true),
    ok.

walk_chain_and_verify([], _PrevTip, _Key) ->
    ok;
walk_chain_and_verify([Event | Rest], PrevTip, Key) ->
    ?assertEqual(ok,
        reckon_gater_integrity:verify_event(Event, PrevTip, Key)),
    NextTip = reckon_gater_integrity:compute_chain_hash(Event, PrevTip),
    walk_chain_and_verify(Rest, NextTip, Key).
