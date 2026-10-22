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
    different_keys_produce_different_macs/1,
    a_sealed_watermark_without_a_life_token_refuses_the_append/1,
    a_store_with_integrity_on_and_no_key_refuses_to_read/1,
    concurrent_first_appends_leave_every_stream_readable/1
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
        different_keys_produce_different_macs,
        a_sealed_watermark_without_a_life_token_refuses_the_append,
        a_store_with_integrity_on_and_no_key_refuses_to_read,
        concurrent_first_appends_leave_every_stream_readable
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

    %% MAC populated as the store- and life-bound scheme 2 shape.
    ?assertMatch({v2, 1, _}, Event#event.mac),
    {v2, 1, MacBytes} = Event#event.mac,
    ?assertEqual(32, byte_size(MacBytes)),

    %% Verifier accepts the event, with the store and life it was written in.
    {ok, #{life := Life, v2_from := V2From}} =
        reckon_db_chain_watermark:binding(StoreId, StreamId),
    ?assertEqual(ok,
        reckon_gater_integrity:verify_event(
          Event, Genesis, Key,
          #{store_id => StoreId, life => Life, v2_from => V2From})),

    %% Watermark recorded, in the authenticated shape.
    {ok, {sealed, 0}} = reckon_db_chain_watermark:lookup(StoreId, StreamId),
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
    walk_chain_and_verify(Events, Genesis, verify_ctx(StoreId, StreamId, Key)),
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

    ?assertEqual({ok, {sealed, 0}},
        reckon_db_chain_watermark:lookup(StoreId, StreamA)),
    %% B still has no watermark.
    ?assertEqual({ok, undefined},
        reckon_db_chain_watermark:lookup(StoreId, StreamB)),

    %% Subsequent appends on A do NOT change the watermark.
    {ok, 1} = reckon_db_streams:append(
        StoreId, StreamA, 0,
        [#{event_type => <<"e">>, data => #{}}]),
    ?assertEqual({ok, {sealed, 0}},
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
    {ok, [#event{mac = {v2, _, Mac1}}]} =
        reckon_db_streams:read(StoreId, StreamId, 0, 10, forward),

    %% Reload with Key2 and append to a different stream so we can compare.
    Stream2 = reckon_db_test_helpers:sid(<<"stream-mac-vs-2">>),
    load_key_directly(StoreId, Key2),
    {ok, 0} = reckon_db_streams:append(StoreId, Stream2, ?NO_STREAM, [EventPayload]),
    {ok, [#event{mac = {v2, _, Mac2}}]} =
        reckon_db_streams:read(StoreId, Stream2, 0, 10, forward),

    ?assertNotEqual(Mac1, Mac2),
    ok.

%% @doc GIVEN several writers opening the SAME brand-new stream at once, each
%%           with ANY_VERSION
%%      WHEN they all append
%%      THEN every stream is still readable afterwards
%%
%% Terra r15 probe_concurrent_first_append_life: minting the life token and the
%% seal as two plain puts outside the append transaction let two writers each
%% mint, and the loser's events were stamped under a life the winner then
%% overwrote - unreadable from version 0, permanently. A gateway pool of two is
%% exactly this shape (realms_store). The mint is one transaction now, and the
%% write transaction refuses a batch whose life has moved.
concurrent_first_appends_leave_every_stream_readable(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    Streams = [reckon_db_test_helpers:sid(<<"concurrent-life-",
                                          (integer_to_binary(I))/binary>>)
               || I <- lists:seq(1, 8)],
    Writers = 8,
    Self = self(),
    Pids = [spawn_monitor(fun() -> first_append_racer(Self, StoreId, Streams) end)
            || _ <- lists:seq(1, Writers)],
    Results = [receive {Pid, Result} -> Result after 30000 -> timeout end
               || {Pid, _Ref} <- Pids],
    ?assertEqual([], [R || R <- Results, R =/= ok]),
    lists:foreach(fun(StreamId) -> assert_stream_readable(StoreId, StreamId) end,
                  Streams).

%% @private
first_append_racer(Parent, StoreId, Streams) ->
    Results = [reckon_db_streams:append(
                 StoreId, StreamId, ?ANY_VERSION,
                 [#{event_type => <<"e">>, data => #{n => 1}}])
               || StreamId <- Streams],
    Parent ! {self(), case [E || {error, _} = E <- Results] of
                          [] -> ok;
                          Errors -> {errors, Errors}
                      end}.

%% @private
assert_stream_readable(StoreId, StreamId) ->
    {ok, Events} = reckon_db_streams:read_all(StoreId, StreamId, 100, forward),
    ?assert(length(Events) >= 1),
    ?assertMatch({ok, _}, reckon_db_streams:read(StoreId, StreamId, 0, 100,
                                                 forward)).

%%====================================================================
%% Helpers
%%====================================================================

%% A sealed watermark with no life token is a broken stream state: the
%% stream is defined as scheme-2 from its chain start, so there is no
%% scheme-1 stamp this release may write. The append is refused with a
%% reason, never stamped store-blind.
a_sealed_watermark_without_a_life_token_refuses_the_append(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"stream-lifeless">>),
    {ok, 0} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"x_happened">>, data => #{n => 1}}]),
    ok = khepri:delete(StoreId,
                       reckon_db_chain_watermark:life_path(StreamId)),
    ?assertEqual({error, {life_missing, StreamId}},
                 reckon_db_streams:append(
                   StoreId, StreamId, 0,
                   [#{event_type => <<"x_happened">>, data => #{n => 2}}])),
    ok.

%% Integrity on with no key is a configuration fault, not tampering. The
%% read refuses rather than passing a non-binary key to gater, which has no
%% clause for it, and rather than serving unverified bytes as ok.
a_store_with_integrity_on_and_no_key_refuses_to_read(Config) ->
    {StoreId, _Key} = setup_integrity_store(Config),
    StreamId = reckon_db_test_helpers:sid(<<"stream-keyless">>),
    {ok, 0} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"x_happened">>, data => #{n => 1}}]),
    %% Drop the key but leave integrity enabled, which clear/1 does not do.
    true = persistent_term:erase({reckon_db, integrity_key, StoreId}),
    ?assertMatch({error, {integrity_violation,
                          #{kind := missing_integrity,
                            context := #{detail := integrity_key_absent}}}},
                 reckon_db_streams:read(StoreId, StreamId, 0, 10, forward)),
    ok.

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

walk_chain_and_verify([], _PrevTip, _Ctx) ->
    ok;
walk_chain_and_verify([Event | Rest], PrevTip, Ctx) ->
    ?assertEqual(ok,
        reckon_gater_integrity:verify_event(Event, PrevTip, maps:get(key, Ctx),
                                            Ctx)),
    NextTip = reckon_gater_integrity:compute_chain_hash(Event, PrevTip),
    walk_chain_and_verify(Rest, NextTip, Ctx).

%% @private The context a scheme-2 verifier needs for this stream.
verify_ctx(StoreId, StreamId, Key) ->
    {ok, #{life := Life, v2_from := V2From}} =
        reckon_db_chain_watermark:binding(StoreId, StreamId),
    #{key => Key, store_id => StoreId, life => Life, v2_from => V2From}.
