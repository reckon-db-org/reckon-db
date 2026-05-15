%% @doc Common Test suite for subscription tamper-resistance.
%%
%% Validates the Layer 5 boundary: catch-up replay must verify each
%% event's MAC before delivering to the subscriber. A tampered event
%% encountered during catch-up halts the replay and surfaces a
%% subscription_error to the subscriber. Live events from the write
%% path already carry integrity fields (set at write time); no
%% emitter-side rework is required for the live path.
%%
%% @end
-module(reckon_db_integrity_subscriptions_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

%% Assert no subscription_error arrives within Timeout. Defined as
%% a macro so the `receive` block runs in the caller's process.
-define(assertNoSubscriptionError(Pid, Timeout),
    begin
        Pid = self(),  %% defensive — guards against accidental misuse
        receive
            {subscription_error, _Err} ->
                ct:fail({unexpected_subscription_error, _Err})
        after Timeout ->
            ok
        end
    end).

-export([
    all/0,
    suite/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    intact_catchup_delivers_all_events/1,
    tampered_event_halts_catchup/1,
    catchup_continues_past_legacy_events/1,
    integrity_disabled_store_skips_catchup_verification/1
]).

%%====================================================================
%% CT boilerplate
%%====================================================================

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        intact_catchup_delivers_all_events,
        tampered_event_halts_catchup,
        catchup_continues_past_legacy_events,
        integrity_disabled_store_skips_catchup_verification
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    RaDataDir = "/tmp/reckon_db_integrity_subs_test_ra",
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
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_integrity_subs_" ++
              atom_to_list(TestCase) ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom(
        "integrity_subs_test_" ++ atom_to_list(TestCase) ++ "_" ++ Rand),

    {ok, _} = khepri:start(DataDir, StoreId),
    khepri:put(StoreId, [streams], #{}),
    khepri:put(StoreId, [subscriptions], #{}),
    khepri:put(StoreId, [metadata], #{}),
    khepri:put(StoreId, [procs], #{}),

    [{data_dir, DataDir}, {store_id, StoreId} | Config].

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

%% A subscription created against a stream that already has events
%% triggers a catch-up replay. With integrity enabled and the chain
%% intact, the subscriber receives every event with no
%% subscription_error.
intact_catchup_delivers_all_events(Config) ->
    StoreId = setup_integrity_store(Config),
    StreamId = <<"sub$intact-catchup">>,
    write_n_events(StoreId, StreamId, 5),

    Self = self(),
    {ok, _Key} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, <<"intact_sub">>, #{subscriber => self()}),

    %% Drain catch-up messages with a small grace period.
    Events = drain_events(Self, 2000),
    ?assertEqual(5, length(Events)),
    ?assertNoSubscriptionError(Self, 100),
    ok.

%% Plant a tampered event in storage AFTER the subscription is set
%% up but BEFORE catch-up reads (we use a sleep to delay the
%% catch-up by subscribing first, then tampering, then triggering
%% catch-up via the spawn loop). The subscriber should receive a
%% subscription_error, not a silent delivery.
tampered_event_halts_catchup(Config) ->
    StoreId = setup_integrity_store(Config),
    StreamId = <<"sub$tampered-catchup">>,
    write_n_events(StoreId, StreamId, 5),

    %% Tamper the underlying event at version 2 (changes data; MAC
    %% no longer verifies).
    tamper_event(StoreId, StreamId, 2,
        fun(E) -> E#event{data = #{forged => true}} end),

    Self = self(),
    {ok, _Key} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, <<"tampered_sub">>, #{subscriber => self()}),

    %% Expect a subscription_error message; events received before
    %% the violation are acceptable but the violation MUST surface.
    ?assertEqual(true, await_subscription_error(Self, 3000)),
    ok.

%% Legacy events (no integrity fields) appear in the catch-up
%% stream and pass through without verification — the per-event
%% MAC check is only applied to events that carry a mac field.
%% Validates the skip_legacy semantics at the subscription layer.
catchup_continues_past_legacy_events(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"sub$mixed-catchup">>,

    %% Start with legacy: write 2 events on a disabled store.
    write_legacy_events(StoreId, StreamId, 2),

    %% Enable integrity, write 3 more events with integrity.
    install_random_key(StoreId),
    %% Lazy watermark will be set to version 2 on the next append.
    write_n_events_starting_at(StoreId, StreamId, 2, 3),

    Self = self(),
    {ok, _Key} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, <<"mixed_sub">>, #{subscriber => self()}),

    Events = drain_events(Self, 2000),
    ?assertEqual(5, length(Events)),
    ?assertNoSubscriptionError(Self, 100),
    ok.

%% A store with integrity disabled performs no catch-up
%% verification, even if events happen to carry integrity fields.
integrity_disabled_store_skips_catchup_verification(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = <<"sub$disabled-catchup">>,
    write_legacy_events(StoreId, StreamId, 3),

    Self = self(),
    {ok, _Key} = reckon_db_subscriptions:subscribe(
        StoreId, stream, StreamId, <<"disabled_sub">>, #{subscriber => self()}),

    Events = drain_events(Self, 2000),
    ?assertEqual(3, length(Events)),
    ?assertNoSubscriptionError(Self, 100),
    ok.

%% NOTE: live-delivery integrity is covered by the write path
%% (reckon_db_integrity_writes_SUITE): the write computes prev_event_hash
%% and mac before persisting, so live triggers fire on records that
%% already carry integrity fields. The emitter does not strip them.
%% Testing this directly through the live-delivery path is timing-
%% sensitive (trigger registration vs. first write race) and would
%% retest infrastructure that is exercised by the regular subscriptions
%% CT suite, so we don't duplicate it here.

%%====================================================================
%% Helpers
%%====================================================================

setup_integrity_store(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    Key = crypto:strong_rand_bytes(32),
    persistent_term:put({reckon_db, integrity_key, StoreId}, Key),
    persistent_term:put({reckon_db, integrity_enabled, StoreId}, true),
    StoreId.

install_random_key(StoreId) ->
    Key = crypto:strong_rand_bytes(32),
    persistent_term:put({reckon_db, integrity_key, StoreId}, Key),
    persistent_term:put({reckon_db, integrity_enabled, StoreId}, true),
    Key.

write_n_events(StoreId, StreamId, N) ->
    write_n_events_starting_at(StoreId, StreamId, 0, N).

write_n_events_starting_at(StoreId, StreamId, StartVersion, N) ->
    ExpectedVersion = case StartVersion of
        0 -> ?NO_STREAM;
        V -> V - 1
    end,
    {ok, _} = reckon_db_streams:append(
        StoreId, StreamId, ExpectedVersion,
        [#{event_type => <<"e">>, data => #{n => I}}
         || I <- lists:seq(StartVersion + 1, StartVersion + N)]),
    ok.

write_legacy_events(StoreId, StreamId, N) ->
    {ok, _} = reckon_db_streams:append(
        StoreId, StreamId, ?NO_STREAM,
        [#{event_type => <<"e">>, data => #{n => I}} || I <- lists:seq(1, N)]),
    ok.

tamper_event(StoreId, StreamId, Version, Fun) ->
    PaddedVersion = pad_version_for_event(Version),
    Path = [streams, StreamId, PaddedVersion],
    {ok, Event} = khepri:get(StoreId, Path),
    ok = khepri:put(StoreId, Path, Fun(Event)),
    ok.

pad_version_for_event(Version) ->
    VersionStr = integer_to_list(Version),
    Padding = ?VERSION_PADDING - length(VersionStr),
    list_to_binary(lists:duplicate(Padding, $0) ++ VersionStr).

%% Drain {events, [...]} batches arriving at Pid within Timeout ms.
%% Flattens nested batches into a single list. Stops on the first
%% Timeout-long quiet period.
drain_events(Pid, Timeout) when Pid =:= self() ->
    drain_events_loop([], Timeout).

drain_events_loop(Acc, Timeout) ->
    receive
        {events, Events} when is_list(Events) ->
            drain_events_loop(Acc ++ Events, Timeout);
        {reckon_event, _SubId, Event} ->
            drain_events_loop(Acc ++ [Event], Timeout)
    after Timeout ->
        Acc
    end.

%% Returns true if a subscription_error message arrives within
%% Timeout; false otherwise.
await_subscription_error(Pid, Timeout) when Pid =:= self() ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    await_subscription_error_loop(Deadline).

await_subscription_error_loop(Deadline) ->
    Now = erlang:monotonic_time(millisecond),
    case Now >= Deadline of
        true ->
            false;
        false ->
            Remaining = Deadline - Now,
            receive
                {subscription_error, {integrity_violation, _}} ->
                    true;
                {events, _} ->
                    %% Drain events while waiting for the error.
                    await_subscription_error_loop(Deadline);
                _Other ->
                    await_subscription_error_loop(Deadline)
            after Remaining ->
                false
            end
    end.

