%% @doc Logged durations are in the unit the log says.
%%
%% Every `duration' reckon_db measures is erlang:monotonic_time/0 arithmetic,
%% i.e. NATIVE time units (nanoseconds on Linux), which is also what
%% telemetry:span/3 reports. reckon_db_telemetry's log handler printed it raw
%% with a `us' suffix, so every logged stream duration was 1000x too large: a
%% read logged as "34 s" had taken milliseconds (found by Saturnus,
%% 2026-09-23, after a day of chasing reads that were never slow).
-module(reckon_db_telemetry_units_tests).

-include_lib("eunit/include/eunit.hrl").
-include("reckon_db_telemetry.hrl").

-export([log/2]).

-define(MS50_NATIVE, erlang:convert_time_unit(50, millisecond, native)).

%% Every handler clause that prints a duration prints 50 ms as 50000us.
every_logged_duration_is_microseconds_test_() ->
    Meta = #{store_id => s, stream_id => <<"st">>, reason => r,
             subscription_id => <<"sub">>, version => 1},
    Events = [?STREAM_WRITE_STOP, ?STREAM_WRITE_ERROR, ?STREAM_READ_STOP,
              ?SUBSCRIPTION_EVENT_DELIVERED, ?SNAPSHOT_CREATED, ?SNAPSHOT_READ,
              ?EMITTER_BROADCAST],
    [{lists:flatten(io_lib:format("~p", [E])),
      fun() ->
          ?assertEqual(50000, logged_us(E, #{duration => ?MS50_NATIVE}, Meta))
      end} || E <- Events].

%% A real 50 ms sleep, measured the way reckon_db measures it, logs in the
%% 50 ms range: well above 40 ms, far below the 50 s the raw native value
%% would have printed as.
a_known_sleep_logs_the_right_order_of_magnitude_test() ->
    T0 = erlang:monotonic_time(),
    timer:sleep(50),
    D = erlang:monotonic_time() - T0,
    Us = logged_us(?STREAM_READ_STOP, #{duration => D},
                   #{store_id => s, stream_id => <<"st">>}),
    ?assert(Us >= 40000),
    ?assert(Us < 1000000).

%%====================================================================
%% Capture what the handler logs, and read the number before "us".
%%====================================================================

logged_us(Event, Measurements, Meta) ->
    ok = logger:add_handler(?MODULE, ?MODULE, #{level => all, config => #{pid => self()}}),
    Prev = logger:get_primary_config(),
    ok = logger:set_primary_config(level, all),
    try
        ok = reckon_db_telemetry:handle_event(Event, Measurements, Meta, #{}),
        Line = receive {logged, L} -> L after 1000 -> error(nothing_logged) end,
        {match, [N]} = re:run(Line, "duration=([0-9]+)us", [{capture, all_but_first, list}]),
        list_to_integer(N)
    after
        logger:set_primary_config(Prev),
        logger:remove_handler(?MODULE)
    end.

log(#{msg := {Format, Args}}, #{config := #{pid := Pid}}) when is_list(Format) ->
    Pid ! {logged, lists:flatten(io_lib:format(Format, Args))};
log(_Event, _Config) ->
    ok.
