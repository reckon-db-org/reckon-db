%% @doc EUnit tests for reckon_db_resource_monitor (CPU + disk sampling).
%% Tolerant of os_mon availability in CI: shape is always asserted; concrete
%% CPU/disk values and telemetry are only asserted when os_mon actually samples.
-module(reckon_db_resource_monitor_tests).
-include_lib("eunit/include/eunit.hrl").
-include("reckon_db_telemetry.hrl").

setup() ->
    application:ensure_all_started(telemetry),
    {ok, Pid} = reckon_db_resource_monitor:start_link(
                  #{interval => 3600000, data_dir => "/tmp"}),
    %% Detach from the (transient) fixture process so the monitor survives
    %% until cleanup runs the deferred tests.
    unlink(Pid),
    Pid.

cleanup(Pid) ->
    case is_process_alive(Pid) of
        true  -> gen_server:stop(Pid);
        false -> ok
    end.

resource_monitor_test_() ->
    {setup, fun setup/0, fun cleanup/1,
     fun(_Pid) ->
        [ {"snapshot shape",       fun shape_t/0}
        , {"data-dir mount flag",  fun disk_flag_t/0}
        , {"telemetry emitted",    fun telemetry_t/0} ]
     end}.

%% get_stats/sample_now always return a well-formed snapshot, os_mon or not.
shape_t() ->
    S = reckon_db_resource_monitor:sample_now(),
    ?assert(is_map(S)),
    ?assert(is_boolean(maps:get(os_mon, S))),
    ?assert(is_list(maps:get(disk, S))),
    Cpu = maps:get(cpu, S),
    ?assert(Cpu =:= undefined orelse is_map(Cpu)),
    %% get_stats after a sample returns that exact snapshot (no re-sample).
    ?assertEqual(S, reckon_db_resource_monitor:get_stats()).

%% When disk data is present, at most one mount (the longest prefix of the data
%% dir "/tmp") is flagged data_dir_mount, and every entry is well-formed.
disk_flag_t() ->
    #{disk := Disk} = reckon_db_resource_monitor:sample_now(),
    case Disk of
        [] -> ok;  %% os_mon/disksup unavailable — nothing to check
        _  ->
            Flagged = [E || #{data_dir_mount := true} = E <- Disk],
            ?assert(length(Flagged) =< 1),
            ?assert(lists:all(fun well_formed_disk/1, Disk))
    end.

well_formed_disk(#{mount := M, total_kb := T, used_percent := U,
                   available_kb := A, data_dir_mount := DDM}) ->
    is_binary(M) andalso is_integer(T) andalso is_number(U)
        andalso is_integer(A) andalso is_boolean(DDM);
well_formed_disk(_) -> false.

%% If os_mon is sampling (CPU busy is a real number), a CPU sample must have
%% been emitted.
telemetry_t() ->
    Ref = make_ref(),
    Self = self(),
    Handler = fun(Event, Meas, Meta, _) -> Self ! {Ref, Event, Meas, Meta} end,
    telemetry:attach_many(Ref, [?CPU_SAMPLE, ?DISK_SAMPLE], Handler, undefined),
    #{cpu := Cpu} = reckon_db_resource_monitor:sample_now(),
    Events = drain(Ref, []),
    telemetry:detach(Ref),
    CpuLive = is_map(Cpu) andalso is_number(maps:get(busy_percent, Cpu, undefined)),
    case CpuLive of
        true  -> ?assert(lists:any(fun({E, _, _}) -> E =:= ?CPU_SAMPLE end, Events));
        false -> ok  %% no live CPU sampling here — nothing emitted, fine
    end.

drain(Ref, Acc) ->
    receive {Ref, E, M, Md} -> drain(Ref, [{E, M, Md} | Acc])
    after 200 -> lists:reverse(Acc) end.
