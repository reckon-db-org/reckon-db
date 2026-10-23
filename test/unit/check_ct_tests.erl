%%% scripts/check_ct.sh against Common Test output (reckon-db#20).
%%%
%%% A suite ct cannot load is reported as `error_in_suite: FAILED', is left
%%% out of "All N tests passed", and `rebar3 ct' exits 0: ct_framework counts
%%% that case as failed only when it was force-failed, and the missing-module
%%% path never is. A hook sees the case but cannot change the count, so the
%%% gate reads ct's own report. The lines below are what `rebar3 ct' printed
%%% for a stale suite left in _build, colour codes included.
-module(check_ct_tests).
-include_lib("eunit/include/eunit.hrl").

-define(CLEAN,
        "%%% reckon_db_gater_e2e_SUITE: ...........................\n"
        "%%% reckon_db_mesh_e2e_SUITE: ..\n"
        "All 29 tests passed.\n").

-define(UNLOADABLE,
        "%%% reckon_db_mesh_e2e_SUITE: ..\n"
        "%%% reckon_db_mesh_e2e_SUITE ==> error_in_suite: \e[0;31mFAILED\e[0m\e[0m\n"
        "%%% reckon_db_mesh_e2e_SUITE ==> \e[0;31m'zz_broken_SUITE cannot be "
        "compiled or loaded'\e[0m\e[0m\n"
        "All 29 tests passed.\n").

a_clean_run_passes_test() ->
    ?assertMatch({0, _}, check_log(?CLEAN)).

a_suite_that_did_not_load_fails_the_gate_test() ->
    {Status, Out} = check_log(?UNLOADABLE),
    ?assertEqual(1, Status),
    ?assertNotEqual(nomatch, string:find(Out, "zz_broken_SUITE cannot be compiled or loaded")).

any_error_in_suite_fails_the_gate_test() ->
    Log = "%%% a_SUITE ==> error_in_suite: FAILED\n"
          "%%% a_SUITE ==> 'a_SUITE:all/0 is missing'\n"
          "All 3 tests passed.\n",
    ?assertMatch({1, _}, check_log(Log)).

a_log_that_is_missing_fails_the_gate_test() ->
    ?assertMatch({2, _}, run(["--log", "/nonexistent/ct.log"])).

%% A relative --log path is the caller's, not the repository root's.
a_relative_log_is_read_from_the_callers_directory_test() ->
    Name = "check_ct_relative_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".log",
    ok = file:write_file(filename:join(test_dir(), Name), ?UNLOADABLE),
    Result = run(["--log", Name], [{cd, test_dir()}]),
    ok = file:delete(filename:join(test_dir(), Name)),
    ?assertMatch({1, _}, Result).

%%% Helpers

check_log(Text) ->
    File = filename:join(test_dir(), "check_ct_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".log"),
    ok = file:write_file(File, Text),
    Result = run(["--log", File]),
    ok = file:delete(File),
    Result.

run(Args) ->
    run(Args, []).

run(Args, PortOpts) ->
    Port = open_port({spawn_executable, "/usr/bin/env"},
                     [{args, ["bash", script() | Args]},
                      exit_status, stderr_to_stdout, binary | PortOpts]),
    {Status, Out} = collect(Port, <<>>),
    {Status, binary_to_list(Out)}.

collect(Port, Acc) ->
    receive
        {Port, {data, Data}} -> collect(Port, <<Acc/binary, Data/binary>>);
        {Port, {exit_status, Status}} -> {Status, Acc}
    after 30000 -> error(timeout)
    end.

test_dir() ->
    filename:dirname(code:which(?MODULE)).

script() ->
    find_script(filename:dirname(?FILE)).

find_script("/") ->
    error(no_scripts_check_ct_sh_above_this_test);
find_script(Dir) ->
    Candidate = filename:join([Dir, "scripts", "check_ct.sh"]),
    case filelib:is_regular(Candidate) of
        true -> Candidate;
        false -> find_script(filename:dirname(Dir))
    end.
