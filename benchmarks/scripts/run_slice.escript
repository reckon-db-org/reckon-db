#!/usr/bin/env escript
%%! -noshell -config config/sys.config

%% Invoke the harness runner on one slice+scenario pair.
%%
%% Usage:
%%   ./scripts/run_slice.escript <slice> <scenario-file> <out-path> <profile>

main([SliceStr, ScenarioFile, OutPath, Profile]) ->
    io:format("[run_slice] starting~n"),
    lists:foreach(fun code:add_pathz/1,
                  filelib:wildcard("_build/bench/checkouts/*/ebin")),
    lists:foreach(fun code:add_pathz/1,
                  filelib:wildcard("_build/default/lib/*/ebin")),
    code:add_pathz("_build/bench/extras/slices"),
    io:format("[run_slice] paths added~n"),

    case application:ensure_all_started(reckon_db) of
        {ok, _}    -> io:format("[run_slice] reckon_db started~n");
        {error, E} -> io:format("[run_slice] reckon_db failed: ~p~n", [E]),
                      halt(1)
    end,
    {ok, _} = application:ensure_all_started(reckon_bench_harness),
    io:format("[run_slice] harness started~n"),

    ok = wait_for_store(bench_store, 30),
    io:format("[run_slice] store ready~n"),

    WarmStream = <<"bench_prewarm">>,
    _ = reckon_db_streams:append(bench_store, WarmStream, -2,
          [#{event_type => <<"bench.prewarm_v1">>, data => #{}}]),
    _ = reckon_db_streams:delete(bench_store, WarmStream),
    io:format("[run_slice] prewarm done~n"),

    SliceMod = list_to_atom(SliceStr),
    try
        reckon_bench_harness:run_slice(
          SliceMod,
          ScenarioFile,
          OutPath,
          list_to_binary(Profile))
    catch
        C:Err:St ->
            io:format("bench crashed: ~p:~p~n~p~n", [C, Err, St]),
            halt(2)
    end,
    halt(0);
main(_) ->
    io:format("usage: run_slice.escript <slice> <scenario-file> <out> <profile>~n"),
    halt(2).

wait_for_store(_StoreId, 0) ->
    error({store_never_ready, _StoreId});
wait_for_store(StoreId, Retries) ->
    Stores = reckon_db_sup:which_stores(),
    case lists:member(StoreId, Stores) of
        true ->
            %% Give khepri/ra another second to fully settle.
            timer:sleep(1000),
            ok;
        false ->
            timer:sleep(500),
            wait_for_store(StoreId, Retries - 1)
    end.
