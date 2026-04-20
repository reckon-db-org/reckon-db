#!/usr/bin/env escript
%%! -noshell -config config/sys.config

main(_) ->
    %% Add all build paths explicitly
    lists:foreach(fun code:add_pathz/1,
                  filelib:wildcard("_build/bench/checkouts/*/ebin")),
    lists:foreach(fun code:add_pathz/1,
                  filelib:wildcard("_build/default/lib/*/ebin")),
    code:add_pathz("_build/bench/extras/slices"),

    io:format("~n== smoke: starting apps~n"),
    case application:ensure_all_started(reckon_db) of
        {ok, _} -> io:format("reckon_db started~n");
        {error, E} -> io:format("reckon_db failed: ~p~n", [E]), halt(1)
    end,
    timer:sleep(500),

    Scenario = #{store_id => bench_store,
                 event_size_bytes => 256,
                 parallelism => 1,
                 duration_seconds => 3,
                 tags => [baseline]},

    io:format("== setup~n"),
    S  = append_single_stream:setup(Scenario),
    io:format("setup state keys: ~p~n", [maps:keys(S)]),

    io:format("== 3 runs~n"),
    {ok, S1} = append_single_stream:run(S,  Scenario),
    {ok, S2} = append_single_stream:run(S1, Scenario),
    {ok, S3} = append_single_stream:run(S2, Scenario),
    io:format("next_seq after 3 appends: ~p~n", [maps:get(next_seq, S3)]),

    io:format("== teardown~n"),
    ok = append_single_stream:teardown(S, Scenario),
    io:format("SMOKE OK~n"),
    halt(0).
