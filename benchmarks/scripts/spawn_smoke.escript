#!/usr/bin/env escript
%%! -noshell -config config/sys.config

main(_) ->
    lists:foreach(fun code:add_pathz/1,
                  filelib:wildcard("_build/bench/checkouts/*/ebin")),
    lists:foreach(fun code:add_pathz/1,
                  filelib:wildcard("_build/default/lib/*/ebin")),

    application:ensure_all_started(reckon_db),
    timer:sleep(2000),

    io:format("main: append~n"),
    {ok, _} = reckon_db_streams:append(bench_store, <<"from_main">>, -2,
        [#{event_type => <<"t">>, data => #{}}]),
    io:format("main: OK~n"),

    io:format("spawning worker~n"),
    Parent = self(),
    StreamId = <<"bench.append_single_stream.123456">>,
    Payload  = binary:copy(<<$x>>, 256),
    spawn_link(fun() ->
        io:format("worker: append with dotted stream id + payload~n"),
        R1 = (catch reckon_db_streams:append(bench_store, StreamId, -2,
            [#{event_type => <<"bench.appended_v1">>,
               data => #{seq => 0, payload => Payload}}])),
        io:format("worker: result1 = ~p~n", [R1]),
        R2 = (catch reckon_db_streams:append(bench_store, StreamId, -2,
            [#{event_type => <<"bench.appended_v1">>,
               data => #{seq => 1, payload => Payload}}])),
        io:format("worker: result2 = ~p~n", [R2]),
        Parent ! done
    end),
    receive done -> ok after 10000 -> io:format("worker timeout~n") end,
    halt(0).
