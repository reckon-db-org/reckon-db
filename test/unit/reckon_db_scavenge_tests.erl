%% @doc Unit tests for scavenge and archive modules
%% @author Macula.io

-module(reckon_db_scavenge_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../../include/reckon_db.hrl").

%%====================================================================
%% Archive Backend Tests
%%====================================================================

archive_key_test_() ->
    [
        {"make_key generates correct format",
         ?_assertEqual(
             <<"my_store/orders-123/0-99.archive">>,
             reckon_db_archive_backend:make_key(my_store, <<"orders-123">>, 0, 99)
         )},

        {"make_key with large versions",
         ?_assertEqual(
             <<"store/stream/1000-2000.archive">>,
             reckon_db_archive_backend:make_key(store, <<"stream">>, 1000, 2000)
         )}
    ].

parse_key_test_() ->
    [
        {"parse_key extracts metadata correctly",
         ?_assertEqual(
             {ok, #{
                 store_id => my_store,
                 stream_id => <<"orders-123">>,
                 from_version => 0,
                 to_version => 99
             }},
             reckon_db_archive_backend:parse_key(<<"my_store/orders-123/0-99.archive">>)
         )},

        {"parse_key with large versions",
         ?_assertEqual(
             {ok, #{
                 store_id => store,
                 stream_id => <<"stream">>,
                 from_version => 1000,
                 to_version => 2000
             }},
             reckon_db_archive_backend:parse_key(<<"store/stream/1000-2000.archive">>)
         )},

        {"parse_key returns error for invalid key",
         ?_assertEqual(
             {error, invalid_key},
             reckon_db_archive_backend:parse_key(<<"invalid">>)
         )},

        {"parse_key roundtrip",
         fun() ->
             Key = reckon_db_archive_backend:make_key(test_store, <<"my-stream">>, 50, 150),
             {ok, Parsed} = reckon_db_archive_backend:parse_key(Key),
             ?assertEqual(test_store, maps:get(store_id, Parsed)),
             ?assertEqual(<<"my-stream">>, maps:get(stream_id, Parsed)),
             ?assertEqual(50, maps:get(from_version, Parsed)),
             ?assertEqual(150, maps:get(to_version, Parsed))
         end}
    ].

%%====================================================================
%% Wildcard Pattern Tests
%%====================================================================

wildcard_pattern_test_() ->
    [
        {"exact match",
         ?_assertEqual(
             [<<"orders-123">>],
             filter_by_pattern([<<"orders-123">>, <<"users-1">>], <<"orders-123">>)
         )},

        {"wildcard suffix",
         ?_assertEqual(
             [<<"orders-123">>, <<"orders-456">>],
             filter_by_pattern([<<"orders-123">>, <<"orders-456">>, <<"users-1">>], <<"orders-*">>)
         )},

        {"wildcard prefix",
         ?_assertEqual(
             [<<"old-orders">>, <<"new-orders">>],
             filter_by_pattern([<<"old-orders">>, <<"new-orders">>, <<"users">>], <<"*-orders">>)
         )},

        {"wildcard middle",
         ?_assertEqual(
             [<<"orders-123-archive">>, <<"orders-456-archive">>],
             filter_by_pattern([<<"orders-123-archive">>, <<"orders-456-archive">>, <<"users">>], <<"orders-*-archive">>)
         )},

        {"no matches",
         ?_assertEqual(
             [],
             filter_by_pattern([<<"users-1">>, <<"users-2">>], <<"orders-*">>)
         )}
    ].

%%====================================================================
%% Helper functions (extracted for testing)
%%====================================================================

filter_by_pattern(Streams, Pattern) ->
    RegexPattern = wildcard_to_regex(Pattern),
    [S || S <- Streams, re:run(S, RegexPattern) =/= nomatch].

wildcard_to_regex(Pattern) ->
    Escaped = re:replace(Pattern, <<"[.^$+?{}\\[\\]\\\\|()]">>, <<"\\\\&">>, [global, {return, binary}]),
    Converted = binary:replace(Escaped, <<"*">>, <<".*">>, [global]),
    <<"^", Converted/binary, "$">>.

%%====================================================================
%% Mock event helpers
%%====================================================================

mock_event(Version, EpochUs) ->
    #event{
        event_id = iolist_to_binary(io_lib:format("event-~p", [Version])),
        event_type = <<"TestEvent">>,
        stream_id = <<"test-stream">>,
        version = Version,
        data = #{version => Version},
        metadata = #{},
        timestamp = EpochUs div 1000,
        epoch_us = EpochUs
    }.

%%====================================================================
%% File Archive Backend Tests (require file system)
%%====================================================================

file_archive_test_() ->
    {setup,
     fun() ->
         %% Create a temporary directory
         TempDir = "/tmp/reckon_db_test_archive_" ++ integer_to_list(erlang:unique_integer([positive])),
         file:make_dir(TempDir),
         TempDir
     end,
     fun(TempDir) ->
         %% Cleanup
         os:cmd("rm -rf " ++ TempDir)
     end,
     fun(TempDir) ->
         [
             {"init creates directory",
              fun() ->
                  {ok, State} = reckon_db_archive_file:init(#{base_dir => TempDir}),
                  ?assert(is_tuple(State))
              end},

             {"archive and read roundtrip",
              fun() ->
                  {ok, State} = reckon_db_archive_file:init(#{base_dir => TempDir}),
                  Events = [mock_event(0, 1000000), mock_event(1, 2000000)],
                  Key = <<"test_store/test_stream/0-1.archive">>,

                  {ok, State2} = reckon_db_archive_file:archive(State, Key, Events),
                  {ok, ReadEvents, _State3} = reckon_db_archive_file:read(State2, Key),

                  ?assertEqual(2, length(ReadEvents)),
                  ?assertEqual(0, (hd(ReadEvents))#event.version)
              end},

             {"exists check",
              fun() ->
                  {ok, State} = reckon_db_archive_file:init(#{base_dir => TempDir}),
                  Events = [mock_event(0, 1000000)],
                  Key = <<"test_store/exist_stream/0-0.archive">>,

                  {false, State2} = reckon_db_archive_file:exists(State, Key),
                  {ok, State3} = reckon_db_archive_file:archive(State2, Key, Events),
                  {true, _State4} = reckon_db_archive_file:exists(State3, Key)
              end},

             {"delete removes archive",
              fun() ->
                  {ok, State} = reckon_db_archive_file:init(#{base_dir => TempDir}),
                  Events = [mock_event(0, 1000000)],
                  Key = <<"test_store/delete_stream/0-0.archive">>,

                  {ok, State2} = reckon_db_archive_file:archive(State, Key, Events),
                  {true, State3} = reckon_db_archive_file:exists(State2, Key),
                  {ok, State4} = reckon_db_archive_file:delete(State3, Key),
                  {false, _State5} = reckon_db_archive_file:exists(State4, Key)
              end}
         ]
     end}.

