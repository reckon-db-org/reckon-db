-module(reckon_db_stream_id_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Accept — user format <prefix>-<hex>
%%====================================================================

valid_user_id_test_() ->
    [
        ?_assertEqual(ok, reckon_db_stream_id:validate(<<"a-0">>)),
        ?_assertEqual(ok, reckon_db_stream_id:validate(<<"account-018f6a7b8c9d4abc8901234567890abc">>)),
        ?_assertEqual(ok, reckon_db_stream_id:validate(<<"Order-DEADBEEF">>)),
        ?_assertEqual(ok, reckon_db_stream_id:validate(<<"order-0123456789abcdef">>)),
        %% Decimal-only tails are valid (every decimal digit is a hex digit).
        ?_assertEqual(ok, reckon_db_stream_id:validate(<<"demo-1779045695829417190">>))
    ].

%%====================================================================
%% Accept — system format $<ns>:<name>
%%====================================================================

valid_system_id_test_() ->
    [
        ?_assertEqual(ok, reckon_db_stream_id:validate(<<"$link:high-value-orders">>)),
        ?_assertEqual(ok, reckon_db_stream_id:validate(<<"$link:foo">>)),
        ?_assertEqual(ok, reckon_db_stream_id:validate(<<"$link-sub:revenue">>)),
        ?_assertEqual(ok, reckon_db_stream_id:validate(<<"$ce:account">>)),
        ?_assertEqual(ok, reckon_db_stream_id:validate(<<"$et:UserCreated">>)),
        %% Underscores + dots in <name> are allowed
        ?_assertEqual(ok, reckon_db_stream_id:validate(<<"$stats:host_01.example">>))
    ].

%%====================================================================
%% Reject
%%====================================================================

empty_test() ->
    ?assertEqual({error, empty}, reckon_db_stream_id:validate(<<>>)).

non_binary_test_() ->
    [
        ?_assertEqual({error, not_binary}, reckon_db_stream_id:validate("a-string")),
        ?_assertEqual({error, not_binary}, reckon_db_stream_id:validate(undefined)),
        ?_assertEqual({error, not_binary}, reckon_db_stream_id:validate(42))
    ].

malformed_user_id_test_() ->
    [
        %% mid-string $ (the pollution we're guarding against)
        ?_assertEqual({error, malformed_user_id},
            reckon_db_stream_id:validate(<<"test$basic-stream">>)),
        ?_assertEqual({error, malformed_user_id},
            reckon_db_stream_id:validate(<<"partition$ABC">>)),
        ?_assertEqual({error, malformed_user_id},
            reckon_db_stream_id:validate(<<"subfailover$XYZ">>)),
        %% no separator
        ?_assertEqual({error, malformed_user_id},
            reckon_db_stream_id:validate(<<"myStream">>)),
        %% non-hex tail
        ?_assertEqual({error, malformed_user_id},
            reckon_db_stream_id:validate(<<"account-xyz">>)),
        ?_assertEqual({error, malformed_user_id},
            reckon_db_stream_id:validate(<<"account-018g">>)),
        %% empty hex
        ?_assertEqual({error, malformed_user_id},
            reckon_db_stream_id:validate(<<"account-">>)),
        %% empty prefix
        ?_assertEqual({error, malformed_user_id},
            reckon_db_stream_id:validate(<<"-deadbeef">>)),
        %% prefix contains digit
        ?_assertEqual({error, malformed_user_id},
            reckon_db_stream_id:validate(<<"acc0unt-deadbeef">>)),
        %% double hyphen
        ?_assertEqual({error, malformed_user_id},
            reckon_db_stream_id:validate(<<"snapdemo-stream-1779045">>))
    ].

malformed_system_id_test_() ->
    [
        %% no : separator
        ?_assertEqual({error, malformed_system_id},
            reckon_db_stream_id:validate(<<"$weird">>)),
        %% empty namespace
        ?_assertEqual({error, malformed_system_id},
            reckon_db_stream_id:validate(<<"$:foo">>)),
        %% empty name
        ?_assertEqual({error, malformed_system_id},
            reckon_db_stream_id:validate(<<"$link:">>)),
        %% uppercase namespace (reserved for lower-case)
        ?_assertEqual({error, malformed_system_id},
            reckon_db_stream_id:validate(<<"$Link:foo">>)),
        %% $all is a SELECTOR sentinel, not a stream id
        ?_assertEqual({error, malformed_system_id},
            reckon_db_stream_id:validate(<<"$all">>))
    ].

%%====================================================================
%% Helpers
%%====================================================================

is_valid_helper_test_() ->
    [
        ?_assert(reckon_db_stream_id:is_valid(<<"account-abc">>)),
        ?_assert(reckon_db_stream_id:is_valid(<<"$link:x">>)),
        ?_assertNot(reckon_db_stream_id:is_valid(<<"test$broken">>)),
        ?_assertNot(reckon_db_stream_id:is_valid(<<>>))
    ].

is_system_helper_test_() ->
    [
        ?_assert(reckon_db_stream_id:is_system(<<"$link:x">>)),
        ?_assertNot(reckon_db_stream_id:is_system(<<"account-abc">>)),
        ?_assertNot(reckon_db_stream_id:is_system(<<"$all">>)),
        ?_assertNot(reckon_db_stream_id:is_system(<<"$weird">>))
    ].
