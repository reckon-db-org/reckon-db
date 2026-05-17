-module(reckon_db_cluster_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Module shape
%%====================================================================

module_exports_test() ->
    Exports = reckon_db_cluster:module_info(exports),
    ?assert(lists:member({health_check, 1}, Exports)),
    ?assert(lists:member({verify_consistency, 1}, Exports)),
    ?assert(lists:member({verify_membership, 1}, Exports)),
    ?assert(lists:member({check_log_consistency, 1}, Exports)).

%%====================================================================
%% Behaviour on a non-existent store
%%
%% The facade delegates to ra/khepri/consistency_checker for state.
%% Against a store that was never started, each function must surface
%% an `{error, _}` rather than crashing — that's what
%% `reckon_db_gateway_worker' relies on to translate into a sane gRPC
%% response.
%%====================================================================

unknown_store_returns_error_test_() ->
    NoSuchStore = list_to_atom("nonexistent_store_" ++ integer_to_list(erlang:unique_integer([positive]))),
    [
        {"health_check/1 errors out cleanly",
         ?_assertMatch({error, _}, reckon_db_cluster:health_check(NoSuchStore))},
        {"verify_membership/1 errors out cleanly",
         ?_assertMatch({error, _}, reckon_db_cluster:verify_membership(NoSuchStore))},
        {"check_log_consistency/1 errors out cleanly",
         ?_assertMatch({error, _}, reckon_db_cluster:check_log_consistency(NoSuchStore))}
    ].
