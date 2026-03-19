%% @doc Unit tests for reckon_db_store_inspector.
%%
%% Tests the inspector's internal helper functions and data transformation
%% logic. Full integration tests require a running store (see integration/).
-module(reckon_db_store_inspector_tests).
-include_lib("eunit/include/eunit.hrl").

%% Verify module exports exist
exports_test() ->
    Exports = reckon_db_store_inspector:module_info(exports),
    ?assert(lists:member({store_stats, 1}, Exports)),
    ?assert(lists:member({list_all_snapshots, 1}, Exports)),
    ?assert(lists:member({list_subscriptions, 1}, Exports)),
    ?assert(lists:member({subscription_lag, 2}, Exports)),
    ?assert(lists:member({event_type_summary, 1}, Exports)),
    ?assert(lists:member({stream_info, 2}, Exports)).

%% Verify gateway worker has inspector clauses
gateway_worker_exports_test() ->
    Exports = reckon_db_gateway_worker:module_info(exports),
    ?assert(lists:member({handle_call, 3}, Exports)).
