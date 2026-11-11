%%% The Khepri under the store must carry rabbitmq/khepri#398 (reckon-db#32).
%%%
%%% `khepri_tree:does_path_match/4' hard-matched the lookup of the node a
%%% path condition is evaluated against. When that node is absent the lookup
%%% answers `{error, {khepri, node_not_found, _}}', the badmatch crashes the
%%% caller, and when the caller is `khepri_machine''s command-apply loop the
%%% aborted command is persisted: every restart replays the crash and the
%%% store is unrecoverable. Upstream merged the fix as #398 (41deb62,
%%% 2026-07-17); it first ships in Khepri 0.17.11 (also 0.18.4, 0.19.3).
%%%
%%% 183a566 had already closed the one way the public API could reach this (a
%%% deletion trigger now evaluates against the pre-delete tree), so #398 is
%%% defense in depth and the case below is the direct call upstream wrote for
%%% it — the only way left to exercise the path. On 0.17.2 it dies on the
%%% badmatch; from 0.17.11 it answers false.
-module(reckon_db_khepri_dependency_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("khepri/include/khepri.hrl").

%% The first release that carries #398. A floor, not the pin: the pin is in
%% rebar.config, and this case names what "fixed" means if someone moves it.
-define(FIRST_FIXED_RELEASE, {0, 17, 11}).

%% A path condition that must query a node absent from the tree cannot be
%% met, so the path does not match; it must not take the store down with it.
an_absent_node_makes_the_path_not_match_test() ->
    {ok, Tree, _, _} = khepri_tree:insert_or_update_node(
                         khepri_tree:new(), [foo],
                         khepri_payload:data(1), #{}, #{}),
    ?assertEqual(
       false,
       khepri_tree:does_path_match(
         [foo, bar], [foo, #if_child_list_length{count = 0}], Tree)).

%% A downgrade below the fixed release must fail here, by name, rather than in
%% a store that can no longer be restarted.
the_loaded_khepri_carries_the_crash_fix_test() ->
    _ = application:load(khepri),
    {ok, Vsn} = application:get_key(khepri, vsn),
    ?assert(version_number(Vsn) >= ?FIRST_FIXED_RELEASE).

version_number(String) ->
    list_to_tuple([list_to_integer(Part) || Part <- string:split(String, ".", all)]).
