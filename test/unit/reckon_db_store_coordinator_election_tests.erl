%%% @doc Unit tests for coordinator election.
%%%
%%% The election must run over the nodes that RUN the store, and pick the
%%% lowest node name as coordinator (everyone else joins it). The historical
%%% split-brain bug was electing over ALL connected nodes on a shared dist
%%% mesh of single-store nodes, so the globally-lowest node name (not running
%%% the store) got elected and joins failed forever.
-module(reckon_db_store_coordinator_election_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, reckon_db_store_coordinator).

%% Lowest of {self, runners} is self -> we are the coordinator.
self_is_lowest_elects_coordinator_test() ->
    ?assertEqual(coordinator, ?M:elect_coordinator('a@h', ['b@h', 'c@h'])).

%% A lower store-runner exists -> we join the lowest.
lower_runner_present_joins_lowest_test() ->
    ?assertEqual({join, 'a@h'}, ?M:elect_coordinator('b@h', ['a@h', 'c@h'])),
    ?assertEqual({join, 'a@h'}, ?M:elect_coordinator('c@h', ['a@h', 'b@h'])).

%% No store-running peers -> a store of one, we are the coordinator.
no_runners_elects_self_test() ->
    ?assertEqual(coordinator, ?M:elect_coordinator('m@h', [])).

%% Election ignores node order in the candidate list (deterministic by name).
order_independent_test() ->
    ?assertEqual({join, 'a@h'}, ?M:elect_coordinator('z@h', ['c@h', 'a@h', 'b@h'])),
    ?assertEqual({join, 'a@h'}, ?M:elect_coordinator('z@h', ['a@h', 'b@h', 'c@h'])).

%% Self appearing in the runner list (belt-and-suspenders) must not make us
%% "join ourselves" — it is deduped, and self-as-lowest stays coordinator.
self_in_runners_is_deduped_test() ->
    ?assertEqual(coordinator, ?M:elect_coordinator('a@h', ['a@h', 'b@h'])),
    ?assertEqual({join, 'a@h'}, ?M:elect_coordinator('b@h', ['a@h', 'b@h'])).

%% The regression scenario: on a shared mesh, a lower node name that does NOT
%% run the store is simply absent from the candidate set, so a store-runner
%% is still (correctly) elected. Here 'antwerp' would have been the old
%% globally-lowest; excluded, the lowest leuven runner wins.
excludes_non_store_runner_test() ->
    LeuvenRunners = ['parksim_leuven@10', 'parksim_leuven@12'],
    ?assertEqual(coordinator,
                 ?M:elect_coordinator('parksim_leuven@10', LeuvenRunners)),
    ?assertEqual({join, 'parksim_leuven@10'},
                 ?M:elect_coordinator('parksim_leuven@11', LeuvenRunners)).

%% Persisted-membership guard: a restarting member whose configured set names
%% >1 member skips re-formation (relies on native Ra rejoin), preventing the
%% roll-time split. A fresh replica (empty/single-member config) does not.
persisted_multi_member_skips_reformation_test() ->
    Three = [{s, 'a@h'}, {s, 'b@h'}, {s, 'c@h'}],
    ?assert(?M:has_persisted_cluster(Three)),
    ?assert(?M:has_persisted_cluster([{s, 'a@h'}, {s, 'b@h'}])).

fresh_or_single_member_reforms_test() ->
    ?assertNot(?M:has_persisted_cluster([{s, 'a@h'}])),  %% single-member config
    ?assertNot(?M:has_persisted_cluster([])),            %% no config
    ?assertNot(?M:has_persisted_cluster(not_a_list)).    %% unreadable
