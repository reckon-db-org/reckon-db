%%% @doc Unit tests for the store healer's pure decision logic.
%%%
%%% The healer's destructive path is gated by three pure functions:
%%%   - majority_view/1  : pick the authoritative majority among peer views
%%%   - classify/2       : self-health + majority view -> verdict
%%%   - safe_to_reset/1  : the data-safety gate (never reset the majority)
%%% These are exercised here without a live cluster.
-module(reckon_db_store_healer_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, reckon_db_store_healer).

%%====================================================================
%% safe_to_reset/1 — the data-safety gate truth table
%%====================================================================

%% The ONLY safe case: a real majority (leader-having) cluster exists, we are
%% not its leader, and we are not one of its members (the never-joined orphan).
safe_when_orphaned_and_majority_present_test() ->
    ?assert(?M:safe_to_reset(#{majority_present => true,
                               self_is_leader => false,
                               self_in_majority => false})).

%% No majority to rejoin -> never reset (would destroy the only copy).
unsafe_when_no_majority_test() ->
    ?assertNot(?M:safe_to_reset(#{majority_present => false,
                                  self_is_leader => false,
                                  self_in_majority => false})).

%% We are the leader -> never reset the authoritative side.
unsafe_when_self_is_leader_test() ->
    ?assertNot(?M:safe_to_reset(#{majority_present => true,
                                  self_is_leader => true,
                                  self_in_majority => true})).

%% We are already a member of the majority -> nothing to heal, never reset.
unsafe_when_self_in_majority_test() ->
    ?assertNot(?M:safe_to_reset(#{majority_present => true,
                                  self_is_leader => false,
                                  self_in_majority => true})).

%%====================================================================
%% classify/2 — verdict from self-health + majority view
%%====================================================================

classify_healthy_is_healthy_test() ->
    ?assertEqual(healthy, ?M:classify(healthy, #{})).

%% Unhealthy locally AND a majority exists without us -> orphaned (actionable).
classify_orphaned_test() ->
    Facts = #{majority_present => true, self_in_majority => false},
    ?assertEqual(orphaned, ?M:classify(no_quorum, Facts)),
    ?assertEqual(orphaned, ?M:classify(degraded, Facts)),
    ?assertEqual(orphaned, ?M:classify(unreachable, Facts)).

%% Unhealthy but no safe majority (genuine quorum loss) -> drift (alarm only).
classify_drift_when_no_majority_test() ->
    ?assertEqual(drift, ?M:classify(no_quorum,
                                    #{majority_present => false,
                                      self_in_majority => false})).

%% Unhealthy but we ARE in the majority set -> drift, not a reset candidate.
classify_drift_when_self_in_majority_test() ->
    ?assertEqual(drift, ?M:classify(degraded,
                                    #{majority_present => true,
                                      self_in_majority => true})).

%%====================================================================
%% majority_view/1 — pick the authoritative view among peer responses
%%====================================================================

%% No peers -> no leader, no members.
majority_view_empty_test() ->
    ?assertEqual(#{leader => undefined, members => []},
                 ?M:majority_view([])).

%% Peers exist but none has an elected leader -> no majority.
majority_view_no_leader_test() ->
    ?assertEqual(#{leader => undefined, members => []},
                 ?M:majority_view([{'n1@h', undefined, []},
                                   {'n2@h', undefined, ['n1@h']}])).

%% Among leader-having views, pick the largest member set.
majority_view_picks_largest_test() ->
    Peers = [{'a@h', 'a@h', ['a@h', 'b@h']},
             {'c@h', 'a@h', ['a@h', 'b@h', 'c@h']}],
    ?assertEqual(#{leader => 'a@h', members => ['a@h', 'b@h', 'c@h']},
                 ?M:majority_view(Peers)).

%% Ignores entries with a leader but empty member list.
majority_view_ignores_empty_members_test() ->
    ?assertEqual(#{leader => 'a@h', members => ['a@h', 'b@h']},
                 ?M:majority_view([{'x@h', 'a@h', []},
                                   {'a@h', 'a@h', ['a@h', 'b@h']}])).

%%====================================================================
%% End-to-end of the pure pipeline: the exact ghent deploy-orphan case
%%====================================================================

%% beam03 booted into its own singleton; the majority (beam00+beam02) elected
%% a leader and never accepted beam03. classify -> orphaned, gate -> safe.
ghent_orphan_is_safely_healable_test() ->
    Peers = [{'beam00@h', 'beam00@h', ['beam00@h', 'beam02@h']},
             {'beam02@h', 'beam00@h', ['beam00@h', 'beam02@h']}],
    #{leader := Leader, members := Members} = ?M:majority_view(Peers),
    Self = 'beam03@h',
    Facts = #{majority_leader => Leader,
              majority_members => Members,
              majority_present => Leader =/= undefined andalso length(Members) >= 2,
              self_is_leader => Leader =:= Self,
              self_in_majority => lists:member(Self, Members)},
    ?assertEqual(orphaned, ?M:classify(no_quorum, Facts)),
    ?assert(?M:safe_to_reset(Facts)).
