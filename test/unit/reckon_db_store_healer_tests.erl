%%% @doc Unit tests for the store healer's pure decision logic.
%%%
%%% The healer's destructive path is gated by three pure functions:
%%%   - majority_view/1  : pick the authoritative majority among peer views
%%%   - classify/2       : self-health + majority view -> verdict
%%%   - safe_to_reset/1  : the data-safety gate (never reset the majority)
%%% These are exercised here without a live cluster.
%%%
%%% Orphan detection keys on `self_clustered_with_leader': is the majority's
%%% elected leader present in THIS node's OWN local member set? A node that
%%% reset into its own singleton has local members = [self] (leader absent);
%%% a node merely partitioned from a cluster it is still configured in keeps
%%% the full local set (leader present) and must NOT be reset (Ra reconciles
%%% it). This distinction is the crux the earlier drafts got wrong.
-module(reckon_db_store_healer_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, reckon_db_store_healer).

%%====================================================================
%% safe_to_reset/1 — the data-safety gate truth table
%%====================================================================

%% The ONLY safe case: a real majority (leader-having) cluster exists, we are
%% not its leader, and its leader is absent from our local cluster (we have
%% diverged into a separate cluster / singleton).
safe_when_diverged_and_majority_present_test() ->
    ?assert(?M:safe_to_reset(#{majority_present => true,
                               self_is_leader => false,
                               self_clustered_with_leader => false})).

%% No majority to rejoin -> never reset (would destroy the only copy).
unsafe_when_no_majority_test() ->
    ?assertNot(?M:safe_to_reset(#{majority_present => false,
                                  self_is_leader => false,
                                  self_clustered_with_leader => false})).

%% We are the majority's leader -> never reset the authoritative side.
unsafe_when_self_is_leader_test() ->
    ?assertNot(?M:safe_to_reset(#{majority_present => true,
                                  self_is_leader => true,
                                  self_clustered_with_leader => true})).

%% We are still locally clustered with the leader (transient partition of a
%% configured member) -> leave it to Ra, never reset.
unsafe_when_clustered_with_leader_test() ->
    ?assertNot(?M:safe_to_reset(#{majority_present => true,
                                  self_is_leader => false,
                                  self_clustered_with_leader => true})).

%%====================================================================
%% classify/2 — verdict from self-health + majority view
%%====================================================================

%% No majority signal + locally healthy -> healthy (nothing to do).
classify_healthy_is_healthy_test() ->
    ?assertEqual(healthy, ?M:classify(healthy,
                                      #{majority_present => false,
                                        self_is_leader => false,
                                        self_clustered_with_leader => false})).

%% A majority exists and we are NOT clustered with its leader -> orphaned,
%% whatever our local self-status.
classify_orphaned_test() ->
    Facts = #{majority_present => true, self_is_leader => false,
              self_clustered_with_leader => false},
    ?assertEqual(orphaned, ?M:classify(no_quorum, Facts)),
    ?assertEqual(orphaned, ?M:classify(degraded, Facts)),
    ?assertEqual(orphaned, ?M:classify(unreachable, Facts)).

%% THE split we exist to fix: a replica that split into its own singleton is
%% LOCALLY healthy (quorum 1/1, leader = self). It must still be orphaned,
%% because the majority's leader is absent from its local cluster.
classify_healthy_singleton_is_orphaned_test() ->
    ?assertEqual(orphaned, ?M:classify(healthy,
                                       #{majority_present => true,
                                         self_is_leader => false,
                                         self_clustered_with_leader => false})).

%% Unhealthy but no majority (genuine quorum loss) -> drift (alarm only).
classify_drift_when_no_majority_test() ->
    ?assertEqual(drift, ?M:classify(no_quorum,
                                    #{majority_present => false,
                                      self_is_leader => false,
                                      self_clustered_with_leader => false})).

%% Partitioned but still locally clustered with the leader -> drift, not a
%% reset candidate; Ra reconciles the partition.
classify_drift_when_clustered_with_leader_test() ->
    ?assertEqual(drift, ?M:classify(degraded,
                                    #{majority_present => true,
                                      self_is_leader => false,
                                      self_clustered_with_leader => true})).

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

%% beam03 reset into its own singleton (local members = [beam03]); the
%% majority (beam00+beam02) elected beam00 and still lists beam03 as a
%% configured-but-lagging member. Because beam00 (the majority leader) is
%% ABSENT from beam03's LOCAL member set, beam03 is orphaned and safely
%% healable — even though the majority's member list still contains it.
ghent_orphan_is_safely_healable_test() ->
    Self = 'beam03@h',
    SelfLocalMembers = [Self],                      %% fresh singleton
    Peers = [{'beam00@h', 'beam00@h', ['beam00@h', 'beam02@h', Self]},
             {'beam02@h', 'beam00@h', ['beam00@h', 'beam02@h', Self]}],
    #{leader := Leader} = ?M:majority_view(Peers),
    Facts = #{majority_leader => Leader,
              majority_present => Leader =/= undefined,
              self_is_leader => Leader =:= Self,
              self_clustered_with_leader => lists:member(Leader, SelfLocalMembers)},
    ?assertEqual(orphaned, ?M:classify(healthy, Facts)),
    ?assert(?M:safe_to_reset(Facts)).

%% Counter-case: beam03 merely partitioned but still configured with the
%% others (local members = all three). beam00 IS in its local set, so it is
%% NOT an orphan and must be left to Ra.
ghent_partition_is_not_reset_test() ->
    Self = 'beam03@h',
    SelfLocalMembers = ['beam00@h', 'beam02@h', Self],   %% still configured
    Leader = 'beam00@h',
    Facts = #{majority_leader => Leader,
              majority_present => true,
              self_is_leader => false,
              self_clustered_with_leader => lists:member(Leader, SelfLocalMembers)},
    ?assertEqual(drift, ?M:classify(no_quorum, Facts)),
    ?assertNot(?M:safe_to_reset(Facts)).
