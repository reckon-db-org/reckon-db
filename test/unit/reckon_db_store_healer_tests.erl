%%% @doc Unit tests for the store healer's pure decision logic.
%%%
%%% The healer's destructive path is gated by pure functions:
%%%   - majority_view/1  : pick the authoritative majority among peer views
%%%   - is_orphan/1      : the single orphan predicate (shared by the below)
%%%   - classify/2       : self-health + majority view -> verdict
%%%   - safe_to_reset/1  : the data-safety gate (== is_orphan/1)
%%%
%%% A replica is an orphan the majority can safely re-absorb when a real
%%% (leader-having) majority exists, we are not its leader, and EITHER we are
%%% not locally clustered with that leader (diverged into our own cluster) OR
%%% our local ra server is wedged (`local_responsive => false' — the exact
%%% fault observed live: the server accepts a members query and never replies).
%%% A node still clustered with the leader AND responsive is a transient
%%% partition -> left to Ra, never reset.
-module(reckon_db_store_healer_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, reckon_db_store_healer).

%% Base facts for a healthy, responsive, in-cluster follower.
base() ->
    #{majority_present => true,
      self_is_leader => false,
      self_clustered_with_leader => true,
      local_responsive => true}.

%%====================================================================
%% safe_to_reset/1 (== is_orphan/1) — the data-safety gate truth table
%%====================================================================

%% Diverged into our own cluster (leader absent from our local set), responsive.
safe_when_diverged_test() ->
    ?assert(?M:safe_to_reset((base())#{self_clustered_with_leader => false})).

%% Local ra server wedged (unresponsive) while a majority exists — heal it even
%% though our config still lists the leader.
safe_when_local_wedged_test() ->
    ?assert(?M:safe_to_reset((base())#{local_responsive => false})).

%% No majority to rejoin -> never reset (would destroy the only copy).
unsafe_when_no_majority_test() ->
    ?assertNot(?M:safe_to_reset((base())#{majority_present => false,
                                        self_clustered_with_leader => false})).

%% We are the majority's leader -> never reset the authoritative side.
unsafe_when_self_is_leader_test() ->
    ?assertNot(?M:safe_to_reset((base())#{self_is_leader => true,
                                        local_responsive => false})).

%% Clustered with the leader AND responsive (transient partition) -> leave it
%% to Ra, never reset.
unsafe_when_clustered_and_responsive_test() ->
    ?assertNot(?M:safe_to_reset(base())).

%%====================================================================
%% classify/2 — verdict from self-health + orphan predicate
%%====================================================================

%% Healthy self + not an orphan -> healthy.
classify_healthy_test() ->
    ?assertEqual(healthy, ?M:classify(healthy, base())).

%% Diverged -> orphaned regardless of self-status (a split singleton looks
%% locally healthy).
classify_orphaned_when_diverged_test() ->
    Facts = (base())#{self_clustered_with_leader => false},
    ?assertEqual(orphaned, ?M:classify(healthy, Facts)),
    ?assertEqual(orphaned, ?M:classify(no_leader, Facts)).

%% Wedged local server -> orphaned.
classify_orphaned_when_wedged_test() ->
    ?assertEqual(orphaned, ?M:classify(healthy, (base())#{local_responsive => false})).

%% Not an orphan, self unhealthy (no majority visible) -> drift (alarm only).
classify_drift_when_no_majority_test() ->
    ?assertEqual(drift, ?M:classify(no_leader,
                                    (base())#{majority_present => false})).

%% Transient partition (clustered + responsive) with degraded self -> drift.
classify_drift_when_partitioned_test() ->
    ?assertEqual(drift, ?M:classify(no_leader, base())).

%%====================================================================
%% majority_view/1 — pick the authoritative view among peer responses
%%====================================================================

majority_view_empty_test() ->
    ?assertEqual(#{leader => undefined, members => []}, ?M:majority_view([])).

majority_view_no_leader_test() ->
    ?assertEqual(#{leader => undefined, members => []},
                 ?M:majority_view([{'n1@h', undefined, []},
                                   {'n2@h', undefined, ['n1@h']}])).

majority_view_picks_largest_test() ->
    Peers = [{'a@h', 'a@h', ['a@h', 'b@h']},
             {'c@h', 'a@h', ['a@h', 'b@h', 'c@h']}],
    ?assertEqual(#{leader => 'a@h', members => ['a@h', 'b@h', 'c@h']},
                 ?M:majority_view(Peers)).

majority_view_ignores_empty_members_test() ->
    ?assertEqual(#{leader => 'a@h', members => ['a@h', 'b@h']},
                 ?M:majority_view([{'x@h', 'a@h', []},
                                   {'a@h', 'a@h', ['a@h', 'b@h']}])).

%%====================================================================
%% End-to-end: the two live ghent cases
%%====================================================================

%% beam03 wedged (members query timed out) while beam00+beam02 hold quorum —
%% the exact state seen live. Orphan, safely healable.
ghent_wedged_orphan_is_healable_test() ->
    Self = 'beam03@h',
    Peers = [{'beam00@h', 'beam00@h', ['beam00@h', 'beam02@h', Self]},
             {'beam02@h', 'beam00@h', ['beam00@h', 'beam02@h', Self]}],
    #{leader := Leader, members := Members} = ?M:majority_view(Peers),
    Facts = #{majority_leader => Leader,
              majority_present => Leader =/= undefined andalso length(Members) >= 2,
              self_is_leader => Leader =:= Self,
              %% config still lists the leader, but the local server is wedged
              self_clustered_with_leader => true,
              local_responsive => false},
    ?assertEqual(orphaned, ?M:classify(healthy, Facts)),
    ?assert(?M:safe_to_reset(Facts)).

%% Counter-case: responsive + still clustered with the leader (transient
%% partition) -> NOT reset; left to Ra.
ghent_partition_is_not_reset_test() ->
    Facts = #{majority_present => true, self_is_leader => false,
              self_clustered_with_leader => true, local_responsive => true},
    ?assertEqual(drift, ?M:classify(no_leader, Facts)),
    ?assertNot(?M:safe_to_reset(Facts)).
