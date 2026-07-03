# PLAN: Fix store-cluster split-brain at simultaneous boot

Status: Implemented (reckon-db 5.5.5, 2026-07-03)
Severity: High (recurring on the parksim beam fleet)
Area: `reckon_db_store_coordinator`

## Implemented (5.5.5)

The actual root cause was narrower and worse than first filed: the election in
`handle_no_existing_clusters` ran over ALL connected nodes, so on a shared dist
mesh of single-store nodes the globally-lowest node NAME (which may not run the
store) was elected coordinator and every join for the store failed forever.
Fix landed:

- Election now runs only over nodes that actually run the store
  (`store_runner_nodes/2`), via a pure `elect_coordinator/2` (unit-tested).
- A self-elected coordinator keeps reconciling in cluster mode until it is
  genuinely multi-member (`join_status = coordinating`, `retry_join` stops on
  `is_multi_member/1`) rather than stopping at `coordinator` — the
  persistent-reconcile behaviour ex-esdb had. Combined with the existing
  nodeup-driven rejoin, a boot-race self-election now self-heals.

The external `converge-parksim.sh` mitigation remains as a belt-and-suspenders
live-repair tool but is no longer required once the fleet runs 5.5.5.

## Symptom

On a cold, simultaneous boot of N replicas of a store (observed repeatedly on
the J4105 beam fleet running parksim, RF=3), the replicas self-form as
separate 1-member Ra/Khepri clusters instead of one N-member cluster. Ra
never merges them, so the split is permanent: `reckon_db_cluster:health_check`
reports `total_nodes => 1` on every replica, quorum is 1, and writes/triggers
happen independently on each (for parksim: tripled writes and mesh facts).
The Erlang dist mesh and gossip are healthy; this is purely a formation bug.

## Root cause

`handle_no_existing_clusters/2` elects a coordinator from the CURRENT
connected-node view:

```erlang
handle_no_existing_clusters(StoreId, ConnectedNodes) ->
    AllNodes = lists:sort([node() | ConnectedNodes]),
    case AllNodes of
        [Coordinator | _] when Coordinator =:= node() -> coordinator;  %% self-elect
        [Coordinator | _]                             -> join_existing_cluster(StoreId, Coordinator)
    end.
```

During a simultaneous boot the dist mesh is still forming, so `ConnectedNodes`
differs per node at the instant each evaluates the election. Node A may see
`{A, C}`, node B may see `{B}` only, node C may see `{C, A}`. Each computes
"am I the lowest of what I can see" against a DIFFERENT set, so more than one
node concludes it is the coordinator. Every self-elected coordinator returns
`coordinator`, which sets `join_status = joined` and stops:

```erlang
    coordinator -> State#state{join_status = joined};   %% no further retry
    waiting     -> schedule_retry(State);
    no_nodes    -> schedule_retry(State);
    failed      -> schedule_retry(State);
```

So `waiting`/`no_nodes`/`failed` retry (good), but a wrongly self-elected
`coordinator` never re-evaluates. After the mesh fully connects, the multiple
1-member "coordinators" never notice each other and never merge. Permanent
split.

## What ex-esdb (the Elixir predecessor) did differently

`reckon_db_store_coordinator` was ported from ex-esdb's `StoreCoordinator` +
`StoreCluster` (`beam-campus/ex-esdb`). The election logic is the same shape.
The difference is that ex-esdb's monolithic `StoreCluster` GenServer runs a
PERSISTENT periodic reconcile in addition to the join retry:

- `handle_info(:join, …)` reschedules `:join` on `:waiting`/`:no_nodes`/
  failure (reckon-db kept this).
- `handle_info(:check_members, …)` runs every `5 * timeout` and keeps the
  membership view fresh and re-acts (reckon-db's split into node_monitor +
  store_coordinator weakened/dropped this reconcile path).
- `{:nodeup, _}` triggers immediate membership + leadership checks.

The persistent `:check_members` reconcile is what let a mis-formed node notice
peers and correct course. reckon-db stops at `coordinator` and has no
equivalent standalone-coordinator reconcile.

## Proposed fix (any one closes it; ideally 2 + 3)

1. Stable election. Do not decide `coordinator` until the connected-node set
   has settled: wait for a quiet window with no new `nodeup`, or for an
   expected peer count, before self-electing. Reduces the racy partial-view
   election.
2. Coordinator reconcile (the real cure). A node that returned `coordinator`
   must NOT be permanently `joined`. Periodically (and on `nodeup`) it should
   re-scan connected nodes for another cluster/coordinator of the same
   store_id and, if one exists, apply the deterministic tie-break (lowest node
   wins) and merge: the higher-named standalone coordinator joins the lower.
   This is split-detection + merge, mirroring ex-esdb's `:check_members`.
3. nodeup-driven re-evaluation. On `{nodeup, N}` a standalone (1-member) node
   re-runs the join/reconcile so a late-connecting lower-named coordinator (or
   any established cluster) is joined.

## Interim mitigation (already shipped, external)

`macula-demo/infrastructure/scripts/converge-parksim.sh` reconciles the fleet
from outside after deploy: for each store it calls
`reckon_db_store_coordinator:join_cluster/2` on every non-coordinator replica
against the lowest-node coordinator and verifies full membership. Deployed as
the final step of `deploy-parksim.sh` and re-runnable for live repair. This
proves the `join_cluster/2` primitive heals the split reliably; the fix above
is to do the same reconcile INSIDE reckon-db so no external step is needed.

## Test plan

- Reproduce: start N replicas of a store with a barrier so they boot within
  the same ~second under staggered dist connectivity; assert they converge to
  one N-member cluster (not N standalones).
- Split-then-heal: force N standalone 1-member clusters, then assert the
  coordinator reconcile merges them to one N-member cluster within a bounded
  time, with the lowest node as the surviving cluster.
- Regression: single-node store still forms a clean 1-member cluster; no
  spurious merges when already converged.
