# Dialyzer Backlog

**Status:** 182 warnings as of v2.2.2 release prep (2026-05-17).

`reckon-db` ships with strict dialyzer options enabled in `rebar.config`:

```erlang
{dialyzer, [
    {warnings, [
        unknown,
        unmatched_returns,
        error_handling,
        underspecs
    ]},
    ...
]}.
```

The `underspecs` and `unmatched_returns` flags are deliberately strict — they catch real classes of bugs (specs looser than reality, error returns silently discarded). The current 182 warnings represent latent issues that have accumulated and have not yet been triaged.

**None of these warnings were introduced by v2.2.1 or v2.2.2** — the count was 197 at the start of v2.2.2 prep; 15 were fixed as a side-effect of the `reckon_db_cluster` facade work and a record-construction tightening in `reckon_db_subscriptions:subscribe/5`. The remaining 182 predate the 2.2.x line and were equally present in the v2.2.0 release on hex.

The full raw dialyzer output is checked in at [`dialyzer-warnings-2.2.2.raw`](dialyzer-warnings-2.2.2.raw) for posterity. The summary below groups the warnings by category and by hot-file so the cleanup work can be scheduled.

## Severity by category

| # | Category | What it means | Typical fix |
|---|----------|---------------|-------------|
| 58 | `is a supertype` (underspecs) | The declared `-spec` is looser than the function's actual return type | Tighten the spec to match observed returns |
| 36 | `can never match` | A clause or pattern is unreachable given upstream types | Delete the dead clause, or fix the upstream type |
| 32 | `value is unmatched` | An expression returns `{ok,_}\|{error,_}` (or similar) and the caller discards both branches | Bind the result with `_ = ` or pattern-match the relevant branch |
| 21 | `extra_range` / `might also return` | The spec lists a return shape that the function never actually produces | Remove the dead branch from the spec |
| 11 | record violation | A record is constructed with a field whose value type disagrees with the field's declared type | Initialise with a value of the correct type, or relax the field type |
| 6 | `no local return` | Dialyzer believes every path through the function crashes | Usually a downstream typing issue propagating — fix the root cause |
| 5 | `will never be called` | Function is unreachable from any public entry point | Delete, or fix the call-site that should have used it |
| 4 | `breaks the contract` | A call passes arguments inconsistent with the callee's spec | Real bug — fix the call or the spec |
| 9 | other (unique typos) | One-off shape mismatches | Per-case |

## Hot-file ranking

| Warnings | File |
|----------|------|
| 23 | `src/reckon_db_store_inspector.erl` |
| 15 | `src/reckon_db_links.erl` |
| 14 | `src/reckon_db_streams.erl` |
| 12 | `src/reckon_db_subscriptions.erl` |
| 12 | `src/reckon_db_gateway_worker.erl` |
| 12 | `src/reckon_db_consistency_checker.erl` |
| 7 | `src/reckon_db_store_coordinator.erl` |
| 7 | `src/reckon_db_persistence_worker.erl` |
| 7 | `src/reckon_db_filters.erl` |
| 6 | `src/reckon_db_node_monitor.erl` |
| 5 | `src/reckon_db_snapshots.erl` |
| 5 | `src/reckon_db_health_prober.erl` |
| 5 | `src/reckon_db_capability_verifier.erl` |
| 4 | `src/reckon_db_scavenge.erl` |
| 4 | `src/reckon_db_cluster.erl` |
| 4 | `src/reckon_db_backpressure.erl` |
| 3 | `src/reckon_db_subscription_health.erl` |
| 3 | `src/reckon_db_leader_tracker.erl` |
| 3 | `src/reckon_db_discovery.erl` |
| 3 | `src/reckon_db_archive_file.erl` |
| ≤2 | (twelve other files) |

The top 6 files account for 88 warnings (~48% of the total). Concentrating cleanup effort there would clear roughly half the backlog.

## Scheduling

This backlog is **not** a release blocker for v2.2.2 — it inherits the dialyzer state of v2.2.0 unchanged. It SHOULD be cleared as a dedicated cleanup release, candidate version **v2.3.0**, to give the work a clean changelog entry and let the SemVer minor bump signal "internal-quality refresh."

Suggested order, hot files first:

1. `reckon_db_store_inspector.erl` (23) — many "will never be called" hint at broken control flow that can fall like dominoes once the root is fixed
2. `reckon_db_links.erl` (15)
3. `reckon_db_streams.erl` (14)
4. `reckon_db_subscriptions.erl` (12)
5. `reckon_db_gateway_worker.erl` (12)
6. `reckon_db_consistency_checker.erl` (12)
7. The long tail

## Reproducing the count

```sh
rebar3 dialyzer 2>&1 | tail -3
grep -c '^/home' _build/default/28.1.dialyzer_warnings   # → 182
```

To inspect a specific category:

```sh
grep -B0 -A6 'is a supertype' _build/default/28.1.dialyzer_warnings
grep -B0 -A1 'value is unmatched' _build/default/28.1.dialyzer_warnings
```
