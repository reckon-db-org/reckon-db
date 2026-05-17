# Changelog

All notable changes to reckon-db will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.1] - 2026-05-17

### Fixed — Add missing `reckon_db_cluster` facade

`reckon_db_gateway_worker` had four `handle_call/3` clauses
(`{verify_cluster_consistency, _}`, `{quick_health_check, _}`,
`{verify_membership_consensus, _}`, `{check_log_consistency, _}`)
that all called into a `reckon_db_cluster` module which never
existed — a dangling reference left over from the `esdb_* →
reckon_db_*` rename in v2.0.0.

The bug was invisible until reckon-gateway 0.4.x exposed those
RPCs over gRPC and a client (the new reckon-go SDK) actually
called them — at which point they hung in `reckon_gater_retry`'s
exponential-backoff loop until the caller timed out, because
each retry attempt died on `{undef, [{reckon_db_cluster, ...}]}`
inside the gateway worker.

This release adds `reckon_db_cluster` as a thin facade over
[[reckon_db_consistency_checker]] and `ra_leaderboard`:

- `health_check/1` — cheap liveness check (quorum + leader presence).
  Used by `HealthService.Check`.
- `verify_consistency/1` — full cluster consistency verdict
  (membership + leader consensus + quorum). Used by
  `HealthService.VerifyClusterConsistency`.
- `verify_membership/1` — membership consensus across nodes.
  Used by `HealthService.VerifyMembershipConsensus`.
- `check_log_consistency/1` — Raft log replication check.
  Used by `HealthService.CheckRaftLogConsistency`.

The facade is intentionally stateless — it gathers state from
ra/khepri on demand rather than depending on the (unsupervised)
`reckon_db_consistency_checker` gen_server, so it works in both
`single` and `cluster` modes.

## [2.2.0] - 2026-05-17

### Added — Cluster-wide store discovery + watcher API

`reckon_db_store_registry' now provides genuine cluster-wide
discovery for the EventStore-style "ephemeral store" model: stores
exist when their supervision tree is running on at least one
cluster node, and the registry tracks the union of who is
currently announcing themselves. No CreateStore/DeleteStore —
lifecycle stays a deployment concern.

#### `subscribe/1' + `unsubscribe/1' (new)

Public API for live store-topology events. Subscribed processes
receive:

    {store_event, announced | retired, EntryMap}

as stores come and go anywhere in the cluster. EntryMap matches
the `list_stores/0' shape. Subscribers are monitored — dead pids
are pruned automatically via the registry's `DOWN' handler, so
no explicit unsubscribe is needed when the watcher crashes.

This is the substrate for the new gRPC
`reckon.gateway.v1.StoresService.WatchStores' RPC in
reckon-gateway 0.4.0.

### Fixed — Cluster-wide discovery actually works

Two latent bugs that meant each node only knew about its own
local store, despite the cluster being healthy:

1. The previous version subscribed to a pg-mailbox message
   pattern that pg never emits (`{pg, Scope, Group, {leave, _,
   _}}'). Node-down cleanup was silently broken.

2. Announcement was one-way: when a registry came up, it
   broadcast its local store to peers but never asked peers
   for THEIR current state. A registry that booted after its
   peers had already announced ended up knowing only about
   itself.

Fix:

- Use `pg:monitor/2', which returns CURRENT members and
  subscribes to live join/leave events idiomatically. The
  initial member list seeds a bilateral state-sync; later
  joins trigger a fresh state request to the new arrival.
- `peer_state_request' / `peer_state_reply' cast pair —
  fully async, no try/catch around dead-peer calls, no
  timeouts. A dead peer just doesn't reply; merge proceeds
  with whatever arrived.

Verified end-to-end against the 4-node beam cluster: every node
sees all 4 store-instances after `pg:monitor' bootstrap.

### Other cleanups

- `find_entry/3' uses `lists:search/2', returns `not_found'
  (was: a `lists:filter' returning `false')
- announce/unannounce handlers are clause-based on entry
  presence; the "no such entry" path is a no-op early return
- `notify_subscribers' uses plain `Pid ! Msg' — runtime drops
  to dead pids silently, no `catch' wrapper needed

## [2.1.4] - 2026-05-17

### Fixed — Cluster bootstrap robustness

Four bugs in `reckon_db_store_coordinator` that, between them,
could permanently strand a node outside the Raft cluster after a
rough restart cycle:

#### Infinite-timeout join

`khepri_cluster:join/2' internally uses
`khepri_app:get_default_timeout/0', which defaults to `infinity'.
Combined with the global lock acquired during the join, simultaneous-
boot nodes could deadlock on lock contention forever. (Setting
khepri's `default_timeout' app env globally would also affect every
other khepri operation, so that's not a usable workaround.)

The exported 2-arg version is now wrapped in a side process that's
killed after `?KHEPRI_JOIN_TIMEOUT' (20s). On timeout the coordinator
returns `failed' and the retry-with-jitter timer picks up the next
round.

(`khepri_cluster:join/3' is defined in the source but NOT exported
in khepri 0.17.2 — only `join/1' and `join/2' are. Passing an
explicit timeout via the 3-arg form fails with `{undef, ...}'.)

#### Self-clusters treated as active

`has_active_cluster/2` treated `{ok, [SingleSelf]}' as an active
cluster. Every freshly-booted Khepri node is a 1-member standalone
cluster, so during a simultaneous boot every node saw every other
node as a cluster and they all raced to join each other under the
same global lock — the worst possible bootstrap shape. Tightened
to `length(Members) > 1'.

#### Coordinator election didn't drive cluster formation

The original `handle_no_existing_clusters` just logged the elected
coordinator and returned. Coordinator stayed as a 1-node cluster,
non-coordinators sat in `waiting' forever, nothing grew the cluster.
With the self-clusters fix above, this previously-latent stalemate
became reachable: 4 standalone clusters forever.

Now: the elected coordinator stays as its 1-node cluster, but each
non-coordinator actively joins via the coordinator. Once anyone
joins, the coordinator's cluster has 2 members and subsequent
retries from remaining non-coordinators find an active cluster
via the regular `has_active_cluster' path.

#### No retry on transient failure

After `waiting | failed | no_nodes', the coordinator gave up
permanently. Added a jittered retry (3-8s) on the coordinator's
own gen_server that re-attempts `do_join_cluster/1' until status
becomes `joined'.

#### Diagnostic for stale local state

Before `khepri_cluster:join' is called, verify the local Ra
server is registered under the StoreId. If not, log a pointer
to the `wipe-and-rejoin.sh' script in
reckon-cluster-compose instead of hanging on infrastructure
that never arrived.

#### Verified end-to-end

Cold-start torture against the 4-node beam cluster:
  * Wipe all 4 data dirs, parallel `docker compose up` on all 4
  * All 4 nodes converge to 4-of-4 Raft membership without
    manual intervention
  * Existing torture trio (leader_kill / partition_heal /
    subscription_failover) all pass against the freshly-formed
    cluster
  * Killing the new leader during the subscription scenario:
    new leader elected on the formerly-stuck beam00 node
    (proves it's a first-class member)

## [2.1.3] - 2026-05-17

### Fixed — Cross-node subscription delivery + registration race

Two bugs that, together, caused stream-scoped subscriptions to
silently miss roughly half their events whenever the subscription
was opened against a non-leader gateway.

#### Cross-node delivery

`reckon_db_emitter:send_to_subscriber/4` had a single clause
guarded on `node(Pid) =:= node()` plus a catch-all that returned
`ok`. `maybe_forward_events/2` had the same shape. When the Khepri
trigger fired on the Raft leader and `reckon_db_emitter_group:broadcast/3`
picked an emitter that wasn't co-located with the subscriber pid,
the emitter silently dropped the event.

Each cluster node spins up its own emitter pool for every
subscription (via `reckon_db_leader_tracker` and `setup_event_notification`),
so the pg group typically holds 2+ emitters on different nodes —
all carrying the same subscriber pid (the one captured by the
client that called `save_subscription`). The random pick had a
~50% chance of landing on an emitter whose node didn't host the
subscriber, and those events were lost.

`Pid ! Msg` works fine across Erlang distribution; the local-only
guard was the bug. Remote pids now receive via
`catch (Pid ! Msg)`. Liveness probing stays local-only because
`erlang:is_process_alive/1` is undefined for remote pids — the
runtime's own dead-process semantics cover remote delivery to a
dead pid.

#### Registration race

`setup_event_notification` registered the Khepri trigger BEFORE
starting the emitter pool. Between those two steps, any event
commit fired the trigger into an empty pg group — `broadcast/3`
logged "No emitters for ..." and dropped the event. Particularly
noticeable on a hot stream during sub registration.

Swapped the order to (persist names → start pool → register
trigger), so the local emitter is in pg before the trigger goes
live.

#### Verification

End-to-end on a 4-node cluster: `subscriber received 25 events
from our stream` out of 25 writer-acked, contiguous version range
0..24, both pre- and post-leader-kill events delivered, zero
cross-stream events received (the 2.1.2 catch-up filter still
holds).

## [2.1.2] - 2026-05-17

### Fixed — Catch-up filter

Catch-up replay (the path that delivers historical events to a
newly-registered subscription) ignored the subscription's selector
and pushed the entire global event log to the subscriber, regardless
of its declared filter. The Khepri trigger filter (live path) was
correct; only the catch-up path was unfiltered.

Net effect on an active store: every subscription opened with
`start_from = 0` received the full history of every stream, then
flipped to correctly filtered live deliveries. Stream-scoped
consumers had to discard 99%+ of what they received on attach.

#### Implementation

- New `reckon_db_filters:matches/3` — in-memory predicate that
  evaluates a `(Type, Selector)` pair against an `#event{}` record.
  Handles `by_stream` (exact stream id; `<<"$all">>` matches all),
  `by_event_type`, and `by_tags` (set inclusion). `by_event_pattern`
  and `by_event_payload` pass through; live trigger filters them
  correctly so the gap is narrower. A real map-pattern evaluator
  is a follow-up.
- `do_catchup/5` now takes the subscription's type + selector and
  applies the predicate to each batch before sending. Read window
  through `read_all_global` still advances by raw batch size so the
  scan progresses even when nothing in a window matches.
- `deliver_catchup_batch` separated into filtered/raw counts; logs
  "events scanned" rather than "delivered" so the metric reflects
  what catch-up actually saw.

#### Behaviour change

Subscribers that relied on receiving cross-stream events from a
single `by_stream` subscription will now miss them. The intended
contract is "subscribe per stream; use `<<"$all">>` for the global
firehose" — this release makes the implementation match.

## [2.1.1] - 2026-05-15

### Added — Backward-direction chain verification

Closes the documented gap from 2.1.0. On integrity-enabled stores,
`reckon_db_streams:read/5,6` now verifies the chain for backward
reads in exactly the same way as forward reads. The only
behavioural difference between directions is the result-ordering
of the returned events; the chain semantics are identical.

#### Implementation

The verifier walks events in forward order regardless of read
direction (the chain runs forward through time and that's the
direction it has to be checked in). For a backward read, the
implementation reverses the result to forward order, runs the
forward verifier, then reverses the verified list before
returning so the caller still sees events highest-version-first.

#### Behaviour change for callers

Backward reads of integrity-enabled stores that previously
succeeded against tampered storage now return
`{error, {integrity_violation, _}}`. This is a hardening, not
a regression: 2.1.0's behaviour was the documented gap. Callers
relying on the old behaviour to access tampered data deliberately
should use the existing `Opts = #{verify => skip_all}` escape
hatch.

#### Tests

`backward_read_bypasses_verification` (which had asserted the
gap) replaced with two tests in
`reckon_db_integrity_reads_SUITE`:

- `backward_read_catches_tampering` — symmetric assertion that
  the same tamper detected on forward reads is also detected on
  backward reads
- `backward_read_returns_events_in_descending_order` — intact
  backward read returns `[v4, v3, v2]` with integrity fields
  populated

Full regression: 514 eunit + 5/21/12/4 CT (writes/reads/snapshots/
subscriptions) = 556 tests pass.

## [2.1.0] - 2026-05-15

### Added — Tamper-resistance for events and snapshots

Implements Layers 2–5 of the cross-package design in
`plans/PLAN_TAMPER_RESISTANCE.md`. Reckon-db now writes
HMAC-protected, chain-hashed events when integrity is enabled
on a store, and verifies them on every read surface.

Requires `reckon_gater >= 2.1.0` for the schema and
verification primitives.

#### Configuration

`#store_config{}` gains an `integrity` field (default `disabled`).
To enable:

```erlang
#store_config{
    %% ... existing fields ...
    integrity = #{
        enabled => true,
        key_source => {env_var, <<"RECKON_DB_KEY_MY_STORE">>}
        %% or: {sealed_file, "/path/to/key"}  (mode 0600 required)
    }
}
```

Keys are 32 random bytes (HMAC-SHA256). Loaded into
`persistent_term` at store startup; cleared on shutdown.
Misconfiguration (missing env, bad base64, insecure file mode,
wrong size) is fail-fast — the store refuses to start.

#### Write path (Layer 2)

- `reckon_db_streams:append/4,5` populates `prev_event_hash` + `mac`
  on every event when integrity is enabled.
- New per-stream watermark stored under
  `[metadata, integrity, chain_start, StreamId]`. Set on the first
  integrity-bearing append to a stream. Events with version below
  the watermark stay legacy; events at or above must carry
  integrity fields.
- Pre-existing legacy streams gain a watermark equal to
  `current_highest_version + 1` on first integrity write — legacy
  data is preserved untouched.

#### Read path (Layer 3)

- New `reckon_db_streams:read/6` accepts an `Opts` map with
  `verify => skip_legacy | strict | skip_all`. Default
  `skip_legacy` for backward compatibility.
- Forward reads on integrity-enabled stores verify each event's
  MAC and chain link against a running tip. Failure surfaces as
  `{error, {integrity_violation, _}}` — non-retriable, distinct
  from `wrong_expected_version`.
- Backward reads bypass chain verification in 2.1.0 (documented
  gap; MAC-only check possible in future).
- New telemetry event `[reckon, db, read, legacy_event_returned]`
  fires when legacy events are returned under `skip_legacy`, for
  operator remediation tracking.

#### Snapshot path (Layer 4)

- `reckon_db_snapshots:save/4,5` populates `anchor_hash` (chain
  hash of the event at the snapshot's version) + `mac` when
  integrity is enabled.
- `load/2` and `load_at/3` recompute the chain hash from the
  underlying event at load time and verify against the stored
  anchor. Detects post-snapshot stream tampering even when the
  snapshot itself is intact — the headline property this layer
  provides over MAC alone.
- Save refused when no event exists at the target version or
  when the target event is legacy — a snapshot whose anchor
  cannot be established is unverifiable and worse than no
  snapshot.

#### Subscription catch-up (Layer 5)

- `reckon_db_subscriptions:do_catchup/3` MAC-verifies each
  integrity-bearing event before delivery. Cross-stream chain
  verification is intentionally NOT performed here (catch-up
  reads sort by `epoch_us` across all streams; per-stream chain
  integrity belongs at the consumer / aggregate-rebuild layer).
- Tampered event during catch-up halts replay and sends
  `{subscription_error, {integrity_violation, _}}` to the
  subscriber. Emits `[reckon, db, subscription, integrity, violation]`
  telemetry.
- Live events come from the write path with integrity fields
  already populated — no emitter-side change needed.

#### New modules

- `reckon_db_integrity_key` — per-store HMAC key loader with
  validation (32-byte size, base64 decode, file mode 0600).
- `reckon_db_chain_watermark` — per-stream watermark CRUD against
  the metadata tree.

#### Tests

41 new Common Test cases plus 12 new eunit tests across four
suites:

- `reckon_db_integrity_key_tests` (12 eunit)
- `reckon_db_integrity_writes_SUITE` (5 CT)
- `reckon_db_integrity_reads_SUITE` (20 CT, 5 groups)
- `reckon_db_integrity_snapshots_SUITE` (12 CT, 2 groups)
- `reckon_db_integrity_subscriptions_SUITE` (4 CT)

Full regression: 514 eunit + 41 integrity CT pass with zero
existing-test regressions.

### Fixed

- `src/reckon_db_log_backend.erl` — converted 11 `@doc` tags on
  `-callback` declarations to plain `%%` comments. EDoc strict
  rules disallow `@doc` before `-callback`; the previous shape
  broke `rebar3 ex_doc` and would have blocked hex publication.
  Text content preserved verbatim.

### Changed

- `src/reckon_db.app.src` — `{links, [{"GitHub", ...}]}` updated
  to `{"Codeberg", ...}` to match canonical hosting.
- `?RECKON_DB_VERSION` macro in `include/reckon_db.hrl` synchronised
  with the package version (was `1.7.2`, now `2.1.0`).
- `README.md` install snippet bumped from `1.0.0` to `2.1.0`.

### Out of scope (deferred)

- Backward-direction read chain verification.
- Cross-stream chain reconstruction on catch-up (per-event MAC
  only at that surface).
- Ed25519 signatures for cross-trust-domain authenticity. The
  `signature` field is reserved on the schema but not populated;
  external authenticity is currently absent over the
  reckon-gateway wire.
- Key rotation. The `key_id` slot is reserved (`{1, MacBytes}`
  shape); 2.1.0 always writes `key_id = 1`.

## [2.0.0] - 2026-04-19

### Changed

**BREAKING**: Internal modules renamed from `esdb_*` to `reckon_db_*`
to match the overall reckon-db-org naming scheme. Most consumers go
through `reckon_gater_api` and should not be affected directly, but
any code that reaches into reckon-db internal modules must update:

| Old module | New module |
|---|---|
| `esdb_aggregate_nif`        | `reckon_db_aggregate_nif`        |
| `esdb_archive_nif`          | `reckon_db_archive_nif`          |
| `esdb_crypto_nif`           | `reckon_db_crypto_nif`           |
| `esdb_filter_nif`           | `reckon_db_filter_nif`           |
| `esdb_graph_nif`            | `reckon_db_graph_nif`            |
| `esdb_hash_nif`             | `reckon_db_hash_nif`             |
| `esdb_capability_verifier`  | `reckon_db_capability_verifier`  |
| `esdb_revocation`           | `reckon_db_revocation`           |

ETS table atoms also renamed:
- `esdb_revoked_tokens`  → `reckon_db_revoked_tokens`
- `esdb_revoked_issuers` → `reckon_db_revoked_issuers`

### Dependencies

- Bumped `reckon_gater` to `~> 2.0` (requires the corresponding renamed API
  from reckon-gater 2.0.0).
- NIF binaries now loaded as `reckon_db_*_nif.so` — requires reckon-nifs 2.0.0.

### Migration

Applications that go through `reckon_gater_api` see only the
reckon-gater 2.0.0 renames. Direct-internal users:

```erlang
%% Before
{ok, Verified} = esdb_capability_verifier:verify(Token).

%% After
{ok, Verified} = reckon_db_capability_verifier:verify(Token).
```

Rebuild from clean: `rm -rf _build rebar.lock && rebar3 compile` will
re-fetch reckon_gater 2.0+ and reckon_nifs 2.0+ and recompile the renamed
NIFs via the rustler hooks.

## [1.7.5] - 2026-03-22

### Fixed

- **Gateway worker version check bypass** — `reckon_db_gateway_worker` had a
  duplicate version check (`version_matches/2`) that used atoms (`any`,
  `stream_exists`) instead of the integer constants (`?ANY_VERSION = -2`,
  `?STREAM_EXISTS = -4`) defined in `esdb_gater_types.hrl`. This caused
  `append_events/4` via the gateway to reject `ANY_VERSION` and `STREAM_EXISTS`
  with `{wrong_expected_version, _}`. Removed the duplicate check — the gateway
  worker now delegates directly to `reckon_db_streams:append/4` which handles
  all version constants correctly.

---

## [1.7.4] - 2026-03-22

### Fixed

- **Non-blocking nodeup handler** — `handle_nodeup_cluster_join` now runs
  entirely in a spawned process. The `should_handle_nodeup` coordinator call
  was blocking the node monitor, causing 5s timeout crashes on every nodeup
  event (same pattern as the leader activation fix in 1.7.3).

---

## [1.7.3] - 2026-03-22

### Fixed

- **Non-blocking leader activation** — `do_activate` now uses `gen_server:cast`
  instead of a blocking `gen_server:call` with 10s timeout. When Khepri/Ra is
  still initializing, `save_default_subscriptions` blocks on Khepri queries,
  causing the node monitor to time out and crash-loop every 15 seconds.
  The leader worker now handles activation asynchronously in its own process.

---

## [1.6.3] - 2026-03-19

### Fixed

- **Store Inspector**: `list_streams/1` returns `[binary()]` not `[{binary(), integer()}]` —
  all inspector functions were destructuring as tuples causing function_clause crashes

## [1.6.2] - 2026-03-19

### Fixed

- **Store Inspector**: Fixed `badarg` crash in `subscription_summary/1` when `subscriber_pid` is undefined
- **Store Inspector**: Made snapshot listing defensive against per-stream errors
- **Store Inspector**: Made subscription listing skip malformed entries instead of crashing
- **Store Inspector**: `format_pid/1` handles undefined, binary, and non-pid terms gracefully

## [1.6.1] - 2026-03-19

### Changed

- Updated reckon_gater dependency to ~> 1.3.1 (includes inspector API exports)

## [1.6.0] - 2026-03-19

### Added

- **Store Inspector** (`reckon_db_store_inspector`): New module for aggregate store-level introspection.
  - `store_stats/1` — stream count, total events, snapshot count, subscription count
  - `list_all_snapshots/1` — all snapshots across all streams (summaries without data payloads)
  - `list_subscriptions/1` — all subscriptions with checkpoint positions
  - `subscription_lag/2` — events behind for a specific subscription
  - `event_type_summary/1` — census of event types with counts
  - `stream_info/2` — detailed info for a single stream (timestamps, snapshot coverage)
- Gateway worker clauses for all inspector operations
- Guide: `guides/store_inspector.md` with usage examples and performance notes
- Architecture diagram: `assets/store_inspector.svg`

## [1.5.1] - 2026-03-08

### Added

- **`reckon_db_streams:has_events/1`**: Check if a store contains at least one event.
  Reads 1 event via `read_all_global` — correctly handles empty streams (truncation,
  GDPR erasure) unlike path-existence checks. Exposed via gateway worker.

## [1.5.0] - 2026-03-06

### Added

- **`reckon_db_streams:read_all_global/3`**: Read all events across all streams in
  global epoch_us order with offset/batch pagination. Used for catch-up subscriptions.

## [1.4.5] - 2026-03-06

### Fixed

- **Stale Khepri triggers after BEAM restart**: When a subscription already existed
  in Khepri (persisted from a previous run), `reregister_subscriber` only updated
  the subscriber PID but did NOT re-register the Khepri trigger. The trigger's stored
  procedure (an Erlang fun/closure) becomes stale after a BEAM restart, so new events
  written to the store would never fire the notification mechanism. This caused
  subscription-based event delivery to silently stop working after daemon restarts.
  Fixed: `reregister_subscriber` now also re-creates the filter and re-registers the
  Khepri trigger, ensuring the stored procedure is fresh.

## [1.4.4] - 2026-03-06

### Fixed

- **Telemetry handler crash on subscription created**: `handle_event(?SUBSCRIPTION_CREATED, ...)`
  pattern-matched on `#{subscription_id := _}` but the metadata from `subscribe/5` sends
  `subscription_name` instead. This caused a `badmatch` that detached the telemetry logger
  handler for the entire session. Fixed: use `maps:get/3` with fallback.

## [1.4.3] - 2026-03-06

### Fixed

- **Crash in `update_subscriber_pid` on re-subscribe**: `reckon_db_subscriptions_store:get/2`
  returns `subscription() | undefined`, not `{ok, subscription()} | {error, _}`. The
  re-registration code from v1.4.2 pattern-matched on `{ok, Existing}` which caused a
  `case_clause` crash, killing the gateway worker and preventing all subscriptions from
  being set up on that store. Fixed: match on the record directly with `is_record` guard.

## [1.4.2] - 2026-03-06

### Fixed

- **Subscriptions not re-registering subscriber PID after restart**: When a projection
  re-subscribes on startup, the subscription already exists in Khepri (persisted from
  the previous BEAM instance). Previously this returned `{error, {already_exists, _}}`
  and the new subscriber PID was never registered. The emitter pool delivered events to
  the dead PID from the previous run, so projections never received events and read
  models stayed empty/stale after restart.
  Fix: when a subscription already exists and a new `subscriber_pid` is provided,
  update the stored subscription with the new PID and return `{ok, Key}`.

### Changed

- **Eliminated all deep case/if nesting across codebase**: Refactored ~50 instances of
  depth-2+ nesting across 25 source files to max depth 1. Extracted helper functions,
  used pattern matching on function heads, and pipeline patterns. No behavioral changes.

## [1.4.1] - 2026-03-06

### Fixed

- **Subscription health monitor kills valid subscriptions after restart**: The health
  monitor treated subscriptions with dead `subscriber_pid` as stale and deleted them,
  even when the emitter pool was running and actively serving events. After a daemon
  restart, ALL persisted subscriptions have dead PIDs (from the previous BEAM instance),
  so the health checker would kill every domain subscription ~2 minutes after boot.
  This left projections without event feeds and read models empty/stale.
  Fix: subscriptions with dead `subscriber_pid` but a running emitter pool are now
  treated as healthy (restarted subscription from a previous BEAM instance).

- **App-level telemetry crashes handler on startup**: `emit_start_telemetry()` fired
  `[reckon_db, store, started]` with app-level metadata (`#{application => reckon_db,
  version => ...}`) instead of the expected `#{store_id := ...}`. This caused a
  `badmatch` in `reckon_db_telemetry:handle_event/4`, which detached the entire
  telemetry logger handler for the rest of the session. Removed the mistyped app-level
  telemetry events (per-store telemetry in `reckon_db_store` is unaffected).

- **Stale `RECKON_DB_VERSION` macro**: Updated from `"0.1.0"` to `"1.4.1"`.

## [1.4.0] - 2026-03-06

### Fixed

- **Per-store Ra system isolation**: Each ReckonDB store now creates its own
  dedicated Ra system with separate WAL, segments, and DETS files. Previously,
  all stores shared the default `khepri` Ra system, causing all event data from
  every bounded context to be written into a single WAL file (whichever store
  started first owned the shared WAL directory). This affected both single and
  cluster modes.

## [1.3.3] - 2026-03-05

### Fixed

- **Late subscription event delivery**: Subscriptions registered after leader activation
  had Khepri triggers but no emitter workers, silently dropping events until the health
  monitor detected missing pools (up to 2 minutes). `setup_event_notification` now
  eagerly starts the emitter pool when the emitter supervisor is available, using
  pattern matching on `whereis/1` to avoid a `gen_server:call` deadlock when called
  from within the leader worker during default subscription setup.

### Added

- `late_subscribe_starts_pool_immediately` integration test in
  `reckon_db_emitter_autostart_SUITE` verifying that the emitter pool exists
  immediately after `subscribe/5` returns when the leader is active.

### Changed

- Bumped `reckon_gater` dependency to `~> 1.1.3` (includes `debug_info` for dialyzer)

## [1.3.2] - 2026-02-21

### Fixed

- **pg scope process dies silently**: `pg:start_link(?RECKON_DB_PG_SCOPE)` was called
  from `reckon_db_app:start/2`, creating an unsupervised pg process linked only to the
  application master. When it died, no supervisor restarted it, silently breaking ALL
  event delivery (emitter workers join pg groups for subscription routing). Moved pg scope
  startup into `reckon_db_sup:init/1` as the first supervised child with
  `restart => permanent`, ensuring it is always restarted on failure.

### Added

- `reckon_db_pg_scope_SUITE` integration tests verifying pg scope supervision,
  automatic restart after crash, and full event delivery after scope restart.

## [1.3.0] - 2026-02-20

### Fixed

- **Leader detection in single mode**: `reckon_db_node_monitor` used a one-shot leader
  check in single mode that never rescheduled. If Ra leader election hadn't completed
  by the first check, the LeaderWorker never activated and emitter pools never started.
  Fixed to retry until leader is detected, then stop polling (no leadership changes in
  single-node mode).
- **Node monitor placement**: Moved `reckon_db_node_monitor` from `cluster_sup` (cluster
  mode only) to `system_sup` (all modes). The node monitor must run in single mode too
  to detect Ra leader and activate leader responsibilities.
- **Supervisor strategies**: Changed `notification_sup` and `leader_sup` from `one_for_one`
  to `rest_for_one`. If `leader_sup` crashes, `emitter_sup` must restart to prevent stale
  emitter pools running without leader coordination. If `leader_tracker` crashes,
  `leader_worker` must restart to re-establish dependency on tracking infrastructure.

### Added

- **Subscription health monitor** (`reckon_db_subscription_health`): Periodic health
  checks (default 60s) that detect and clean up stale subscriptions (dead subscriber),
  orphaned emitter pools (pool without subscription), and missing emitter pools
  (subscription without pool). Only performs cleanup on the Ra leader node. Includes
  on-demand `health_check/1` API returning a health report map.
- **Dead subscriber cleanup in emitter**: When an emitter worker detects its subscriber
  PID is dead during event delivery, it now asynchronously stops the emitter pool
  (matching ex-esdb's `send_or_kill_pool` pattern). Previously dead subscribers
  accumulated silently.
- **Emitter autostart integration tests**: New CT suite
  `reckon_db_emitter_autostart_SUITE` with 13 end-to-end tests covering leader
  activation, subscription lifecycle, event delivery, dead subscriber cleanup,
  and health monitor operation.

## [1.2.7] - 2026-02-18

### Fixed

- **Persistence worker crash on undefined options**: `get_persistence_interval/1` called
  `maps:get/3` on the `options` field of `store_config`, which crashed with `{badmap, undefined}`
  when `options` was not explicitly set. Fixed by adding a guard clause for `is_map(Options)`
  and a fallback clause that returns the default persistence interval. Also set the default
  value of `options` in the `store_config` record to `#{}` (empty map) to prevent this class
  of bug in other code paths.

## [1.2.6] - 2026-02-13

### Fixed

- **Subscription id not populated**: `subscribe/5` created the `#subscription{}` record
  without setting the `id` field, leaving it as `undefined`. The subscription key was
  computed and used for Khepri storage and trigger registration, but the subscription
  record passed to `notify_created` (and thus to the leader_tracker and emitter pool)
  still had `id = undefined`. This caused emitter workers to join pg group
  `{StoreId, undefined, emitters}` while Khepri triggers broadcast to
  `{StoreId, CorrectKey, emitters}` — a different group. Events were silently dropped
  because no emitters were found in the broadcast group. Fixed by setting
  `Subscription#subscription{id = Key}` before passing to downstream consumers.

## [1.2.5] - 2026-02-13

### Fixed

- **Stream subscription filter path mismatch**: `by_stream/1` was stripping the category
  prefix from stream IDs (e.g., `<<"test$delivery-001">>` became `<<"delivery-001">>`),
  creating Khepri trigger filters that never matched stored events. This caused ALL
  stream-based subscriptions to silently fail — triggers never fired, subscribers never
  received events. Fixed to use the full stream ID in the filter path.
- **Event type filter record matching**: `by_event_type/1` used a map pattern
  (`#{event_type => Type}`) to match stored events, but events are stored as `#event{}`
  records (tuples). Map patterns cannot match records. Fixed to use proper record pattern
  matching with `#event{event_type = Type, _ = '_'}`.

### Added

- **Subscription delivery integration tests**: New CT suite
  `reckon_db_subscription_delivery_SUITE` with 5 end-to-end tests verifying the full
  subscribe → append → trigger → emitter → deliver pipeline.

## [1.2.4] - 2026-02-13

### Fixed

- **Subscription Filter Error Handling**: `create_filter/2` errors no longer crash the
  gateway worker. Invalid stream names (e.g., missing `$` separator) now return
  `{error, {invalid_filter, Reason}}` instead of propagating to `khepri_evf:wrap/1`
  which caused a `function_clause` crash.
- **Gateway Worker Resilience**: `handle_cast` for `save_subscription` now matches
  the result and logs a warning on failure instead of crashing. Previously, a single
  invalid subscription could crash the worker and lose all 28+ pending subscription
  messages in its queue.

## [1.2.3] - 2026-02-06

### Fixed

- **Subscription Filter Types**: Fixed `create_filter/2` function_clause error
  - Added support for gater-style subscription types: `by_stream`, `by_event_type`,
    `by_event_pattern`, `by_event_payload`, `by_tags`
  - Maintains backward compatibility with evoq-style types
  - Required for reckon_evoq_adapter type translation through the gater layer

## [1.2.2] - 2026-02-01

### Documentation

- **Event Envelope Documentation**: Improved event structure documentation
  - Added note about evoq event envelope in `guides/event_sourcing.md`
  - Documented metadata standardization (required vs optional fields)
  - Cross-referenced evoq Event Envelope Guide
  - Clarified simplified vs full envelope formats

## [1.2.1] - 2026-01-21

### Fixed

- **Documentation**: Corrected asset paths for hexdocs SVG rendering
  - Changed `../assets/` to `assets/` in all guides

## [1.2.0] - 2026-01-21

### Added

- **Distributed Store Registry**: Cluster-wide store discovery using pg groups
  - `reckon_db_store_registry` GenServer with pg-based distributed membership
  - Automatic store announcement/unannouncement on start/stop
  - Cross-node store visibility via broadcast mechanism
  - `list_stores/0` - List all stores in the cluster
  - `get_store_info/1` - Get detailed info about a specific store
  - `list_stores_on_node/1` - List stores on a specific node
  - 11 new unit tests for store registry
  - Gateway worker calls registry directly (no facade layer)

## [1.1.1] - 2026-01-21

### Added

- **Documentation**: Added Event Sourcing Paradigms guide to hexdocs
  - Entity-Centric (Traditional DDD)
  - Relationship-Centric (DCB - Dynamic Consistency Boundaries)
  - Process-Centric (Dossier metaphor with tags)

## [1.1.0] - 2026-01-21

### Added

- **Tag-Based Querying**: Cross-stream event queries using tags
  - `read_by_tags/4` - Query events by tags across all streams
  - Support for `any` (union) and `all` (intersection) matching modes
  - Tags field added to event records and storage
  - 15 new unit tests for tag filtering
  - Tags are for QUERY purposes only, NOT for concurrency control

### Changed

- **Dependencies**: Updated reckon_gater from `~> 1.0.3` to `~> 1.1.0` for tags support

## [1.0.3] - 2026-01-19

### Changed

- **Dependencies**: Updated reckon_gater from exact `1.0.0` to `~> 1.0.3` to include
  critical double-wrapping bugfix

## [1.0.2] - 2026-01-09

### Fixed

- **Documentation**: Minor documentation improvements

## [1.0.0] - 2026-01-03

### Changed

- **Stable Release**: First stable release of reckon-db under reckon-db-org
- All APIs considered stable and ready for production use
- Updated Dockerfile with correct package names (reckon_db)
- Fixed guide asset paths for hexdocs compatibility

## [0.4.6] - 2025-12-26

### Fixed

- **Dependency conflict**: Removed direct `ra` dependency (khepri provides it).
  Updated to `reckon_db_gater ~> 0.6.5` which removed stale ra from its lock file.

## [0.4.5] - 2025-12-26

### Fixed

- **Dependency conflict**: Updated `ra` dependency from exact `2.16.12` to `~> 2.17.1`
  to resolve conflict with `reckon_db_gater ~> 0.6.4` which requires `ra ~> 2.17.1`

## [0.4.4] - 2025-12-22

### Added

- **Configuration Guide**: Comprehensive configuration documentation
  - Store configuration options (data_dir, mode, pool sizes)
  - Health probing configuration
  - Consistency checking and persistence intervals
  - Erlang (sys.config) and Elixir (config.exs) examples
  - Complete development/staging/production examples
  - Performance tuning recommendations
  - Telemetry events reference

## [0.4.3] - 2025-12-22

### Added

- **Gateway Worker Handlers**:
  - `delete_stream` - Delete streams via gateway
  - `read_by_event_types` - Native Khepri type filtering via gateway
  - `get_subscription` - Get subscription details including checkpoint

These handlers support the erl-evoq-esdb adapter improvements.

## [0.4.2] - 2025-12-22

### Added

- **Cluster Consistency Checker** (`reckon_db_consistency_checker.erl`):
  - Split-brain detection via membership consensus verification
  - Leader consensus verification across all cluster nodes
  - Raft log consistency checks (term and commit index)
  - Quorum status monitoring with margin calculation
  - Four status levels: `healthy`, `degraded`, `split_brain`, `no_quorum`
  - Configurable check intervals (default: 5000ms)
  - Status change callbacks for alerting
  - Telemetry events: `[reckon_db, consistency, ...]`

- **Active Health Prober** (`reckon_db_health_prober.erl`):
  - Fast failure detection via active probing (default: 2000ms intervals)
  - Three probe types: `ping`, `rpc`, `khepri`
  - Configurable failure threshold (default: 3 consecutive failures)
  - Node status tracking: `healthy`, `suspect`, `failed`, `unknown`
  - Recovery detection with callbacks
  - Telemetry events: `[reckon_db, health, ...]`

- **Cluster Consistency Guide** (`guides/cluster_consistency.md`):
  - Split-brain problem explanation and prevention strategies
  - Consistency checker usage and configuration
  - Health prober integration patterns
  - Quorum management and recovery procedures
  - Circuit breaker and load balancer integration examples

- **Architecture Diagrams** (SVG):
  - `assets/consistency_checker.svg` - Consistency checker architecture
  - `assets/split_brain_detection.svg` - Split-brain detection flow
  - `assets/health_probing.svg` - Health probing timeline

### Tests

- 35 unit tests for consistency checker
- 37 unit tests for health prober
- All 72 new tests passing

## [0.4.1] - 2025-12-22

### Added

- **Server-Side Documentation Guides**:
  - `guides/temporal_queries.md` - Point-in-time queries, timestamp filtering, cluster behavior
  - `guides/scavenging.md` - Event lifecycle, archival backends, safety guarantees
  - `guides/causation.md` - Causation/correlation tracking, graph building, DOT export
  - `guides/stream_links.md` - Derived streams, filter/transform patterns
  - `guides/schema_evolution.md` - Schema registry, version-based upcasting, validation
  - `guides/memory_pressure.md` - Pressure levels, callbacks, integration patterns
  - `guides/storage_internals.md` - Khepri paths, version padding, cluster replication

- **Architecture Diagrams** (SVG):
  - `assets/temporal_query_flow.svg` - Temporal query processing flow
  - `assets/scavenge_lifecycle.svg` - Event lifecycle state machine
  - `assets/causation_graph.svg` - Causation chain visualization
  - `assets/stream_links.svg` - Stream linking architecture
  - `assets/schema_upcasting.svg` - Schema version upcasting flow
  - `assets/memory_levels.svg` - Memory pressure level thresholds
  - `assets/khepri_paths.svg` - Khepri storage path structure

### Changed

- **Documentation Improvements**:
  - Replaced ASCII diagrams with professional SVG graphics
  - `snapshot_recovery.svg` - Performance comparison visualization
  - `event_fanout.svg` - Multi-subscriber event delivery diagram
  - Updated `rebar.config` ex_doc with new guides organized into Core Concepts, Advanced Features, and Operations sections

## [0.4.0] - 2025-12-22

### Added

- **Enterprise Edition NIFs**: High-performance Rust NIFs with pure Erlang fallbacks
  - Community Edition (hex.pm) uses pure Erlang implementations
  - Enterprise Edition (git + Rust) gets 5-100x speedups for specific operations
  - Automatic fallback detection via `persistent_term`

- **reckon_db_crypto_nif** (Phase 1):
  - `nif_base58_encode/1` - Fast Base58 encoding for DIDs
  - `nif_base58_decode/1` - Fast Base58 decoding
  - Uses Bitcoin alphabet, ~5x faster than pure Erlang

- **reckon_db_archive_nif** (Phase 2):
  - `nif_compress/1,2` - Zstd compression with configurable level
  - `nif_decompress/1` - Zstd decompression
  - `nif_compress_batch/1,2` - Batch compression for multiple items
  - `nif_decompress_batch/1` - Batch decompression
  - ~10x faster than zlib, better compression ratios

- **reckon_db_hash_nif** (Phase 3):
  - `nif_xxhash64/1,2` - 64-bit xxHash with optional seed
  - `nif_xxhash3/1` - Modern xxHash3 (SIMD optimized)
  - `nif_partition_hash/2` - Hash to partition number
  - `nif_stream_partition/3` - Combined store+stream routing
  - `nif_partition_hash_batch/2` - Batch hashing for bulk ops
  - `nif_fnv1a/1` - FNV-1a for small keys
  - `nif_fast_phash/2` - Drop-in phash2 replacement

- **reckon_db_aggregate_nif** (Phase 3):
  - `nif_aggregate_events/2` - Bulk fold with tagged value semantics
  - `nif_sum_field/2` - Vectorized sum accumulation for numeric fields
  - `nif_count_where/3` - Count events matching field condition
  - `nif_merge_tagged_batch/1` - Batch map merge with tagged values
  - `nif_finalize/1` - Unwrap tagged values ({sum, N}, {overwrite, V})
  - `nif_aggregation_stats/1` - Event statistics (counts, unique fields)

- **reckon_db_filter_nif** (Phase 3):
  - `nif_filter_events/2` - Filter events by compiled predicate
  - `nif_filter_count/2` - Count matching events without collecting
  - `nif_compile_predicate/1` - Pre-compile filter predicates
  - `nif_partition_events/2` - Partition events by predicate (matching/non-matching)
  - `nif_first_match/2` - Find first matching event
  - `nif_find_all/2` - Find all matching events with indexes
  - `nif_any_match/2`, `nif_all_match/2` - Boolean aggregate predicates

- **reckon_db_graph_nif** (Phase 4):
  - `nif_build_edges/1` - Build edge list from event causation relationships
  - `nif_find_roots/1`, `nif_find_leaves/1` - Find root/leaf nodes
  - `nif_topo_sort/1` - Topological sort (Kahn's algorithm via petgraph)
  - `nif_has_cycle/1` - Detect cycles in causation graph
  - `nif_graph_stats/1` - Calculate node/edge/depth statistics
  - `nif_to_dot/1,2` - Generate Graphviz DOT format
  - `nif_has_path/2` - Check if path exists between nodes
  - `nif_get_ancestors/2`, `nif_get_descendants/2` - BFS path finding

### Changed

- **Build profiles**:
  - Added `enterprise` profile with Rust NIF compilation hooks
  - Added `enterprise_test` profile for testing with NIFs
  - Build with `rebar3 as enterprise compile` to enable NIFs

### Documentation

- Updated README with Enterprise/Community edition information
- Added NIF function documentation with academic references

## [0.3.1] - 2025-12-20

### Changed

- **Version padding**: Increased from 6 to 12 characters (`?VERSION_PADDING` macro)
  - Previous: 999,999 events per stream max (~2.7 hours at 100 events/sec)
  - Now: 999,999,999,999 events per stream max (~317 years at 100 events/sec)
  - Supports long-running neuroevolution, IoT, and continuous event streams

### Fixed

- **EDoc errors**: Removed backticks and markdown from EDoc comments (breaks hex.pm docs)

## [0.3.0] - 2025-12-20

### Added

- **Capability-Based Security** (`reckon_db_capability_verifier.erl`, `reckon_db_revocation.erl`):
  - Server-side verification of UCAN-inspired capability tokens
  - Ed25519 signature verification using issuer's public key from DID
  - Token expiration and not-before time validation
  - Resource URI pattern matching (exact, wildcard suffix, prefix)
  - Action permission checking with wildcard support
  - Token revocation management (ETS-based, gossip integration planned)
  - Issuer revocation for compromised identities
  - Content-addressed token IDs (CIDs) for revocation tracking
  - Comprehensive unit tests (13 verifier tests + 6 revocation tests)

This completes Phase 3 of the decentralized security implementation.
Client-side token creation is in reckon-gater, server-side verification is here.

### Changed

- **Documentation**: Replaced ASCII diagrams with SVG in README and guides

### Fixed

- **README API documentation**: Fixed incorrect function signatures
  - Subscriptions: Added missing `unsubscribe/3`, `get/2` functions
  - Snapshots: Fixed `load/3` → `load_at/3`, `delete/3` → `delete_at/3`, added `exists/2`, `exists_at/3`
  - Aggregator: Completely rewrote section - was showing non-existent API (`foldl/4`, `foldl_from_snapshot/4`)
- **guides/snapshots.md**: Fixed `load/3` → `load_at/3`, `delete/3` → `delete_at/3`, rewrote aggregator example
- **guides/cqrs.md**: Fixed subscription key usage in emitter group join
- **guides/subscriptions.md**: Fixed invalid map access syntax
- **guides/event_sourcing.md**: Fixed aggregator foldl signature (takes events list, not store/stream)

## [0.2.0] - 2024-12-19

### Added

- **End-to-end tests**: 24 comprehensive e2e tests for gater integration:
  - Worker registration (4 tests)
  - Stream operations via gater (9 tests)
  - Subscription operations (4 tests)
  - Snapshot operations (4 tests)
  - Load balancing (3 tests)
- **Subscriptions**: Added `ack/4` function for acknowledging event delivery

### Fixed

- **Gateway worker API compatibility**:
  - `get_version` now handles integer return correctly
  - Snapshot operations use correct function names (`save`, `load_at`, `delete_at`)
  - Subscription unsubscribe uses correct 3-arg version
- **Header conflicts**: Added `ifndef` guards for `DEFAULT_TIMEOUT` macro

### Changed

- **reckon-gater integration**: Updated to work with gater's pg-based registry (replacing Ra)
- **Test counts**: Now 72 unit + 53 integration + 24 e2e = 149 total tests

## [0.1.0] - 2024-12-18

### Added

- Initial release of reckon-db, a BEAM-native Event Store built on Khepri/Ra
- Event stream operations:
  - `append/4,5` - Write events with optimistic concurrency control
  - `read/5` - Read events from streams (forward/backward)
  - `get_version/2` - Get current stream version
  - `exists/2` - Check if stream exists
  - `list_streams/1` - List all streams in store
  - `delete/2` - Soft delete streams
- Subscription system:
  - Stream subscriptions - events from specific streams
  - Event type subscriptions - events by type across streams
  - Pattern subscriptions - wildcard stream matching
  - Payload subscriptions - content-based filtering
- Snapshot management:
  - `save/5` - Save aggregate state snapshots
  - `load/2,3` - Load latest or specific version snapshots
  - `list/2` - List all snapshots for a stream
  - `delete/3` - Delete old snapshots
- Aggregation utilities:
  - `foldl/4` - Fold over events with accumulator
  - `foldl_from_snapshot/4` - Fold starting from latest snapshot
- Cluster support:
  - UDP multicast discovery (LibCluster gossip compatible)
  - Automatic Khepri/Ra cluster formation
  - Node monitoring and failover
  - Leader election and tracking
- Emitter pools for high-throughput event delivery
- Partitioned writers for concurrent stream writes
- BEAM telemetry integration with configurable handlers
- Comprehensive test suite (72 unit + 53 integration tests)
- Educational guides:
  - Event Sourcing fundamentals
  - CQRS patterns
  - Subscriptions usage
  - Snapshots optimization

### Dependencies

- Khepri 0.17.2 - Raft-based distributed storage
- Ra 2.16.12 - Raft consensus implementation
- Telemetry 1.3.0 - BEAM telemetry for observability
