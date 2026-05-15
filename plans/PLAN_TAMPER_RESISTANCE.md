# Plan: Tamper Resistance for ReckonDB 2.1.0

**Status:** Design / Open questions resolved / Not Started
**Created:** 2026-05-15
**Last Updated:** 2026-05-15
**Target release:** `reckon-db` 2.1.0, `reckon-gater` 2.1.0, `evoq` 1.15.0, `reckon-gateway` 0.2.0
**Spans repos:** `reckon-gater`, `reckon-db`, `evoq`, `reckon-gateway`

---

## Overview

Today's `reckon-db` event store stores events as plain Erlang records via `khepri:put/3`. The `#reckon_event{}` record carries no hash, no MAC, no signature, and no `prev_event_hash`. The read path deserializes and returns the binary as-is with no verification. Snapshots are equally trusted blobs.

The cryptographic primitives required (SHA-256, HMAC, Ed25519, constant-time compare) already ship in the `reckon_db_crypto_nif` crate — but they are wired only into capability-token verification, not the event path. Khepri/Ra provide WAL CRC, which detects corruption but not tampering.

This plan adds tamper-evidence to the event store via:

- **Option A — per-event HMAC** over canonical bytes, keyed by per-store secret (authenticity)
- **Option B — `prev_event_hash` chain** linking each event to the prior version in its stream (ordering integrity)
- **Verify-at-read** enforcement on every read surface (storage, emitter, snapshot load, aggregate rebuild)

A+B together provide: mutation-detection (HMAC), reordering/insertion/deletion-detection (chain), end-to-end coverage across the event lifecycle, no false sense of security from snapshot bypass.

Ed25519 signing (option C) and cross-region anchoring (option D) are explicitly deferred to later versions.

---

## Goals (in scope for 2.1)

1. Every event written via `reckon_db_streams:append/4` carries a verifiable HMAC and a `prev_event_hash` link to the prior version in its stream.
2. Every event read from storage is verified before delivery; verification failures surface as a non-retriable `{integrity_violation, _}` error.
3. Every snapshot carries an HMAC and an `anchor_hash` recording the chain hash at the snapshot's version. Snapshots that fail verification on load are refused; the aggregate falls back to full replay.
4. The emitter passes verified events to internal subscribers (no re-verification per fan-out); external subscribers (gateway) receive the chain hash but not the HMAC.
5. `evoq_aggregate:rebuild_from_events/3` walks the chain during replay and aborts on integrity violation. `evoq_dispatcher` classifies `integrity_violation` as terminal, not retriable.
6. The gateway proto carries the chain hash on egress, strips the HMAC, and surfaces verification failures as a defined gRPC status code.
7. Pre-2.1 events remain readable; new writes carry integrity fields. Each stream tracks a `chain_start_version` watermark below which verification is skipped.
8. Single per-store HMAC key, loaded at startup from env or sealed file into `persistent_term`. Key-ID byte reserved in the schema for future rotation.
9. Tamper-simulation tests at every layer; performance regression tests; migration roundtrip tests.

## Non-goals (deferred)

- **Ed25519 signatures on events.** Deferred to 2.2 or later. The schema reserves a field but the writer does not populate it in 2.1.
- **Key rotation.** Single key per store in 2.1. The `mac` field is structured `{KeyId, MacBytes}` so rotation is a format-compatible addition later.
- **Cross-region / external anchoring.** No external witness, no certificate transparency, no published chain roots. Deferred.
- **Polyglot canonical encoding (CBOR / explicit).** Internal canonical form uses `term_to_binary/2` with the `deterministic` flag. Cross-language verification by external consumers is deferred until there's a concrete requirement.
- **Retroactive integrity backfill on legacy events.** Backfilled MACs would prove nothing; `chain_start_version` is the honest answer.
- **Tamper-detection for capability tokens and metadata other than events/snapshots.** Out of scope; capability path already has its own crypto.

---

## Key design decisions (already settled in design discussion)

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | A+B together, not either alone | A alone misses deletion/reordering; B alone is forgeable by an attacker who controls the disk. Together they cover both threat surfaces. |
| 2 | HMAC, not signature, in 2.1 | Symmetric key, no public-key distribution problem, sub-µs cost with NIF. Ed25519 deferred. |
| 3 | Verify at read, not just at write | Write-time integrity without read-time enforcement is theatre. Read is the chokepoint. |
| 4 | Chain check (B-half) is keyless | Projections and external consumers verify chain continuity without holding the secret. |
| 5 | `term_to_binary(_, [deterministic])` for canonical encoding | OTP 26+ guarantees canonical map encoding under this flag. Cross-language portability is a non-goal in 2.1. |
| 6 | `chain_start_version` watermark migration | Don't backfill MACs on legacy events — backfill proves nothing. Mark the boundary honestly. |
| 7 | Internal trust boundary at the storage layer | Once verified post-read, the emitter delivers without re-verifying for every subscriber. Projections may re-verify chain (cheap, keyless). |
| 8 | External wire format strips MAC, keeps chain | MAC is a symmetric secret; leaking it to gateway clients defeats its purpose. External consumers verify the chain only. |
| 9 | `integrity_violation` is non-retriable | Distinct from `wrong_expected_version`. Must not enter the rebuild-and-retry loop. |
| 10 | Single key MVP, key-ID slot reserved | Format-forward: write `{1, MacBytes}` from day one, no schema break when rotation lands in 2.2. |

---

## Schema changes

### `#reckon_event{}` (in `reckon-gater/include/reckon_gater_types.hrl`)

Current fields preserved. Three additions:

```erlang
-record(event, {
    %% existing fields unchanged ...
    event_id              :: binary(),
    event_type            :: binary(),
    stream_id             :: binary(),
    version               :: non_neg_integer(),
    data                  :: map() | binary(),
    metadata              :: map(),
    tags                  :: [binary()] | undefined,
    timestamp             :: integer(),
    epoch_us              :: integer(),
    data_content_type     :: binary(),
    metadata_content_type :: binary(),

    %% new in 2.1 — all undefined for pre-2.1 (legacy) events
    prev_event_hash       :: binary() | undefined,
    %% sha256 over canonical_encode(event_minus_integrity_fields) for version 0
    %% sha256 over canonical_encode(event_minus_integrity_fields ++ prev_event_hash) for version >= 1

    mac                   :: {KeyId :: non_neg_integer(), MacBytes :: binary()} | undefined,
    %% HMAC-SHA256 over canonical_encode(event_minus_mac)
    %% KeyId is reserved for future rotation; 2.1 always writes KeyId = 1

    signature             :: binary() | undefined
    %% reserved for Ed25519 in 2.2+; not populated in 2.1
}).
```

### `#reckon_snapshot{}` (same header)

```erlang
-record(snapshot, {
    %% existing fields preserved
    stream_id  :: binary(),
    version    :: non_neg_integer(),
    state      :: term(),
    timestamp  :: integer(),

    %% new in 2.1
    anchor_hash :: binary() | undefined,
    %% prev_event_hash of the event at `version`, captured at snapshot time

    mac         :: {KeyId :: non_neg_integer(), MacBytes :: binary()} | undefined
    %% HMAC-SHA256 over canonical_encode(snapshot_minus_mac), domain-tagged "snap|"
}).
```

### Canonical encoder

New module `reckon_gater_canonical` (in `reckon-gater`):

```erlang
-module(reckon_gater_canonical).
-export([encode/1, encode_for_mac/2, encode_for_chain/2]).

%% Canonical encoding: term_to_binary with deterministic flag.
%% Domain-separation tags prevent cross-protocol confusion.
encode(Term) -> term_to_binary(Term, [deterministic, {minor_version, 2}]).

encode_for_mac(event, EventMinusMac)    -> [<<"evt|">>, encode(EventMinusMac)];
encode_for_mac(snapshot, SnapMinusMac)  -> [<<"snap|">>, encode(SnapMinusMac)].

encode_for_chain(EventMinusIntegrity, PrevHash) ->
    [<<"chain|">>, encode(EventMinusIntegrity), PrevHash].
```

### Integrity helpers

New module `reckon_gater_integrity` (in `reckon-gater`):

```erlang
-module(reckon_gater_integrity).
-export([
    compute_chain_hash/2,
    compute_mac/2,
    verify_event/2,
    verify_snapshot/2,
    is_legacy_event/1
]).

%% Pure functions over records + key material.
%% NIF-accelerated via reckon_db_crypto_nif when available; pure-Erlang fallback otherwise.
```

The verifiers return `ok | {error, mac_mismatch} | {error, chain_mismatch} | {error, missing_integrity}`.

---

## Per-layer implementation (topological order)

### Layer 1 — `reckon-gater` (foundational)

**Files:**
- `include/reckon_gater_types.hrl` — schema additions above
- `src/reckon_gater_canonical.erl` — new
- `src/reckon_gater_integrity.erl` — new

**Tests:**
- Canonical encoder determinism: encode the same map 1000x, compare bytes
- Chain hash correctness vs hand-computed reference vectors
- MAC verify roundtrip
- Legacy-event detection (prev_event_hash =:= undefined)
- Domain-separation: an `evt|`-prefixed MAC must not validate against `snap|`

**Release:** `reckon-gater` 2.1.0 must ship before any other repo can consume.

### Layer 2 — `reckon-db` write path

**Files touched:**
- `src/reckon_db_streams.erl` — `create_event_record/5`, `append/4`
- `src/reckon_db_app.erl` or store-startup path — load HMAC key into `persistent_term`
- Store config schema (wherever the per-store config record lives) — add `integrity_key_source`

**Behaviour:**
1. On store startup, load the HMAC key from env var `RECKON_DB_INTEGRITY_KEY_<STORE>` or sealed file path; refuse to start if integrity is enabled and the key is missing.
2. On `append/4` (leader side, before Raft proposal):
   - Assign version (existing logic)
   - Fetch prev tip hash from stream tail (cached in writer process for hot streams)
   - Compute `prev_event_hash` per canonical encoder
   - Compute `mac` using loaded key
   - Set both fields on the event record before serialization
3. The serialized command sent through Raft already contains the fixed integrity fields; followers verify identically by re-running the same canonical encoding (sanity check, not necessary for correctness but cheap).

**Tests:**
- Append → read returns event with integrity fields populated
- Tip-hash caching: appending N events, verifying chain at every step matches re-fetched tip
- Concurrent appends from two leader candidates: Raft serializes; both versions chain correctly; no orphan hashes

### Layer 3 — `reckon-db` read path

**Files touched:**
- `src/reckon_db_streams.erl` — `convert_result_to_event/2`, `read_stream/2,3`, plus all callers
- `src/reckon_db_subscriptions.erl` (or equivalent) — catch-up reads must also verify

**Behaviour:**
1. On every storage read of an event with `prev_event_hash =/= undefined`:
   - Compute expected MAC from canonical bytes + key
   - Compare against stored MAC (constant-time)
   - Compare event's `prev_event_hash` against the running tip hash for that stream (or fetch the predecessor's hash if running tip not cached)
2. On failure: return `{error, {integrity_violation, #{stream_id => SID, version => V, kind => mac | chain}}}`
3. On legacy events (`prev_event_hash =:= undefined`): skip verification, return event as-is. Caller responsible for treating these as "pre-integrity."

**New API:**
- `reckon_db:read_stream(Store, StreamId, Opts)` — `Opts` may include `verify => strict | skip_legacy | skip_all`; default `skip_legacy`.
- `reckon_db:integrity_check_stream(Store, StreamId)` — admin operation that walks the chain end-to-end and reports any breaks. Useful for audit and for migration validation.

**Tests:**
- Roundtrip: append → read → verify passes
- Tamper simulation: directly mutate Khepri value out-of-band, read → returns integrity_violation
- Tamper simulation: directly mutate one event's `prev_event_hash`, read → returns chain_mismatch on the *next* event
- Legacy-events-only stream: reads return events untouched (skip_legacy)

### Layer 4 — `reckon-db` snapshot path

**Files touched:**
- `src/reckon_db_snapshots.erl` — `save/3`, `load/2`
- `src/reckon_db_snapshots_store.erl` (or equivalent) — `put/2`, `get_latest/2`

**Behaviour:**
1. On `save/3`:
   - Compute `anchor_hash` = chain hash of the event at `version` (fetch from the stream)
   - Compute snapshot `mac` over canonical encoding (snapshot-domain-tagged)
   - Persist with both fields
2. On `load/2`:
   - Verify snapshot `mac`
   - Verify that the recorded `anchor_hash` matches the actual chain hash at `version`
   - On either failure: refuse the snapshot, fall back to replay from `chain_start_version`
   - Emit a telemetry event `[reckon, db, snapshot, integrity_violation]`

**Tests:**
- Save → load roundtrip passes
- Tamper snapshot's state field → load fails on MAC mismatch
- Tamper the underlying stream → load fails on anchor_hash mismatch
- Aggregate falls back to full replay on snapshot rejection

### Layer 5 — `reckon-db` emitter

**Files touched:**
- `src/reckon_db_emitter.erl` (or equivalent subscription-delivery path)

**Behaviour:**
- The emitter receives already-verified events from the read path. It marks them with an internal `verified_within_cluster` flag (record or process-dictionary; not on the wire).
- Internal BEAM subscribers receive the event as-is. They may re-verify the chain (keyless, cheap) for defense-in-depth; not required.
- External subscribers (anything that crosses a node boundary other than via Erlang distribution) receive the event with the MAC stripped. The `prev_event_hash` is preserved. This includes the gateway egress path.

**Tests:**
- Subscription roundtrip: append → catch-up subscription delivers verified event
- External-subscriber-shape delivery (simulated via boundary helper): MAC field is empty

### Layer 6 — `evoq` aggregate rebuild + dispatcher error class

**Files touched:**
- `src/evoq_aggregate.erl` — `rebuild_from_events/3`, `load_or_init/3`
- `src/evoq_dispatcher.erl` — error classification, retry policy

**Behaviour:**
1. `rebuild_from_events/3` walks events in order from `chain_start_version` (or snapshot anchor). For each:
   - Verify chain (keyless, cheap)
   - Verify MAC (if key is available on the node)
   - On any failure: abort with `{error, {integrity_violation, _}}`. Do NOT continue replay.
2. `evoq_dispatcher` recognises `{error, {integrity_violation, _}}` as a new error class:
   - Distinct from `{error, wrong_expected_version}` (the three forms recently fixed in 1.14.4) — must NOT enter retry loop
   - Distinct from `{error, unknown_command}` and `{error, stream_not_found}`
   - Surfaces immediately to the caller; logs at error level; emits telemetry `[evoq, dispatch, integrity_violation]`

**Tests:**
- Replay with intact chain → success
- Replay with planted tamper on event N → integrity_violation, no retry, error to caller
- Dispatcher: tamper detected during pre-execute replay → command never reaches `execute/2`
- Compose with idempotency: idempotency hit returns cached events, which still verify

### Layer 7 — `reckon-gateway` wire format

**Files touched:**
- `priv/protos/reckon_streams.proto` — add `prev_event_hash` field; do NOT add `mac`
- `priv/protos/reckon_subscriptions.proto` — same
- `priv/protos/reckon_snapshots.proto` — add `anchor_hash`; do NOT add `mac`
- Gateway handlers — egress: strip MAC, populate `prev_event_hash`; ingress: do not accept MAC fields from clients
- gRPC status mapping: define `INTEGRITY_VIOLATION` (or use `FAILED_PRECONDITION` with structured detail message)

**Behaviour:**
- Outbound events carry `prev_event_hash`; clients may verify chain locally with SHA-256
- The MAC is never exposed over the wire — defends the symmetric secret
- Future option C (Ed25519 signature) would attach an external-facing `signature` field for cross-trust-boundary authenticity; the proto reserves space for it

**Tests:**
- gRPC roundtrip: append → subscribe → received message contains `prev_event_hash`, does NOT contain MAC field
- Tampered server-side event → gateway delivers `INTEGRITY_VIOLATION` status to client (does not silently drop or pretend healthy)

---

## Migration via `chain_start_version` watermark

Each stream gets a per-stream `chain_start_version :: non_neg_integer() | undefined` recorded once at the moment integrity is enabled for that store/stream.

- Streams that exist before 2.1 enablement: `chain_start_version` set to `next_unwritten_version` at enablement time
- Streams created after enablement: `chain_start_version = 0`
- Reads of events with `version < chain_start_version[stream]` skip verification entirely and return as legacy
- Reads of events with `version >= chain_start_version[stream]` require integrity fields; missing fields = integrity_violation

**Enablement procedure (per store):**

1. Operator generates 32-byte HMAC key, places it in env/sealed file
2. Operator restarts store with `integrity_enabled => true` in config
3. Store reads existing high-water-mark per stream, persists each as `chain_start_version`
4. From this point forward, all new writes carry integrity fields
5. Operator can run `reckon_db:integrity_check_stream/2` against any stream to audit the chain from `chain_start_version` to current tip

**Disabling integrity** is intentionally not supported as a one-way operation — once a stream has integrity-bearing events, they remain verifiable. To "disable" requires creating a new store and copying with verification stripped, which is a much louder operation.

---

## Key management MVP (2.1 scope)

- One symmetric HMAC key per store, 32 random bytes (256 bit)
- Sources, in priority order:
  1. Environment variable `RECKON_DB_INTEGRITY_KEY_<STORE_NAME>` (base64-encoded)
  2. Sealed file path from config `{integrity_key_file, "/path/to/key"}` (file mode 0600 required, refused otherwise)
  3. Vault/KMS integration — deferred to 2.2
- Key is loaded into `persistent_term` under `{reckon_db, integrity_key, StoreName}` at store startup
- Key never appears in logs, telemetry, error messages, or process state dumps
- All Raft followers must hold the same key — config distribution is the operator's responsibility (same as any other store config secret)

**Key-ID slot is reserved in the `mac` tuple** as `{KeyId, MacBytes}`. 2.1 always writes `KeyId = 1`. 2.2 will add a keyring per store, allow rotation via "introduce key 2, write new events with key 2, retain key 1 for verifying old events."

---

## New error class — `integrity_violation`

Defined in `reckon-gater` types header so all repos can pattern-match:

```erlang
-type integrity_failure_kind() :: mac_mismatch
                                | chain_mismatch
                                | missing_integrity
                                | snapshot_anchor_mismatch
                                | snapshot_mac_mismatch.

-type integrity_violation() :: {integrity_violation, #{
    layer       := storage | snapshot | replay | gateway,
    stream_id   := binary(),
    version     := non_neg_integer() | undefined,
    kind        := integrity_failure_kind(),
    context     => map()
}}.
```

**Propagation rules:**

- Storage layer: returns the error tuple
- Emitter: refuses to deliver, returns error to caller of subscribe; logs at error level
- evoq aggregate rebuild: aborts rebuild, returns the error
- evoq dispatcher: classifies as terminal, returns to caller; **never** triggers `wrong_expected_version`-style rebuild-and-retry
- Gateway: maps to gRPC `FAILED_PRECONDITION` with structured detail

**Telemetry events emitted:**

- `[reckon, db, integrity, violation]` (storage layer)
- `[reckon, db, snapshot, integrity, violation]` (snapshot)
- `[evoq, dispatch, integrity, violation]` (evoq)
- `[reckon, gateway, integrity, violation]` (gateway)

These should be wired to alerting in any production deployment.

---

## Test plan

### Per-layer unit tests
Per the per-layer sections above. Each layer's PR must ship its own tests.

### Cross-layer integration tests (in `reckon-db/test/integration/`)

- **End-to-end roundtrip:** append (with key) → read (verify) → subscribe (verify) → rebuild aggregate (verify) → snapshot save → reload snapshot → continue replay
- **Tampered storage simulation:**
  - Plant a mutation directly into Khepri via `khepri:put/3` bypass
  - Reader returns `integrity_violation`
  - Emitter refuses delivery
  - Aggregate rebuild aborts
- **Tampered snapshot simulation:** mutate snapshot blob; loader rejects; aggregate falls back to replay
- **Legacy + integrity mixed stream:** events 0–10 are pre-2.1 legacy; events 11+ have integrity. `read_stream/3` with `verify => skip_legacy` returns all; with `verify => strict` returns the legacy ones as integrity_violation (or filters, configurable)
- **Migration roundtrip:** create v2.0-style stream, enable integrity, write more events, verify chain from `chain_start_version` works
- **Key absence:** start a store with integrity enabled but no key configured → store refuses to start with a clear error

### Performance regression tests

- Append throughput with vs without integrity, under representative payload sizes (1KB, 10KB, 100KB)
- Read throughput with vs without verification, including replay rebuild scenarios
- Acceptance criterion: integrity overhead < 10% of baseline append throughput, < 15% of baseline read throughput, with `reckon_db_crypto_nif` loaded

### Property-based tests (PropEr)

- For any sequence of N appends, the chain verifies end-to-end
- For any single byte mutation at any position in storage, verification detects it (mutation testing)
- Concurrent appends from multiple writers (when Raft serializes them) produce a valid chain

### Tamper-detection coverage report

Generated as part of CI:
- For each tampering location (event data, event metadata, prev_event_hash, mac, snapshot state, snapshot anchor_hash, snapshot mac), confirm that verification catches it at every read surface (storage, emitter, snapshot load, aggregate rebuild, gateway).

---

## Release sequencing

| Step | Repo | Version | Depends on |
|------|------|---------|------------|
| 1 | `reckon-gater` | 2.1.0 | — |
| 2 | `reckon-db` | 2.1.0 | gater 2.1.0 |
| 3 | `evoq` | 1.15.0 | — (gater types are pulled in via reckon_evoq; evoq itself is gater-independent in current design — verify) |
| 4 | `reckon-evoq` | 2.1.0 | evoq 1.15.0, gater 2.1.0 |
| 5 | `reckon-gateway` | 0.2.0 | gater 2.1.0, reckon-db 2.1.0 |
| 6 | `reckon-ecosystem` | 0.3.0 docs refresh | all of the above |

Each release in the chain ships its own CHANGELOG entry naming the tamper-resistance work.

Hex publication order matches the version order above (gater first, then anything depending on gater).

Downstream consumers (Hecate, Macula-using applications) must update `rebar.config` in lockstep — at minimum bump `reckon_db` and `reckon_evoq`. Operators of those apps must follow the migration procedure (generate key, set env, restart with `integrity_enabled => true`).

---

## Resolved open questions

### 1. `chain_start_version` watermark storage — RESOLVED

**Decision:** Dedicated Khepri tree node under the existing `[metadata]` root, lazily cached in writer process.

**Path:** `[metadata, integrity, chain_start, StreamId]` → `non_neg_integer()`

**Why this answer:**
- The `[metadata]` Khepri root already exists (`reckon-db/include/reckon_db.hrl:26: -define(METADATA_PATH, [metadata])`), but is currently unused for stream-level concerns. Clean separation from `[streams, ...]`, `[snapshots, ...]`, `[subscriptions, ...]`.
- Adding fields to `#reckon_event{}` at version 0 only would couple migration state to event schema — and would not survive a Khepri compaction that drops version-0 records via TTL or scavenging.
- A single store-config map would grow unbounded with stream count and serialize all writer-process reads through it.

**Caching:** Writer process keeps `chain_start :: #{StreamId => Version}` in process state; populated lazily on first append to a stream after process start; invalidated by any explicit re-enablement event (rare).

**Cost:** One extra Khepri get on first append to each stream per writer-process lifetime. Negligible.

### 2. Hot key reload vs restart-only — RESOLVED

**Decision:** Restart-only for 2.1. Hot rotation is part of the 2.2 key-rotation feature.

**Why:**
- Hot rotation introduces a race between the rotation operation and in-flight Raft proposals carrying events under the old key. Solvable but expensive — requires per-event key-ID stamping, a quiescence protocol, and rotation-aware verifiers. All of that is already part of the 2.2 keyring + rotation design.
- For 2.1 a planned restart is a known, controlled operational procedure with no race surface. Operators rotate at maintenance windows, which is what they would do anyway for key changes.
- Keeps the 2.1 surface area smaller and shippable.

### 3. Gateway client integrity-algorithm discovery — RESOLVED

**Decision:** Document the algorithm in `reckon-gateway` README *and* expose it via a `GetServerInfo` gRPC method returning a machine-readable identifier.

**Algorithm identifier string (initial):** `sha256-deterministic-etf-v1`

This string encodes:
- The chain hash function (`sha256`)
- The canonical encoding (`deterministic-etf` = `term_to_binary/2` with `deterministic` flag, ETF version 2)
- The format version (`v1`) — bumps if the canonical encoding ever changes

**GetServerInfo response shape (proto sketch):**

```proto
message ServerInfo {
  string  reckon_db_version          = 1;
  string  reckon_gateway_version     = 2;
  string  integrity_algo             = 3;   // e.g. "sha256-deterministic-etf-v1"
  bool    integrity_enabled          = 4;
  uint32  hmac_key_id                = 5;   // 0 if integrity_enabled = false
  string  api_compatibility_version  = 6;
}
```

The `hmac_key_id` informs the client of the *current* writer key ID so external auditors who hold the public chain-verification material can also know which HMAC key was active at a given point. (The key itself is never returned — only the ID.)

### 4. Default `verify` mode for `read_stream/3` — RESOLVED

**Decision:** `verify => skip_legacy` is the default for 2.1.0. Plan to flip to `verify => strict` in 3.0.0.

**Why:**
- 2.1 ships into installed bases with legacy events (`prev_event_hash =:= undefined`). A `strict` default breaks every existing read on upgrade — that is unacceptable.
- `skip_legacy` reads pre-2.1 events transparently (returns them untouched) and verifies post-2.1 events strictly. Verification failures on post-2.1 events still surface as `integrity_violation`.
- Operators opt into stricter modes per call (`Opts = #{verify => strict}`) or per store (`store config: default_verify => strict`).
- 3.0 plans the flip after operators have had a release cycle to remediate legacy streams (re-key, re-snapshot, archive-and-rewrite where they choose to invest).
- Telemetry: emit `[reckon, db, read, legacy_event_returned]` on every legacy-event read so operators can monitor remediation progress without surprise.

**Mode semantics:**

| Mode | Pre-2.1 event | Post-2.1 event, intact | Post-2.1 event, tampered |
|------|---------------|------------------------|--------------------------|
| `skip_all` | returned | returned, no verify | returned, no verify (dangerous; do not use in production) |
| `skip_legacy` *(default)* | returned, telemetry emitted | returned, verified | `integrity_violation` |
| `strict` | `integrity_violation` (missing) | returned, verified | `integrity_violation` |

### 5. evoq / reckon-gater coupling — RESOLVED

**Investigation finding:** evoq does NOT couple to `reckon-gater` directly. The path is:

```
evoq_aggregate:rebuild_from_events
  → evoq_event_store:read   (configurable adapter behaviour)
     → reckon_evoq_adapter:read   (from reckon_evoq package)
        → reckon_db_streams:read_stream   (gater types)
```

The adapter does `#event{}` (gater record) → `#evoq_event{}` (evoq record) translation in `reckon_evoq_adapter:event_to_evoq/1`. The map produced by `evoq_event_store:event_to_map/1` then flows to projections and aggregate `apply/2` callbacks.

**Decision:** Verification chokepoint is `reckon-db` storage layer. evoq stays storage-agnostic.

Concretely:

- `reckon-db` performs MAC + chain verification on every read. Returns either the event (verified) or `{error, {integrity_violation, _}}`.
- `reckon_evoq_adapter` propagates the error tuple faithfully. No verification logic in the adapter.
- `evoq` only needs **two** changes:
  - Add `prev_event_hash :: binary() | undefined` field to `#evoq_event{}` and to the map produced by `event_to_map/1`. This lets projections do keyless chain re-verification as defense-in-depth (cheap, no key needed, and the field survives the boundary to user projection code).
  - Recognize `{error, {integrity_violation, _}}` in `evoq_dispatcher` as a terminal error class — non-retriable, distinct from `wrong_expected_version`.
- The `mac` field is NOT propagated through `#evoq_event{}` or the map — it stays inside the gater record at the storage layer where the key lives.

**Implication for the per-layer sequencing:** Layer 6 (evoq) becomes smaller than originally drafted. The aggregate replay loop does not need to know about MACs at all. Projections can opt into chain verification; the framework doesn't enforce it.

### 6. NIF fallback performance — RESOLVED

**Investigation finding:** OTP's `crypto:hash/2` and `crypto:mac/4` (HMAC) are themselves C NIFs backed by OpenSSL. They are **not** pure-Erlang and they are **not** slow.

Concretely:

- `crypto:hash(sha256, Bin)` — OpenSSL SHA-256, ~0.3–3 µs for typical 1–10 KB event payloads
- `crypto:mac(hmac, sha256, Key, Bin)` — OpenSSL HMAC-SHA256, ~0.5–5 µs for the same range
- Per-event total overhead (compute MAC + compute chain hash + verify both on read) — ~5–10 µs

**The existing `reckon_db_crypto_nif` (Rust) is not on the critical path for 2.1.** It exposes `verify_ed25519`, `hash_sha256`, `secure_compare`, but no `hmac_sha256`. Adding HMAC to the Rust NIF would yield minimal additional speedup over OTP's OpenSSL-backed primitives.

**Decision:** Use OTP `crypto:hash/2`, `crypto:mac/4`, and `crypto:hash_equals/2` (constant-time compare in OTP 26+) directly. No NIF extension required for 2.1.

**Performance budget verification:**

- 1,000 events/sec sustained writes: ~10 ms/sec ≈ 1% of one core for integrity overhead
- 10,000 events/sec sustained writes: ~100 ms/sec ≈ 10% of one core
- Catch-up replay at 100,000 events/sec: ~1 second/sec ≈ 100% of one core (worth optimizing if it becomes a bottleneck; can parallelize verification across streams)

All within the 10%/15% acceptance criteria for normal operating workloads.

**Implication for the implementation plan:** the integrity helpers in `reckon_gater_integrity` use OTP `crypto` directly. The plan does NOT require any change to the `reckon-nifs` package for 2.1. (The Rust NIF crate remains useful for capability-token Ed25519 verification, which is a different code path.)

### 7. Capability tokens × integrity interaction — RESOLVED

**Decision:** They are independent, orthogonal trust layers. Document explicitly.

**Authoritative layering:**

```
1. Capability layer    — controls WHO may invoke the public reckon-gater API
                         (authorization at the gateway, capability-token verification)

2. API layer           — provides reckon_db_streams:append/4 etc.;
                         the only sanctioned entry to the event store

3. Integrity layer     — every event passing through the API layer is
                         MAC-signed and chained at the moment of storage
```

**The trust boundary for integrity is the BEAM process holding the HMAC key, NOT the capability layer.** This means:

- A holder of a valid capability token, calling the API, produces events with valid MAC and chain — because the API path always invokes integrity computation. Capability authorization is necessary but not sufficient to produce integrity-bearing events; the API path is what enforces integrity.
- An actor with **direct write access to Khepri** (bypassing the API — e.g., operator with shell access on a Raft node) can write events that lack integrity fields or carry forged ones. Detection happens at the next read via `integrity_violation`. The system surfaces tampering; it does not prevent privileged Khepri writes.
- Backup/restore operations must preserve the integrity fields verbatim. Future backup tooling (out of scope here) must not rewrite or normalize events in flight.

**What this is NOT:**

- Integrity does not authenticate the *caller* of an append. That is the capability layer's job.
- Capability does not detect post-write tampering. That is integrity's job.

**Documentation requirement:** This trust-layering note must appear in:
- `reckon-db` README under a new "Trust boundaries" heading
- `reckon-gateway` README in the security section
- `reckon-ecosystem/guides/architecture.md` Tamper Resistance subsection (already drafted, just needs the explicit capability-vs-integrity callout)

---

## Plan adjustments arising from resolutions

The following sections of this plan need lightweight updates to reflect the resolutions above. Items marked here so they aren't lost when implementation starts:

- **Layer 1 (gater):** integrity helpers use `crypto:hash/2` and `crypto:mac/4` directly. No dependency on `reckon-nifs`.
- **Layer 4 (snapshot):** `anchor_hash` stores `prev_event_hash` of the event *at* `version`, not "of the chain at version" — clarify wording.
- **Layer 6 (evoq):** scope reduced — only `#evoq_event{}` field addition, `event_to_map/1` plumbing, and `evoq_dispatcher` error classification. No replay-time verification (that already happened in the storage read).
- **Layer 7 (gateway):** add `GetServerInfo` RPC returning the integrity algorithm identifier and current HMAC key ID.
- **Migration:** `[metadata, integrity, chain_start, StreamId]` is the canonical path.
- **Default `verify` mode:** `skip_legacy` documented as the 2.1 default; `strict` documented as the 3.0 target. New telemetry event `[reckon, db, read, legacy_event_returned]` for remediation tracking.
- **Trust boundary docs:** README updates in `reckon-db`, `reckon-gateway`, plus architecture guide in `reckon-ecosystem`.

---

## Estimated effort (single developer, focused)

- Layer 1 (gater): 2–3 days
- Layer 2–4 (db write/read/snapshot): 4–5 days
- Layer 5 (emitter): 1 day
- Layer 6 (evoq): 2 days
- Layer 7 (gateway): 2 days
- Migration + key plumbing: 2 days
- Test plan execution: 3–4 days
- Release coordination + docs: 2 days

**Total:** 18–21 working days for a single developer. Compresses with parallel layer work if multiple developers; the gater→db→evoq→gateway sequence has hard dependencies.

For a passion-project cadence (evenings/weekends), realistic shipping window is 8–12 weeks elapsed time.

---

## Related plans

- [PLAN_FUTURE_RESEARCH.md](PLAN_FUTURE_RESEARCH.md) — research topics, including consistency-related items that may interact with integrity
- [PLAN_PLUGGABLE_BACKENDS.md](PLAN_PLUGGABLE_BACKENDS.md) — alternative storage backends; integrity layer must be implementable against each backend

## Cross-repo plan visibility

This plan should be linked from the master plan index at `/home/rl/work/codeberg.org/macula-internal/macula-architecture/plans/PLAN_MACULA_ROOT.md` so downstream Macula and Hecate work is aware of the upcoming reckon-db 2.1.0 release.
