# DESIGN: Stream Namespace — structural aggregate-type discrimination

**Status:** Draft / RFC (no code — decision required before implementation)
**Date:** 2026-06-08
**Author:** design discussion (rl + apprentice)
**Affects:** reckon-db core storage layout (Khepri paths). Breaking — requires a migration.

---

## 1. Problem

A reckon-db **store** is a Khepri store = a single Ra (Raft) cluster. Today a
store holds **N aggregate types** in one flat namespace:

```
[streams, StreamId, PaddedVersion] -> #event{}
```

`StreamId` is an **opaque binary** (`{prefix}-{uuid}`, or a `$ns:name` system
stream, or the `_dcb` pseudo-stream). The aggregate *type* is encoded only as a
**string prefix** inside the opaque id — there is no structural boundary. So a
single tenant store (`parksim_leuven_store`) holds `ride-*`, `vehicle-*`,
`session-*`, `fleet-*` streams all intermingled under `[streams, *]`,
distinguished by nothing but their leading characters.

This is inherited from erl-esdb / EventStoreDB (opaque ids, category = string
convention, `$ce-{category}` projections derive type by string-split).

### What it does and does NOT cost

It does **not** break consistency. The consistency unit is the
*aggregate = stream version* (`expected_version == current_version` on one
stream). Mixing types in a store cannot violate per-stream concurrency.

What it costs is **scoping**:

1. **Every type-scoped operation is a whole-store scan.** There is no
   `[streams, type]` subtree to navigate — `list_streams/1`,
   `read_by_tags/4`, `read_by_event_types/3`, and the (now-removed) causation
   queries all do `khepri:get_many([streams, *, has_data])` + filter in Erlang.
   On a 29k+-stream store that is O(whole store) for a question that is
   logically O(one type). See the companion problem in §9.
2. **Store-level ops couple all types into one blast radius.** DCB's `_dcb`
   pseudo-stream is per-store, so its consistency boundary spans *every*
   aggregate type. Scavenge, retention, backup, and the Ra cluster's
   liveness are all per-store too. The types don't just look mixed — they
   share a fate.

The second point is the real smell: the **operational and consistency scope is
undifferentiated**, not the bytes being adjacent.

---

## 2. Current layout (facts)

From `include/reckon_db.hrl` + `src/reckon_db_streams.erl`:

| Path | Holds |
|------|-------|
| `[streams, StreamId, PaddedVersion]` | a recorded `#event{}` (`VERSION_PADDING = 12`) |
| `[streams, <<"_dcb">>, SeqKey]` | DCB pseudo-stream events (`DCB_SEQ_KEY_WIDTH = 20`) |
| `[by_tag, Tag, SeqKey]` | **the only write-maintained secondary index** — DCB events only |
| `[snapshots, …]` | per-stream snapshots |
| `[links, StoreId, Name]` | link (materialized derived-stream) definitions |
| `[metadata, integrity, chain_start, StreamId]` | per-stream tamper watermark |

`StreamId` shapes (validated by `reckon_gater_stream_id`):
- **User:** `<prefix>-<hex>`, `prefix = [a-z]{1,32}` (`prefix_of/1` extracts it)
- **System:** `$ns:name`
- **DCB:** `_dcb` (reserved)

Stores are independent Ra clusters started per `store_id` by
`reckon_db_sup:start_store/1` → booting N at once is the readiness/retry storm
(`ANTIPATTERNS_RELEASE.md` "Dynamic BEAM Node Name"). This is why the project
settled on **one store per service/tenant, N types within**.

---

## 3. The three models

### Model A — store-per-aggregate-type (the original vision)

`1 CMD app = 1 aggregate_type = 1 store = 1 stream_type`. Each type is its own
Ra cluster; every `[streams, *]` is homogeneous.

- ✅ Maximum isolation; homogeneous namespace; per-type ops are trivial.
- ❌ N types in a bounded context = N Ra clusters = the boot storm.
- ❌ Finer-grained than the bounded context: a Division's read models and any
  cross-type DCB now span multiple stores/clusters.
- **Verdict:** rejected — the granularity is below the natural store boundary
  (the bounded context / tenant) and is what produced the operational pain.

### Model B — flat opaque id, prefix convention (current)

`[streams, StreamId, Version]`, type = string prefix.

- ✅ Store is maximally domain-agnostic; simplest paths; EventStoreDB-faithful.
- ✅ Category views available via **links** (materialized `$ce`-style streams) —
  reckon-db already does this correctly (`reckon_db_links`).
- ❌ No structural type boundary → type-scoped ops are whole-store scans.
- ❌ Store-level consistency/ops scope is undifferentiated across types.
- **Verdict:** correct boundary (store = tenant/Division), wrong discriminator
  (string prefix instead of structure).

### Model C — structural aggregate-type subtree (proposed)

`[streams, AggregateType, AggregateId, Version]`. One store (one Ra cluster, no
storm), each type its own subtree.

- ✅ Type-scoped read/list/replay/subscribe = `get_many([streams, ride, *])` →
  O(type), not O(store).
- ✅ Namespace un-mixed by construction; `list_streams(ride)` cannot see vehicles.
- ✅ Consistency/ops scope becomes a **deliberate choice** — DCB / scavenge /
  retention can be per-type-subtree or per-store, not "whole store by accident".
- ✅ Clean model mapping: **store ↔ Division/tenant**, **type ↔ subtree**,
  **aggregate ↔ stream**. Recovers the original "1 type = 1 boundary" instinct
  *as a subtree inside a shared store*, which is also what dodges the storm.
- ❌ Breaking path-layout migration (see §6–§8).
- ❌ The store now derives `category` from the id. NOTE: this is **not** the
  causation-style domain leak — "category" is a first-class event-store concept
  (EventStoreDB `$ce`); the store splits the prefix, it does not learn that
  "ride" *means* anything.
- **Verdict:** recommended. It is the structural expression of "don't mix types"
  at the right boundary.

---

## 4. Recommendation

**Adopt Model C.** Keep store = bounded-context/tenant (one Ra cluster), make
aggregate type a structural path level.

---

## 5. Proposed layout

```
[streams, Type, AggregateId, PaddedVersion] -> #event{}
```

- `Type` — derived from the StreamId prefix at write time
  (`reckon_gater_stream_id:prefix_of/1`); stored as a binary path node.
- `AggregateId` — the remaining id (e.g. the `<hex>`), unique within `Type`.
- `PaddedVersion` — unchanged (`VERSION_PADDING`).

### Reserved / special streams

| StreamId today | Proposed home | Rationale |
|---|---|---|
| `<<"_dcb">>` | `[streams, <<"_dcb">>, SeqKey]` (a reserved Type with no AggregateId level — DCB events are a flat seq-keyed log, not aggregates) | DCB is cross-cutting; it is its own "type". Keep its existing 2-level shape under the reserved `_dcb` type node. |
| `$ns:name` (system) | `[streams, <<"$ns">>, name, PaddedVersion]` — system namespace `ns` becomes the Type node | System streams get a structural namespace too; `$`-prefixed types are reserved. |
| user `ride-abc` | `[streams, <<"ride">>, <<"abc">>, PaddedVersion]` | the common case |

`read_all_global` becomes `get_many([streams, *, *, has_data])` (one extra
wildcard level) and must tolerate the `_dcb` 2-level shape — already a special
case today.

### Indexes & adjacent trees

- `[by_tag, …]`, `[snapshots, …]`, `[links, …]`, `[metadata, …]` are
  **unaffected by the primary re-key** (they key by StreamId / Tag / Name, not
  by the streams path). They keep working as-is; only their *contents'* event
  references change if they embed full paths (audit during impl).
- This RFC does **not** add the generic `by_meta` secondary index — that is a
  separate, complementary change (§9).

---

## 6. The consistency / ops scoping decision (must be made)

Making `Type` structural forces an explicit answer to questions that are
silently "whole store" today:

| Concern | Per-store (today) | Per-type option (Model C unlocks) |
|---|---|---|
| **DCB `_dcb` boundary** | one `_dcb` per store; cross-cutting consistency spans all types | a `_dcb` per type subtree → isolated cross-cutting boundaries per type; cross-type DCB explicitly opt-in |
| **Scavenge / retention** | whole store | per-type retention policy |
| **Backup / export** | whole store | per-type export |
| **Subscriptions catch-up** | global scan | per-type replay (cheap) |

**Recommendation:** keep DCB **per-store** by default (a bounded context often
*wants* one cross-cutting consistency domain — that is the whole point of DCB),
but make per-type scavenge/retention/export available because they are pure
wins with no semantic downside. Revisit per-type DCB only if a concrete need
appears. (`DCB_COUNTER.md`: don't invent boundaries that aren't there.)

---

## 7. Affected surface (impl checklist)

Everything that constructs or matches `[streams, …]`:

- `reckon_db_streams`: `do_append` (write path), `read/5`, `read_all/4`,
  `read_all_global/3`, `read_by_event_types/3`, `read_by_tags/4`,
  `get_version/2`, `exists/2`, `list_streams/1`, `delete/2`,
  `convert_result_to_event/2`, `extract_stream_id/1`.
- `reckon_db_dcb` / `reckon_db_dcb_paths`: confirm `_dcb` stays a reserved Type.
- `reckon_db_links`: source-pattern matching (`order-*`) becomes a subtree
  navigation `[streams, order, *]` — a *simplification*.
- `reckon_db_subscriptions` / Khepri triggers: filter paths
  (`by_stream`, `by_event_type`, `by_tags`) — the `by_stream` path-mismatch
  demon (ANTIPATTERNS_INTEGRATION #24) gets *easier* with a structural type.
- `reckon_db_snapshots_store`, `reckon_db_temporal`, `reckon_db_scavenge`,
  `reckon_db_store_inspector`: audit for `[streams, …]` assumptions.
- `reckon_gater_stream_id`: `prefix_of/1` becomes load-bearing (the Type
  derivation); add a `parts/1 -> {Type, Id}` helper.

---

## 8. Migration (dual-read)

Existing stores hold data at the old `[streams, Id, V]` paths; a hard cutover
would orphan it. Plan:

1. **Layout version marker.** Write `[metadata, layout, version] -> 2` when a
   store is created/upgraded under the new scheme. Absent ⇒ legacy (v1).
2. **Dual-read window.** Readers check the marker; v1 stores read the old flat
   paths, v2 stores read structural paths. New writes always go structural.
3. **Backfill.** A one-shot, idempotent migration walks the global log once
   (the efficient single `get_many`, *not* per-stream reads — the same fix we
   want for the query layer) and re-keys each event to its structural path,
   then stamps the marker v2. Run on boot behind a config flag, or as an
   offline `reckon_db migrate` task. Khepri is a Ra log — the re-key is a
   transactional batch; size the batches.
4. **Drop dual-read** in a later major once all stores are v2.

For parksim specifically: the simulator regenerates data continuously, so a
**clean sweep + fresh v2 stores** (we already did the sweep dance this session)
is cheaper than backfilling — note this as the recommended path for *simulation*
stores. Real stores get the backfill.

---

## 9. Relationship to the secondary-index RFC (separate work)

Model C and a generic index are **complementary**, not the same fix:

- **Model C (this doc)** fixes *type-scoped* ops ("everything in this aggregate
  type") via a path level.
- A **generic write-maintained `by_meta` / generalized `by_tag` index** fixes
  *cross-type* lookups (tags, and — if an app wants it — causation/correlation
  modelled as tags or indexed metadata keys). Today only DCB events have a
  `by_tag` index; `read_by_tags` on regular streams scans (its own code says
  "consider maintaining a separate tag index").

Together: **type → path level; cross-cutting keys → secondary index; lineage
*interpretation* → app read model / link.** That is the full cure for
reckon-db's "everything is a scan" query layer. Causation/correlation
*traversal* stays out of the store (see `causation_removed_from_store_stack`
decision) — this RFC does not re-open that.

---

## 10. Open questions

1. Per-type vs per-store DCB — confirm the §6 default (per-store) is acceptable.
2. System-stream namespace shape (`[streams, $ns, name, V]`) — bikeshed the
   reserved-Type encoding for `$`-prefixed ids.
3. Backfill-on-boot vs offline task vs clean-sweep-for-sims — pick the default.
4. Does `Type` need its own metadata node (`[streams, Type, '$meta']` for
   per-type config like retention)? Probably yes once per-type retention lands.

---

## 11. Decision required

- [ ] Adopt Model C (structural type subtree)? (recommend **yes**)
- [ ] DCB stays per-store by default? (recommend **yes**)
- [ ] Migration default = dual-read backfill for real stores, clean-sweep for
      simulation stores? (recommend **yes**)

No code until these are checked.
