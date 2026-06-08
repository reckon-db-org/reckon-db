# DESIGN: Generic write-maintained secondary index

**Status:** Draft / RFC (no code — decision required before implementation)
**Date:** 2026-06-08
**Author:** design discussion (rl + apprentice)
**Affects:** reckon-db core write path + query layer. Additive (opt-in per store) but touches `do_append`.
**Companion:** [DESIGN_STREAM_NAMESPACE.md](DESIGN_STREAM_NAMESPACE.md) — *complementary, not the same fix.*

---

## 1. Problem

reckon-db's **cross-stream / cross-cutting** query layer scans the whole store:

| Function | How it works today | Cost |
|----------|--------------------|------|
| `read_by_tags/4` (regular streams) | `get_many([streams, *, has_data])` → load every event → filter in Erlang. Its own comment: *"For large stores, consider maintaining a separate tag index."* | O(total events) |
| `read_by_event_types/3` | `#if_data_matches` pushed into Khepri (DB-side) | O(total events) tree walk |
| causation (removed 2026-06-07) | per-stream scan, twice per `get_cause` | O(total events) |

The **only** write-maintained secondary index in reckon-db is DCB's
`[by_tag, Tag, SeqKey]` — and it exists **only for `_dcb` events**, to serve the
conditional-append primitive's *write-path* tag check. Regular events have no
index at all.

[DESIGN_STREAM_NAMESPACE.md](DESIGN_STREAM_NAMESPACE.md) (Model C, accepted) fixes
**type-scoped** queries by making the aggregate type a path level
(`[streams, Type, Id, Version]`). It does **not** help **cross-cutting** lookups —
"all events with tag X", "all events of type Y across aggregate types", "all
events whose `metadata.causation_id == Z`". Those are what this RFC addresses.

---

## 2. Goal / non-goals

**Goal:** one generic, **opt-in, write-maintained** secondary index that turns
cross-cutting lookups from O(total) scans into O(matches) subtree reads — by
generalizing the DCB `by_tag` mechanism to all events.

**Non-goals (explicit):**
- **No lineage interpretation in the store.** This provides the *index
  primitive* an app *could* use to build a causation/correlation read model. It
  does NOT re-add `GetCause` / `GetEffects` / `BuildGraph` — those were removed
  on purpose (see `causation_removed_from_store_stack`). The store indexes a
  metadata key; the app decides what the graph means.
- **Not a projection engine.** Derived/transformed streams remain `links`
  (materialized). An index stores *references*, not transformed events (§9).
- **Not "index everything."** Per the QRY guidance — *"each index exists because
  there's a query that needs it; don't add indexes just in case"* — every index
  is **opt-in per store** (§5).

---

## 3. The index model

One mechanism, three concrete index *kinds*, all maintained the same way and
declared the same way:

| Kind | Indexes by | Serves |
|------|-----------|--------|
| `tags` | each tag in `#event.tags` | `read_by_tags` (all events, not just DCB) |
| `event_type` | `#event.event_type` | `read_by_event_types` |
| `{meta, Key}` | `maps:get(Key, metadata)` for a declared `Key` | `read_by_metadata/3` — the primitive apps build causation/correlation/saga views on |

`tags` is literally the existing DCB `by_tag` index, generalized to fire on
**every** append rather than only the DCB write path.

---

## 4. Path layout

Mirror the DCB `by_tag` shape (fixed-width ordered leaf keys, subtree iteration):

```
[by_tag,        Tag,        OrderKey] -> EventRef
[by_event_type, EventType,  OrderKey] -> EventRef
[by_meta, Key,  Value,      OrderKey] -> EventRef
```

- **`OrderKey`** — a fixed-width, lexicographically-orderable key so a subtree
  `get_many([by_tag, Tag, *])` returns refs in event order. Regular events have
  no global seq (unlike DCB), so use **`pad(epoch_us)` + tiebreak**:
  `<<PaddedEpochUs/binary, "|", StreamId/binary, "|", PaddedVersion/binary>>`.
  This is globally ordered and unique (no two events share stream+version).
- **`EventRef`** — points at the primary event under the Model C layout:
  `#{type => Type, id => AggregateId, version => Version}` (resolve with a point
  `khepri:get([streams, Type, Id, PaddedVersion])`). Storing a ref (not the
  event) keeps the index small and avoids duplicate-write divergence. (DCB's
  `by_tag` stores `#{}` and derives the path from the seq key; we store an
  explicit ref because regular events aren't seq-keyed.)

`OrderKey` design is the main open question (§13).

---

## 5. Opt-in configuration

Indexes are declared **per store** in `store_config` (none on by default):

```erlang
#store_config{
    indexes = [
        tags,                    %% maintain [by_tag, ...] for all appends
        event_type,             %% maintain [by_event_type, ...]
        {meta, <<"causation_id">>},
        {meta, <<"correlation_id">>}
    ]
}
```

- Default `indexes = []` — a store pays nothing unless it declares an index.
  (DCB's own `by_tag` for `_dcb` events stays unconditional — it's required by
  the conditional-append primitive, independent of this opt-in list.)
- Declaring an index on a store with existing data triggers a **backfill** (§10)
  before the index is marked usable.
- Apps declare exactly the indexes their queries need — the QRY discipline.

---

## 6. Maintenance (write path)

On `do_append`, for each event, write the index entries for the store's declared
indexes **atomically with the event**.

**Completeness is mandatory.** If the index is the query path and an index write
is dropped, queries return silently-incomplete results (worse than slow). So:

- Wrap the event put + its index puts in a **`khepri:transaction`** (the DCB
  conditional-append path already writes transactionally — same precedent). One
  Ra command per append batch; either all entries land or none do.
- Preserve the append path's hard-won robustness: a not-ready store surfaces as
  a retriable `{error, _}` (the `noproc` lesson in `do_append`), not a badmatch.
- This is the one real change to the otherwise-non-transactional append loop —
  call it out in review; it is the riskiest part of this RFC.

(Alternative considered: async index via a built-in subscription/projection.
Rejected for v1 — introduces lag + a second consistency story; the index would
be eventually-consistent, which breaks "the index IS the answer".)

---

## 7. Read API

```erlang
%% rewritten to use the index when present, fall back to scan when absent
read_by_tags(StoreId, Tags, Match, BatchSize)
read_by_event_types(StoreId, Types, BatchSize)

%% new — the primitive
-spec read_by_metadata(atom(), Key :: binary(), Value :: binary()) ->
    {ok, [event()]} | {error, term()}.
read_by_metadata(StoreId, Key, Value) ->
    %% get_many([by_meta, Key, Value, *]) -> refs -> point-get each event
```

- **Single-value lookup** = one bounded subtree iteration + N point-gets. O(matches).
- **Compound** (`match=all` over N tags) = N subtree reads, intersect refs in
  Erlang (exactly how DCB's filter algebra refines — `reckon_db_dcb_filter`).
- **Un-indexed fallback:** if a query targets a key/kind the store didn't
  declare, fall back to the current scan and `logger:warning` once ("query on
  un-indexed key K — O(store) scan; declare `{meta, K}` to index"). No silent
  cap (the "no silent truncation" rule).

---

## 8. Causation / correlation as a *use* (not a feature)

This is the whole point of the split. With `{meta, <<"causation_id">>}` and
`{meta, <<"correlation_id">>}` declared, an **application** builds lineage views:

```
get_effects(EventId)   ≈ read_by_metadata(Store, <<"causation_id">>,   EventId)
get_correlated(CorrId) ≈ read_by_metadata(Store, <<"correlation_id">>, CorrId)
```

The store returns *events matching a metadata key=value* — bounded, indexed. It
does **not** walk chains, build graphs, or know these ids mean "lineage". The
app composes `read_by_metadata` into whatever traversal/graph/read-model it
needs (or a `link`, or a tracing export). The removed `CausationService` was
wrong because it fused that app semantics onto a naive scan inside the store;
this gives the app the fast primitive and keeps the semantics where they belong.

Equivalently, an app that prefers it can model the ids as **tags**
(`causation:evt-7`) and use the (now-indexed) `read_by_tags` — same outcome, no
`{meta, …}` declaration needed.

---

## 9. Index vs link — the boundary

| | Secondary index (this RFC) | Link / projection (`reckon_db_links`) |
|---|---|---|
| Stores | event **references** keyed by value | **materialized transformed events** as a stream |
| Use | high-cardinality point lookups (`causation_id`, `correlation_id`, a tag) | stable named derived streams, category views (`$ce-order`), transforms |
| Cost | small per-event ref writes | one-time build scan + ongoing materialization |
| Read | `get_many(subtree)` + point-gets | read the derived stream like any stream |

Rule of thumb: **point lookup by a key value → index; durable named/transformed
derived stream → link.** Don't materialize a link per `causation_id` (millions
of tiny streams); don't index to produce a transformed, subscribable stream.

---

## 10. Migration / backfill

Per-index, idempotent, behind a marker:

1. `[metadata, index, <kind/key>, status] -> building | ready`.
2. On declaring an index for a store with data, run a **one-shot backfill**: the
   single efficient global traversal (`get_many([streams, *, *, has_data])` under
   Model C — *not* per-stream reads), emit index entries, set `ready`.
3. Queries on a `building` index fall back to scan (with the warning) until `ready`.
4. Backfill is restartable (idempotent puts; re-run overwrites).

For simulation stores (parksim) a clean-sweep into fresh stores with indexes
pre-declared is cheaper than backfilling — same note as the namespace RFC.

---

## 11. Costs

- **Write amplification.** Each append now writes `len(tags) + 1(type) +
  len(declared meta keys present)` extra index entries, inside the transaction.
  Latency + storage scale with how many indexes a store declares — hence opt-in.
- **Storage.** Index size ≈ events × indexed-keys-per-event. High-cardinality
  meta values (event-ids) make many small subtrees; fine for point lookups,
  watch total size.
- **Append-path transactionality.** The riskiest code change (§6).

---

## 12. Relationship to Model C

Complementary, layered:

- **Model C** (path) → *type-scoped* ops O(type).
- **This RFC** (index) → *cross-cutting* lookups O(matches).
- **App read model / link** → *interpretation* (lineage graphs, sagas, categories).

`EventRef`s in §4 point at Model C paths, so the two are designed together:
implement Model C first (primary layout), then this index on top.

---

## 13. Open questions

1. **`OrderKey` encoding** — `pad(epoch_us)|stream|pad(version)` vs a store-global
   append seq counter (like DCB) shared across all streams. The latter gives a
   clean total order but adds a contended counter on every append.
2. **Transactional vs async** maintenance — confirm transactional (§6) is
   acceptable given the append-path scar tissue.
3. **Value cardinality guardrails** — any cap/warn on `{meta, Key}` where Key has
   near-unique values (degenerate index)? Probably just document it.
4. **Index removal** — declaring-then-undeclaring an index: GC the subtree
   (`khepri:delete([by_meta, Key])`) on undeclare; confirm lifecycle.
5. **Tag index unification** — make DCB's `_dcb` `by_tag` and the generalized
   `by_tag` the *same* tree, or keep DCB's separate (its seq-keyed leaves differ
   from the `OrderKey` scheme)? Likely keep DCB's as-is and add the general one.

---

## 14. Decision required

- [ ] Adopt the generic opt-in secondary index (tags / event_type / `{meta,K}`)?
      (recommend **yes**, after Model C)
- [ ] Maintenance is **transactional** with the append (not async)? (recommend **yes**)
- [ ] `read_by_metadata/3` is the sanctioned primitive for app-built
      causation/correlation read models — store never interprets lineage?
      (recommend **yes**)
- [ ] `OrderKey` = `pad(epoch_us)|stream|pad(version)` (no global counter)?
      (recommend **yes** — avoids a contended counter)

No code until these are checked, and not before Model C lands.
