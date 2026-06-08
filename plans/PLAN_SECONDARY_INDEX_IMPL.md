# Plan: Generic write-maintained secondary index

**Status:** ✅ Implemented (2026-06-08) — see Implementation Record below
**Created:** 2026-06-08
**Target release:** `reckon-db` (next major, after Model C)
**Design source:** [DESIGN_SECONDARY_INDEX.md](DESIGN_SECONDARY_INDEX.md)
**Prerequisite:** ✅ Model C ([PLAN_STREAM_NAMESPACE_MODEL_C.md](PLAN_STREAM_NAMESPACE_MODEL_C.md)) — landed.

## Decisions locked (2026-06-08, RFC §14)
- [x] Adopt the generic opt-in index (`tags` / `event_type` / `{meta, Key}`).
- [x] Maintenance is **transactional** with the append (not async).
- [x] `read_by_metadata/3` is the sanctioned app primitive; store never interprets lineage.
- [x] `OrderKey = pad(epoch_us) | stream_id | pad(version)` (no global counter).
- [x] Scope: **all three index kinds** in this pass.

## Layout
```
[by_tag,        Tag,       OrderKey] -> EventRef
[by_event_type, EventType, OrderKey] -> EventRef
[by_meta, Key,  Value,     OrderKey] -> EventRef
```
- `OrderKey = <<Pad(epoch_us)/binary, "|", StreamId/binary, "|", Pad(version)/binary>>`
  — globally ordered, unique (stream+version is unique).
- `EventRef = #{stream_id => StreamId, version => Version}` — resolved via
  `reckon_db_stream_path:event_path/2` (point get under the Model C layout).
  (Storing `stream_id`+`version` rather than the RFC's `{type,id,version}` is
  equivalent and reuses the existing path module directly.)
- DCB's own `[by_tag, Tag, SeqKey]` for `_dcb` events stays **separate and
  unconditional** (RFC §13.5) — different leaf-key scheme (seq vs OrderKey),
  required by the conditional-append primitive regardless of opt-in.

## New constants/paths (reckon_db.hrl)
`?BY_TAG_PATH` exists (DCB). Add `?BY_EVENT_TYPE_PATH = [by_event_type]`,
`?BY_META_PATH = [by_meta]`, `?INDEX_ORDER_KEY_WIDTH` (epoch_us padding).
`store_config.indexes = [] :: [index_decl()]` field +
`index_decl() :: tags | event_type | {meta, binary()}`.

## Modules
1. **`reckon_db_index_config`** (mirror `reckon_db_integrity_key`):
   `load/1` (store_config → persistent_term `{reckon_db, indexes, StoreId}`),
   `declared/1 -> [index_decl()]`, `clear/1`. Loaded at `reckon_db_store`
   startup next to integrity; cleared on shutdown.
2. **`reckon_db_index`** — the index mechanism:
   - `entries(Event, Declared) -> [{Path, EventRef}]` — index entries an event
     produces under the store's declared indexes (tags → one per tag,
     event_type → one, {meta,K} → one per declared K present in metadata).
   - `order_key(Event)`, `event_ref(Event)`, path builders.
   - `read(StoreId, Kind, Value) -> {ok,[event()]}` — `get_many(subtree)` → refs
     → point-gets → sort by OrderKey. Compound tag `all` = intersect ref sets.
   - `is_indexed(StoreId, Kind) -> boolean()` (drives read fallback).

## Write path (the risky change — reckon_db_streams:append_events_to_stream)
Replace the per-event `khepri:put` fold with **one `khepri:transaction`** per
batch that writes every event record **and** its index entries via
`khepri_tx:put`. Mirror DCB precisely:
- Integrity MAC + tip are computed **outside** the tx (persistent_term/crypto
  are unavailable inside a Ra tx) — stamp records first, exactly as DCB does.
- **Inside** the tx: verify the stream's current version still equals the
  `CurrentVersion` the batch was computed against (snapshot-verify, like DCB's
  counter check); on skew `khepri_tx:abort({stream_changed, …})` and retry
  outside. This also closes the pre-existing version-check TOCTOU.
- Preserve the `noproc`→retriable `{error,_}` contract (no badmatch); a failed
  transaction returns `{error, Reason}`, never crashes the worker.
- Stores with `indexes = []` (default) take the same transactional path but
  write zero index entries — atomic batch append is a strict improvement and
  keeps one code path.

## Read path (reckon_db_streams)
- `read_by_tags/4`, `read_by_event_types/3`: use the index when the store
  declared it; else current two-depth scan + `logger:warning` once
  ("un-indexed … declare to index"). No silent cap.
- New `read_by_metadata/3` (+ facade export): index-only; un-indexed key →
  warn + scan fallback (filter `maps:get(Key, metadata)` in Erlang).

## Index lifecycle
- Declared at store creation; built from genesis transactionally. **No backfill,
  no building/ready marker** (RFC §10 — not in production; recreate to add).
- Undeclare → on next load, GC the now-undeclared subtree
  (`khepri:delete([by_meta, Key])` etc.). Keep simple; document.

## Steps (each compiles + tests green before next)
1. hrl: `store_config.indexes` field + `index_decl()` + paths/widths.
2. `reckon_db_index_config` + unit tests; load/clear in `reckon_db_store`.
3. `reckon_db_index` (paths, order_key, entries, read) + unit tests (pure).
4. Transactional `append_events_to_stream` — **integrity-disabled first**;
   run `reckon_db_streams_SUITE`. Then integrity snapshot-verify path; run all
   four integrity SUITEs (the canary).
5. Wire index entry writes into the tx; new `reckon_db_index_SUITE`
   (write→read round-trip; tags/event_type/meta; compound `all`; ordering).
6. Rewire `read_by_tags`/`read_by_event_types` to index-or-scan; add
   `read_by_metadata/3` + facade. Run tags/subscription suites.
7. Full eunit + ct + dialyzer. Grep gate: index paths only in `reckon_db_index`.

## Test plan
- Unit: `reckon_db_index_config` (declared/clear), `reckon_db_index`
  (order_key ordering property, entries for each kind, path round-trip).
- Integration `reckon_db_index_SUITE`: per kind, write N events → indexed read
  returns exactly the matches in epoch order; compound `all` intersection;
  `{meta,K}` present/absent; un-indexed fallback warns + still correct;
  transactional atomicity (a forced mid-batch failure leaves no partial index).
- Regression: all four integrity SUITEs (transactional rewrite is the risk),
  streams, tags, subscriptions, dcb (its separate by_tag must be untouched).

---

## Implementation Record (2026-06-08)

**Done.** All three index kinds, transactional maintenance, `read_by_metadata/3`.

**Modules:**
- `reckon_db_index_config` — per-store declared-index registry in
  persistent_term (mirrors `reckon_db_integrity_key`); loaded/cleared in
  `reckon_db_store`. + `reckon_db_index_config_tests` (4).
- `reckon_db_index` — owns the `[idx, …]` layout: `entries/2` (write-path),
  `order_key/1`, `event_ref/1`, `lookup_tags/3`, `lookup_event_types/2`,
  `lookup_meta/3`. Subtree reads propagate Khepri errors (no silent-empty on a
  not-ready store); resolve drops scavenged refs best-effort. +
  `reckon_db_index_tests` (10).

**Write path (`reckon_db_streams:append_events_to_stream`):** replaced the
per-event `khepri:put` fold with `build_event_writes` (records + index entries
built OUTSIDE the tx — integrity MAC reads persistent_term/crypto, unavailable
inside a Ra tx) + `write_batch` (ONE `khepri:transaction` writing every event
record and every index entry via `khepri_tx:put`). Atomic batch; preserves the
`noproc`→retriable `{error,_}` contract. The optimistic version check stays
outside (the per-stream same-version TOCTOU is pre-existing and intentionally
unchanged).

**Read path:** `read_by_tags`/`read_by_event_types` dispatch index-or-scan
(`is_indexed/2`); the old scans are kept as `scan_by_*` fallbacks behind a
`warn_unindexed/2` (once per store+kind, persistent_term-gated). New
`read_by_metadata/3` (+ gateway-worker route) — index when `{meta,Key}`
declared, else scan fallback.

**Layout decision:** generalized indexes live under a fresh `[idx]` root,
deliberately separate from DCB's `[by_tag]` (RFC §13.5) — avoids mixing the
seq-keyed DCB leaves with OrderKey leaves in one subtree. DCB untouched.

**Tests:** `reckon_db_index_SUITE` (8) — round-trip per kind, compound `all`,
index/scan parity, un-indexed fallback, multi-event-batch atomicity.
**Full eunit: 579 pass. Full ct: 143 pass, 0 fail.** New modules dialyzer-clean
of real issues (only benign supertype-spec notes, consistent with the codebase).

**Deferred (RFC, explicitly out of scope):** no backfill / building-ready
marker (recreate to add an index — not in production); index-removal subtree GC
on undeclare (noted, not built); cardinality guardrails (documented only).
