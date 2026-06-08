# Plan: Stream Namespace — Model C structural type subtree

**Status:** ✅ Implemented (2026-06-08) — see Implementation Record below
**Created:** 2026-06-08
**Last Updated:** 2026-06-08

---

## Implementation Record (2026-06-08)

**Done.** Model C is live. `reckon_db_stream_path` is the sole owner of the
4-level layout; all per-stream event-path construction routes through it.

**Results:**
- `reckon-gater`: `parts/1` added — **77 unit tests pass**, dialyzer-clean.
- `reckon_db_stream_path` + `reckon_db_stream_path_tests` — **13 tests pass**
  (incl. the opaque round-trip property), dialyzer-clean.
- **Full eunit: 565 pass. Full ct: 135 pass, 0 fail.**
- Every stream-layout-touching suite green, including all four integrity suites
  (38), streams (19), subscriptions/delivery, dcb, snapshots, tags, naming,
  scavenge, links.

**Pre-existing breakage discovered & repaired (not caused by Model C):**
Most integration suites hardcoded stream ids that the strict
`reckon_gater_stream_id` 2.2.0 regex (`^[a-z]{1,32}-[a-f0-9]{32}$`) rejects at
the append gate (`stream-disabled`, `clean-chain`, `teststreamsub-001`, …).
These had been failing since the regex tightening, masked behind the validation
gate. Repaired at the source with `reckon_db_test_helpers:sid/1` (deterministic
label→conforming-id) + tamper-path routing through `event_path/2`, via three
idempotent scripts in `scripts/`:
- `fix_integrity_stream_ids.sh`, `fix_subscription_suite_stream_ids.sh` — wrap
  non-conforming stream-id literals in `sid/1`.
- `fix_integrity_tamper_paths.sh` — route direct-Khepri tamper writes through
  `reckon_db_stream_path:event_path/2` (the suites mutate events by raw path).

**Two pre-existing failures (surfaced once the invalid-id gate was lifted) —
now also fixed:**
1. `reckon_db_emitter_autostart_SUITE:duplicate_subscribe_returns_already_exists`
   — the `already_exists` dedup was unimplemented; `subscribe` always took the
   reconnect path and returned `{ok, Key}`. **Fixed** in `reckon_db_subscriptions`:
   a re-subscribe to a key whose subscriber pid is **still alive** now returns
   `{error, {already_exists, Name}}`; a dead/undefined subscriber pid still
   reclaims via the reconnect path (keeps `subscribe_duplicate_is_idempotent`
   green). New `subscriber_alive/1` (local probe; remote pids conservatively
   treated as alive).
2. `reckon_db_pg_scope_SUITE:emitter_can_join_pg_group_after_scope_restart`
   — test bug: it subscribed to stream `…-001` but appended to `…-002`. Model C's
   structural `by_stream` filter correctly matches only the subscribed stream's
   subtree (the old over-matching "by_stream path-mismatch demon" had masked the
   mismatch). **Fixed** the test to append to the stream it subscribed to.

**Decisions confirmed at implementation:** DCB stays **per-store**. System type
node keeps its leading `$` (`[streams, <<"$link">>, Name, V]`) so the `$`
namespace is structurally reserved. DCB log unchanged at `[streams, _dcb, SeqKey]`;
global readers (`read_all_global`/`read_by_event_types`/`read_by_tags`) query
both depths and merge.

---
**Target release:** `reckon-db` (next major), `reckon-gater` (minor — adds `parts/1`)
**Spans repos:** `reckon-gater` (id helper), `reckon-db` (storage layout)
**Design source:** [DESIGN_STREAM_NAMESPACE.md](DESIGN_STREAM_NAMESPACE.md) — accepted 2026-06-08

> Sequencing rule (design §11): implement Model C **first**, then the secondary
> index ([DESIGN_SECONDARY_INDEX.md](DESIGN_SECONDARY_INDEX.md)) on top. This plan
> is Model C only.

---

## 0. Decisions locked for this implementation

| Question | Decision |
|----------|----------|
| Consistency/ops scope of DCB | **DCB stays per-store** (confirmed 2026-06-08). One `_dcb` log per store, one cross-cutting consistency domain per bounded context. Per-type DCB is explicitly *not* built now. |
| Migration | **None.** reckon-db not in production. Old flat `[streams, Id, V]` code deleted outright; dev/demo stores recreated fresh. No v1/v2 branch, no layout marker, no backfill. |
| Per-type scavenge/retention/export | **Not in this plan.** Model C *unlocks* them; building them is follow-up. This plan only re-keys the primary layout and keeps every existing op working. |
| Snapshots / links / by_tag / metadata trees | **Layout unchanged** — keyed by StreamId / Tag / Name, not by the streams path. Only the one spot in `reckon_db_snapshots` that reads an *event* path is touched. |

---

## 1. Target layout

```
USER:    [streams, <<"ride">>, <<"abc…hex">>, PaddedVersion]   -> #event{}
SYSTEM:  [streams, <<"$ns">>,  <<"name">>,    PaddedVersion]   -> #event{}   (ns from $ns:name)
DCB:     [streams, <<"_dcb">>, SeqKey]                          -> #event{}   (2-level, reserved type, unchanged shape)
```

- `Type` derived from StreamId at write time. Stored as a binary path node.
- `AggregateId` is the remaining id, unique within `Type`.
- `_dcb` keeps its current 2-level `[streams, _dcb, SeqKey]` shape (a flat
  seq-keyed log, not an aggregate). All global readers must tolerate the
  2-level `_dcb` node alongside 3-level aggregate nodes — they already special-case
  `_dcb` today.
- Opaque round-trip invariant: a StreamId written as `[streams, Type, Id, V]`
  must reconstruct **byte-identical** to the original `ride-abc…` / `$ns:name`
  when read back (events carry `stream_id`; `list_streams` rebuilds it). This is
  the single most important correctness property — every consumer outside the
  store still sees opaque ids.

---

## 2. The one new abstraction (kills the inline-path sprawl)

Today ~15 call sites build `[streams, StreamId, …]` inline. That sprawl is what
makes this a "touches most stream code" change. Fix it **once**:

### 2a. `reckon-gater`: `reckon_gater_stream_id:parts/1`

Pure id-shape logic belongs in the protocol-contract module (design §7).

```erlang
-spec parts(binary()) ->
      {user,   Type :: binary(), Id :: binary()}
    | {system, Ns   :: binary(), Name :: binary()}
    | {error, malformed}.
%% parts(<<"ride-abc…">>)        -> {user, <<"ride">>, <<"abc…">>}
%% parts(<<"$link:hot-orders">>) -> {system, <<"link">>, <<"hot-orders">>}
%% parts(<<"_dcb">>)             -> {error, malformed}  %% DCB is reckon-db-internal, not an id
```

- Reuses the already-compiled regexes; `prefix_of/1` becomes a thin wrapper over
  `parts/1`. No behaviour change to `validate/1`, `new/1`, `is_system/1`.
- `_dcb` is deliberately *not* a valid id here — it never flows through
  `validate/1` (it is reckon-db's reserved internal stream). reckon-db's path
  module handles `_dcb` itself (2b).

### 2b. `reckon-db`: new `reckon_db_stream_path` module (the only place that knows the 4-level layout)

```erlang
-module(reckon_db_stream_path).
-export([event_path/2, stream_path/1, type_subtree/1,
         all_events_pattern/0, stream_id_from_path/1]).

%% [streams, Type, Id, PaddedVersion]  (user/system) OR
%% [streams, _dcb, SeqKey]             (DCB pseudo-stream)
event_path(StreamId, PaddedVersion) -> ...

%% [streams, Type, Id]  — the aggregate node (for count/exists/delete)
stream_path(StreamId) -> ...

%% [streams, Type, *]   — one aggregate type's subtree (list_streams(Type), links)
type_subtree(Type) -> ...

%% [streams, *, *, #if{has_data}] tolerant of the 2-level _dcb node
%% — the global-read wildcard
all_events_pattern() -> ...

%% inverse of event_path/stream_path: rebuild the opaque StreamId from a
%% Khepri path. THE round-trip guarantee lives here.
stream_id_from_path([streams, Type, Id | _]) -> ...
```

Every inline `[streams, …]` builder in §3 is replaced by a call into this module.
`?STREAMS_PATH` (root `[streams]`) stays as-is in `reckon_db.hrl`; the deeper
`[streams, StreamId, PaddedVersion]` shape is deleted everywhere and only
`reckon_db_stream_path` reconstructs it.

---

## 3. Call-site change list (exhaustive — grepped, not guessed)

### `reckon_db_streams.erl` (primary)
| Fn | Today | Change |
|----|-------|--------|
| `do_append` (append_events_to_stream, L579) | `?STREAMS_PATH ++ [StreamId, PaddedVersion]` | `reckon_db_stream_path:event_path/2` |
| `resolve_initial_tip` (L640) | same inline path | `event_path/2` |
| `resolve_read_initial_tip` (L905) | same inline path | `event_path/2` |
| `read_events` (L923) | same inline path | `event_path/2` |
| `get_version` (L450) | `?STREAMS_PATH ++ [StreamId]` + `count(... ++ [*])` | `stream_path/1` + `[* ]`; count now counts versions under `[streams,Type,Id,*]` — unchanged semantics |
| `exists` (L460) | `?STREAMS_PATH ++ [StreamId]` | `stream_path/1` |
| `delete` (L506) | `?STREAMS_PATH ++ [StreamId]` | `stream_path/1` (deletes the aggregate node = all its versions) |
| `list_streams` (L482, `extract_stream_id` L498) | `[streams, *]`, take 2nd element | walk `[streams, *, *]`, `stream_id_from_path/1`, `usort`. **Skip the `_dcb` node** (it is not a user stream). |
| `read_all_global` (L247) | `[streams, *, #if{*, has_data}]` | `all_events_pattern/0` (extra wildcard level; `_dcb` tolerated) |
| `read_by_event_types` (L308) | `[streams, *, #if{*, …}]` | `all_events_pattern/0` + the `#if_data_matches` type condition |
| `read_by_tags` (L371) | `[streams, *, #if{*, has_data}]` | `all_events_pattern/0` |
| `convert_result_to_event` (L435/437) | `[streams, StreamId | _]` → set `stream_id` | `[streams, Type, Id | _]` → `stream_id_from_path/1`; **also handle `[streams, _dcb, SeqKey]`** (DCB event carries its own stream_id already) |

### `reckon_db_filters.erl` (subscription trigger filters — all gain one wildcard level)
| Fn | Change |
|----|--------|
| `by_stream(<<"$all">>)` | `[streams, *, #if{*, has_data}]` → `[streams, *, *, #if{has_data}]` (Type, Id, Version) |
| `by_stream(Stream)` (literal id) | split via `reckon_db_stream_path:stream_path/1` → `[streams, Type, Id, #if{*, has_data}]` |
| `by_event_type`, `by_event_pattern`, `by_event_payload`, `by_tags` | add one `#if_path_matches{regex=any}` level (Type) above the existing Id/Version wildcards |
| `matches/3` (in-memory) | **no change** — matches on the `#event{}` record's `stream_id`/`event_type`/`tags` fields, which are layout-independent |

### `reckon_db_snapshots.erl`
- `compute_event_chain_hash/3` (L296): inline `[streams, StreamId, PaddedVersion]`
  → `reckon_db_stream_path:event_path/2`. (Snapshot *storage* path `[snapshots, …]`
  is unchanged.)

### `reckon_db_scavenge.erl`
- `delete_event_versions/4` (L269): inline `?STREAMS_PATH ++ [StreamId, PaddedVersion]`
  → `event_path/2`. Everything else in scavenge goes through the `reckon_db_streams`
  API (`get_version`, `read`, `list_streams`) and needs **no change**.

### `reckon_db_dcb.erl` / `reckon_db_dcb_paths.erl`
- Confirm `_dcb` stays a reserved Type with the 2-level `[streams, _dcb, SeqKey]`
  shape (it already is — `DCB_STREAM_PATH = ?STREAMS_PATH ++ [?DCB_STREAM]`). The
  only requirement is that the global readers (`all_events_pattern/0`,
  `convert_result_to_event`) tolerate the 2-level node. **No DCB consistency change
  — per-store, as decided.** `by_tag` index path unchanged.

### `reckon_db_links.erl`
- `get_source_streams(_, #{type := stream_pattern, pattern := <<"order-*">>})`:
  today = `list_streams/1` (whole store) then `filter_by_pattern/2` regex. Rewrite
  to navigate `reckon_db_stream_path:type_subtree(Type)` when the pattern is the
  common `prefix-*` shape — O(type) instead of O(store). Keep the regex path as a
  fallback for non-`prefix-*` patterns. This is the design's promised *simplification*.

### No change (verified)
- `reckon_db_store.erl` `init_store_paths` — only creates root `[streams]`.
- `reckon_db_temporal.erl`, `reckon_db_store_inspector.erl` — pure `reckon_db_streams`
  API consumers (StreamId in, StreamId out). Work unchanged once the API resolves
  paths internally.
- `reckon_db_snapshots_store.erl` — keyed by StreamId under `[snapshots, …]`.
- `reckon_db_subscriptions.erl` — uses `reckon_db_filters` + `read_all_global`
  (both updated) + in-memory `matches/3` (layout-independent).

---

## 4. Implementation order (each step compiles + tests green before next)

1. **`reckon-gater`: add `parts/1`** + tests. Bump gater minor. Refactor
   `prefix_of/1` onto `parts/1`. (No reckon-db dep yet — isolated.)
2. **`reckon-db`: add `reckon_db_stream_path`** module + unit tests for it in
   isolation (path build + round-trip for user/system/`_dcb`). Nothing calls it yet.
3. **Re-key the write path**: `reckon_db_streams` append + integrity tip resolution
   (`do_append`, `resolve_initial_tip`). Run `reckon_db_streams_SUITE` write cases.
4. **Re-key the read path**: `read_events`, `get_version`, `exists`, `delete`,
   `resolve_read_initial_tip`, `convert_result_to_event`, `list_streams`. Full
   `reckon_db_streams_SUITE` green.
5. **Global/cross-cutting reads**: `read_all_global`, `read_by_event_types`,
   `read_by_tags` via `all_events_pattern/0`. Run `reckon_db_tags_tests`,
   subscription catch-up cases.
6. **Filters**: `reckon_db_filters` wildcard depth + `by_stream` literal split.
   Run `reckon_db_subscriptions_SUITE`, `reckon_db_subscription_delivery_SUITE`.
7. **Snapshots + scavenge** path spots. Run `reckon_db_snapshots_SUITE`,
   `reckon_db_scavenge_tests`, all `reckon_db_integrity_*_SUITE`.
8. **DCB tolerance** check + `links` subtree rewrite. Run `reckon_db_dcb_SUITE`,
   `reckon_db_dcb_paths_tests`, `reckon_db_dcb_filter_tests`, `reckon_db_links_tests`.
9. **Delete dead code**: any old flat-path helper, `extract_stream_id` (folded into
   `stream_id_from_path/1`). Grep `[streams,` again → only `reckon_db_stream_path`
   and `?STREAMS_PATH = [streams]` remain.
10. **Full suite + dialyzer + store_inspector/temporal regression**.

---

## 5. Test plan (this is not optional — every step above lists its gate)

### 5.1 New unit tests
- **`reckon_gater_stream_id_tests`** (extend existing): `parts/1` for user, system
  (`$link:hot-orders` → `{system,<<"link">>,<<"hot-orders">>}`), hyphenated ns
  (`$link-sub:x`), malformed → `{error, malformed}`, `_dcb` → `{error, malformed}`.
  Property: for any `new(Prefix)`, `parts/1` ∘ id reproduces `{user, Prefix, _}`.
- **`reckon_db_stream_path_tests`** (new): `event_path/2`, `stream_path/1`,
  `type_subtree/1`, `all_events_pattern/0`, and the **round-trip property**
  `stream_id_from_path(event_path(Id, V)) =:= Id` for user + system ids; `_dcb`
  2-level path builds and round-trips its own way.

### 5.2 New integration coverage (the structural guarantees)
Add to `reckon_db_streams_SUITE` (or a new `reckon_db_namespace_SUITE`):
- **Namespace isolation**: write `ride-*` and `vehicle-*` into one store;
  `list_streams/1` returns both full opaque ids; a `type_subtree(<<"ride">>)`
  read sees zero `vehicle-*` events. (Direct assertion of the core win.)
- **Opaque round-trip**: append to `order-<hex>`, read back, assert
  `Event#event.stream_id =:= <<"order-…">>` byte-for-byte; same for `$link:x`.
- **`_dcb` coexistence**: with DCB events + regular events in one store,
  `read_all_global` returns both, sorted by `epoch_us`, none dropped, none
  mis-keyed. (Guards the 2-level/3-level tolerance.)
- **Cross-cutting still works post-rekey**: `read_by_tags`, `read_by_event_types`
  return the same sets as before across multiple aggregate types.

### 5.3 Regression suites that MUST stay green (no new assertions, just must pass)
`reckon_db_streams_SUITE`, `reckon_db_subscriptions_SUITE`,
`reckon_db_subscription_delivery_SUITE`, `reckon_db_snapshots_SUITE`,
`reckon_db_scavenge_tests`, `reckon_db_temporal_tests`,
`reckon_db_store_inspector_tests`, `reckon_db_links_tests`,
`reckon_db_dcb_SUITE`, `reckon_db_dcb_paths_tests`, `reckon_db_dcb_filter_tests`,
all four `reckon_db_integrity_*_SUITE`, `reckon_db_tags_tests`,
`reckon_db_naming_tests`.

### 5.4 Integrity-specific attention (highest-risk regression)
The tamper chain reads predecessor events by exact path
(`resolve_initial_tip`, `resolve_read_initial_tip`, `compute_event_chain_hash`).
All three must move to `event_path/2` **together** — a half-migrated path here
silently breaks chain verification (predecessor "not found" →
`integrity_setup_failed`). The `reckon_db_integrity_writes_SUITE` +
`reckon_db_integrity_reads_SUITE` are the canary; run them immediately after
step 3 and step 4.

### 5.5 Property test (optional but cheap, high value)
`property/` dir exists (proper configured). Add a property:
*for any sequence of appends across N random aggregate types, `read_all_global`
returns exactly the appended events and every event's `stream_id` round-trips.*
This catches path-encoding bugs no example test enumerates.

### 5.6 Commands
```
rebar3 eunit                         # unit (gater + db)
rebar3 ct --suite reckon_db_streams_SUITE
rebar3 ct                            # full integration
rebar3 proper                        # property
rebar3 dialyzer                      # types (parts/1 spec, new path module)
```
Gate: **all green + dialyzer clean** before the layout change is considered done.

---

## 6. Risks / watch-items

| Risk | Mitigation |
|------|-----------|
| Half-migrated integrity path → silent chain break | Step 3+4 move all three tip/hash sites together; integrity SUITEs run right after (§5.4) |
| `_dcb` 2-level node crashes a 3-level wildcard reader | `convert_result_to_event` + `all_events_pattern/0` explicitly tolerate 2-level; covered by §5.2 `_dcb` coexistence test |
| `list_streams` leaks `_dcb` or `$ns` nodes as "user streams" | `stream_id_from_path/1` + filter; assert in namespace isolation test |
| Round-trip not byte-identical (system ids: `$` + `:` reassembly) | dedicated round-trip property §5.1 covering both id shapes |
| Stray inline `[streams,…]` left behind | Step 9 re-grep gate: only `reckon_db_stream_path` + `?STREAMS_PATH` may remain |

---

## 7. Done criteria

- [ ] `reckon_gater_stream_id:parts/1` shipped + tested; gater minor bumped.
- [ ] `reckon_db_stream_path` is the **only** module that knows the 4-level layout.
- [ ] `grep -rE '\[streams,' src/` shows no event-path construction outside
      `reckon_db_stream_path` (root `?STREAMS_PATH` excepted).
- [ ] All suites in §5.3 green; new tests §5.1/§5.2/§5.5 green; dialyzer clean.
- [ ] Old flat-layout code deleted (no v1/v2 branch, no marker).
- [ ] `DESIGN_STREAM_NAMESPACE.md` §11 checkbox "DCB per-store" confirmed; this
      plan linked from `PLAN_ROOT.md`.
- [ ] Dev/demo stores (parksim, reckon-portal blog) recreated fresh under new layout.
```
