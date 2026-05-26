# Plan: DCB — Query-Based Concurrency, Full Stack

**Status:** Active — Design / Not Started
**Created:** 2026-05-26
**Last Updated:** 2026-05-27 — pre-flight spike + Khepri deep-dive landed; storage approach changed from custom-command to `khepri:transaction`; tag index reshaped as path structure; P3.1 sub-divided
**Target release:** `reckon-db` 2.4.0, `reckon-gater` 2.3.0, `reckon-evoq` 2.3.0, `evoq` 1.18.0
**Spans repos:** `reckon-db`, `reckon-gater`, `reckon-evoq`, `evoq`, plus one reference example
**Supersedes (in scope):** PLAN_FUTURE_RESEARCH.md § DCB Phase 3 (was deferred; now active)
**Related:** `hecate-corpus/philosophy/CONSISTENCY_BOUNDARIES.md` (user-facing doctrine)

---

## Goal

Ship full-stack DCB capability across the Reckon stack: storage primitive → wire → adapter → framework → reference example. Apps that need query-based concurrency can opt into it without touching internals.

## Why now

Want to be DCB-ready before need (per scoping decision 2026-05-26). No specific Hecate use case is driving; this is proactive capability work. Optimize for shippability + correctness; production-hardening over time as real workloads exercise it.

## Non-goals (this round)

- **Replacing aggregates.** `evoq_aggregate` stays. `evoq_decision` is added alongside, not as a replacement.
- **Migrating the Dossier model.** The doctrine in `CONSISTENCY_BOUNDARIES.md` is unchanged. DCB becomes a real escape hatch, not the new default.
- **Sharding `?DCB_STREAM`.** Single-pseudo-stream model is the v1. Horizontal partitioning by tag-hash is future work if throughput becomes a constraint.
- **Snapshot support for DCB events.** Aggregate snapshotting doesn't apply (no aggregates). Out of scope.
- **Production-grade reference example.** The reference example is a learning artefact, not a customer-grade app.

---

## Spike findings (2026-05-27)

Two findings before P3.1 starts:

**1. Phase 1 did NOT ship a real tag index.** `read_by_tags` (`src/reckon_db_streams.erl:333`) fetches *all events from all streams* via `khepri:get_many` and filters client-side. The code comments admit it: *"For large stores, consider maintaining a separate tag index."* Tags-as-event-field shipped; tags-as-index did not. P3.1 has to build the real index as a prerequisite.

**2. Khepri primitives are richer than initially assumed.** `khepri:transaction/2` + `khepri_tx:get_many/2` + `khepri_tx:put/3` + `khepri_tx:abort/1` give us atomic multi-path read + conditional write inside a single Ra consensus operation — exactly the DCB primitive. **We do NOT need a custom Khepri machine command.** Transactions are heavily restricted (pure functions, whitelisted BIFs, no message-sending), but our DCB body fits inside those constraints.

**3. The tag index belongs in the tree, not in a separate data structure.** Khepri's tree paths are lexicographically ordered. Writing each event's tag-bindings as additional tree nodes — `/by_tag/{tag}/{seq} → {}` — gives us a native subtree-scan index. `khepri_tx:get_many([by_tag, Tag, ?KHEPRI_WILDCARD_STAR])` is a bounded scan inside the transaction, no projection-in-transaction question to resolve.

**Implications:** No `reckon_db_dcb_command.erl`. No custom Ra machine extension. Storage model unchanged (`?DCB_STREAM`) but accompanied by `/by_tag/` mirror entries written in the same transaction as the event. P3.1 scope grows to include the tag-index path-write paired with each event-write.

Full spike record (Khepri capability survey + reckon-db code-path verification) lives in conversation history 2026-05-27. Source-of-truth pointers: `khepri:transaction/2` and `khepri_projection` doc pages on hexdocs.pm; `src/reckon_db_streams.erl:333` for the no-index admission.

### P3.0 verification — Khepri 0.17.2 compat (executed 2026-05-27)

A throwaway Common Test suite (`dcb_transaction_spike_SUITE`, since deleted) ran against the real `rebar.config` Khepri pin. **All four scenarios passed**:

1. **Basic put + get in one transaction** — `khepri_tx:put` then `khepri_tx:get` inside `khepri:transaction/2` works as expected.
2. **Abort rolls back atomically** — `khepri_tx:abort(Reason)` rolls back partial writes; the path written inside the transaction is NOT visible after abort, while pre-transaction writes remain.
3. **DCB-shape conditional append** — read `/by_tag/{tag}/*` subtree, filter seqs by cutoff, either `khepri_tx:abort({context_changed, MaxSeq})` or `khepri_tx:put` event + tag-index entries. Both branches behave correctly.
4. **BIF whitelist** — `sets:{from_list,intersection,to_list}`, `lists:{flatmap,foldl,foreach,max}`, `maps:keys`, `binary_to_integer`, `io_lib:format`, `iolist_to_binary`, list comprehensions, `case` expressions, anonymous functions all permitted inside a transaction body.

**Concrete return shapes (Khepri 0.17.2, NOT Mnesia-style):**

| Function | Success | Failure |
|----------|---------|---------|
| `khepri:put/3` | `ok` | `{error, _}` |
| `khepri:get/2,3` | `{ok, Payload}` | `{error, _}` |
| `khepri:transaction/2` success | `{ok, BodyReturn}` | — |
| `khepri:transaction/2` after abort | `{error, AbortReason}` | — |
| `khepri_tx:put/2,3` | `ok` | exception |
| `khepri_tx:get/1,2` | `{ok, Payload}` | `{error, _}` |
| `khepri_tx:get_many/1,2` | `{ok, #{Path => Payload}}` | `{error, _}` |
| `khepri_tx:abort/1` | does not return (transforms into `{error, Reason}` at the outer transaction) | — |

**Required init sequence** (no shortcut):

```erlang
application:set_env(ra, data_dir, RaDataDir),
{ok, _} = ra:start([{data_dir, RaDataDir}]),
%% Per-store ra_system (matches reckon_db_store production pattern)
RaSystemName = list_to_atom("ra_" ++ atom_to_list(StoreId)),
RaSystemConfig = (ra_system:default_config())#{
    name => RaSystemName,
    data_dir => StoreDataDir,
    wal_data_dir => StoreDataDir,
    names => ra_system:derive_names(RaSystemName)
},
case ra_system:start(RaSystemConfig) of
    {ok, _}                       -> ok;
    {error, {already_started, _}} -> ok
end,
{ok, _} = application:ensure_all_started(khepri),
{ok, _} = khepri:start(RaSystemName, StoreId, 5000).
```

P3.2's test setup should reuse this pattern. The existing `reckon_db_test_helpers:ensure_store/1` works for some cases but uses an undocumented `khepri:start(StoreId, Map)` signature; the per-store ra_system pattern is the production-correct way.

P3.0 status: ✅ complete. No blockers identified for P3.1.

---

## Architecture overview

```
┌─────────────────────────────────────────────────────────────────┐
│ Reference example app: hecate-corpus/examples/DCB_*             │
│   uses evoq_decision behaviour                                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────────┐
│ evoq (1.18.0)                                                   │
│   evoq_decision behaviour    + runtime that wires context →     │
│   evoq_decision_runtime        decide → append_if_no_tag_matches│
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────────┐
│ reckon-evoq (2.3.0)                                             │
│   adapter passthrough for append_if_no_tag_matches              │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────────┐
│ reckon-gater (2.3.0)                                            │
│   new wire verb: append_if_no_tag_matches                       │
│   request/response records in reckon_gater_types.hrl            │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────────┐
│ reckon-db (2.4.0)                                               │
│   reckon_db_log_backend:append_if_no_tag_matches/5 callback     │
│   Implementation:    khepri:transaction/2 body (no custom cmd)  │
│   Storage:           ?DCB_STREAM pseudo-stream + path-based      │
│                      tag index at /by_tag/{tag}/{seq}            │
│   Bounded scan via subtree iteration on /by_tag/{tag}/**         │
└─────────────────────────────────────────────────────────────────┘
```

---

## Storage model decision: `?DCB_STREAM` + path-based tag index

Two coupled storage decisions for v1:

### 1. DCB events land in `?DCB_STREAM` pseudo-stream

All DCB events land in one stream named `?DCB_STREAM` = `<<"_dcb">>`. Per-event identity is the global `sequence_number`. Stream version on `?DCB_STREAM` is meaningful internally (monotonic write counter) but is not the concurrency unit — the tag-filter check is.

| Pro | Con |
|-----|-----|
| Zero schema change | All DCB writes serialize through one Ra consensus group |
| Existing stream machinery (read, subscribe) works without modification | Per-stream throughput cap (~10k–100k ops/s on standard hardware) becomes the ceiling for all DCB writes combined |
| Easy to identify DCB events in operator tools (filter by stream) | Future partitioning means revisiting the constant |

Justification for accepting the throughput cap: DCB targets cross-cutting decisions (uniqueness, allocation, idempotency) which are typically lower-volume than per-aggregate flows. Per-aggregate flows stay on per-stream Ra groups (unchanged). If DCB write volume ever approaches the single-stream ceiling, we partition; that's a v2 problem.

### 2. Tag index as Khepri path structure under `/by_tag/`

Every DCB event with `tags = [T1, T2, ..., Tn]` and `sequence_number = Seq` writes:

```
/events/_dcb/<zero-padded-seq>      →  full event payload
/by_tag/T1/<zero-padded-seq>        →  #{}    (empty marker, just for path existence)
/by_tag/T2/<zero-padded-seq>        →  #{}
...
/by_tag/Tn/<zero-padded-seq>        →  #{}
```

All N+1 path writes happen inside the same `khepri:transaction`, so they're atomic with the event append.

**Zero-padded seq** (e.g., 20 hex digits) gives lexicographic ordering equivalent to numeric ordering — required so that subtree iteration over `/by_tag/{tag}/` returns events in seq order, and so that "seq > N" can be a prefix-comparison filter.

**Bounded scan** for tag-filter check inside the transaction:
- `{any_of, [T1, T2]}`: N subtree scans, one per tag, union the results
- `{all_of, [T1, T2]}`: scan the smallest-cardinality tag's subtree, intersect against the others
- `{and_, ...}` / `{or_, ...}`: recursive composition

**Storage cost**: one tree node per (event × tag). For typical DCB workloads (uniqueness checks with 1-2 tags per event), the index is ~1-2× the event count in tree nodes, which is acceptable. Empty payload (`#{}`) keeps the per-node memory minimal.

**Tag-index is forward-only**: existing aggregate-style events (not appended via DCB primitive) do NOT get `/by_tag/` mirror entries. They remain queryable via the old `read_by_tags` (full scan + filter) but are NOT visible to the DCB consistency check. This is acceptable because DCB consistency is meaningful only across DCB-appended events; Dossier-stream events live in a different consistency universe.

**Note**: This index also makes the existing `read_by_tags` fast for DCB events. The old full-scan path stays as a fallback for cross-stream queries that need to see Dossier events too. Optimizing the general `read_by_tags` to use `/by_tag/` for all events is a follow-on (would need backfill).

### Khepri version requirement

`khepri:transaction/2`, `khepri_tx:get_many/2`, `khepri_tx:put/2`, `khepri_tx:abort/1` are required. Confirmed available in Khepri 0.11+. Verify `rebar.config` pin matches.

---

## Tag filter type

Defined in `reckon-gater` (shared types):

```erlang
%% reckon-gater/include/reckon_gater_types.hrl
-type tag() :: binary().

-type tag_filter() ::
      {any_of, [tag()]}             %% match if event has ANY tag in list
    | {all_of, [tag()]}             %% match if event has ALL tags in list
    | {and_, [tag_filter()]}        %% logical AND of sub-filters
    | {or_, [tag_filter()]}.        %% logical OR of sub-filters
```

Note: `{tags, Tags}` (existing read API convention) maps to `{all_of, Tags}`. The read API may grow `tag_filter()` support as a follow-on; not in scope for this plan.

---

## Phase plan

Each phase is one PR. Phases are sequential (later depends on earlier).

| # | Layer | Scope | Repo | Estimate |
|---|-------|-------|------|----------|
| **P3.0** | Spike | Verify `khepri:transaction/2` semantics with throwaway test; confirm Khepri version pin | `reckon-db` | 0.5 day |
| **P3.1a** | Storage | Path helpers + `?DCB_STREAM_PATH` / `?BY_TAG_PATH` constants + zero-padded seq formatting | `reckon-db` | 0.5 day |
| **P3.1b** | Storage | Tag-filter evaluation inside transactions (any_of, all_of, and_, or_) | `reckon-db` | 1 day |
| **P3.1c** | Storage | `append_if_no_tag_matches/5` as a `khepri:transaction` body — reads `/by_tag/` subtrees, scans for seq > cutoff, atomically writes event + tag-index entries OR aborts | `reckon-db` | 1.5 days |
| **P3.1d** | Storage | Behaviour callback in `reckon_db_log_backend` + facade in `reckon_db` / `reckon_db_streams` | `reckon-db` | 0.5 day |
| **P3.1e** | Storage | Decide + document tag-index forward-only policy; add CHANGELOG entry | `reckon-db` | 0.25 day |
| **P3.2** | Storage | Unit + integration tests (concurrent contention, large scans, edge cases, transaction-replay determinism) | `reckon-db` | 2 days |
| **P3.3** | Wire | `reckon-gater` types + verb | `reckon-gater` | 1 day |
| **P3.4** | Wire | `reckon-db` gateway worker dispatch | `reckon-db` | 1 day |
| **P3.5** | Adapter | `reckon-evoq` passthrough | `reckon-evoq` | 0.5 day |
| **P3.6** | Framework | `evoq_decision` behaviour + runtime | `evoq` | 3 days |
| **P3.7** | Framework | `evoq_decision` tests (property-based, concurrent contention) | `evoq` | 2 days |
| **P3.8** | Example | Reference example: `examples/dcb_counter` | `hecate-corpus` | 1 day |
| **P3.9** | Docs | Flip `CONSISTENCY_BOUNDARIES.md` "Decision" entry from reserved → active; update CODEX.md cornerstone chapter; update GLOSSARY | `hecate-corpus` | 0.5 day |

**Total estimate:** ~14.75 working days for one person. ~3-4 weeks calendar with reviews + integration.

P3.1a–e are all in `reckon-db`. They can be one PR or split — recommended split is `{P3.0, P3.1a, P3.1b}` as PR-1 (foundations), `{P3.1c, P3.1d, P3.1e, P3.2}` as PR-2 (the primitive + tests). Two PRs total for the reckon-db side.

---

## P3.1 — reckon-db: storage primitive

Implemented as a `khepri:transaction/2` body (no custom Ra machine command). The transaction reads `/by_tag/{tag}/` subtrees, filters by seq > cutoff, and either appends the event + tag-index entries OR aborts with `{context_changed, MaxSeq}`. All N+1 writes (1 event + N tag-index entries) happen atomically inside the same transaction.

### Path helpers + constants (P3.1a)

`include/reckon_db_internal.hrl`:

```erlang
-define(DCB_STREAM,        <<"_dcb">>).
-define(DCB_STREAM_PATH,   [events, ?DCB_STREAM]).
-define(BY_TAG_PATH,       [by_tag]).
-define(SEQ_KEY_WIDTH,     20).
```

Helpers in `src/reckon_db_dcb_paths.erl` (new file):

```erlang
-spec event_path(non_neg_integer()) -> [term()].
event_path(Seq) -> ?DCB_STREAM_PATH ++ [seq_key(Seq)].

-spec by_tag_path(binary(), non_neg_integer()) -> [term()].
by_tag_path(Tag, Seq) -> ?BY_TAG_PATH ++ [Tag, seq_key(Seq)].

-spec by_tag_pattern(binary()) -> [term()].
by_tag_pattern(Tag) -> ?BY_TAG_PATH ++ [Tag, ?KHEPRI_WILDCARD_STAR].

-spec seq_key(non_neg_integer()) -> binary().
seq_key(Seq) ->
    iolist_to_binary(io_lib:format("~*.16.0B", [?SEQ_KEY_WIDTH, Seq])).

-spec seq_from_key(binary()) -> non_neg_integer().
seq_from_key(Bin) -> binary_to_integer(Bin, 16).
```

Zero-padded uppercase-hex format gives lexicographic == numeric ordering, so subtree iteration returns events in seq order.

### Tag-filter evaluation inside transactions (P3.1b)

`src/reckon_db_dcb_filter.erl` (new file). Pure functions only — transaction-safe.

```erlang
-spec match_any_above_cutoff(
    reckon_gater_types:tag_filter(),
    non_neg_integer()
) -> {true, MaxSeq :: non_neg_integer()} | false.
match_any_above_cutoff({any_of, Tags}, Cutoff) ->
    Seqs = lists:flatmap(fun seqs_for_tag/1, Tags),
    Hits = [S || S <- Seqs, S > Cutoff],
    case Hits of
        []   -> false;
        _    -> {true, lists:max(Hits)}
    end;
match_any_above_cutoff({all_of, Tags}, Cutoff) ->
    [HeadTag | RestTags] = Tags,
    HeadSet = sets:from_list(seqs_for_tag(HeadTag)),
    Intersection = lists:foldl(
        fun(T, Acc) -> sets:intersection(Acc, sets:from_list(seqs_for_tag(T))) end,
        HeadSet, RestTags),
    Above = [S || S <- sets:to_list(Intersection), S > Cutoff],
    case Above of
        []   -> false;
        _    -> {true, lists:max(Above)}
    end;
match_any_above_cutoff({and_, Filters}, Cutoff) ->
    Results = [match_any_above_cutoff(F, Cutoff) || F <- Filters],
    case lists:all(fun(R) -> R =/= false end, Results) of
        true  -> {true, lists:max([M || {true, M} <- Results])};
        false -> false
    end;
match_any_above_cutoff({or_, Filters}, Cutoff) ->
    Results = [match_any_above_cutoff(F, Cutoff) || F <- Filters],
    case [M || {true, M} <- Results] of
        []   -> false;
        Hits -> {true, lists:max(Hits)}
    end.

%% Reads the tag-index subtree for one tag. Called from inside a
%% khepri transaction so the read is consistent with the conditional append.
-spec seqs_for_tag(binary()) -> [non_neg_integer()].
seqs_for_tag(Tag) ->
    Pattern = reckon_db_dcb_paths:by_tag_pattern(Tag),
    {ok, Map} = khepri_tx:get_many(Pattern),
    [reckon_db_dcb_paths:seq_from_key(SeqKey)
     || [_, _, SeqKey] <- maps:keys(Map)].
```

No message-sending, no ETS, no I/O. `khepri_tx:get_many/1` is whitelisted inside transactions.

### The transaction body (P3.1c)

`src/reckon_db_dcb.erl` (new file):

```erlang
-spec append_if_no_tag_matches(
    StoreId   :: binary(),
    TagFilter :: reckon_gater_types:tag_filter(),
    SeqCutoff :: non_neg_integer(),
    Events    :: [new_event()]
) -> {ok, NewVersion :: non_neg_integer()}
   | {error, {context_changed, non_neg_integer()}}
   | {error, term()}.
append_if_no_tag_matches(StoreId, TagFilter, SeqCutoff, Events) ->
    khepri:transaction(
        StoreId,
        fun() ->
            case reckon_db_dcb_filter:match_any_above_cutoff(TagFilter, SeqCutoff) of
                {true, MaxSeq} ->
                    khepri_tx:abort({context_changed, MaxSeq});
                false ->
                    {ok, BaseSeq} = next_dcb_seq(),
                    lists:foldl(
                        fun(Event, Seq) ->
                            ok = write_event_with_tag_index(Event, Seq),
                            Seq + 1
                        end,
                        BaseSeq, Events),
                    NewVersion = BaseSeq + length(Events) - 1,
                    {ok, NewVersion}
            end
        end).

%% Inside the transaction. Writes the event + one /by_tag/T/Seq entry per tag.
write_event_with_tag_index(Event, Seq) ->
    EventPayload = stamp_event(Event, Seq),
    Tags = maps:get(tags, Event, []),
    ok = khepri_tx:put(reckon_db_dcb_paths:event_path(Seq), EventPayload),
    lists:foreach(
        fun(Tag) ->
            ok = khepri_tx:put(reckon_db_dcb_paths:by_tag_path(Tag, Seq), #{})
        end,
        Tags),
    ok.

%% Read current head of ?DCB_STREAM_PATH inside the transaction.
%% Implementation detail deferred to P3.1c; perf-tune later.
next_dcb_seq() -> ...
```

### Behaviour callback addition (P3.1d)

`src/reckon_db_log_backend.erl`:

```erlang
%% Append events to ?DCB_STREAM atomically conditional on the absence of
%% any events matching TagFilter beyond SeqCutoff.
%%
%% Returns:
%%   {ok, version()}  — appended; version is the new ?DCB_STREAM head
%%   {error, {context_changed, max_seq :: non_neg_integer()}} — a matching
%%     event existed at or after SeqCutoff; caller must re-read and retry
%%   {error, Reason} — backend-specific
%%
%% MUST be atomic: scan + append happen under a single consensus log entry.
-callback append_if_no_tag_matches(
    State     :: state(),
    StoreId   :: store_id(),
    TagFilter :: reckon_gater_types:tag_filter(),
    SeqCutoff :: non_neg_integer(),
    Events    :: [new_event()]
) ->
      {ok, version()}
    | {error, {context_changed, non_neg_integer()}}
    | {error, term()}.
```

Add to `-optional_callbacks/1`. Backends that don't implement it return `{error, not_supported}` via a default.

### Facade (P3.1d cont'd)

`src/reckon_db_streams.erl` (matches existing convention — `read_by_tags` lives there):

```erlang
-spec append_if_no_tag_matches(
    StoreId   :: binary(),
    TagFilter :: reckon_gater_types:tag_filter(),
    SeqCutoff :: non_neg_integer(),
    Events    :: [new_event()]
) -> {ok, version()} | {error, term()}.
append_if_no_tag_matches(StoreId, TagFilter, SeqCutoff, Events) ->
    reckon_db_gateway:call(StoreId,
        {append_if_no_tag_matches, StoreId, TagFilter, SeqCutoff, Events}).
```

### Policy: tag-index is forward-only (P3.1e)

Existing aggregate-stream events (appended before P3.1 ships) do NOT receive `/by_tag/` mirror entries. They remain queryable via the old `read_by_tags` (full scan + filter) but are NOT visible to the DCB consistency check.

This is correct for DCB because consistency only makes sense over the DCB event set. Cross-mode reads (general analytics over BOTH Dossier events and DCB events) continue to work via the existing full-scan `read_by_tags`.

Documented in `CHANGELOG.md` + `philosophy/CONSISTENCY_BOUNDARIES.md` discrimination rule (P3.9).

### Errors

New error class: `{context_changed, MaxSeq :: non_neg_integer()}`. The `evoq_decision_runtime` (P3.6) catches and retries with backoff + jitter, bounded by `retry_budget/0`. Document in CHANGELOG and as a note in `skills/antipatterns/event_sourcing.md` ("unbounded retry on context_changed is the cardinal sin of DCB users; always use a bounded retry budget").

---

## P3.2 — reckon-db: tests

### Unit tests (`test/unit/reckon_db_dcb_tests.erl`)

- Append with empty store, no matching tags → succeeds, new event at seq=1
- Append with matching tag below cutoff → succeeds (cutoff filters out)
- Append with matching tag above cutoff → `{error, {context_changed, _}}`
- Concurrent appends with same context → one succeeds, other gets `context_changed`
- Empty event list → `{error, no_events}` (don't allow empty appends)
- Backend without callback → `{error, not_supported}`

### Integration tests (`test/integration/reckon_db_dcb_SUITE.erl`)

- 1000 concurrent processes each trying to append "register unique X" with the same X → exactly one succeeds; others get `context_changed`
- Long-running tag-filter scan under contention (10k tagged events; verify scan stays bounded)
- DCB events appear in `read_by_tags` results (Phase 1 interop)
- DCB events trigger tag-aware subscriptions (Phase 2 interop)
- Restart resilience: leader killed mid-flight; cluster recovers; no duplicate events

### Property tests (optional, P3.7 also has property tests)

`proper`-based: any sequence of concurrent DCB appends produces a serializable outcome consistent with single-threaded execution.

---

## P3.3 + P3.4 — reckon-gater wire + gateway worker

### `reckon-gater/include/reckon_gater_types.hrl`

Add the `tag_filter()` type definition + request/response records:

```erlang
-record(append_if_no_tag_matches_req, {
    store_id   :: binary(),
    tag_filter :: tag_filter(),
    seq_cutoff :: non_neg_integer(),
    events     :: [event_in()]
}).

-record(append_if_no_tag_matches_res, {
    new_version :: non_neg_integer()
}).

-record(context_changed, {
    max_seq :: non_neg_integer()
}).
```

### `reckon-gater/src/reckon_gater_api.erl`

```erlang
-spec append_if_no_tag_matches(Pid, StoreId, TagFilter, SeqCutoff, Events) ->
    {ok, version()} | {error, term()}.
append_if_no_tag_matches(Pid, StoreId, TagFilter, SeqCutoff, Events) ->
    gen_server:call(Pid,
        #append_if_no_tag_matches_req{
            store_id = StoreId, tag_filter = TagFilter,
            seq_cutoff = SeqCutoff, events = Events
        }).
```

### `reckon-db/src/reckon_db_gateway_worker.erl`

Add `handle_call/3` clause for the new request shape; route to `reckon_db_dcb:append_if_no_tag_matches/4`.

---

## P3.5 — reckon-evoq adapter

`reckon-evoq/src/reckon_evoq_adapter.erl` — add passthrough:

```erlang
-spec append_if_no_tag_matches(StoreId, TagFilter, SeqCutoff, Events) ->
    {ok, non_neg_integer()} | {error, term()}.
append_if_no_tag_matches(StoreId, TagFilter, SeqCutoff, Events) ->
    reckon_gater_api:append_if_no_tag_matches(
        get_gater_pid(StoreId), StoreId, TagFilter, SeqCutoff, Events).
```

That's the whole change. Adapter stays thin.

---

## P3.6 — evoq: `evoq_decision` behaviour

### Behaviour definition

`evoq/src/evoq_decision.erl`:

```erlang
-module(evoq_decision).

-export_type([decision_id/0, context/0]).

-type decision_id() :: binary().
-type context()    :: #{tag_filter := reckon_gater_types:tag_filter(),
                        seq_cutoff := non_neg_integer()}.

%% Define the context this decision needs: which events to query, up to
%% what sequence number. The runtime captures the current max sequence
%% before reading; SeqCutoff returned here is typically that value.
-callback context(Command :: map()) -> reckon_gater_types:tag_filter().

%% Make the decision given the context events. Pure function.
-callback decide(ContextEvents :: [event()], Command :: map()) ->
      {ok, [new_event()]}
    | {error, Reason :: term()}.

%% Optional: report the retry budget. Default: 3 retries with exponential
%% backoff + jitter.
-callback retry_budget() -> non_neg_integer().
-optional_callbacks([retry_budget/0]).
```

### Runtime

`evoq/src/evoq_decision_runtime.erl`:

```erlang
%% Public API
-spec dispatch(module(), Command :: map()) ->
    {ok, [event()]} | {error, term()}.
dispatch(DecisionMod, Command) ->
    dispatch(DecisionMod, Command, retry_budget(DecisionMod)).

dispatch(_DecisionMod, _Command, 0) ->
    {error, retry_budget_exhausted};
dispatch(DecisionMod, Command, Retries) ->
    TagFilter = DecisionMod:context(Command),
    StoreId = current_store_id(),
    %% Capture cutoff BEFORE reading
    {ok, SeqCutoff} = reckon_evoq_adapter:current_max_sequence(StoreId),
    {ok, ContextEvents} = reckon_evoq_adapter:read_by_tag_filter(StoreId, TagFilter),
    case DecisionMod:decide(ContextEvents, Command) of
        {ok, NewEvents} ->
            case reckon_evoq_adapter:append_if_no_tag_matches(
                   StoreId, TagFilter, SeqCutoff, NewEvents) of
                {ok, _Version} ->
                    {ok, NewEvents};
                {error, {context_changed, _}} ->
                    %% retry with fresh context
                    backoff(Retries),
                    dispatch(DecisionMod, Command, Retries - 1);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
```

Note: `read_by_tag_filter` is a new helper that translates `tag_filter()` into the existing `read_by_tags` API (`{all_of, Tags}` → `read_by_tags(StoreId, Tags, all, ...)`). For `or_`/`and_` filters, multiple reads + client-side combination (acceptable for the v1).

### Registration

`evoq_decision` modules don't need supervisor registration (no per-decision state). They're stateless. The runtime takes the module name + command and runs.

---

## P3.8 — Reference example

`hecate-corpus/examples/dcb_counter/`:

Demonstrates the canonical "increment a counter respecting a maximum" pattern. Files:

- `dcb_counter.erl` — implements `evoq_decision` behaviour
- `commands.erl` — `#increment_counter{counter_id, max}`
- `events.erl` — `#counter_incremented_v1{counter_id, new_value}`
- `dcb_counter_test.erl` — concurrent test: 100 procs each trying to increment past max=10; verify exactly 10 succeed

```erlang
%% dcb_counter.erl — illustrates the evoq_decision behaviour
-module(dcb_counter).
-behaviour(evoq_decision).
-export([context/1, decide/2]).

context(#{counter_id := CounterId}) ->
    {all_of, [<<"counter:", CounterId/binary>>]}.

decide(ContextEvents, #{counter_id := CounterId, max := Max}) ->
    Current = length([E || E = #{event_type := <<"counter_incremented_v1">>} <- ContextEvents]),
    case Current < Max of
        true ->
            NewValue = Current + 1,
            {ok, [#{
                event_type => <<"counter_incremented_v1">>,
                data => #{counter_id => CounterId, new_value => NewValue},
                tags => [<<"counter:", CounterId/binary>>]
            }]};
        false ->
            {error, counter_full}
    end.
```

Plus a markdown narrative `examples/DCB_COUNTER.md` explaining what's happening and when to reach for `evoq_decision` instead of `evoq_aggregate`.

---

## P3.9 — Hecate-corpus doc updates

**`philosophy/CONSISTENCY_BOUNDARIES.md`** — flip the "Decision (reserved)" status:
- "Reserved" → "Active capability, opt-in"
- Add cross-reference to the `examples/dcb_counter` example
- Add discrimination rule sub-section: when to reach for `evoq_decision` vs `evoq_aggregate`

**`GLOSSARY.md`** — flip the "Decision (reserved term)" entry to "Decision":
- Drop "reserved" qualifier
- Point to the example
- Note: `evoq_decision` behaviour available since `evoq` 1.18.0; `reckon-db` 2.4.0

**`CODEX.md`** — cornerstone Chapter 4 (5D Hierarchy):
- Add subsection "Two write-side constructs": Dossier + Decision
- Reference the example
- Don't change the 5D mnemonic (Domain/Division/Department/Desk/Dossier stays); Decision is a sibling-to-Dossier write construct, not a sixth D

**`skills/antipatterns/event_sourcing.md`** — note about Demon 41:
- Add brief note: "Demon 41 is structural inside `evoq_aggregate`. `evoq_decision` has no inside — the query IS the read, by design. The aggregate-side cure (command pipelines) doesn't apply."

---

## Cross-repo coordination

Versions move together. The version matrix at completion:

| Repo | Pre-DCB | Post-DCB | Compatibility |
|------|---------|----------|---------------|
| `reckon-db` | 2.3.x | 2.4.0 | backward-compat: existing append APIs unchanged |
| `reckon-gater` | 2.2.x | 2.3.0 | backward-compat: new verb added, existing unchanged |
| `reckon-evoq` | 2.2.x | 2.3.0 | backward-compat: new passthrough; existing API unchanged |
| `evoq` | 1.17.x | 1.18.0 | backward-compat: new behaviour; `evoq_aggregate` unchanged |

Apps using only `evoq_aggregate` stay on whatever evoq version they're on. Apps that want `evoq_decision` pull the new versions.

Hex publish order: reckon-db → reckon-gater → reckon-evoq → evoq → reference example.

---

## Test strategy

- **Unit tests** for the storage primitive (P3.2)
- **Integration tests** for concurrent contention (P3.2)
- **Property tests** for serializability (P3.2 optional, P3.7 required)
- **Reference example tests** for the full stack (P3.8)
- **Performance benchmarks** in `benchmarks/slices/dcb_*/` (P3.2):
  - Throughput at various tag cardinalities (10, 100, 1k, 10k unique tag values)
  - Tail latency under contention
  - Compare against per-aggregate baseline at equivalent workloads

---

## Performance considerations

Critical hot path: the tag-filter scan inside the `khepri:transaction/2` body. The transaction executes on every Ra cluster member identically (deterministic-replay requirement); slow transaction bodies block consensus on the leader and slow apply on followers.

**Concerns:**
- Scan cost is O(matching_events) per tag, bounded by subtree size at `/by_tag/{tag}/`. Whole-tag cardinality matters, not whole-store cardinality.
- Under heavy DCB write contention, transaction bodies serialize through Ra consensus on `?DCB_STREAM`'s consensus group.
- The single `?DCB_STREAM` is a serialization point — all DCB writes synchronize through one Ra consensus group.
- `khepri_tx:get_many/1` materializes the matching subtree as a map; large subtrees → memory pressure inside the transaction.

**Mitigations:**
- Bound scan by `SeqCutoff` — caller passes a recent sequence number; the filter function discards seqs ≤ Cutoff before materializing the result. Combined with zero-padded seq keys, this is a lexicographic range filter on the subtree.
- Document expected SeqCutoff selection (typically: "max seq at read time", captured by `evoq_decision_runtime` before reading context).
- Avoid hot tags. If one tag (e.g., a tenant-wide rate-limit key) accumulates millions of events, the scan grows linearly. Use compound tags or sharded tags at the application layer.
- Benchmark and document the throughput ceiling clearly. Recommend `evoq_decision`'s retry budget defaults match measured contention rates.

**Acceptance threshold for v1:**
- 10k DCB appends/sec sustained on the reference 3-node cluster (per-tag cardinality ≤ 1k)
- p99 latency under 100ms with 100 concurrent contenders on the same context
- No leader crashes under sustained load (24h soak test)
- Transaction-body memory footprint per call bounded ≤ 10MB at 10k matching tag entries

---

## Migration / compatibility

Zero migration. Existing Dossiers + `evoq_aggregate` keep working unchanged. DCB is purely additive.

Rollback: if Phase 3 ships and we find a deal-breaker in production, the storage callback can be removed without affecting existing data. DCB events in `?DCB_STREAM` remain readable via existing stream APIs.

---

## Open questions

1. **`OR_` and `AND_` filter scan semantics** — resolved by P3.1b's `match_any_above_cutoff/2` recursive composition. `any_of` unions tag subtrees; `all_of` intersects them; `or_`/`and_` compose recursively. No separate v2 design needed.

2. **`next_dcb_seq/0` implementation.** Inside the transaction, we need the next-to-assign seq. Two options: (a) count children of `?DCB_STREAM_PATH` (O(stream-size), slow); (b) maintain a counter node at `/_dcb_seq` updated atomically with each append (O(1)). Decision: option (b). Document in P3.1c.

3. **Cutoff semantics for empty stores.** `SeqCutoff = 0` on an empty store should be valid (no events match, append succeeds). Test in P3.2.

4. **Should DCB events appear in `read_all_global`?** Yes — they're real events under `?DCB_STREAM_PATH`. They appear in the global log alongside Dossier events. Document in the example narrative.

5. **Snapshot story for `?DCB_STREAM`.** No snapshotting in v1. Phase 1 + 2 read APIs don't snapshot either. Aggregate snapshotting doesn't apply.

6. **`evoq_decision` and process managers.** Can a PM dispatch a Decision? Yes — same as dispatching a Command. The PM's `dispatch` call routes through `evoq_decision_runtime:dispatch/2` instead of `evoq_dispatcher`. Document in P3.6.

7. **Decision-side replay.** When replaying events for analytics or rebuilding projections, DCB events flow through the same triggers as Dossier events. No special handling needed.

8. **`khepri_tx` whitelist.** The implementation relies on `khepri_tx:get_many/1`, `khepri_tx:put/2`, `khepri_tx:abort/1`, plus `sets:from_list/1`, `sets:intersection/2`, `sets:to_list/1`, `lists:max/1`, `lists:flatmap/2`, `lists:foldl/3`, `lists:foreach/2`, `maps:keys/1`, `maps:get/2,3`, `binary_to_integer/2`, `io_lib:format/2`, `iolist_to_binary/1`. Verify all are inside Khepri's transaction whitelist during P3.0 spike. If any aren't, push them outside the transaction (preprocess in the facade).

---

## Doc updates (already drafted at plan-time)

- ✅ `PLAN_FUTURE_RESEARCH.md` § DCB — Phase 3 marked as "active work in PLAN_DCB_IMPLEMENTATION.md" (this file)
- ⏳ `PLAN_ROOT.md` — add this plan to Active Plans table
- ⏳ `hecate-corpus/philosophy/CONSISTENCY_BOUNDARIES.md` — Decision section flips reserved → active when P3.8 ships
- ⏳ `hecate-corpus/GLOSSARY.md` — Decision entry de-reserved when P3.8 ships
- ⏳ `hecate-corpus/CODEX.md` — Chapter 4 amendment when P3.8 ships
- ⏳ `reckon-db/CHANGELOG.md` — entry per PR

---

## Ready-to-cut checklist

Spike findings already resolve some original items. Updated list:

- [x] ~~reckon-db tag index supports range scan by `sequence_number > N`~~ — **CONFIRMED: it does not exist.** Tag index is the FIRST thing P3.1a/b builds (path structure under `/by_tag/`). The plan adapts accordingly.
- [x] ~~Khepri version supports custom commands~~ — **NOT NEEDED.** Implementation uses `khepri:transaction/2`, not a custom Ra machine command. Required Khepri functions: `transaction/2`, `khepri_tx:get_many/1`, `khepri_tx:put/2`, `khepri_tx:abort/1` — all available in Khepri 0.11+. **Pre-flight P3.0: verify the version pin in `rebar.config` matches.**
- [ ] **P3.0 spike**: write a throwaway test that runs `khepri:transaction/2` with `khepri_tx:get_many/1` + conditional `khepri_tx:put/2` + `khepri_tx:abort/1`. Confirm semantics: abort rolls back, conditional path executes atomically, BIF whitelist accepts our planned helper modules.
- [ ] CHANGELOG conventions allow multi-PR feature work (yes, per existing CHANGELOG)
- [ ] Hex publish credentials current (`mix hex.user whoami` / `rebar3 hex user whoami`)
- [ ] No outstanding `reckon-db` PRs touching `reckon_db_log_backend.erl` or `reckon_db_streams.erl` (check `git log` since last release)
- [ ] No `?KHEPRI_WILDCARD_STAR` / `?KHEPRI_WILDCARD_STAR_STAR` import conflicts in existing reckon-db modules

---

*The Dossier is the default. The Decision is the escape hatch. Both shipped.*
