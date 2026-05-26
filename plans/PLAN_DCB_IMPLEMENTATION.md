# Plan: DCB — Query-Based Concurrency, Full Stack

**Status:** Active — Design / Not Started
**Created:** 2026-05-26
**Last Updated:** 2026-05-26
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
│   Khepri command:    append_if_no_tag_matches_command            │
│   Storage:           single ?DCB_STREAM pseudo-stream            │
│   Bounded scan via existing Phase 1 tag index                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Storage model decision: single `?DCB_STREAM` pseudo-stream

Three alternatives were considered (see `PLAN_FUTURE_RESEARCH.md` § Coexistence Sketch). The decision for v1:

**All DCB events land in one stream named `?DCB_STREAM` = `<<"_dcb">>`.** Per-event identity is the global `sequence_number`. Stream version on `?DCB_STREAM` is meaningful internally (monotonic write counter) but is not the concurrency unit — the tag-filter check is.

| Pro | Con |
|-----|-----|
| Zero schema change | All DCB writes serialize through one Ra consensus group |
| Existing stream machinery (read, subscribe) works without modification | Per-stream throughput cap (~10k–100k ops/s on standard hardware) becomes the ceiling for all DCB writes combined |
| Easy to identify DCB events in operator tools (filter by stream) | Future partitioning means revisiting the constant |

Justification for accepting the throughput cap: DCB targets cross-cutting decisions (uniqueness, allocation, idempotency) which are typically lower-volume than per-aggregate flows. Per-aggregate flows stay on per-stream Ra groups (unchanged). If DCB write volume ever approaches the single-stream ceiling, we partition; that's a v2 problem.

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
| **P3.1** | Storage | Khepri command + behaviour callback + facade function | `reckon-db` | 2-3 days |
| **P3.2** | Storage | Unit + integration tests (concurrent contention, large scans, edge cases) | `reckon-db` | 2 days |
| **P3.3** | Wire | `reckon-gater` types + verb | `reckon-gater` | 1 day |
| **P3.4** | Wire | `reckon-gater` gateway worker dispatch | `reckon-db` | 1 day |
| **P3.5** | Adapter | `reckon-evoq` passthrough | `reckon-evoq` | 0.5 day |
| **P3.6** | Framework | `evoq_decision` behaviour + runtime | `evoq` | 3 days |
| **P3.7** | Framework | `evoq_decision` tests (property-based, concurrent contention) | `evoq` | 2 days |
| **P3.8** | Example | Reference example: `examples/dcb_counter` | `hecate-corpus` | 1 day |
| **P3.9** | Docs | Flip `CONSISTENCY_BOUNDARIES.md` "Decision" entry from reserved → active; update CODEX.md cornerstone chapter; update GLOSSARY | `hecate-corpus` | 0.5 day |

**Total estimate:** ~12 working days for one person. ~3 weeks calendar with reviews + integration.

---

## P3.1 — reckon-db: storage primitive

### Behaviour callback addition

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

### Khepri command

`src/reckon_db_dcb_command.erl` (new file). Custom Ra/Khepri command:

```erlang
%% Command shape (binary-tagged record passed to Khepri state machine):
%%   {append_if_no_tag_matches, StoreId, TagFilter, SeqCutoff, Events}
%%
%% State machine apply:
%%   1. Use the existing tag index to enumerate events matching TagFilter
%%   2. Filter to events with sequence_number > SeqCutoff
%%   3. If non-empty:
%%        max_seq = max of matching sequence numbers
%%        return {error, {context_changed, max_seq}}
%%      else:
%%        for each Event in Events:
%%          assign new sequence_number = global_seq_counter++
%%          set stream_id = ?DCB_STREAM
%%          set stream_version = next ?DCB_STREAM version
%%          insert into log + tag index
%%        return {ok, NewStreamVersion}
```

The scan in step 1 MUST be bounded by the tag index — never a full log scan. Phase 1's tag index supports this (key → event_ids).

### Facade

`src/reckon_db.erl` (or `src/reckon_db_streams.erl`):

```erlang
-spec append_if_no_tag_matches(
    StoreId   :: binary(),
    TagFilter :: reckon_gater_types:tag_filter(),
    SeqCutoff :: non_neg_integer(),
    Events    :: [new_event()]
) -> {ok, version()} | {error, term()}.
append_if_no_tag_matches(StoreId, TagFilter, SeqCutoff, Events) ->
    %% delegate to backend via gateway worker
    reckon_db_gateway:call(StoreId,
        {append_if_no_tag_matches, StoreId, TagFilter, SeqCutoff, Events}).
```

### Constants

`include/reckon_db_internal.hrl`:

```erlang
-define(DCB_STREAM, <<"_dcb">>).
```

### Errors

New error class: `{context_changed, MaxSeq}`. Document in CHANGELOG and ANTIPATTERNS_EVENT_SOURCING (the cure for unbounded retries on context_changed: bounded retry budget + jitter, or rethrow).

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

Critical hot path: the tag-filter scan inside the Khepri apply function.

**Concerns:**
- Scan cost is O(matching_events) where "matching" is bounded by the tag index
- Under heavy DCB write contention, the leader's apply queue can grow
- The single `?DCB_STREAM` is a serialization point — all DCB writes synchronize through one Ra consensus group

**Mitigations:**
- Bound scan by `SeqCutoff` — caller passes a recent sequence number, scan only events after it
- Document expected SeqCutoff selection (typically: "max seq at read time")
- Index keying: tag → list of event_ids (or sorted set keyed by sequence_number for efficient cutoff filtering)
- Benchmark and document the throughput ceiling clearly

**Acceptance threshold for v1:**
- 10k DCB appends/sec sustained on the reference 3-node cluster
- p99 latency under 100ms with 100 concurrent contenders on the same context
- No leader crashes under sustained load (24h soak test)

---

## Migration / compatibility

Zero migration. Existing Dossiers + `evoq_aggregate` keep working unchanged. DCB is purely additive.

Rollback: if Phase 3 ships and we find a deal-breaker in production, the storage callback can be removed without affecting existing data. DCB events in `?DCB_STREAM` remain readable via existing stream APIs.

---

## Open questions

1. **`OR_` filter scan semantics.** A `{or_, [F1, F2]}` filter inside `append_if_no_tag_matches` could be implemented as union of matches. Performance: two index lookups, deduplicate. Defer to P3.1 implementation; benchmark later.

2. **Cutoff semantics for empty stores.** `SeqCutoff = 0` on an empty store should be valid (no events match, append succeeds). Test in P3.2.

3. **Should DCB events appear in `read_all_global`?** Yes — they're real events. They appear in the global log alongside Dossier events. Document this in the example narrative.

4. **Snapshot story for `?DCB_STREAM`.** No snapshotting in v1. Phase 1 + 2 read APIs don't snapshot either. Aggregate snapshotting doesn't apply.

5. **`evoq_decision` and process managers.** Can a PM dispatch a Decision? Yes — same as dispatching a Command. The PM's `dispatch` call routes through `evoq_decision_runtime:dispatch/2` instead of `evoq_dispatcher`. Document in P3.6.

6. **Decision-side replay.** When replaying events for analytics or rebuilding projections, DCB events flow through the same triggers as Dossier events. No special handling needed.

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

Before starting P3.1, confirm:

- [ ] reckon-db tag index supports range scan by `sequence_number > N` (Phase 1 may or may not — verify)
- [ ] Khepri version supports custom commands of the shape we need (Khepri 0.x machines yes)
- [ ] CHANGELOG conventions allow multi-PR feature work (yes, per existing CHANGELOG)
- [ ] Hex publish credentials current (`mix hex.user whoami` / `rebar3 hex user whoami`)
- [ ] No outstanding `reckon-db` PRs touching `reckon_db_log_backend.erl` (check `git log -- src/reckon_db_log_backend.erl`)

---

*The Dossier is the default. The Decision is the escape hatch. Both shipped.*
