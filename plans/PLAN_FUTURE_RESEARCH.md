# Plan: Future Research Topics

**Status:** Research
**Created:** 2026-01-19
**Last Updated:** 2026-05-26 — DCB Phase 1 + 2 shipped; Phase 3 deferral re-affirmed with explicit revisit criteria; coexistence sketch added.

## Overview

This document tracks research topics and novel concepts that may be relevant for future ReckonDB development.

---

## Research Topics

### Dynamic Consistency Boundaries (DCB)

**Status:** Research Complete - Recommendation Made
**Priority:** Low (tags for queries) / Not Recommended (query-based concurrency)
**Origin:** Sara Pellegrini (https://dcb.events/, https://sara.event-thinking.io/)
**Related work surveyed (2026-05-26):** Rico Fritzsche, [Aggregateless Event Sourcing](https://ricofritzsche.me/aggregateless-event-sourcing/); FACTSTR ([factstr.com](https://factstr.com/), the Rust reference implementation, same author); Ralf Westphal, "command-context-consistency."

#### Status Update — 2026-05-26

**Phase 1 + Phase 2 are SHIPPED.** The capability ReckonDB lacked when this document was first written now exists:

| Phase | Status | Code references |
|-------|--------|-----------------|
| Phase 1: Tagged events + tag indexing + `read_by_tags` | ✅ Shipped | `src/reckon_db_log_backend.erl:96` (`tags :: [binary()]` on `new_event/0`); `src/reckon_db_streams.erl:333` (`read_by_tags/4`); `test/unit/reckon_db_tags_tests.erl` |
| Phase 2: Tag-aware subscriptions | ✅ Shipped | `src/reckon_db_subscriptions.erl:370` (`create_filter(by_tags, Tags)`); `src/reckon_db_gateway_worker.erl:110` (`read_by_tags` over the wire) |
| Phase 3: Query-based concurrency | 🛑 Deferred (not rejected) | (none — primitive does not exist) |
| Phase 4: Decision model helpers | ⏸ Optional, blocked on Phase 3 | (none) |

**Net effect on ReckonDB capability:** Read-side DCB is fully usable today. Projections, subscriptions, and ad-hoc analytics can slice across streams by tag without scaffolding. The write side remains stream-version optimistic concurrency.

#### Deferral criteria — what would move Phase 3 from "deferred" to "active"

Phase 3 (query-based concurrency, the `append_if_no_tag_matches` primitive) is NOT permanently rejected. It is deferred until one of the following becomes true:

1. **A real Hecate process surfaces where all three Dossier-model workarounds (registry Dossier / process guard with read models / Process Manager dance) feel structurally wrong** — not just inconvenient, but wrong enough that we keep paying the cost repeatedly across multiple Divisions.
2. **A Domain's work is dominated by cross-cutting decisions without identity-carriers.** If a Domain's Desks are mostly "given tag-filtered facts, decide" rather than "given Dossier state, decide," the Dossier-first defaults are wrong for that Domain. We would adopt Phase 3 for that Domain rather than retrofit.
3. **Upstream support arrives.** If Khepri or Ra gain a native tag-filter conditional append primitive, the cost of adding Phase 3 to ReckonDB drops by an order of magnitude. Worth revisiting at that point regardless of (1) and (2).
4. **Operational evidence against the dossier-with-read-model-guard pattern at scale.** If the read-then-append race window causes real production incidents in a Hecate deployment despite idempotency + reconciliation, Phase 3 becomes the cleanest cure.

Until at least one is true, the position holds. See `hecate-corpus/philosophy/CONSISTENCY_BOUNDARIES.md` for the user-facing doctrine.

#### Coexistence sketch — if Phase 3 ever ships

For future planning: the two concurrency models CAN coexist on a single ReckonDB instance. Same store. Same events. Same triggers. Same projections. The only difference is which precondition check fires at append time.

**Storage schema** — DCB events still need a `stream_id` field (the current schema assumes it). Three options:

| Option | What | Cost | Recommended? |
|--------|------|------|--------------|
| Synthetic per-decision stream | Each decision context gets a unique stream id; version is meaningless | Wasteful (stream metadata bloat) | No |
| Single global `_facts` pseudo-stream | All DCB events land in `?DCB_STREAM`; global `sequence_number` carries the ordering | Lowest cost; preserves schema | **Yes** |
| Nullable `stream_id` | Schema change | Cleanest semantics, biggest migration | Only if Phase 3 becomes the primary write path |

**Behaviour surface** — one new callback:

```erlang
%% src/reckon_db_log_backend.erl — proposed addition
-callback append_if_no_tag_matches(
    State           :: state(),
    TagFilter       :: [binary()] | {any_of, [binary()]} | {all_of, [binary()]},
    SeqCutoff       :: non_neg_integer(),
    Events          :: [new_event()]
) ->
      {ok, version()}
    | {error, {context_changed, last_seq :: non_neg_integer()}}
    | {error, Reason :: term()}.
```

**Khepri implementation** — a new custom command. The machine state can perform `scan-tagged-events-since-cutoff` + `append-if-empty` atomically under a single Ra log entry. Critical: the scan must be bounded — index lookup, not full scan. Phase 1's tag index is the substrate; Phase 3 needs it queryable inside the machine apply function.

**Gateway surface** — one new verb in `reckon_gater`, propagated through `reckon_evoq` if/when an `evoq_decision` behaviour is added in Evoq.

**Naming convention if added** — at the Hecate user-facing level, call them **Decisions** (sibling to Dossier in the 5D model). At the ReckonDB level, the primitive name stays mechanical: `append_if_no_tag_matches`.

#### Coexistence — answers to the open Research Questions

The open questions at the bottom of this doc are answered by the coexistence sketch:

- *Can we support both models simultaneously (hybrid)?* — **Yes.** Same store, two append primitives, events distinguished by which stream they land in (`?DCB_STREAM` vs per-Dossier streams).
- *How do tagged events interact with existing stream versioning?* — **They don't interfere.** Tags are a parallel filter axis. Stream version is per-stream. A DCB event ignores stream version (or uses `?ANY_VERSION`); a Dossier event ignores tag-filter consistency. Both pathways can tag for read purposes.
- *Should streams and tags coexist or be mutually exclusive modes?* — **Coexist.** Tags are universal; the concurrency primitive is what differs.
- *Performance implications of tag indexing at scale?* — Phase 1 + 2 already exercise this. Hot path: `read_by_tags` performance under load. Phase 3 would add scan-during-apply cost to the leader; needs to be bounded by index lookup.
- *How to handle tag index consistency in distributed cluster?* — Ra/Khepri keeps the tag index in the machine state, replicated by consensus, eventually consistent on followers. Phase 3 leader scans always read from up-to-date local state.
- *Migration path for existing stream-per-aggregate users?* — Zero migration needed. Phase 3 is opt-in per-call. Existing aggregates keep using `append_events`; new Decisions opt into `append_if_no_tag_matches`.



#### What is DCB?

Dynamic Consistency Boundary is a technique for enforcing consistency in event-driven systems without relying on rigid aggregate boundaries. It challenges the traditional DDD assumption that aggregates are the fundamental unit of consistency.

**Core Thesis:** Aggregates are a *solution*, not a *requirement*. When cross-entity invariants dominate your domain, DCB may be simpler than sagas and compensating transactions.

---

### The Three Paradigms of Event Sourcing

ReckonDB's architecture must be understood in context of three distinct paradigms:

| Aspect | Entity-Centric (Traditional DDD) | Relationship-Centric (DCB) | Process-Centric (Macula/ReckonDB) |
|--------|----------------------------------|---------------------------|-----------------------------------|
| **Stream identity** | Entity ID (user-123, order-456) | Context name (enrollments) | Process instance ID (workflow-789) |
| **What stream represents** | Entity's complete history | All facts in a bounded context | Business process execution log |
| **Aggregate purpose** | Entity state reconstruction | Ad-hoc decision model (query-time) | Process context ("dossier") |
| **Concurrency unit** | Single entity's stream | Query-based tag intersection | Single process instance |
| **Cross-cutting concerns** | Sagas, process managers | Tags + query-based locking | Process flow owns all participants |
| **Mental model** | "Entity has history" | "Facts are tagged for slicing" | "Dossier passed desk to desk" |

#### The Dossier Metaphor (Process-Centric)

**ReckonDB's recommended paradigm:** An event stream is an ordered log of facts that represent a **business process**. The aggregate is the "payload of the process" - the **dossier that is passed from desk to desk and gradually fills with facts**.

```
PROCESS-CENTRIC ("Dossier" Model)
================================

Process: Loan Application
Stream: loan-application-2024-0042

┌─────────────────────────────────────────────────────────────────┐
│ The "Dossier" - accumulated context passed from desk to desk   │
├─────────────────────────────────────────────────────────────────┤
│ Desk 1 (Intake): ApplicationSubmitted                          │
│   → dossier gains: applicant info, requested amount            │
│                                                                 │
│ Desk 2 (Credit Check): CreditScoreRetrieved                    │
│   → dossier gains: credit score, risk assessment               │
│                                                                 │
│ Desk 3 (Underwriting): UnderwritingDecisionMade                │
│   → dossier gains: approval status, conditions                 │
│                                                                 │
│ Desk 4 (Documentation): DocumentsVerified                      │
│   → dossier gains: verification status, issues found           │
│                                                                 │
│ Desk 5 (Closing): LoanDisbursed                                │
│   → dossier complete: final state, all accumulated context     │
└─────────────────────────────────────────────────────────────────┘

The PROCESS owns the stream, not any single entity.
The applicant, the loan, the credit report - all are PARTICIPANTS in the process.
```

**Key insight:** The dossier records WHAT WE KNEW at each decision point. This provides complete auditability that query-time aggregation (DCB) cannot match.

---

#### Traditional Model vs DCB

| Aspect | Traditional (Current ReckonDB) | DCB Approach |
|--------|-------------------------------|--------------|
| Stream granularity | One stream per aggregate | One stream per bounded context |
| Event ownership | Event belongs to one aggregate | Event is a pure fact, tagged with multiple entities |
| Consistency check | Stream version (optimistic concurrency) | Query-based locking (last position + query) |
| Process binding | One process hydrates one stream | No fixed binding; build model on-demand |
| Cross-aggregate invariants | Sagas, compensating transactions | Single event, single stream, no compensation |

#### The Enrollment Problem: Three Solutions Compared

Classic example: Course enrollment with two invariants:
1. Courses accept maximum 30 students
2. Students subscribe to maximum 10 courses

##### Entity-Centric Solution (Two Streams + Saga)

```
Stream: course-123          Stream: student-456
├── CourseCreated           ├── StudentCreated
├── StudentEnrolled ←───────┼── CourseJoined      ← DUPLICATE FACT
├── StudentEnrolled         ├── CourseJoined
└── ...                     └── ...
```

**Problems:**
- Same fact recorded twice (enrollment exists in both streams)
- Need saga/process manager to coordinate
- Compensating transactions if one succeeds and other fails
- Complex failure modes

##### DCB Solution (Single Stream with Tags)

```
Stream: enrollment-context
├── Enrolled { student: 456, course: 123, tags: [student:456, course:123] }
├── Enrolled { student: 456, course: 789, tags: [student:456, course:789] }
├── Enrolled { student: 111, course: 123, tags: [student:111, course:123] }
└── ...

Query by tags:
- tags contains "course:123" → 2 enrollments (check < 30)
- tags contains "student:456" → 2 enrollments (check < 10)
```

**Benefits:** One fact, one event, no duplication, no saga needed.
**Trade-offs:** Double-query cost, requires tag indexes, query-time aggregation loses temporal context.

##### Process-Centric Solution (Dossier Model)

```erlang
%% Process: EnrollmentRequest
%% Stream: enrollment-request-{uuid}

%% The "dossier" accumulates context at each step
-record(enrollment_dossier, {
    student_id,
    course_id,
    student_current_courses = [],   %% fetched at process start
    course_current_students = [],   %% fetched at process start
    validation_status,
    outcome
}).

%% Guard: check invariants BEFORE starting the process
handle_command(#request_enrollment{student_id = S, course_id = C}, _State) ->
    StudentCourses = query_student_courses(S),  %% read from projection
    CourseStudents = query_course_students(C),  %% read from projection

    case {length(StudentCourses) < 10, length(CourseStudents) < 30} of
        {true, true} ->
            [#enrollment_requested{student_id = S, course_id = C,
                                   student_courses_at_request = StudentCourses,
                                   course_students_at_request = CourseStudents}];
        {false, _} ->
            [#enrollment_rejected{reason = student_limit_reached}];
        {_, false} ->
            [#enrollment_rejected{reason = course_limit_reached}]
    end.
```

**Benefits:**
- Single stream per process instance (no coordination)
- Invariants checked as process guards using read models
- Dossier records WHAT WE KNEW at decision time (full auditability)
- "Why did we approve this?" → replay the dossier

**This is ReckonDB's recommended approach.**

| Paradigm | Solution | Complexity | Auditability |
|----------|----------|------------|--------------|
| **Entity-Centric** | Two streams + saga | High | Medium |
| **DCB** | Tags + query-based locking | Medium | Low (query-time) |
| **Process-Centric** | Process stream + read model guards | Low | High (temporal) |

#### Conflict with Current ReckonDB Architecture

**1. Stream-per-aggregate assumption**

Current API:
```erlang
reckon_db:append("course-123", [Event], ExpectedVersion)
```

DCB needs:
```erlang
reckon_db:append("enrollment", [TaggedEvent], {query, Query, LastPosition})
```

**2. Version-based optimistic concurrency only**

Current: Fails if stream version != ExpectedVersion
```erlang
reckon_db:append(Stream, Events, ExpectedVersion)
```

DCB needs: Fails if any event matching Query was added after LastPosition
```erlang
reckon_db:append(Stream, Events, {query, #{tags => Tags}, LastPosition})
```

**3. Process-per-stream binding**

Current supervision assumes one GenServer per stream for aggregate state.
DCB doesn't need persistent processes - decision models built on-demand from queries.

**4. Subscription model**

Current subscriptions are stream-centric:
```erlang
reckon_db:subscribe(StreamName, FromVersion)
```

DCB needs tag-based subscriptions:
```erlang
reckon_db:subscribe({tags, ["student:456"]}, FromPosition)
```

#### Existing ReckonDB Features That Support DCB

ReckonDB already has partial support:

| Feature | DCB Compatibility | Notes |
|---------|------------------|-------|
| Pattern subscriptions | Partial | Can subscribe to event types across streams |
| Payload-based filtering | Partial | Can filter events by content |
| Stream links | Partial | Derived streams with filtering |
| Event type subscriptions | Partial | Cross-stream event delivery |

#### Missing Features for Full DCB Support

1. **Tag-based event storage and indexing**
   - Events need first-class `tags` field
   - Khepri storage needs tag index for efficient queries
   - Tags should be indexed, not just stored

2. **Query-based optimistic concurrency**
   - New concurrency mode: `{query, Query, LastPosition}`
   - Query specifies filter (tags, event types, payload predicates)
   - Write fails if any matching event added after LastPosition

3. **Tag-aware subscriptions**
   - Subscribe to tag patterns, not just streams
   - Efficient delivery via tag index

4. **Decision model builders**
   - Helper functions to build state from filtered queries
   - Replace aggregate hydration pattern

---

### Research Conclusion: Does DCB Add Value to ReckonDB?

#### Arguments AGAINST Full DCB Implementation

1. **Process-centric already solves the cross-entity problem**
   - Each process instance owns its own stream
   - Invariants checked as process guards using read models
   - No need for tags or query-based concurrency

2. **DCB trades one complexity for another**
   - Removes saga complexity → adds query/indexing complexity
   - Double-query cost (decision + precondition check)
   - Tag index maintenance in distributed Raft cluster

3. **Process-centric is more auditable**
   - The dossier records WHAT WE KNEW at each decision point
   - DCB's query-time aggregation loses this temporal context
   - "Why did we approve this loan?" → replay the dossier

4. **Philosophical mismatch**
   - DCB: "Events are facts about entities, tagged for multi-dimensional slicing"
   - Process-centric: "Events are facts about process execution, participants are secondary"

#### Arguments FOR Partial DCB Features

1. **Analytics and reporting use cases**
   - Tag-based queries useful for cross-cutting analytics
   - "All enrollments for student X across all processes"
   - Doesn't require DCB-style concurrency, just tag indexes

2. **Hybrid scenarios**
   - Some domains truly are entity-centric (user profiles, product catalogs)
   - Process-centric ideal for workflows; entity-centric for master data
   - Tags could support both models

3. **Community expectations**
   - Developers familiar with EventStoreDB/Axon may expect tag-based features
   - Reduces adoption friction

---

### Final Recommendation

**Process-centric should remain ReckonDB's core paradigm.** DCB's value proposition (cross-entity invariants without sagas) is already addressed by treating processes as first-class citizens.

**Implement tag-based event indexing for QUERY purposes only, NOT for concurrency control:**

```erlang
%% Tags for query/analytics, NOT for concurrency
Event = #{
    type => enrollment_completed,
    data => #{process_id => <<"enroll-123">>, student_id => 456, course_id => 789},
    tags => [<<"student:456">>, <<"course:789">>]  %% For analytics queries only
},

%% Query by tags (read model support)
{ok, Events} = reckon_db:read_by_tags([<<"student:456">>], #{limit => 100}).

%% Concurrency REMAINS stream-version-based (process instance)
{ok, V} = reckon_db:append(<<"enrollment-process-123">>, [Event], ExpectedVersion).
```

| Feature | Implement? | Rationale |
|---------|------------|-----------|
| Tag-based event storage | ✅ Yes | Useful for cross-cutting queries |
| Tag-based subscriptions | ✅ Yes | Enables entity-centric projections |
| Query-based concurrency | ❌ No | Process-centric model solves this more elegantly |
| Decision model helpers | ⚠️ Maybe | Could help hybrid use cases |

---

#### Revised Implementation Phases

**Phase 1: Tagged Events (SHIPPED)**
- [x] Add optional `tags` field to event metadata (`reckon_db_log_backend.erl:96`)
- [x] Store tags in Khepri alongside event data
- [x] Basic tag index for query routing
- [x] Query events by tags: `reckon_db:read_by_tags(Tags, Options)` (`reckon_db_streams.erl:333`)
- [x] Tests: `test/unit/reckon_db_tags_tests.erl`

**Phase 2: Tag-Aware Subscriptions (SHIPPED)**
- [x] Subscription filter: `create_filter(by_tags, Tags)` (`reckon_db_subscriptions.erl:370`)
- [x] Filter dispatch: `matches(by_tags, RequestedTags, #event{tags = EventTags})` (`reckon_db_filters.erl:189`)
- [x] Gateway exposure: `{read_by_tags, StoreId, Tags, Match, BatchSize}` (`reckon_db_gateway_worker.erl:110`)

**Phase 3: Query-Based Concurrency (DEFERRED, not rejected)**
- [ ] New append callback: `append_if_no_tag_matches/4` (see Coexistence Sketch above)
- [ ] Khepri custom command: atomic scan-tagged-events-since-cutoff + append-if-empty
- [ ] Gateway verb in `reckon_gater`
- [ ] Schema decision: `?DCB_STREAM` pseudo-stream (Option 2 from sketch)
- **Status:** Deferred 2026-05-26. Revisit criteria documented above. Until at least one criterion fires, the dossier-with-pipeline pattern is the preferred shape.

**Phase 4: Decision Model Helpers (OPTIONAL, blocked on Phase 3)**
- [ ] `reckon_db:fold_by_tags(Tags, Fun, Acc)` - build state from filtered events
- [ ] Documentation and examples for hybrid Dossier / Decision patterns
- [ ] Bridge to `evoq_decision` behaviour (Evoq side)

#### Research Questions

- [ ] How do tagged events interact with existing stream versioning?
- [ ] Should streams and tags coexist or be mutually exclusive modes?
- [ ] Performance implications of tag indexing at scale?
- [ ] How to handle tag index consistency in distributed cluster?
- [ ] Migration path for existing stream-per-aggregate users?
- [ ] Can we support both models simultaneously (hybrid)?

#### API Design Sketch

```erlang
%% Phase 1: Tagged events
Event = #{
    type => enrollment_created,
    data => #{student_id => 456, course_id => 123},
    tags => [<<"student:456">>, <<"course:123">>]  %% NEW
},
reckon_db:append(<<"enrollments">>, [Event], any).

%% Read by tags
{ok, Events} = reckon_db:read_by_tags([<<"course:123">>], #{limit => 100}).

%% Phase 2: Query-based concurrency
Query = {tags, [<<"course:123">>]},
LastPosition = 42,
case reckon_db:append(<<"enrollments">>, [Event], {query, Query, LastPosition}) of
    {ok, NewPosition} -> ok;
    {error, {conflict, ConflictingEvents}} -> retry_with_new_state
end.

%% Phase 3: Tag subscriptions
{ok, Sub} = reckon_db:subscribe({tags, [<<"student:456">>]}, 0),
receive
    {reckon_db, Sub, Events} -> handle(Events)
end.

%% Phase 4: Decision model
Model = reckon_db:decision_model([
    {student_courses, {tags, [<<"student:456">>]}},
    {course_students, {tags, [<<"course:123">>]}}
]),
%% Model = #{student_courses => [...], course_students => [...]}
```

#### Compatibility Strategy

DCB should be **additive**, not a replacement:

1. Existing stream-per-aggregate code continues to work unchanged
2. Tags are optional - events without tags behave as before
3. Version-based concurrency remains the default
4. Query-based concurrency is opt-in per append call
5. Users can mix both patterns in same system

#### References

**Primary Sources:**
- https://dcb.events/ - Official DCB documentation
- https://sara.event-thinking.io/ - Sara Pellegrini's Event Thinking blog
- Sara Pellegrini's talks on "Killing the Aggregate"

**Related Concepts:**
- CockroachDB follower reads (bounded staleness)
- Spanner's TrueTime and consistency modes
- Hybrid Logical Clocks (HLC)
- Event Store's projections and persistent subscriptions

**Academic:**
- "Consistency Tradeoffs in Modern Distributed Database System Design" (Abadi)
- "Building on Quicksand" (Helland & Campbell)

#### Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-01-21 | Research priority: High | Cross-aggregate invariants are common pain point |
| 2026-01-21 | Approach: Additive, not replacement | Backward compatibility is critical |
| 2026-01-21 | Start with Phase 1 (tagged events) | Lowest risk, immediate value for querying |
| 2026-01-21 | **Process-centric paradigm identified** | ReckonDB's "dossier model" already solves cross-entity invariants |
| 2026-01-21 | **Query-based concurrency NOT recommended** | Process-centric provides better auditability |
| 2026-01-21 | **Tags for queries only** | Tag indexing adds value for analytics without DCB complexity |

---

#### References (Archived)

**Primary Sources on DCB:**
- [DCB.events](https://dcb.events/) - Official DCB documentation by Sara Pellegrini
- [Sara Pellegrini's Event Thinking](https://sara.event-thinking.io/) - Original "Killing the Aggregate" thesis
- [Kill Aggregate? Interview on DCB](https://docs.eventsourcingdb.io/blog/2025/12/15/kill-aggregate-an-interview-on-dynamic-consistency-boundaries/) - Bastian Waidelich interview (EventSourcingDB)
- [DCB Best Practices](https://www.eventsourcing.dev/best-practices/dynamic-consistency-boundaries) - Event Sourcing Guide

**Production Implementations (as of 2026-01):**
- [Axon Framework 5.0 with DCB](https://www.axoniq.io/blog/dcb-in-af-5) - First major framework support (experimental)
- [Axon Server 2025.1](https://www.axoniq.io/blog/axon-server-future-proof-event-store) - Event store with DCB support (experimental)
- [bwaidelich/dcb-eventstore](https://github.com/bwaidelich/dcb-eventstore) - Reference implementation interfaces
- [EventSourcingDB](https://docs.eventsourcingdb.io/best-practices/dynamic-consistency-boundaries/) - Native DCB support

**Community Discussion:**
- [Milan Savić on DCB](https://milan.event-thinking.io/2025/05/dynamic-consistency-boundaries.html) - May 2025 analysis
- [JAVAPRO on DCB](https://javapro.io/2025/10/28/dynamic-consistency-boundaries/) - October 2025 overview

**Academic/Conceptual:**
- "Consistency Tradeoffs in Modern Distributed Database System Design" (Abadi)
- "Building on Quicksand" (Helland & Campbell)
- CockroachDB follower reads, Spanner TrueTime, Hybrid Logical Clocks

**Related Educational Content:**
- See `guides/event_sourcing_paradigms.md` for detailed comparison of Entity-Centric, Relationship-Centric (DCB), and Process-Centric paradigms

---

## Notes

Add new research topics as they arise. Each topic should include:
- Description of the concept
- Potential applications for ReckonDB
- Research questions to answer
- References to explore
