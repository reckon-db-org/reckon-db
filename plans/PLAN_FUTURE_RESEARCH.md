# Plan: Future Research Topics

**Status:** Research
**Created:** 2026-01-19
**Last Updated:** 2026-01-21

## Overview

This document tracks research topics and novel concepts that may be relevant for future ReckonDB development.

---

## Research Topics

### Dynamic Consistency Boundaries (DCB)

**Status:** Active Research
**Priority:** High
**Origin:** Sara Pellegrini (https://dcb.events/, https://sara.event-thinking.io/)

#### What is DCB?

Dynamic Consistency Boundary is a technique for enforcing consistency in event-driven systems without relying on rigid aggregate boundaries. It challenges the traditional DDD assumption that aggregates are the fundamental unit of consistency.

**Core Thesis:** Aggregates are a *solution*, not a *requirement*. When cross-entity invariants dominate your domain, DCB may be simpler than sagas and compensating transactions.

#### Traditional Model vs DCB

| Aspect | Traditional (Current ReckonDB) | DCB Approach |
|--------|-------------------------------|--------------|
| Stream granularity | One stream per aggregate | One stream per bounded context |
| Event ownership | Event belongs to one aggregate | Event is a pure fact, tagged with multiple entities |
| Consistency check | Stream version (optimistic concurrency) | Query-based locking (last position + query) |
| Process binding | One process hydrates one stream | No fixed binding; build model on-demand |
| Cross-aggregate invariants | Sagas, compensating transactions | Single event, single stream, no compensation |

#### The Problem DCB Solves

Classic example: Course enrollment with two invariants:
1. Courses accept maximum 30 students
2. Students subscribe to maximum 10 courses

**Traditional approach (two streams):**
```
Stream: course-123          Stream: student-456
├── CourseCreated           ├── StudentCreated
├── StudentEnrolled ←───────┼── CourseJoined      ← DUPLICATE FACT
├── StudentEnrolled         ├── CourseJoined
└── ...                     └── ...
```

Problems:
- Same fact recorded twice (enrollment exists in both streams)
- Need saga/process manager to coordinate
- Compensating transactions if one succeeds and other fails
- Complex failure modes

**DCB approach (single stream with tags):**
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

Benefits:
- One fact, one event
- No duplication
- No saga needed
- Both invariants checked atomically at write time

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

#### Implementation Phases

**Phase 1: Tagged Events**
- [ ] Add optional `tags` field to event metadata
- [ ] Store tags in Khepri alongside event data
- [ ] Basic tag index (ETS or Khepri secondary index)
- [ ] Query events by tags: `reckon_db:read_by_tags(Tags, Options)`

**Phase 2: Query-Based Concurrency**
- [ ] New append mode: `{query, Query, LastPosition}`
- [ ] Query types: `{tags, Tags}`, `{event_type, Type}`, `{and, [Q1, Q2]}`
- [ ] Atomic check-and-append with query filter
- [ ] Backward compatible - version-based still works

**Phase 3: Tag-Aware Subscriptions**
- [ ] New subscription type: `{tags, TagPattern}`
- [ ] Tag pattern matching (exact, prefix, wildcard)
- [ ] Efficient routing via tag index

**Phase 4: Decision Model Helpers**
- [ ] `reckon_db:fold_by_tags(Tags, Fun, Acc)` - build state from filtered events
- [ ] `reckon_db:decision_model(Queries)` - multi-query state builder
- [ ] Documentation and examples

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

---

## Notes

Add new research topics as they arise. Each topic should include:
- Description of the concept
- Potential applications for ReckonDB
- Research questions to answer
- References to explore
