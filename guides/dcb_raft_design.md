# DCB Design: Why Raft Changes Everything

*This guide explains the architectural decisions behind reckon-db's DCB
implementation — specifically how running inside a Raft consensus group
reshapes the design compared to every other DCB implementation, which
sits on top of a single-node relational database.*

---

## The problem DCB solves

Event sourcing with per-aggregate stream locks handles per-entity
invariants well: "no concurrent writer on this order's stream." It
cannot handle cross-entity invariants: "no two users share this email."
The canonical workaround — create a special `email-uniqueness` stream per
email address — creates unbounded stream proliferation and forces callers
to understand a coordination protocol that isn't domain logic.

DCB solves this with a conditional-append primitive: *append these events
only if no event matching this filter has appeared since seq N*. The
filter selects a cross-cutting slice of history; the seq cutoff is the
caller's observed frontier; the store enforces both atomically.

## How every other implementation does it

PostgreSQL-based DCB implementations (PHP, .NET, Python) use one of two
mechanisms:

**Serializable snapshot isolation (SSI):** the append runs in a
`SERIALIZABLE` transaction that reads the tag index and writes the new
events. If a concurrent transaction wrote a conflicting event, PostgreSQL's
SSI detects the read-write conflict and aborts one of the transactions.
The application retries.

**Advisory locks + read-modify-write:** acquire a per-filter advisory
lock, read the tag index, decide, write, release. Simpler to reason about
but serializes all DCB appends behind one lock.

Both approaches answer the question: "is this write safe given what this
single PostgreSQL primary knows?" They are correct as long as the primary
is the only writer — which it is, by definition, in a primary-replica
setup.

## What changes when the store is a Raft cluster

Raft clusters have no "primary database" in the PostgreSQL sense. There
is a Raft leader — the node that currently proposes log entries — but
leadership is not permanent and there is no external coordinator.
Consistency across the cluster is guaranteed by a single rule: every
state change is a log entry, and a log entry is only applied after a
quorum of nodes acknowledges it.

**The consequence for DCB:** the conditional check and the event write
must both happen inside the same log entry. Not "in the same
transaction on the primary" — in the same Raft log entry that is
replicated to a quorum before the result is returned to the caller.

reckon-db uses [Khepri](https://github.com/rabbitmq/khepri) (built on
[Ra](https://github.com/rabbitmq/ra), RabbitMQ's Raft implementation)
as its storage engine. Khepri exposes `khepri:transaction/2`, which
compiles a pure-function Erlang closure into a Raft log entry via the
[Horus](https://github.com/rabbitmq/horus) extractor. The closure runs
on every node that applies the log entry, deterministically. The result
is returned to the caller only after a quorum acknowledges the entry.

The DCB conditional-append is this closure:

```erlang
khepri:transaction(StoreId, fun() ->
    %% 1. Check: any matching event above cutoff?
    case reckon_db_dcb_filter:match_any_above_cutoff(TagFilter, SeqCutoff) of
        {true, MaxSeq} ->
            khepri_tx:abort({context_changed, MaxSeq});  %% conflict
        false ->
            %% 2. Write events + update indexes, all in one log entry
            LastSeq = write_events_and_indexes(Stamped),
            ok = khepri_tx:put(?DCB_SEQ_COUNTER_PATH, LastSeq),
            LastSeq
    end
end)
```

This is not a database transaction on a primary — it is a function that
runs as part of Raft log application, on every cluster node, in lock-step.
The consistency guarantee is linearizable: every subsequent read from
any cluster node will see either this write or a later one, never a stale
view that misses it.

## The Horus constraint: no crypto inside transactions

Horus extracts the closure's bytecode by tracing the call graph from the
entry point. It rejects any function that references modules it cannot
extract — including `crypto`. This is not a runtime check; Horus rejects
closures at extraction time if they mention `crypto:*` anywhere, even on
branches that can never execute.

This creates a challenge for the integrity chain (HMAC-signed events).
HMAC computation requires `crypto:mac/4`. We cannot call it inside the
transaction body.

**Solution: pre-stamp outside, verify inside.**

Before calling `khepri:transaction/2`, the code outside the transaction:

1. Reads the current seq counter and chain-tip from Khepri (two plain
   `khepri:get/2` calls — not transactional, just reads).
2. Pre-assigns seqs to the new events (counter + 1, counter + 2, …).
3. Computes each event's `prev_event_hash` and `mac` using the pre-read
   chain-tip, chaining them forward.

Inside the transaction, the code verifies that the counter and chain-tip
still match what was observed during pre-stamping. If they do, it writes
the pre-stamped records at their pre-assigned seqs and updates the counter
and chain-tip atomically. If they don't (a concurrent writer moved them
between the read and the transaction), it aborts with
`{dcb_state_changed, _}` and the outer loop retries.

```
Time →

[Read counter=N, tip=T]        outside transaction (cheap, non-blocking)
[Compute MACs for events]       outside transaction (crypto OK here)
                                ↓
[khepri:transaction/2 ──────── Raft log entry ─────────────────────────]
  verify counter == N           abort → {dcb_state_changed} if not
  verify tip == T               abort → {dcb_state_changed} if not
  check tag filter > cutoff?    abort → {context_changed} if yes
  write events at pre-stamped seqs
  update counter, update tip
[──────────────────────────────────────────────────────────────────────]
                                ↑
                                quorum ACK → result returned to caller
```

The outer retry loop (`try_append/6`) handles `{dcb_state_changed}` with
a budget of 5 retries before surfacing
`{error, dcb_concurrent_writer_exhausted}`. In practice the race window
is sub-millisecond; contention on the pre-stamp read is rare.

Two distinct retry vectors, each handled differently:

| Error | Cause | Handler |
|-------|-------|---------|
| `{context_changed, MaxSeq}` | A conflicting event exists | Caller retries with updated context (domain-level retry) |
| `{dcb_state_changed, _}` | Concurrent writer moved counter/tip between pre-stamp and tx | Internal retry with fresh snapshot (transparent to caller) |

## The tag index: why it exists

Checking "does any event with tag X have seq > cutoff?" naïvely requires
scanning all DCB events. That is O(total events) inside a Raft transaction
— unacceptable.

reckon-db maintains a secondary index alongside each DCB event:

```
[by_tag,        Tag,       SeqKey] → #{}
[by_event_type, EventType, SeqKey] → #{}
```

SeqKey is a fixed-width zero-padded decimal binary (`"00000000000000000042"`).
Zero-padding ensures lexicographic order matches numeric order, so
iterating `[by_tag, Tag, *]` in Khepri returns seqs in ascending order.

Inside the transaction, checking `{any_of, [<<"email:foo">>]}` reads the
`[by_tag, <<"email:foo">>, *]` subtree — bounded by the number of events
with that tag, not the total event count. For a uniqueness claim on a
specific email, this is typically O(1) or O(2) (zero or one matching
event).

Both the event record and the index entries are written in the same
transaction body, so the index is always consistent with the events.

## Comparing to PostgreSQL-based implementations

| Property | PostgreSQL (SSI) | PostgreSQL (advisory lock) | reckon-db (Raft) |
|----------|------------------|---------------------------|-----------------|
| Consistency guarantee | Serializable (single primary) | Serializable (single primary) | Linearizable (cluster-wide) |
| Fault tolerance | Primary HA via replication lag | Same | Raft: tolerates f failures in 2f+1 nodes |
| Throughput | ~1k–5k writes/sec (SSI benchmarks) | Lower (sequential) | ~200–500 writes/sec (3-node LAN, Raft RTT) |
| Write cost | One transaction | Lock + transaction + unlock | One Raft log entry (replicated) |
| Contention | SSI abort + retry | Lock contention serializes | Raft leader serializes; abort + retry |
| External coordinator | None | None | None (leader is internal) |
| Tamper-evidence | No | No | Optional HMAC chain |

Raft's throughput per operation is lower than a local PostgreSQL
transaction — a Raft log entry requires a quorum round-trip (~2–5 ms on
LAN vs. ~0.5–1 ms for a local PG transaction). The tradeoff:

- PostgreSQL HA relies on replication lag and manual failover procedures.
  During a primary failure, DCB writes are unavailable until a replica is
  promoted.
- Raft HA is automatic and continuous. A 3-node cluster tolerates one
  node failure transparently; clients that retry will succeed as soon as
  a new leader is elected (typically 150–300 ms).

For workloads that batch multiple events per `append_if_no_tag_matches`
call, the per-event cost amortizes across the batch — Raft pays one
round-trip per call regardless of batch size.

## The hybrid aggregate + DCB model

The canonical DCB position (dcb.events) advocates replacing aggregates
with DCB entirely. reckon-db takes a pragmatic position: both models
coexist on the same store.

- **Per-aggregate invariants** (order state machine, account balance):
  use `append/4,5` with per-stream version locking. The aggregate owns
  its stream; concurrent writers are rejected immediately with
  `{wrong_expected_version, _, _}`.

- **Cross-aggregate invariants** (email uniqueness, slot allocation):
  use `append_if_no_tag_matches/4` with a tag filter. The consistency
  scope is the filter, not a stream.

This is intentional. Aggregate-stream OCC is cheaper (no tag index, no
filter evaluation, no global seq counter) and appropriate for entities
with clear ownership. DCB adds cost and complexity; use it only where
cross-cutting invariants require it.

The two models are orthogonal at the storage layer. An event written via
`append/4,5` lives on its own stream and is invisible to DCB filter
checks (it has no entry in `[by_tag]` or `[by_event_type]`). An event
written via `append_if_no_tag_matches/4` lives on `_dcb` and is visible
to all read paths but only participates in DCB consistency checks.

## Migration note for {event_type} filters (5.2.0+)

The `[by_event_type]` index was introduced in reckon-db 5.2.0. DCB events
written before upgrading have no entries in this index. An
`{event_type, T}` filter will not detect pre-existing events.

If you need full coverage of historical DCB events in a type-based filter:

1. **Accept the gap**: if your invariant only cares about events written
   after the upgrade, -1 as the initial cutoff is safe — no pre-5.2.0
   event can conflict.
2. **Re-emit**: if you need historical events in the type index, read
   them from `_dcb` and re-write them through
   `append_if_no_tag_matches/4` with the same tags. This creates new
   events with new seqs; you must update projections accordingly.

For new stores (no pre-5.2.0 history), there is nothing to do.

## See also

- [DCB usage guide](dcb.md) — the API and decision loop
- [Khepri](https://github.com/rabbitmq/khepri) — the Raft-backed tree
  store reckon-db uses
- [Horus](https://github.com/rabbitmq/horus) — the Erlang function
  extractor that compiles closures into Raft log entries
- [dcb.events](https://dcb.events/) — the canonical DCB spec
- [storage_internals.md](storage_internals.md) — reckon-db's Khepri
  path layout
