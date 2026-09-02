# Dynamic Consistency Boundary (DCB)

*Available in reckon-db 3.1.1+.*

DCB is the cross-cutting complement to per-aggregate optimistic
concurrency. Where `reckon_db_streams:append/4,5` enforces "no new
events on THIS stream since version N" before writing, DCB enforces
"no event matching THIS tag-filter has been written since seq N" —
without ever scoping to a single stream.

Use DCB when the invariant a write must preserve crosses streams:

- **Uniqueness**: "no other user has claimed this email"
- **Allocation**: "this slot has not been reserved by anyone else"
- **Rate limit**: "this caller has not exceeded N writes in the last
  window"
- **Eligibility**: "no exclusion event applies to this principal"

These don't fit per-aggregate locks because the invariant references
data living in events the aggregate doesn't own. DCB lets you read a
query-shaped slice of history, decide on it, and atomically commit
new events conditionally on nothing in that slice having moved.

---

## The Decision loop

The canonical pattern:

```
1. Read context events matching a tag-filter, get max observed seq
2. Run domain logic on those events, produce new events
3. Conditionally append the new events: write iff no event matching
   the same filter has seq > the observed max
4. On conflict, refresh the context and retry (bounded)
```

The conditional-append is atomic across the cluster — either all
events commit, or none do, with the precondition evaluated inside
the same Ra log entry as the write.

---

## API

### Direct (reckon-db)

```erlang
%% Read context: events matching the filter, scoped to the DCB
%% pseudo-stream. The runtime equivalent (evoq_decision behaviour)
%% wraps this; raw users can read via tags + filter client-side.

%% Conditional append:
case reckon_db_streams:append_if_no_tag_matches(
       StoreId, TagFilter, SeqCutoff, Events) of
    {ok, LastSeq} ->
        %% Committed. LastSeq is the seq assigned to the final
        %% event. Use it as the cutoff for follow-up writes
        %% against an overlapping tag set.
        ok;
    {error, {context_changed, MaxSeq}} ->
        %% Conflict: an event matching TagFilter has seq > SeqCutoff.
        %% Nothing was written. Refresh the context (re-read events
        %% matching the filter, observe new max seq) and retry.
        retry_with(MaxSeq);
    {error, no_events} ->
        %% Empty Events list; client error.
        ok;
    {error, Reason} ->
        %% Backend error (Ra unavailable, etc.).
        {error, Reason}
end.
```

### Through the gateway (Erlang clients on remote BEAMs)

```erlang
reckon_gater_api:append_if_no_tag_matches(StoreId, TagFilter, SeqCutoff, Events).
```

Same signature, same return shape. Routes via the gater worker pool.

### Through the evoq behaviour (high-level)

For BEAM consumers, the `evoq_decision` behaviour wraps the
read/decide/write loop with retry + backoff. See
[evoq's decisions guide](https://github.com/reckon-db-org/evoq/blob/main/guides/decisions.md).

### Through gRPC (polyglot clients)

reckon-gateway 0.7.0+ exposes `DcbService` over gRPC. Go SDK:

```go
d := client.Dcb("orders")
res, _ := d.Read(ctx, dcb.MatchAny("slot:42"), 100)
committed, conflict, err := d.Append(ctx,
    dcb.MatchAny("slot:42"), res.MaxSeq,
    []dcb.ProposedEvent{...})
```

---

## TagFilter algebra

A `tag_filter()` is a recursive predicate over an event's fields.
Seven shapes:

| Shape | Meaning | Erlang term | Index |
|-------|---------|-------------|-------|
| `any_of` | Event matches if it carries ANY of these tags | `{any_of, [Tag]}` | `[by_tag, Tag, *]` |
| `all_of` | Event matches if it carries ALL of these tags | `{all_of, [Tag]}` | intersection over `[by_tag, Tag, *]` |
| `event_type` | Event matches if its `event_type` field equals Type | `{event_type, Type}` | `[by_event_type, Type, *]` |
| `payload_match` | Event matches if `data[Key] == Value` (JSON string field) | `{payload_match, Key, Value}` | `[by_payload, Key, Value, *]` (5.3.0+, opt-in) |
| `payload_hash_match` | Event matches if the given fields jointly equal the given values | `{payload_hash_match, [Key], [Value]}` | `[by_payload_hash, Hash, *]` (5.3.0+, opt-in) |
| `and_` | Event matches if ALL sub-filters match | `{and_, [Filter]}` | set intersection |
| `or_` | Event matches if ANY sub-filter matches | `{or_, [Filter]}` | set union |

`payload_match` and `payload_hash_match` require the matching index declaration
in the store's `store_config`. See the [CCC guide](ccc.md) for payload index
setup and the multi-field hash pattern.

> **CCC superset note.** `payload_match` and `payload_hash_match` are CCC
> (Command Context Consistency) extensions — they allow the command to scope
> its consistency window to payload field values, not just tags. DCB
> (tag/event_type filters) is a strict subset of CCC. See [ccc.md](ccc.md)
> for the full CCC framing.

Filters compose to arbitrary depth. Examples:

```erlang
%% "Has tag 'slot:42' AND either tag 'tenant:acme' or tag 'tenant:globex'"
Filter = {and_, [
    {any_of, [<<"slot:42">>]},
    {or_, [
        {any_of, [<<"tenant:acme">>]},
        {any_of, [<<"tenant:globex">>]}
    ]}
]}.

%% "Event of type user_registered_v1 AND carries this email tag"
%% (canonical DCB spec query-item pattern: type + tags)
Filter = {and_, [
    {event_type, <<"user_registered_v1">>},
    {any_of, [<<"email:alice@example.com">>]}
]}.
```

---

## The DCB pseudo-stream

DCB events live under the pseudo-stream id `<<"_dcb">>`. They are
real events — they appear in `read_all_global`, `read_by_event_types`,
subscription deliveries, the gateway gRPC surface, every existing
read path — but the consistency-check semantics only apply to
events written *through* `append_if_no_tag_matches/4`. Direct
`append/4,5` to the `_dcb` stream is not a meaningful operation
and should be avoided.

The seq counter used by DCB is **separate from** the per-stream
version counter. It is a monotonic per-store integer, incremented
only by successful DCB commits.

---

## The seq cutoff

`SeqCutoff` is the highest seq the caller has observed for events
matching `TagFilter`. The server rejects the write if any event
matching the filter has seq **strictly greater than** the cutoff.

The sentinel `-1` means "I have observed nothing yet." Use it when:

- The caller is making a uniqueness claim and expects no matching
  event to exist (e.g., "this email has never been registered").
- The caller is the first writer in a fresh boundary.

After a successful commit, the returned `LastSeq` is the cutoff for
a follow-up write against an overlapping tag set, *if* you can
guarantee no concurrent third party wrote between your commit and
your follow-up. Most callers re-read fresh context instead.

---

## Worked example: email uniqueness

Goal: register a user with an email, atomically guaranteeing no
other user already holds that email.

```erlang
-module(register_user).

-export([register/2]).

-define(STORE, my_store).

register(Email, UserData) ->
    EmailTag = <<"email:", Email/binary>>,
    Filter = {any_of, [EmailTag]},

    %% 1. Read context: any user_registered event with this email
    %% tag. The runtime read uses read_by_tags + client-side filter
    %% scope to the _dcb stream. Higher-level libraries
    %% (evoq_decision) do this automatically.
    {ok, MatchingEvents} = read_dcb_events_matching(?STORE, Filter),
    Cutoff = max_seq(MatchingEvents),

    %% 2. Decide.
    case has_registration(MatchingEvents) of
        true ->
            {error, email_already_registered};
        false ->
            Event = #{
                event_type => <<"user_registered_v1">>,
                data => UserData#{email => Email},
                tags => [EmailTag]
            },
            %% 3. Append conditionally.
            case reckon_db_streams:append_if_no_tag_matches(
                   ?STORE, Filter, Cutoff, [Event]) of
                {ok, Seq} ->
                    {ok, Seq};
                {error, {context_changed, _}} ->
                    %% Concurrent writer claimed the email.
                    register(Email, UserData)
            end
    end.

%% Helpers omitted for brevity.
```

Note that the `Filter` and the events written share the same tag
`<<"email:Email>>`. This is how DCB events become visible to future
consistency checks: their own tags are indexed alongside, so a
subsequent read with the same filter will see them.

---

## Integrity (tamper-resistance)

On integrity-enabled stores, DCB events carry the same
`prev_event_hash` + `mac` fields as regular events, linked into a
per-store DCB chain. The Khepri transaction body can't run
`crypto:*` (Horus extractor rejects it), so MAC chains are
pre-computed outside the transaction and the chain-tip is verified
inside. Concurrent-writer races on the chain-tip are bounded by
`?INTEGRITY_RETRY_BUDGET` (5) before surfacing
`{error, dcb_concurrent_writer_exhausted}`.

This is invisible to callers: same API, same return shape.

---

## When NOT to use DCB

- **Per-aggregate invariants**: use stream-version concurrency
  (`append/4,5`) on the aggregate's own stream. DCB is for
  cross-cutting invariants.
- **Read-modify-write within one aggregate**: an aggregate's
  `apply/2` callback already serializes through the aggregate
  process. DCB doesn't add value here.
- **Eventual consistency is acceptable**: if the read model can
  reconcile after the fact, you don't need DCB's atomic precondition.

---

## See also

- [CCC guide](ccc.md) — payload-indexed filters, multi-field hash pattern, and the full CCC framing (5.3.0+)
- [DCB Raft design](dcb_raft_design.md) — why Raft changes the design vs. PostgreSQL, the Horus pre-stamp pattern, and the hybrid aggregate+DCB model
- [evoq decisions guide](https://github.com/reckon-db-org/evoq/blob/main/guides/decisions.md) — the high-level Erlang API
- [`plans/PLAN_DCB_IMPLEMENTATION.md`](https://github.com/reckon-db-org/reckon-db/blob/main/plans/PLAN_DCB_IMPLEMENTATION.md) — implementation design
- [hecate-corpus `CONSISTENCY_BOUNDARIES.md`](https://github.com/hecate-social/hecate-corpus/blob/main/philosophy/CONSISTENCY_BOUNDARIES.md) — the doctrine
- Ericsson Architects' [aggregateless event sourcing](https://ricofritzsche.me/aggregateless-event-sourcing/) — the inspiration
- Rico Fritzsche's *Simply Event Sourcing* (2025) — formal CCC treatment and worked DCB patterns
