# Plan: Command Context Consistency (CCC)

**Status:** Design
**Created:** 2026-06-23
**Last Updated:** 2026-06-23
**Target release:** reckon-db 5.3.0, reckon-gater 3.5.0, reckon-gateway 0.13.0
**Spans repos:** reckon-db, reckon-gater, reckon-gateway, reckon-go (proto)

---

## Background

Command Context Consistency (CCC) is a pattern by Rico Fritzsche that generalizes
DCB. Specification: https://architecture.ricofritzsche.me/specifications/command-context-consistency/specification/

The three CCC primitives:

```
query(event_query)                                             → query_result
append(new_events)                                             → append_result
append_if(new_events, context_query, expected_context_version) → append_result | conflict
```

`context_version` = highest sequence number among all events matching
`context_query`. If a conflict occurs, the response carries both the expected
and actual version.

CCC extends DCB in one direction: `event_query` can include `payload_predicates`
— conditions on event content, not just on pre-indexed tags.

---

## Relationship to existing DCB

ReckonDB's DCB is already CCC for the `tag_filter` case:

| CCC concept | ReckonDB equivalent |
|---|---|
| `context_version` | `max_seq` in context query response |
| `append_if` | `append_if_no_tag_matches` |
| `min_sequence_number` cursor | `from` param in `read_log` |
| `event_types` filter | `{event_type, T}` in `tag_filter()` |

The only gap is **payload predicates** — filtering by event content rather
than by pre-indexed tags or event types.

---

## The trick: same mechanism as by_tag

DCB maintains the tag index at write time:

```
[by_tag,        Tag,       SeqKey] → #event{}
[by_event_type, EventType, SeqKey] → #event{}
```

CCC payload predicates use the same trick — two new DCB-level path families:

```
[by_payload,      FieldKey, FieldValue, SeqKey] → #event{}   %% single-field
[by_payload_hash, Hash,                 SeqKey] → #event{}   %% composite
```

Both are populated at DCB append time from declared fields. The in-transaction
conflict check reads these subtrees with `khepri_tx:get_many/1`, exactly as
it reads `by_tag` today.

### Single-field vs composite

**Single-field** (`by_payload`): one path entry per declared field per event.
The filter algebra composes them: `{and_, [{payload_match, K1, V1}, {payload_match, K2, V2}]}`
intersects two sequence sets inside `match_seqs` — correct with no false
positives, since the DCB SeqKey is globally unique per event.

**Composite** (`by_payload_hash`): one path entry per declared field *combination*
per event, keyed by a SHA-256 hash of the sorted `[{K, V}]` pairs. A
`{payload_hash_match, [K1,K2], [V1,V2]}` query reads a single Khepri subtree —
no intersection, one transaction read regardless of how many fields are in
the combination.

The composite path is an **optimization** for combinations that are always
queried together. The single-field path is for ad-hoc composition via the
filter algebra. Both are correct. Choose by query pattern, not by correctness.

### Path anatomy (composite)

```
[by_payload_hash, <<A3...F7>>, <<"00000000000000000007">>] → #event{}
[by_payload_hash, <<A3...F7>>, <<"00000000000000000042">>] → #event{}
```

`Hash` is an **intermediate node** (the subtree partition), `SeqKey` is the
**leaf** (unique per DCB event). Multiple events with the same composite value
produce the same hash but different SeqKeys — siblings in the same subtree,
readable with a single wildcard pattern `[by_payload_hash, Hash, ?WILDCARD]`.

### Hash computation

```erlang
payload_combo_hash(Fields, Values) ->
    Pairs = lists:sort(lists:zip(Fields, Values)),
    crypto:hash(sha256, term_to_binary(Pairs)).
```

Sorting by field name makes query field order irrelevant — `[K1, K2]` and
`[K2, K1]` with matching values produce the same hash.

### Collision safety

A SHA-256 collision between two *different* composite values would produce a
false-positive conflict rejection — the append is spuriously rejected. This is
the safe failure direction (too conservative, not too permissive). The
application retries and succeeds. At DCB event volumes (millions of events,
bounded distinct composite values), collision probability is negligible.

For the `ReadDcbContext` path the gateway applies `event_matches/2` as a
post-filter, which checks actual event data — false positives from collisions
are eliminated there regardless.

### Why composite hash beats a composite tuple path

A variable-arity tuple `{V1, V2, V3}` as a Khepri path component works, but
the path depth varies with field count, making wildcard patterns per-declaration
specific. A SHA-256 binary is always 32 bytes — fixed depth, fixed component
size, one pattern for all composite declarations.

### Constraint

Both index types require fields to be **declared at store-config time**. Undeclared
fields cannot participate in conditional appends. Only top-level JSON string
values are indexable. This is intentional — the same philosophy as DCB's tag
declarations.

---

## Scope

**In scope (this plan):**

- Single-field payload index: `{payload, Key}` declaration, `[by_payload, ...]` path
- Composite payload index: `{payload_hash, [Key]}` declaration, `[by_payload_hash, ...]` path
- `{payload_match, K, V}` and `{payload_hash_match, [K], [V]}` as new `tag_filter()` variants
- Extend `reckon_db_dcb_filter` to check both payload index paths
- Extend gateway context query and conditional append to expose both
- Proto + Go SDK update

**Out of scope (future):**

- Global CCC across all streams (secondary index, not DCB-scoped)
- Numeric range predicates
- Nested payload field access (only top-level keys)

---

## Implementation steps

### Step 1 — Extend `tag_filter()` type in reckon-gater

**File:** `reckon-gater/src/reckon_gater_types.hrl`

Add two new leaf variants:

```erlang
-type tag_filter() ::
    {any_of,            [binary()]}              |
    {all_of,            [binary()]}              |
    {event_type,        binary()}                |
    {payload_match,     binary(), binary()}      |   %% NEW: single-field
    {payload_hash_match,[binary()], [binary()]}  |   %% NEW: composite
    {and_,              [tag_filter()]}          |
    {or_,               [tag_filter()]}.
```

`{payload_hash_match, Keys, Values}` — `Keys` and `Values` must be the same
length; the hash is computed from `sort(zip(Keys, Values))`.

Also add to the proto definition (`TagFilter` oneof in reckon-proto) and
bump reckon-gater to 3.5.0.

---

### Step 2 — Declare payload fields for indexing in reckon-db

**File:** `reckon-db/include/reckon_db.hrl`

```erlang
-type index_decl() ::
    tags                                |
    event_type                          |
    {meta,         Key  :: binary()}    |
    {payload,      Key  :: binary()}    |   %% NEW: single-field DCB payload index
    {payload_hash, Keys :: [binary()]}. %% NEW: composite hash DCB payload index
```

**File:** `reckon-db/src/reckon_db_index_config.erl`

Add to `is_valid_decl/1` and `normalize/1`:

```erlang
is_valid_decl({payload, K})      when is_binary(K)      -> true;
is_valid_decl({payload_hash, Ks}) when is_list(Ks),
                                       Ks =/= [],
                                       lists:all(fun is_binary/1, Ks) -> true.
```

Note: `{payload, K}` and `{payload_hash, Ks}` drive the DCB `[by_payload, ...]`
and `[by_payload_hash, ...]` paths respectively — separate from the secondary
index `[idx, ...]` paths, with different path roots and sequential keys
(DCB SeqKey vs secondary OrderKey).

---

### Step 3 — DCB write path: populate payload index entries

**File:** `reckon-db/src/reckon_db_dcb_paths.erl`

New path builders:

```erlang
by_payload_path(Key, Value, SeqKey) ->
    [by_payload, Key, Value, SeqKey].

by_payload_pattern(Key, Value) ->
    [by_payload, Key, Value, ?KHEPRI_WILDCARD_STAR].

by_payload_hash_path(Hash, SeqKey) ->
    [by_payload_hash, Hash, SeqKey].

by_payload_hash_pattern(Hash) ->
    [by_payload_hash, Hash, ?KHEPRI_WILDCARD_STAR].

payload_combo_hash(Keys, Values) ->
    Pairs = lists:sort(lists:zip(Keys, Values)),
    crypto:hash(sha256, term_to_binary(Pairs)).
```

**File:** `reckon-db/src/reckon_db_dcb.erl`

In the write batch builder, after stamping `SeqKey`, loop over declared
payload indexes. JSON decode is done once per event and reused:

```erlang
%% For each declared {payload, FieldKey}:
%%   1. Decode event.data as JSON (once per event, cached)
%%   2. Extract maps:get(FieldKey, Data, undefined)
%%   3. If binary: write [by_payload, FieldKey, Value, SeqKey] → Event
%%   4. If absent or non-binary: skip (no index entry)
%%
%% For each declared {payload_hash, Fields}:
%%   1. Extract each field value from decoded Data
%%   2. If ALL values are binary: compute hash, write [by_payload_hash, Hash, SeqKey] → Event
%%   3. If any field absent or non-binary: skip (partial combos not indexed)
```

JSON decode via `jsx:decode(Data, [return_maps])`. If `event.data` is not
valid JSON, skip all payload indexing for that event — never fail the append.

---

### Step 4 — Extend reckon_db_dcb_filter for payload_match and payload_hash_match

**File:** `reckon-db/src/reckon_db_dcb_filter.erl`

Add two new `match_seqs` clauses:

```erlang
match_seqs({payload_match, Key, Value}, Provider)
        when is_binary(Key), is_binary(Value) ->
    sets:from_list(Provider({payload, Key, Value}));

match_seqs({payload_hash_match, Keys, Values}, Provider)
        when is_list(Keys), is_list(Values),
             length(Keys) =:= length(Values) ->
    sets:from_list(Provider({payload_hash, Keys, Values}));
```

Extend `seqs_for_payload/2` and add `seqs_for_payload_hash/2` in the default
provider:

```erlang
seqs_for_payload(Key, Value) ->
    Pattern = reckon_db_dcb_paths:by_payload_pattern(Key, Value),
    case khepri_tx:get_many(Pattern) of
        {ok, Map} ->
            [reckon_db_dcb_paths:seq_from_key(SeqKey)
             || Path <- maps:keys(Map),
                [by_payload, _K, _V, SeqKey] <- [Path]];
        {error, _} -> []
    end.

seqs_for_payload_hash(Keys, Values) ->
    Hash = reckon_db_dcb_paths:payload_combo_hash(Keys, Values),
    Pattern = reckon_db_dcb_paths:by_payload_hash_pattern(Hash),
    case khepri_tx:get_many(Pattern) of
        {ok, Map} ->
            [reckon_db_dcb_paths:seq_from_key(SeqKey)
             || Path <- maps:keys(Map),
                [by_payload_hash, _H, SeqKey] <- [Path]];
        {error, _} -> []
    end.

%% In default_provider/1:
default_provider({payload, Key, Value})        -> seqs_for_payload(Key, Value);
default_provider({payload_hash, Keys, Values}) -> seqs_for_payload_hash(Keys, Values);
```

`match_any_above_cutoff` and the set algebra (`and_`, `or_`, `any_of`,
`all_of`) compose with both new leaf types at no additional cost.

---

### Step 5 — Gateway: decode and match for both payload filter types

**File:** `reckon-gateway/src/reckon_gateway_dcb_service.erl`

Extend `decode_filter/1`:

```erlang
decode_filter(#{kind := {payload_match, #{key := K, value := V}}})
        when is_binary(K), is_binary(V) ->
    {ok, {payload_match, K, V}};

decode_filter(#{kind := {payload_hash_match, #{keys := Ks, values := Vs}}})
        when is_list(Ks), is_list(Vs), length(Ks) =:= length(Vs) ->
    {ok, {payload_hash_match, Ks, Vs}};
```

Extend `collect_tags/1` (returns [] — payload queries use a separate index):

```erlang
collect_tags({payload_match, _, _})      -> [];
collect_tags({payload_hash_match, _, _}) -> [];
```

Extend `collect_event_types/1` similarly.

Extend `event_matches/2` with post-filter clauses for `ReadDcbContext`:

```erlang
event_matches(#event{data = Data}, {payload_match, Key, Value})
        when is_binary(Data) ->
    try
        Decoded = jsx:decode(Data, [return_maps]),
        maps:get(Key, Decoded, undefined) =:= Value
    catch _:_ -> false
    end;
event_matches(Event, {payload_match, Key, Value}) when is_map(Event) ->
    Data = maps:get(data, Event, maps:get(<<"data">>, Event, #{})),
    is_map(Data) andalso maps:get(Key, Data, undefined) =:= Value;

event_matches(#event{data = Data}, {payload_hash_match, Keys, Values})
        when is_binary(Data) ->
    try
        Decoded = jsx:decode(Data, [return_maps]),
        lists:all(fun({K, V}) -> maps:get(K, Decoded, undefined) =:= V end,
                  lists:zip(Keys, Values))
    catch _:_ -> false
    end;
event_matches(Event, {payload_hash_match, Keys, Values}) when is_map(Event) ->
    Data = maps:get(data, Event, maps:get(<<"data">>, Event, #{})),
    is_map(Data) andalso
        lists:all(fun({K, V}) -> maps:get(K, Data, undefined) =:= V end,
                  lists:zip(Keys, Values));
```

The `event_matches` post-filter on the gateway side also eliminates any
theoretical hash collisions from the `by_payload_hash` index for the
`ReadDcbContext` path.

---

### Step 6 — Gateway: fetch_by_payload in do_context

**File:** `reckon-gateway/src/reckon_gateway_http_dcb.erl`

Add two collectors:

```erlang
collect_payload_queries({payload_match, K, V})        -> [{single, K, V}];
collect_payload_queries({payload_hash_match, Ks, Vs}) -> [{combo, Ks, Vs}];
collect_payload_queries({any_of, _})                  -> [];
collect_payload_queries({all_of, _})                  -> [];
collect_payload_queries({event_type, _})               -> [];
collect_payload_queries({and_, Fs}) ->
    lists:usort(lists:flatmap(fun collect_payload_queries/1, Fs));
collect_payload_queries({or_, Fs}) ->
    lists:usort(lists:flatmap(fun collect_payload_queries/1, Fs)).
```

In `do_read/4`, extend the fetch step:

```erlang
PayloadQueries = collect_payload_queries(Filter),
Singles  = [{K, V} || {single, K, V} <- PayloadQueries],
Combos   = [{Ks, Vs} || {combo, Ks, Vs} <- PayloadQueries],
SingleEvents = fetch_by_payload(StoreId, Singles, BS),
ComboEvents  = fetch_by_payload_hash(StoreId, Combos, BS),
```

Merge all four result sets (tags, event_types, payload singles, payload
combos) before deduplication and `event_matches` refinement.

---

### Step 7 — reckon-db: dcb_read_by_payload and dcb_read_by_payload_hash

**File:** `reckon-db/src/reckon_db_dcb.erl`

```erlang
read_by_payload(StoreId, Key, Value, Limit) ->
    Pattern = reckon_db_dcb_paths:by_payload_pattern(Key, Value),
    read_from_pattern(StoreId, Pattern, Limit).

read_by_payload_hash(StoreId, Keys, Values, Limit) ->
    Hash = reckon_db_dcb_paths:payload_combo_hash(Keys, Values),
    Pattern = reckon_db_dcb_paths:by_payload_hash_pattern(Hash),
    read_from_pattern(StoreId, Pattern, Limit).

read_from_pattern(StoreId, Pattern, Limit) ->
    case khepri:get_many(StoreId, Pattern) of
        {ok, NodeMap} ->
            Events = [E || E <- maps:values(NodeMap), is_record(E, event)],
            Sorted = lists:sort(fun(A, B) -> A#event.version =< B#event.version end, Events),
            {ok, lists:sublist(Sorted, Limit)};
        {error, {khepri, node_not_found, _}} -> {ok, []};
        {error, _} = Err -> Err
    end.
```

**File:** `reckon-db/src/reckon_db_gateway_worker.erl`

```erlang
handle_call({dcb_read_by_payload, StoreId, Key, Value, Limit}, _From, State) ->
    {reply, reckon_db_dcb:read_by_payload(StoreId, Key, Value, Limit), State};

handle_call({dcb_read_by_payload_hash, StoreId, Keys, Values, Limit}, _From, State) ->
    {reply, reckon_db_dcb:read_by_payload_hash(StoreId, Keys, Values, Limit), State};
```

---

### Step 8 — reckon-gater API

**File:** `reckon-gater/src/reckon_gater_api.erl`

```erlang
dcb_read_by_payload(StoreId, Key, Value, Limit) ->
    route_call(StoreId, {dcb_read_by_payload, StoreId, Key, Value, Limit}).

dcb_read_by_payload_hash(StoreId, Keys, Values, Limit) ->
    route_call(StoreId, {dcb_read_by_payload_hash, StoreId, Keys, Values, Limit}).
```

---

### Step 9 — reckon-gateway dispatch wiring

Same pattern as `dcb_all_tags` — route `dcb_read_by_payload` and
`dcb_read_by_payload_hash` calls through `reckon_gateway_dispatch`.

---

### Step 10 — Proto + Go SDK

**reckon-proto:** Add two new messages to `TagFilter`:

```proto
message PayloadMatch {
    string key   = 1;
    string value = 2;
}

message PayloadHashMatch {
    repeated string keys   = 1;
    repeated string values = 2;
}

// In TagFilter oneof kind:
PayloadMatch     payload_match      = 6;
PayloadHashMatch payload_hash_match = 7;
```

**reckon-go:** Add builders:

```go
func PayloadMatch(key, value string) *TagFilter { ... }
func PayloadHashMatch(keys, values []string) *TagFilter { ... }
```

---

## Index type summary

| Declaration | Khepri path | Filter variant | Reads | Use case |
|---|---|---|---|---|
| `{payload, K}` | `[by_payload, K, V, SeqKey]` | `{payload_match, K, V}` | N per distinct field | Ad-hoc single-field; compose with `and_`/`or_` |
| `{payload_hash, [K1,K2,...]}` | `[by_payload_hash, Hash, SeqKey]` | `{payload_hash_match, Ks, Vs}` | 1 always | Fixed combination; all fields must be supplied |

---

## Version bumps

| Repo | Current | Target | Reason |
|---|---|---|---|
| reckon-gater | 3.4.1 | 3.5.0 | Two new `tag_filter()` variants |
| reckon-db | 5.2.2 | 5.3.0 | Two new index kinds, two DCB path families, write path |
| reckon-gateway | 0.12.1 | 0.13.0 | Decode, dispatch, fetch, event_matches |
| reckon-go | current | +minor | Proto update, two new builders |

---

## Release order

1. **reckon-gater 3.5.0** — types + proto
2. **reckon-db 5.3.0** — depends on reckon-gater 3.5.0
3. **reckon-gateway 0.13.0** — depends on both
4. **reckon-go** — proto-driven, can parallel with gateway

---

## What this is NOT

This plan does not implement global CCC across all streams. That would require
either a global sequence counter on every append, using `epoch_us` as the
context version (weaker — clock skew), or a CCC-scoped stream that all
participating streams reference. DCB-scoped CCC (this plan) covers the main
use case: all consistency-relevant events go through the DCB path.

---

## Test plan

**Unit — `reckon_db_dcb_filter`:**
- `{payload_match, K, V}` in `match_seqs`
- `{payload_hash_match, Ks, Vs}` in `match_seqs`
- Both composed with `and_`/`or_` and nested alongside `event_type`
- `match_any_above_cutoff` with both filter types

**Unit — `reckon_db_dcb_paths`:**
- `payload_combo_hash/2` is order-independent: `hash([K1,K2],[V1,V2]) =:= hash([K2,K1],[V2,V1])`
- Distinct composite values produce distinct hashes

**Integration — `reckon_db_dcb`:**
- Append with `{payload, <<"userId">>}` declared; verify `[by_payload, <<"userId">>, V, SeqKey]` written
- Append with `{payload_hash, [<<"userId">>, <<"orderId">>]}` declared; verify `[by_payload_hash, Hash, SeqKey]` written
- Append with non-JSON `event.data`; verify no crash, no payload index entry

**Integration — conditional append:**
- `append_if` with `{payload_match, K, V}` filter: conflict on conflicting write, success on clear context
- `append_if` with `{payload_hash_match, Ks, Vs}` filter: same
- Multiple events under same composite hash: max SeqKey used for conflict check

**Gateway HTTP — end-to-end:**
- `payload_match` in `tag_filter` JSON: correct events returned
- `payload_hash_match` in `tag_filter` JSON: correct events returned
- Composite filter with `and_` over hash match + event_type: correct refinement
